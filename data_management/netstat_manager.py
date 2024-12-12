# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import copy
import json
import logging
import random
import re
import socket
import struct
import threading
import time

from data_management.database import CMD as DB_CMD
from definitions import CMD_NONE, CMD_UDP, CWD_NONE, KEY
from utilities.print_binary import print_binary
from utilities.thread_monitor import ThreadMonitor
from utilities.utils import get_host_name, make_app_id, remove_leading_space

TM = ThreadMonitor()

log = logging.getLogger("PVMLogger")


class NetstatHost(object):

    def __init__(self, lines):

        self._pid_dict = {}
        self._fd_dict = {}
        self._port_dict = {}

        self._procserv_list = []

        self._sorted_procserv_list = None

        self._test_line_list = []

        self.load(lines)
        self.process_procserv_list()

    def get_procserv_data(self):
        return self._sorted_procserv_list

    def process_procserv_list(self):
        """
        This method takes the raw procserv lines and
        """
        log.debug("called")
        result = self.find_telnet_ports(self._procserv_list)
        self._sorted_procserv_list = result

    def find_telnet_ports(self, data):
        """
        Extracts telnet ports from the commands, then sorts items by
        telnet port ascending.  Port numbers then converted to strings
        """
        log.debug("called")

        no_port = 999999999
        result = []
        for item in data:
            log.debug("consider item: %r", item)
            command = item

            parts = command.split(" ")
            parts = [part.strip() for part in parts if len(part.strip())]

            parts.reverse()

            port = None
            for part in parts:
                try:
                    port = int(part)
                    break
                except:
                    continue

            if not port:
                port = no_port
            result.append((port, item))

        result = sorted(result)
        result2 = []
        for item in result:
            if item[0] == no_port:
                port = ""
            else:
                port = str(item[0])
            result2.append((port, item[1]))

        return result2

    # Deleted the unreachable part of this function, seems the whole function could
    # be deleted if we don't intend to fix it? - Chloe
    def load(self, lines):

        debug_flag = False

        if lines is None:
            log.error("Lines is NONE!!")
            return

        log.debug("len(lines): %s", len(lines))

        for line in lines:

            if line is None:
                log.error("Line is None")
                debug_flag = True
                continue

            line = line.strip()

            self._test_line_list.append(line)

            if line.startswith("pid:"):
                self.process_pid(line)

            elif line.startswith("tcp:"):
                self.process_line_tcp(line)

            elif line.startswith("udp:"):
                pass
            else:
                pass

        if debug_flag:
            for line in lines:
                log.debug("DEBUG LINE: '%r'", line)

        return

    def get_data_for_port(self, port):

        cmd = None
        cwd = None

        have_cmd = None

        # Port keys must be 4 digits
        hex_port = "%04X" % port

        total_pids = []
        fd_list = self._port_dict.get(hex_port)

        log.debug(
            "called; port: %d %r fd_list: %r",
            port,
            hex_port,
            fd_list,
        )

        if fd_list is None:
            # I think this loop is STRICTLY for debugging
            for key, value in self._port_dict.items():
                log.debug(
                    "PORT KEY: %r VAL: %r",
                    key,
                    value,
                )

            for key, value in self._pid_dict.items():
                log.debug(
                    "PID KEY: %r VAL: %r",
                    key,
                    value,
                )
            fd_list = []

        for fd in fd_list:
            pid_list = self._fd_dict.get(fd, [])
            total_pids.extend(pid_list)

        total_pids = list(set(total_pids))

        log.debug("checking %d PIDs", len(total_pids))

        for pid in total_pids:
            pid_data = self._pid_dict.get(pid, {})
            cmd = pid_data.get("cmdline", CMD_NONE)
            cwd = pid_data.get("cwd", CWD_NONE)
            log.debug("PID: %d CMD: %s CWD: %s", pid, cmd, cwd)

            # Note: I am seeing this fail when the fetch times out and we
            # just get partial data.  I have seen this on IOC2405-104
            if cmd == CMD_NONE:
                log.error(
                    "Error mapping data!!! pid: %r data: %r",
                    pid,
                    pid_data,
                )
                continue

            # NOTE: whenever I've seen multiple pids the CMD and CWD always
            # match for all of them.  Should be safe to break out of loop here
            # break

            # This is just a sanity check
            if have_cmd is not None and have_cmd != cmd:
                raise ValueError("ERROR", cmd, have_cmd)

            have_cmd = cmd

        if cmd is not None and cmd == CMD_NONE:
            cmd = "Error mapping cmd"
            cwd = "Error mapping cwd"

        log.debug("checking %d PIDs complete", len(total_pids))

        return cwd, cmd

    def process_line_tcp(self, line):

        log.debug("called")

        parts = line.split()

        local_addr = parts[2]
        inode = parts[10]

        try:
            inode = int(inode)

        except Exception:
            # NOTE: This may be a common line in responses
            if line.find("local_address"):
                log.debug("ignore line: %r", line)
                return

            log.exception(
                "process_tcp: invalid inode: %r %r",
                inode,
                line,
            )
            return

        addr_parts = local_addr.split(":")

        port = addr_parts[1]

        fd_list = self._port_dict.get(port, [])
        fd_list.append(inode)

        log.debug(
            "port: %r fd_list: %r",
            port,
            fd_list,
        )

        self._port_dict[port] = fd_list

    def process_pid(self, line):

        log.debug("called")

        parts = line.split(" ")

        pid = int(parts[1])

        pid_data = self._pid_dict.get(pid, {})

        pid_thing = parts[2]

        update_pid_dict = True
        if pid_thing.startswith("cmdline"):

            if "cmdline" in pid_data:
                raise ValueError("this pid already has a command line!!!")

            command_line = " ".join(parts[3:])
            command_line = command_line.strip()

            pid_data["cmdline"] = command_line

            procServ = False
            if command_line.find("procServ") > 0:
                procServ = True
            elif command_line.find("procserv") > 0:
                procServ = True

            if procServ:
                log.debug(
                    "found procServ: %r",
                    command_line,
                )
                self._procserv_list.append(command_line)

        elif pid_thing.startswith("cwd"):

            if "cwd" in pid_data:
                raise ValueError("this pid already has a command line!!!")

            cwd = " ".join(parts[3:])
            cwd = cwd.strip()
            pid_data["cwd"] = cwd

        elif pid_thing.startswith("socket"):
            update_pid_dict = False
            fd = ""
            for c in pid_thing:
                if c < "0" or c > "9":
                    continue
                fd += c

            fd = int(fd)

            pid_list = self._fd_dict.get(fd, [])
            pid_list.append(pid)
            self._fd_dict[fd] = pid_list

        if update_pid_dict:
            self._pid_dict[pid] = pid_data


class NetstatManager(object):
    """
    This class manages the "new" feature that allows netstat data to be
    downloaded from the detected IOCs
    """

    def __init__(self, dataman):

        self._gateway_alive = False
        self._gateway_alive_time = 0
        self._dataman = dataman

        # Dictionary for storing downloaded netstat data
        self._netstat_data = {}
        self._netstat_data_lock = threading.Lock()

        # This counter is incremented each time the beacom mapper runs.
        # It must run a certain number of times before the system will
        # allow beacon configuration updates
        self._map_beacon_count = 0

        self._map_beacon_timer = threading.Timer(3, self.worker_map_beacons)

        self._callback_database_func = None

        self._debug_no_netstat = {}
        self._debug_fetch_counts = {}

    def start(self):
        self._map_beacon_timer.start()

    def set_callback_database(self, func):
        self._callback_database_func = func

    def send_to_database(self, cmd, data):
        self._callback_database_func(cmd, data)

    def handle_gateway_alive(self, alive):
        log.debug("alive: %r", alive)
        self._gateway_alive = alive
        self._gateway_alive_time = time.time()

    def fetch_netstat(self, ip_key):

        tx_addr, seq, app_info = self._dataman.get_heartbeat_for_netstat(ip_key)

        if tx_addr is None:
            log.error("tx_addr is None")
            return

        log.debug(
            "tx_addr: %r seq: %r",
            tx_addr,
            seq,
        )
        counter = int(seq)

        start_time = time.time()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", 0))
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        lines = []
        offset = 0

        while True:

            handle = random.randint(0, 100000)

            # Format the message to send
            m = struct.pack("!LLLL", CMD_UDP.NETSTAT, counter, offset, handle)

            sock.settimeout(60)
            sock.sendto(m, tx_addr)

            try:
                data_str, rx_addr = sock.recvfrom(100000)

            except Exception as err:
                host_name = get_host_name(ip_key)
                log.exception(
                    "Exception getting NETSTAT data: host: %s ip_addr: %r, %s",
                    host_name,
                    ip_key,
                    str(err),
                )
                break

            offset += 1

            try:
                data = json.loads(data_str)

            except Exception as err:
                print_binary(data_str)
                log.exception(
                    "json.loads() failed: %r %r",
                    ip_key,
                    err,
                )
                break

            try:
                rx_handle = int(data.get("h", 0))

            except:
                log.error("Error getting handle from response")
                break

            if rx_handle != handle:
                log.error("INVALID HANDLE IN RESPONSE!!")
                break

            line = data.get("l")
            if line == "__NONE__":
                break

            # Require related upgrade: 
            # Heartbeat provides cmdline and cwd when request NETSTAT
            # the cmdline for require app contains random number suffix each time it restarts,
            # which will make PV Monitor to see a different app when require app restarts, 
            # the following code detect require app using key word "iocsh" and then try
            # to remove the trailing random suffix and replace the random suffix with the 
            # normal require start cmd "startup.iocsh"
            
            # PV Monitor sometimes sees a "(deleted)" in cwd, which also make PVM see a 
            # new app, the following code resolve this issue as well
            if line is not None and "iocsh" in line:
                line_require = re.sub(r'/[^ ]*iocsh.startup[^ ]*', 'startup.iocsh', line.lstrip())
                lines.append(line_require)
            elif "cwd" in line and "deleted" in line:
                line = line.replace(" (deleted)", "")
                lines.append(line)
            else:
                lines.append(line)

        duration = time.time() - start_time

        log.debug(
            "done; %s lines: %d duration: %f",
            get_host_name(ip_key),
            len(lines),
            duration,
        )

        return lines

    def worker_map_beacons(self):
        """
        This worker loops through the known beacons and maps them to
        allocations using the netstat feature.  This is the thread that
        will download netstat data

        NOTE: It can take many minutes for this loop to run the first time
        through.  Subsequent runs should be fast, though.  Sometime it can
        take many seconds to fetch the netstat data for a target host
        """

        thread_id = TM.add("Netman: Map beacons", 600)
        last_time_dict = {}
        last_ping_time = time.time()

        while True:
            TM.ping(thread_id)
            time.sleep(2)

            if not self._gateway_alive:
                log.debug("gateway not running")
                continue

            cur_time = time.time()
            if cur_time - self._gateway_alive_time < 35:
                log.debug("gateway not ready")
                continue

            log.debug("running...")

            ioc_id_list = self._dataman.get_beacon_ioc_id_list()

            for ioc_id in ioc_id_list:
                cur_time = time.time()
                if (cur_time - last_ping_time) > 30:
                    TM.ping(thread_id)
                    last_ping_time = cur_time

                # First check if this ip address supports netstat
                tx_addr, seq, app_info = (
                    self._dataman.get_heartbeat_for_netstat(ioc_id)
                )

                log.debug(
                    "host: %r, IP: %r addr: %r seq: %r",
                    get_host_name(ioc_id),
                    ioc_id,
                    tx_addr,
                    seq,
                )

                if tx_addr is None:
                    log.debug(
                        "%s does not support netstat", get_host_name(ioc_id), 
                    )
                    self._netstat_data_lock.acquire()
                    self._debug_no_netstat[ioc_id] = time.time()
                    self._netstat_data_lock.release()
                    continue

                # Limit how frequently we fetch the data.  We can end up doing repeated
                # fetches if, for example, we are fetching from a non-root heartbeat
                last_time = last_time_dict.get(ioc_id)
                if last_time is not None:
                    if cur_time - last_time < 60:
                        log.debug(
                            "SKIP netstat fetch interval too small: %r",
                            ioc_id,
                        )
                        continue

                last_time_dict[ioc_id] = cur_time

                # Make a list of beacon ports to check
                beacon_ports = self._dataman.get_beacon_ports_unmapped(ioc_id)

                if not beacon_ports:
                    log.debug(
                        "No unmapped beacons for %s", get_host_name(ioc_id), 
                    )
                    continue

                else:
                    log.debug(
                        "%d unmapped beacons for '%s'",
                        len(beacon_ports),
                        get_host_name(ioc_id),
                    )

                for port in beacon_ports:
                    log.info(
                        "retrieving beacon port information using netstat",
                    )

                ioc_id = int(ioc_id)
                nestat_data = self.get_netstat_data(ioc_id)

                if not nestat_data:
                    log.error(
                        "Failed to fetch netstat data for %s using %r",
                        get_host_name(ioc_id),
                        app_info,
                    )
                    continue

                log.debug(
                    "Got netstat data for: %s using %r",
                    get_host_name(ioc_id),
                    app_info,
                )

                for port in beacon_ports:
                    log.debug("map beacon on port: %d", port)

                    cwd, cmd = nestat_data.get_data_for_port(port)
                    cmd = remove_leading_space(cmd)

                    if cwd is not None:
                        app_id = make_app_id(cmd, cwd, ioc_id)

                        data = {
                            KEY.IP_KEY: ioc_id,
                            KEY.PORT: port,
                            KEY.CMD: cmd,
                            KEY.CWD: cwd,
                            KEY.APP_ID: app_id,
                        }

                        log.debug(
                            "mapped netstat response to a beacon!",
                        )

                        ## Add the application to the database
                        self.send_to_database(DB_CMD.APP_UPDATE, data)

                        ## What does this do?
                        self._dataman.map_beacon(ioc_id, port, cwd, cmd, app_id)
                    else:
                        log.error(
                            "Failed to map beacon: %r %r using %r",
                            ioc_id,
                            port,
                            app_info,
                        )

            self._map_beacon_count += 1

    def get_beacon_map_count(self):
        return self._map_beacon_count

    def purge_netstat_data(self, ioc_id):
        try:
            self._netstat_data_lock.acquire()
            del self._netstat_data[ioc_id]
        except:
            pass

        finally:
            self._netstat_data_lock.release()

    def get_procserv_data(self, ip_key):

        log.debug("called; ip_key: %d", ip_key)
        try:
            self._netstat_data_lock.acquire()
            meta = self._netstat_data.get(ip_key)

            if meta is None:
                log.error("no meta for %d", ip_key)
                ## print(self._netstat_data)
                return

            data = meta.get(KEY.DATA)
            if data is None:
                return

            # I *think* its safe to call this with no lock
            # Once created its never changed
            result = data.get_procserv_data()

            if result:
                return copy.copy(result)

            return

        finally:
            self._netstat_data_lock.release()

    def get_netstat_fetch_counts(self):

        log.debug("called")
        try:
            # For testing/debugging
            self._netstat_data_lock.acquire()
            return copy.deepcopy(self._debug_fetch_counts)
        finally:
            self._netstat_data_lock.release()

    def get_netstat_unsupported(self):

        log.debug("called")
        try:
            # For testing/debugging
            self._netstat_data_lock.acquire()
            return copy.deepcopy(self._debug_no_netstat)
        finally:
            self._netstat_data_lock.release()

    def get_netstat_data(self, ip_key):

        data = None

        meta = self._netstat_data.get(ip_key)

        log.debug(
            "get netstat data for: ip_addr: %r",
            ip_key,
        )

        if meta is not None:
            cur_time = int(time.time())
            download_time = meta.get(KEY.TIME, 0)
            age = cur_time - download_time

            if age < 60:
                # If age > 600, data is not set (i.e., aged out of cache)

                # 2019-12-03: There is also some kind of bug wherein we
                # keep returning cached data for the same unmapped beacon
                # (this was before I added the aging feature).  Still, I
                # must not totally understand the beacon timestamps.
                # time_diff = download_time - required_time
                # log.debug("return cached data (%d)", time_diff)
                data = meta.get(KEY.DATA)
        else:
            log.debug("no meta; fetch...")

        if data is None:
            # Download the netstat data
            host_name = get_host_name(ip_key)
            log.debug(
                "Fetching NETSTAT for %r (%r)",
                host_name,
                ip_key,
            )

            # For testing/debugging
            self._netstat_data_lock.acquire()
            d = self._debug_fetch_counts.get(ip_key, {})
            count = d.get("c", 0)
            count += 1
            d["c"] = count
            self._debug_fetch_counts[ip_key] = d
            try:
                del self._debug_no_netstat[ip_key]
            except:
                pass
            self._netstat_data_lock.release()

            # This can take a long long time.. . even a minute or two
            start_time = time.time()
            lines = self.fetch_netstat(ip_key)
            end_time = time.time()

            # More debugging information
            self._netstat_data_lock.acquire()
            d = self._debug_fetch_counts.get(ip_key, {})
            if lines is not None:
                lc = len(lines)
            else:
                lc = 0

            d["l"] = lc
            d["s"] = start_time
            d["e"] = end_time
            self._debug_fetch_counts[ip_key] = d
            self._netstat_data_lock.release()

            log.debug(
                "Netstat fetch returned %d lines for %s",
                lc,
                host_name,
            )

            # If successful store the data
            if lines:
                data = NetstatHost(lines)

                if data:
                    self._netstat_data_lock.acquire()

                    try:
                        self._netstat_data[ip_key] = {
                            KEY.TIME: int(time.time()),
                            KEY.DATA: data,
                        }
                    finally:
                        self._netstat_data_lock.release()
            else:
                log.debug("no netstat lines")
        else:
            log.info("DOING NOTHING FOR %s", ip_key)

        return data
