# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
# System Imports
import copy
import json
import logging
import pickle
import queue
import random
import socket
import struct
import threading
import time

import epics

from data_management.database import CMD as DB_CMD
from definitions import (CMD, CMD_UDP, EPICS, HISTORY, HISTORY_STRING, KEY,
                         PRIORITY, SETTINGS, SORT_MODE)
from utilities.file_saver import FileSaver
from utilities.print_binary import print_binary
from utilities.pvmonitor_lock import PVMonitorLock
from utilities.pvmonitor_time import PVMonitorTime
from utilities.statistics import StatisticsManager
from utilities.thread_monitor import ThreadMonitor
from utilities.utils import (get_host_name, get_ip_addr, make_app_id,
                             make_duration_string, make_host_sort_name,
                             remove_leading_space)

TM = ThreadMonitor()

log = logging.getLogger("PVMLogger")

STATS = StatisticsManager()


class HeartbeatManager(object):
    """ """

    def __init__(self, dataman):

        self._lock = PVMonitorLock()
        self._heartbeats = {}
        self._heartbeat_map = {}

        self._history = {}
        self._history_lock = PVMonitorLock()

        self._pvs = {}
        self._pvs_lock = PVMonitorLock()

        self._pv_map = {}
        self._pv_map_lock = PVMonitorLock()

        self._debug_fetch_count = {}
        self._debug_no_netstat = []
        self._debug_fetch_count_lock = PVMonitorLock()

        self._queue_update_history = queue.Queue()
        self._queue_update_pv = queue.Queue()

        self._queue_cmd = queue.PriorityQueue()

        self._prefix = "heartbeats"
        self._searcher = None
        self._pvman = None
        self._dataman = dataman
        self._gateway_alive = False
        self._gateway_alive_time = 0
        self._max_age_lateness = 0

        self._sort_lock = PVMonitorLock()
        self._sorted_uptime = []
        self._sorted_name = []
        self._sorted_last_seen = []

        # The App Config variables are used to update an external app
        # that serves PVs indicating the heartbeat status
        self._app_config = {}
        self._app_config_lock = threading.Lock()

        self._app_update_queue_new = queue.Queue()

        self._app_data = {}

        self._callback_database_func = None

        self._update_timer = threading.Timer(
            0, self.worker_process_queue_history
        )
        self._pv_timer = threading.Timer(0, self.worker_process_queue_pv)
        self._timer_cmd_queue = threading.Timer(0, self.worker_cmd)
        self._timer_check_age = threading.Timer(5, self.worker_check_age)
        self._timer_sort = threading.Timer(10, self.worker_sort)
        self._app_queue_timer = threading.Timer(
            0, self.worker_process_app_queue
        )
        self._mytime = PVMonitorTime()

    def set_callback_database(self, func):
        self._callback_database_func = func

    def send_to_database(self, cmd, data):
        self._callback_database_func(cmd, data)

    def start(self):

        self._timer_cmd_queue.start()
        self._update_timer.start()
        self._pv_timer.start()
        self._app_queue_timer.start()
        self._timer_check_age.start()
        self._timer_sort.start()

    def queue_cmd(self, cmd, data, priority=PRIORITY.NORMAL):
        serialized = pickle.dumps((priority, (cmd, data)))
        self._queue_cmd.put_nowait(serialized)

    def handle_heartbeat(self, ip_key, port, data, qtime):
        self.queue_cmd(CMD.HEARTBEAT, (ip_key, port, data, qtime))

    def my_caput(self, pv_name, value):
        log.debug(
            "HEARTBEAT: ---->> Writing %s to %s",
            str(value),
            str(pv_name),
        )
        results = epics.caput(pv_name, value)

    def handle_gateway_alive(self, alive):
        log.debug("alive: %r", alive)
        self._gateway_alive = alive
        self._gateway_alive_time = time.time()

    def worker_process_app_queue(self):

        while True:
            try:
                data = self._app_update_queue_new.get(block=True, timeout=10)

            except queue.Empty:
                continue

            # At this point we got a heartbeat
            # print "heartbeat for app update:", data

            # Make a key into the configuration
            key = "%s|%s|%s" % (
                data.get(KEY.IMAGE),
                data.get(KEY.DIRECTORY),
                data.get(KEY.START_CMD),
            )

            # Get the config index
            try:
                self._app_config_lock.acquire()
                app_cfg = self._app_config.get(key)
                index = app_cfg[0]
                description = app_cfg[1]

            except:
                index = None
            finally:
                self._app_config_lock.release()

            if index is None:
                continue

            try:
                seq_num = int(data.get(KEY.SEQ))
                uptime = int(data.get(KEY.UP))

                have_data = self._app_data.get(index)

                if not have_data:
                    have_seq = None
                    have_count = 0
                else:
                    have_seq = have_data[0]
                    have_count = have_data[1] + 1

                if have_count > 50:
                    have_count = 0

                self._app_data[index] = (seq_num, have_count)

                started = False
                resumed = False

                if not have_seq:
                    started = True
                else:
                    if (int(seq_num) - int(have_seq)) > 3:
                        resumed = True

                if seq_num == 0:
                    started = True

                pv_name = "iocmon:hb:%04d:count" % index
                self.my_caput(pv_name, 120)

                if started:
                    pv_name = "iocmon:hb:%04d:restarts.PROC" % index
                    self.my_caput(pv_name, 1)

                if resumed:
                    pv_name = "iocmon:hb:%04d:resumes.PROC" % index
                    self.my_caput(pv_name, 1)

                pv_name = "iocmon:hb:%04d:status" % index
                self.my_caput(pv_name, 1)

                pv_name = "iocmon:hb:%04d:uptime" % index
                self.my_caput(pv_name, uptime)

                if have_count > 0:
                    continue

                if description:
                    pv_name = "iocmon:hb:%04d:status.DESC" % index
                    self.my_caput(pv_name, description)

            except Exception as err:
                log.exception(
                    "HEARTBEAT: Exception updating App: %s",
                    str(err),
                )

    def queue_heartbeat_for_app(self, data):
        """
        Copy the heartbeat data to a queue that will be processed by an
        independent thread
        """
        warning = None

        qsize = self._app_update_queue_new._qsize()
        if qsize < 100:
            copied = copy.copy(data)
            self._app_update_queue_new.put_nowait(copied)
            if qsize > 75:
                warning = "HEARTBEAT: App update Queue Length: %d" % len(
                    self._app_update_queue
                )
        else:
            warning = (
                "HEARTBEAT: WARNING!!! App update queue full..."
                " discarding heartbeat"
            )

        if warning:
            log.warning(warning)

    def read_app_config(self):
        """
        This method reads the heartbeat app config file.  This is the file that
        tells iocmon what PVs to update when it processes heartbeats.
        """
        try:
            self._app_config_lock.acquire()
            self._app_config = {}

            f = None
            try:
                f = open("heartbeat_app_config.txt")

                for line in f:
                    log.debug("read line: %s", line)
                    self.process_app_config_line(line)

            except Exception as err:
                log.exception(
                    "Exception: read_config: %s",
                    str(err),
                )

            finally:
                if f:
                    f.close()

            return "Read config"

        finally:
            self._app_config_lock.release()

    def process_app_config_line(self, line):

        line = line.strip()
        if len(line) == 0:
            return

        if line.startswith("#"):
            return

        parts = line.split(",")
        if len(parts) != 5:
            log.error(
                "Warning, unexpected number of parts: %s",
                line,
            )
            return

        index = int(parts[0].strip())
        description = parts[1].strip(' "')
        image = parts[2].strip(' "')
        working_dir = parts[3].strip(' "')
        cmd_file = parts[4].strip(' "')

        key = "%s|%s|%s" % (image, working_dir, cmd_file)

        if key in self._app_config:
            log.error(
                "Error: duplicate key detected: line: %r",
                line,
            )
            return

        self._app_config[key] = [index, description]

    def worker_process_queue_pv(self):

        thread_id = TM.add("Heartman: Fetch PVs", 60)

        while True:

            TM.ping(thread_id)

            try:
                item = self._queue_update_pv.get(block=True, timeout=5)
            except queue.Empty:
                continue

            self.fetch_pvs(item)

    def worker_process_queue_history(self):

        thread_id = TM.add("Heartman: Update history", 60)

        while True:
            TM.ping(thread_id)

            try:
                item = self._queue_update_history.get(block=True, timeout=5)
            except queue.Empty:
                continue

            app_id = item[0]
            ioc_id = item[1]
            port = item[2]

            self._dataman.update_ioc(ioc_id)
            self.add_log(item)

            # Send this heartbeat over to the beacon manager so that it can
            # check to see if there is an associated beacon.  If there is
            # then it is possible to assign an accurate "start time" to the
            # application.  There is also a chance that the beacon was not
            # properly mapped.  That can happen on an IOC that is not running
            # a root V2 heartbeat

            self._dataman.heartbeat_to_beaconman((app_id, ioc_id, port))

    def add_log(self, item):
        app_id = item[0]
        ioc_id = item[1]
        port = item[2]
        history = item[3]
        uptime = item[4]

        log.debug("called; item: %r", item)

        if history == HISTORY.DETECTED:
            return

        age_str = make_duration_string(uptime, verbose=False)

        kind = HISTORY_STRING.get(history, "bad kind: %r" % history)

        heartbeat = self.get_heartbeat(app_id=app_id)

        if heartbeat:
            data = heartbeat.get(KEY.DATA)
            cmd = "%s %s" % (data.get(KEY.IMAGE), data.get(KEY.START_CMD))
            cwd = "%s" % data.get(KEY.CWD)
            msg = "Heartbeat %s; port %s (up: %s) CMD: %s CWD: %s" % (
                kind,
                port,
                age_str,
                cmd,
                cwd,
            )
        else:
            msg = "Heartbeat %s; port %s (up; %s)" % (kind, port, age_str)

        self._dataman.add_ioc_log(ioc_id, msg, needs_ack=True, app_id=app_id)

    def queue_update_pvs(self, item):
        self._queue_update_pv.put_nowait(item)

    def queue_update_history(self, item):
        self._queue_update_history.put_nowait(item)

    def worker_sort(self):
        thread_id = TM.add("Heartman: Sort", 120)
        last_ping_time = time.time()

        while True:
            cur_time = time.time()

            # Ping thread monitor every 30 seconds
            if cur_time - last_ping_time > 30:
                TM.ping(thread_id)
                last_ping_time = cur_time

            time.sleep(10)

            self.handle_cmd_sort()

    def worker_check_age(self):

        thread_id = TM.add("Heartman: Check Heartbeat Age", 120)

        cur_time = time.time()

        next_ping_time = cur_time
        next_check_time = cur_time

        while True:
            cur_time = time.time()

            # Ping thread monitor every 30 seconds
            if cur_time > next_ping_time:
                TM.ping(thread_id)
                next_ping_time += 30

            if cur_time >= next_check_time:
                self.handle_cmd_check_age(next_check_time)
                next_check_time += 20

            time.sleep(1)

    def worker_cmd(self):

        thread_id = TM.add("Heartman: CMD Worker ", 120)

        next_ping_time = time.time()

        while True:

            cur_time = time.time()

            qsize = self._queue_cmd.qsize()
            if qsize > 2000:
                # This can run away on us and cause database to get flooded with bogus events
                log.error(
                    "PANIC!!!!! heartman qsize: %d TERMINATING!!!",
                    qsize,
                )
                STATS.set("PANIC: Heartman qsize", qsize)
                break

            if cur_time >= next_ping_time:
                TM.ping(thread_id)
                next_ping_time += 30
                log.debug("queue size: %d", qsize)
                STATS.set("Queue Size: Heartman", qsize)

            try:
                serialized = self._queue_cmd.get(block=True, timeout=10)
                thing = pickle.loads(serialized)

            except queue.Empty:
                continue

            ### priority = thing[0]
            item = thing[1]
            cmd = item[0]
            data = item[1]

            if cmd == CMD.HEARTBEAT:
                self.handle_cmd_heartbeat(data)

    def handle_cmd_sort(self):
        """
        {u't': 1608674446.477331, u'pvc': 43, u'sp': 5064, u'd':
            {
                u'addr': '10.51.0.166', u'stcmd': u'st.cmd', u'ver': u'2.0',
                u'cur': u'1608674446', u'seq': u'1221566',
                u'ai': 3528094969826511344L, u'sp': 5064, u'pid': u'21182',
                u'up': u'36646982', u'addr_orig': (171114662, 36909),
                u'img': u'./bin/linux-x86_64/LinuxMonitor',
                u'p': 36909, u't': 1608674446.477331,
                u'qt': 1608674446.438763, u'cwd':
                u'/iocApps/LinuxMonitor/v12/LinuxMonitor.20190116',
                u'ip_k': 171114662
            },
        u'L': False}
        """
        self._lock.acquire()
        heartbeat_ids = [k for k in self._heartbeats.keys()]
        self._lock.release()

        cur_time = time.time()

        temp_name = []
        temp_uptime = []
        temp_last_seen = []

        for heartbeat in heartbeat_ids:
            heartbeat_data = self.get_heartbeat(heartbeat)
            data = heartbeat_data.get(KEY.DATA)
            ioc_id = data.get(KEY.IP_KEY)
            host_name = get_host_name(ioc_id)
            temp_name.append((host_name.lower(), heartbeat))

            uptime = int(data.get(KEY.UP, 0))
            temp_uptime.append((uptime, heartbeat))

            beat_time = heartbeat_data.get(KEY.TIME)
            last_seen = cur_time - beat_time
            temp_last_seen.append((last_seen, heartbeat))

        temp_last_seen.sort()
        temp_last_seen.reverse()
        sorted_last_seen = [item[1] for item in temp_last_seen]

        temp_name.sort()
        sorted_name = [item[1] for item in temp_name]

        temp_uptime.sort()
        temp_uptime.reverse()
        sorted_uptime = [item[1] for item in temp_uptime]

        self._sort_lock.acquire()
        self._sorted_name = sorted_name
        self._sorted_uptime = sorted_uptime
        self._sorted_last_seen = sorted_last_seen
        self._sort_lock.release()

    def handle_cmd_heartbeat(self, data_in):

        ip_key = data_in[0]
        port = data_in[1]
        data_str = data_in[2]
        qtime = data_in[3]

        data = json.loads(data_str)
        data[KEY.IP_KEY] = ip_key
        data[KEY.PORT] = port
        data[KEY.IP_ADDR] = get_ip_addr(ip_key)
        data[KEY.QTIME] = qtime

        self.beat(data)

    def handle_cmd_check_age(self, desired_check_time):

        if not self._gateway_alive:
            log.debug(
                "gateway alive: FALSE; skip lost heartbeat check",
            )
            return

        start_time = time.time()

        if (start_time - self._gateway_alive_time) < (
            EPICS.MAX_HEARTBEAT_AGE * 2
        ):
            log.debug(
                "gateway alive time < MAX_HEARTBEAT_AGE; skip lost heartbeat check",
            )
            return

        updates = []

        try:
            cur_time = time.time()

            late = cur_time - desired_check_time
            if late > self._max_age_lateness:
                self._max_age_lateness = late
                STATS.set("Max Lateness: Heartman", "%.2f" % late)

            if late > 5:
                log.debug(
                    "running; late: %.3f (max: %.3f)",
                    late,
                    self._max_age_lateness,
                )

            self.lock()

            for app_id, heartbeat in self._heartbeats.items():

                if heartbeat.get(KEY.LOST, False):
                    continue

                heartbeat_age = cur_time - heartbeat[KEY.TIME]

                if heartbeat_age > (EPICS.MAX_HEARTBEAT_AGE + late):
                    data = heartbeat.get(KEY.DATA)
                    heartbeat[KEY.LOST] = True
                    ioc_id = data.get(KEY.IP_KEY)
                    port = data.get(KEY.SERVER_PORT)
                    heartbeat_port = data.get(KEY.PORT)
                    updates.append(
                        (app_id, ioc_id, port, HISTORY.LOST, data.get(KEY.UP))
                    )

                    log.debug(
                        "LOST HEARTBEAT: %s heartbeat_port: %d "
                        "epics port: %d age: %d",
                        get_host_name(ioc_id),
                        heartbeat_port,
                        port,
                        heartbeat_age,
                    )

        finally:
            self.unlock()

        for item in updates:
            self.queue_update_history(item)

        elapsed_time = time.time() - start_time

        if elapsed_time > 5:
            log.debug(
                "late: %.2f (max: %.2f); elapsed time: %.2f",
                late,
                self._max_age_lateness,
                elapsed_time,
            )

    def fetch_pvs(self, item):
        """
        This method reads a list of the PVs via the heartbeat
        """

        app_id = item[0]
        force = item[1]

        heartbeat = self.get_heartbeat(app_id=app_id)
        if not heartbeat:
            log.error("did not find heartbeat: %d", app_id)
            return

        log.debug("called; app_id: %d", app_id)

        current_time = int(time.time())

        data = heartbeat[KEY.DATA]
        heartbeat_version = data.get(KEY.VERSION)
        ioc_id = data.get(KEY.IP_KEY)
        port = data.get(KEY.SERVER_PORT)

        if not isinstance(ioc_id, int):
            raise ValueError("sanity check")

        if not isinstance(port, int):
            raise ValueError("sanity check")

        try:
            self._pvs_lock.acquire()
            temp = self._pvs.get(ioc_id, {})
            pv_data = temp.get(port)

        finally:
            self._pvs_lock.release()

        if not force and pv_data:
            last_time = pv_data.get(KEY.LAST_TIME, 0)
            if last_time > 0:
                if current_time - last_time < 24 * 60 * 60:
                    ## print "No need to update PV data for", key
                    log.debug("skipping fetch")
                    return

        # Send to the originally detected address, NOT the mapped address
        addr = data.get(KEY.ADDRESS_ORIG)

        log.debug(
            "fetching IOC data for %s ",
            get_host_name(ioc_id),
        )

        if addr is None:
            log.error("ADDRESS ORIG NOT SET: %r", data)
            STATS.increment(
                "ADDRESS_ORIG missing: %s" % get_host_name(ioc_id)
            )
            return

        tx_addr = (get_ip_addr(addr[0]), addr[1])
        counter = int(data.get(KEY.SEQ, data.get(KEY.COUNTER)))

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", 0))
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        pvs = []
        offset = 0
        handle = random.randint(0, 0xFFFF)
        expect = 0

        while True:

            # Format the message to send
            m = struct.pack("!LLLL", CMD_UDP.LIST_PVS, counter, offset, handle)

            sock.settimeout(5)
            sock.sendto(m, tx_addr)

            try:
                data_str, rx_addr = sock.recvfrom(100000)

            except Exception as err:
                log.exception(
                    "Error getting PV data: %s",
                    str(err),
                )
                break

            try:
                data = json.loads(data_str)
                if int(data[KEY.HANDLE]) != handle:
                    log.error(
                        "Unexpected handle: %r != %r",
                        int(data[KEY.HANDLE]),
                        handle,
                    )
                    continue

            except Exception as err:
                log.exception(
                    "Exception: bad packet: %s: %r",
                    rx_addr,
                    err,
                )
                print_binary(data_str)
                continue

            expect = int(data[KEY.TOTAL])
            new_pvs = data[KEY.PV_LIST]

            if len(new_pvs) == 0:
                log.debug("got 0 PVs for: %s",get_host_name(ioc_id))
                break

            pvs.extend(new_pvs)

            if len(pvs) == expect:
                log.debug(
                    "got %d PVs for: %s", len(pvs), get_host_name(ioc_id),
                )
                break

            offset += len(new_pvs)
            handle += 1

        # Record this fetch in debug fetch list
        try:
            self._debug_fetch_count_lock.acquire()
            ioc_data = self._debug_fetch_count.get(ioc_id, {})
            port_data = ioc_data.get(port, {})
            count = port_data.get(KEY.COUNTER, 0)
            count += 1
            port_data = {
                KEY.TIME: time.time(),
                KEY.PV_COUNT: len(pvs),
                KEY.EXPECT: expect,
                KEY.VERSION: heartbeat_version,
                KEY.COUNTER: count,
            }
            ioc_data[port] = port_data
            self._debug_fetch_count[ioc_id] = ioc_data
        finally:
            self._debug_fetch_count_lock.release()

        if len(pvs) != expect:
            log.error(
                "did not get all %d expected PVS.  Got %d. Timeout?",
                expect,
                len(pvs),
            )

        self.set_pv_count(app_id, len(pvs))

        # Record this list of PVs in the PV dict
        try:
            self._pvs_lock.acquire()
            data = {KEY.LAST_TIME: current_time, KEY.PV_LIST: pvs}
            temp = self._pvs.get(ioc_id, {})
            temp[port] = data
            self._pvs[ioc_id] = temp
        finally:
            self._pvs_lock.release()

        # Update pv_to_heartbeat map
        try:
            self._pv_map_lock.acquire()
            for pv in pvs:
                self._pv_map[pv] = (ioc_id, port)

                # Search for these PVs so that pv_mon learns about them using
                # the CA_PROTO_SEARCH method
                if SETTINGS.PV_SEARCH_ON_HEARTBEAT_FETCH:
                    self._searcher.search(pv, PRIORITY.NORMAL)

                # Note!!! This was an old method when iocmon and pvmon were
                # seperate programs.  Now, we can just tell the pv_manager
                # directly about this PV (although we don't know which beacon
                # these PVs belong to)
                pv_data = {
                    KEY.NAME: pv,
                    KEY.IOC_ID: ioc_id,
                    KEY.LAST_TIME: current_time,
                    KEY.PORT: port,
                }
                self.send_to_pvman(CMD.HEARTBEAT_PV, pv_data)

        finally:
            self._pv_map_lock.release()

    def set_searcher(self, searcher):
        self._searcher = searcher

    def set_pvman(self, pvman):
        self._pvman = pvman

    def send_to_pvman(self, cmd, data, priority=PRIORITY.NORMAL):
        self._pvman.queue_cmd(cmd, data, priority=priority)

    def forget(self, ip_key, port):

        try:
            self.lock()
            self._history_lock.acquire()
            self._pv_map_lock.acquire()

            try:
                temp = self._history[ip_key]
                del temp[port]
            except:
                pass

            try:
                temp = self._heartbeats[ip_key]
                del temp[port]
            except:
                pass

            try:
                temp = self._pvs[ip_key]
                del temp[port]
            except:
                pass

        finally:
            self._pv_map_lock.release()
            self._history_lock.release()
            self.unlock()

    def make_heartbeat_app_id(self, data):
        """
        The same heartbeat can appear on different ports, so the port cannot be
        used to identify hearbeats.  We must use the image cmd dir ioc_ic as
        will applications
        """
        cmd = "%s %s" % (data.get(KEY.IMAGE), data.get(KEY.START_CMD))
        cmd = remove_leading_space(cmd)
        ioc_id = int(data.get(KEY.IP_KEY))
        cwd = data.get(KEY.CWD)
        
        app_id = make_app_id(cmd, cwd, ioc_id)
        return app_id

    def purge_heartbeats(self, ioc_id):
        log.debug("called; ioc_id: %d", ioc_id)

        try:
            self.lock()

            app_ids = self._heartbeat_map.get(ioc_id, [])
            for app_id in app_ids:

                try:
                    del self._heartbeats[app_id]
                except:
                    pass

            try:
                del self._heartbeat_map[ioc_id]
            except:
                pass

        finally:
            self.unlock()

    def beat(self, data):
        """
        Called whenever a heartbeat is detected
        """
        log.debug("called; data: %r", data)

        ioc_id = int(data.get(KEY.IP_KEY))
        heartbeat_port = int(data.get(KEY.PORT))
        port = int(data.get(KEY.SERVER_PORT))
        ip_key_orig = ioc_id

        if not ioc_id or not heartbeat_port:
            log.error(
                "invalid address in heartbeat: %r",
                data,
            )
            return

        # Only the app_id can be used to uniquely identify heartbeats
        app_id = self.make_heartbeat_app_id(data)
        data[KEY.APP_ID] = app_id

        log.debug("got app_id: %d", app_id)

        update = None
        update_pvs = None

        if SETTINGS.UPDATE_HEARTBEAT_APP:
            self.queue_heartbeat_for_app(data)

        msg_time = data.get(KEY.QTIME)
        uptime = data.get(KEY.UP)

        # Send data to the database
        # Why are we sending every heartbeat to the database???
        self.send_to_database(
            DB_CMD.HEARTBEAT_UPDATE, (app_id, ioc_id, port, msg_time, uptime)
        )

        self.lock()

        cur_time = time.time()
        ### time_in_q = cur_time - data.get(KEY.QTIME)

        try:
            heartbeat = self._heartbeats.get(app_id)

            if heartbeat is None:
                update_pvs = (app_id, True)
                seq = data.get(KEY.SEQ)
                if seq == 0:
                    update = (app_id, ioc_id, port, HISTORY.NEW, 0)
                else:
                    update = (
                        app_id,
                        ioc_id,
                        port,
                        HISTORY.DETECTED,
                        data.get(KEY.UP),
                    )

                heartbeat = {}

                # Send to database
                cmd = "%s %s" % (data.get(KEY.IMAGE), data.get(KEY.START_CMD))
                cmd = remove_leading_space(cmd)
                cwd = data.get(KEY.CWD)

                # Create / update this application
                app_data = {
                    KEY.IP_KEY: ioc_id,
                    KEY.PORT: port,
                    KEY.CMD: cmd,
                    KEY.CWD: cwd,
                    KEY.APP_ID: app_id,
                }

                # Add the application to the database
                self.send_to_database(DB_CMD.APP_UPDATE, app_data)

            else:
                existing = heartbeat.get(KEY.DATA)

                seq = int(data.get(KEY.SEQ))
                existing_seq = int(existing.get(KEY.SEQ))

                if seq != (existing_seq + 1):
                    log.warning(
                        "------- HEARTBEAT MISSING SEQ: %s port: %d got: %r "
                        "have: %r",
                        get_host_name(ioc_id),
                        port,
                        seq,
                        existing_seq,
                    )

                if existing_seq > seq:
                    log.debug(
                        "restarted: existing seq: %d seq: %d",
                        existing_seq,
                        seq,
                    )
                    update = (
                        app_id,
                        ioc_id,
                        port,
                        HISTORY.RESTARTED,
                        existing.get(KEY.UP),
                    )
                    update_pvs = (app_id, True)

                elif heartbeat.get(KEY.LOST, False):
                    # If this HEARTBEAT was marked lost it now is found
                    update = (
                        app_id,
                        ioc_id,
                        port,
                        HISTORY.RESUMED,
                        data.get(KEY.UP),
                    )

                else:
                    update_pvs = (app_id, False)

            data[KEY.ADDRESS_ORIG] = (ip_key_orig, heartbeat_port)
            data[KEY.TIME] = cur_time
            data[KEY.SERVER_PORT] = port

            heartbeat[KEY.SERVER_PORT] = (
                port  # The port for an app_id can change!!!
            )
            heartbeat[KEY.DATA] = data
            heartbeat[KEY.TIME] = cur_time
            heartbeat[KEY.LOST] = False

            # Store the updated heartbeat
            self._heartbeats[app_id] = heartbeat

            # Keep a list of all app_ids on an IOC to speed heartbeat retrieval
            ioc_heartbeats = self._heartbeat_map.get(ioc_id, [])
            if app_id not in ioc_heartbeats:
                ioc_heartbeats.append(app_id)
                self._heartbeat_map[ioc_id] = ioc_heartbeats

        finally:
            self.unlock()

        if update:
            self.queue_update_history(update)

        if update_pvs:
            self.queue_update_pvs(update_pvs)

    def get_history_string(self, event):

        history_string = HISTORY_STRING.get(event)
        if not history_string:
            history_string = "Unknown Event: %r" % event

        return history_string

    def get_pv_heartbeat(self, pv_name):

        try:
            self._pv_map_lock.acquire()
            return self._pv_map.get(pv_name)

        finally:
            self._pv_map_lock.release()

    def get_pv_fetch_counts(self, sort=False):

        try:
            self._debug_fetch_count_lock.acquire()
            data = copy.deepcopy(self._debug_fetch_count)
        finally:
            self._debug_fetch_count_lock.release()

        if sort:
            return self.sort_key_port(data)

        result = []
        for ip_key, ioc_data in data.items():
            for port, app_data in ioc_data.items():
                result.append((ip_key, port, app_data))
        return result

    def sort_key_port(self, data):

        temp = []
        result = []

        for ip_key in data.keys():
            host_name = get_host_name(ip_key)
            host_name_sort = make_host_sort_name(host_name)
            temp.append((host_name_sort, ip_key))

        # Sort by host_name
        temp.sort()
        for item in temp:
            key = item[1]
            temp2 = []
            port_data = data.get(key)
            for port, app_data in port_data.items():
                temp2.append((port, app_data))

            temp2.sort()

            for app in temp2:
                port = app[0]
                app_data = app[1]
                result.append((key, port, app_data))

        return result

    def get_heartbeat_for_netstat(self, ioc_id):
        """
        Get the address of an app that can return netstat data
        (there could be more than one app per IP address).
        Prefer a LinuxMonitor app if there is one because it will run as root
        """

        addr = None
        seq = None
        heartbeat_port = None
        data = None
        heartbeat = None

        log.debug("called; ioc_id: %r", ioc_id)

        try:
            self.lock()

            app_ids = self._heartbeat_map.get(ioc_id, [])

            for app_id in app_ids:
                heartbeat = self._heartbeats.get(app_id)

                log.debug(
                    "consider heartbeat: %r",
                    heartbeat,
                )

                if heartbeat.get(KEY.LOST, False):
                    log.debug(
                        "ioc_id: %d Heartbeat LOST.... cannot use!",
                        ioc_id,
                    )
                    continue

                data = heartbeat.get(KEY.DATA)

                if data.get(KEY.VERSION, "unknown") not in ["2.0"]:
                    log.debug(
                        "This heartbeat is not V2.... cannot use!!!",
                    )
                    continue

                image = data.get(KEY.IMAGE, "")
                if image.find("LinuxMonitor") > 0:
                    # Prefer LinuxMonitor heartbeat over any other
                    # as it usually runs as root and can fetch full netstat
                    break

            if data is not None:
                seq = data.get(KEY.SEQ)
                heartbeat_port = data.get(KEY.PORT)

        finally:
            self.unlock()

        if heartbeat_port is None:
            log.debug(
                "No V2 heartbeat found for: %s",
                get_host_name(ioc_id),
            )
        else:
            addr = (get_ip_addr(ioc_id), heartbeat_port)

        return addr, seq, heartbeat

    def get_heartbeat(self, app_id=None, ioc_id=None, port=None):

        log.debug(
            "called; app_id: %r ioc_id: %r port: %r",
            app_id,
            ioc_id,
            port,
        )

        # Sanity check
        if app_id is not None and ioc_id is not None:
            raise ValueError("Cannot specify app_id and ioc_ic")

        try:
            self.lock()

            if app_id:
                heartbeat = self._heartbeats.get(app_id)
                return copy.deepcopy(heartbeat)
            else:
                app_ids = self._heartbeat_map.get(ioc_id, [])
                for app_id in app_ids:
                    heartbeat = self._heartbeats.get(app_id)
                    tcp_port = heartbeat.get(KEY.SERVER_PORT)
                    if tcp_port == port:
                        return copy.deepcopy(heartbeat)

        finally:
            self.unlock()

    def get_history(self, ip_key, port):

        history = None
        try:
            self._history_lock.acquire()
            ioc_data = self._history.get(ip_key, {})
            result = ioc_data.get(port)
            if result:
                history = copy.copy(result)
        finally:
            self._history_lock.release()
        return history

    def get_pvs(self, ip_key, port, sort=False):
        temp = None
        try:
            self._pvs_lock.acquire()
            data = self._pvs.get(ip_key, {})
            result = data.get(port)
            if result:
                temp = copy.copy(result)
        finally:
            self._pvs_lock.release()

        if not temp:
            return None

        pv_list = temp.get(KEY.PV_LIST)

        if sort:
            temp = [(pv.lower(), pv) for pv in pv_list]
            temp.sort()
            pv_list = [item[1] for item in temp]

        return pv_list

    def set_pv_count(self, app_id, value):

        try:
            self.lock()
            data = self._heartbeats.get(app_id)
            if not data:
                log.error("did not find app_id: %d", app_id)
                return

            data[KEY.PV_COUNT] = value

        finally:
            self.unlock()

    def get_list(self, offset, count, sort):
        """
        Get all heartbeats.  If optional ip_key is provided,
        return heartbeats for that IOC only
        """
        log.debug(
            "called; offset: %d count: %d sort: %s",
            offset,
            count,
            sort,
        )

        try:
            self._sort_lock.acquire()
            if sort == SORT_MODE.IOC:
                heartbeats = copy.deepcopy(self._sorted_name)

            elif sort == SORT_MODE.UPTIME:
                heartbeats = copy.deepcopy(self._sorted_uptime)

            elif sort == SORT_MODE.LAST_SEEN:
                heartbeats = copy.deepcopy(self._sorted_last_seen)
            else:
                raise ValueError("Sort mode not supported: %s" % sort)

        finally:
            self._sort_lock.release()

        total_len = len(heartbeats)

        return heartbeats[offset : offset + count], total_len

    def get_ioc_id_list(self):

        try:
            self.lock()
            result = [ioc_id for ioc_id in list(self._heartbeat_map.keys())]
        finally:
            self.unlock()

        return result

    def get_list_for_ioc(self, ioc_id):
        """
        Get all heartbeats for the specified IOC
        """
        log.debug("called; ioc_id: %r", ioc_id)
        try:
            self.lock()

            result = []
            if ioc_id is None:
                app_ids = iter(self._heartbeats.keys())
            else:
                app_ids = self._heartbeat_map.get(ioc_id, [])

                for app_id in app_ids:
                    heartbeat = self._heartbeats.get(app_id)
                    heartbeat_data = heartbeat.get(KEY.DATA)
                    port = heartbeat_data.get(KEY.SERVER_PORT)
                    ioc_id = heartbeat_data.get(KEY.IP_KEY)
                    result.append((ioc_id, port, heartbeat, app_id))

            return copy.deepcopy(result)

        finally:
            self.unlock()

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()

    def save(self):
        self.lock()
        self._history_lock.acquire()

        try:
            data = {"HEARTBEATS": self._heartbeats, "HISTORY": self._history}
            data_str = json.dumps(data)

        except Exception as err:
            data_str = None
            log.exception(
                "Exception generating HEARTBEATS json: %s",
                str(err),
            )

        finally:
            self._history_lock.release()
            self.unlock()

        if data_str:
            saver = FileSaver(prefix=self._prefix)
            saver.save(data_str)
            log.debug(
                "HEARTBEATS data saved to: %s ",
                saver.get_newest(),
            )
