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
import os
import queue
import socket
import struct
import sys
import threading
import time

from data_management.database import CMD as DB_CMD
from definitions import CMD_NONE, KEY, KIND, SETTINGS
from utilities.file_saver import FileSaver
from utilities.statistics import StatisticsManager
from utilities.thread_monitor import ThreadMonitor
from utilities.utils import (beacon_estimated_age_str, config, get_host_name,
                             get_ip_addr, get_ip_key)

log = logging.getLogger("PVMLogger")

TM = ThreadMonitor()
STATS = StatisticsManager()


class BEACON_CHECK(object):
    NEW = 100
    EXISTING = 101
    UNDETERMINED = 102


class CMD(object):
    BEACON = 100
    PURGE = 200
    HEARTBEAT = 300
    BAD_BEACON = 400
    BAD_BEACON_ADDR = 500


class DB_KEY(object):
    BEACONS = "beacons"


class BeaconNew(object):

    def __init__(self):

        self._address = None
        self._data = None
        self._seq = None
        self._ioc_id_addr = None
        self._ioc_id_beacon = None
        self._data_length = None
        self._is_beacon = False
        self._bad_address = True
        self._tcp_port = None
        self._ip_addr = None

    def set(self, address, data):

        self._address = address
        self._data = data
        self._data_length = len(data)
        self._bad_address = True
        self._is_beacon = False
        self._seq = 0

        result = True

        try:
            c = struct.unpack_from("HHHHII", data)
            self._tcp_port = socket.ntohs(c[3])
            self._seq = socket.ntohl(c[4])
            self._ioc_id_beacon = socket.ntohl(c[5])

            log.debug(
                "port: %d seq: %d ip_key: %d addr[0]: %r",
                self._tcp_port,
                self._seq,
                self._ioc_id_beacon,
                address[0],
            )

            # Looks like a valid beacon
            self._is_beacon = True
            self._ioc_id_addr = address[0]
            self._ip_addr = get_ip_addr(self._ioc_id_addr)

            # Sanity check
            if self._ioc_id_addr != self._ioc_id_beacon:
                result = False
            if not self._ip_addr.startswith("10."):
                pass
            elif self._ip_addr.startswith(("10.10.10")):
                pass
            else:
                self._bad_address = False

        except Exception as err:
            log.exception(
                "exception: %r",
                err,
            )

        return result

    def set_seq(self, seq):
        self._seq = seq

    def get_seq(self):
        return self._seq

    def get_tcp_port(self):
        return self._tcp_port

    def get_ioc_id(self):
        return self._ioc_id_addr

    def get_ioc_id_beacon(self):
        return self._ioc_id_beacon

    def bad_address(self):
        return self._bad_address

    def get_ip_addr(self):
        return self._ip_addr

    def get_beacon_key(self):
        return "%d-%d" % (self._ioc_id_addr, self._tcp_port)

    def is_beacon(self):
        return self._is_beacon

    def get_data_len(self):
        return self._data_length


class BeaconLegacy(object):
    """
    This class checks legacy beacons that do not have a sequence number
    Check that the sequence number is actually increasing (or not).  If it is
    not increasing, assume that it is a legacy beacon
    """

    def __init__(self):

        self._data = {}
        self._start_time = None

    def check(self, beacon):

        current_time = int(time.time())
        if self._start_time is None:
            self._start_time = current_time

        seq = beacon.get_seq()

        if seq > 2:
            # We will actually watch for a couple beacons because they come very
            # fast when an app is started (is there a risk of missing one?)
            return BEACON_CHECK.EXISTING

        key = beacon.get_beacon_key()

        if seq == 0:
            # This could be:
            # - a new NEW beacon
            # - a new LEGACY beacon
            # - an existing LEGACY beacon (i.e., all seq are zero)

            # Get the beacon data
            beacon_data = self._data.get(key)

            if beacon_data is not None:
                # We have seen this beacon before with a sequence number of
                # zero. Therefore it must be a legacy beacon.

                interval = current_time - beacon_data.get("lt")

                if interval > 120:

                    # This must be a new beacon
                    data = {"ft": current_time, "lt": current_time, "seq": 0}
                    self._data[key] = data

                    log.debug(
                        "BEACON_CHECK.UNDETERMINED: %r",
                        beacon,
                    )
                    return BEACON_CHECK.UNDETERMINED

                else:
                    seq = beacon_data.get("seq")
                    seq += 1

                    beacon_data["seq"] = seq
                    beacon_data["lt"] = current_time
                    beacon.set_seq(seq)

                    if "kind" in beacon_data:
                        kind = BEACON_CHECK.EXISTING
                        log.debug(
                            "LEGACY 1 --> BEACON_CHECK.EXISTING: %r",
                            beacon,
                        )

                    else:

                        if current_time - self._start_time > 90:
                            # This is likely a new beacon as it happened well
                            # after startup time
                            kind = BEACON_CHECK.NEW
                            log.debug(
                                "LEGACY --> BEACON_CHECK.NEW: %r",
                                beacon,
                            )
                        else:
                            # This is very close to the startup time; lets
                            # assume this LEGACY beacon is an existing
                            # beacon
                            kind = BEACON_CHECK.EXISTING
                            log.debug(
                                "LEGACY 2 --> BEACON_CHECK.EXISTING: %r",
                                beacon,
                            )

                        # Must record the initial classification of this LEGACY beacon
                        # for the next check
                        beacon_data["kind"] = kind

                    # Do I need to do this? I don't think so
                    self._data[key] = beacon_data
                    return kind

            else:
                # This is the first time we have seen this beacon
                data = {"ft": current_time, "lt": current_time, "seq": 0}
                self._data[key] = data
                log.debug(
                    "BEACON_CHECK.UNDETERMINED: %r",
                    beacon,
                )
                return BEACON_CHECK.UNDETERMINED

        else:
            # We got a beacon with a sequence number > 0
            beacon_data = self._data.get(key)
            if beacon_data is not None:
                # We have seen this just seen this beacon with a sequence of 0
                try:

                    del self._data[key]
                except:
                    pass

                log.debug("BEACON_CHECK.NEW: %r", beacon)
                return BEACON_CHECK.NEW

            # There is no beacon data for this beacon with seq > 0
            # The only way this could happen is that we already returned
            # BEACON_CHECK.NEW above
            log.debug("BEACON_CHECK.EXISTING: %r", beacon)
            return BEACON_CHECK.EXISTING


class BeaconManager(object):

    def __init__(self, dataman, my_hostname):

        self._my_hostname = my_hostname
        self._my_ip_addr = socket.gethostbyname(self._my_hostname)
        self._my_ip_key = get_ip_key(self._my_ip_addr)
        self._dataman = dataman
        self._count_saves = 0
        self._poll_cache = None
        self._iocman = None
        self._callback_database_func = None
        self._gateway_alive = False
        self._gateway_alive_time = 0
        self._max_age_lateness = 0

        self._beacon_legacy = BeaconLegacy()

        self._queue = queue.Queue()
        self._timer_worker = threading.Timer(5, self.worker)
        self._timer_check_age = threading.Timer(5, self.worker_check_age)

        self._beacon_lock = threading.Lock()
        self._beacons = {}
        self._beacons_lost = {}
        self._beacons_duplicate = {}

        self._beacon = BeaconNew()

        # For keeping track of bad beacons (e.g., from 192.168.X.X)
        self._bad_beacon = BeaconNew()
        self._bad_beacon_dict = {}

        # For keeping track of beacon address mismatches
        self._bad_beacon_addr_dict = {}

        # For computing beacon interval - so that uptime can be computed
        self._beacon_interval = {}
        self._timer_save = threading.Timer(60, self.worker_save)

    def worker_save(self):

        thread_id = TM.add("Beaconman: Data saver", 1200)

        save_interval = config.SAVE_INTERVAL

        next_ping_time = time.time()
        next_save_time = time.time() + save_interval

        while True:
            cur_time = time.time()
            if cur_time >= next_ping_time:
                TM.ping(thread_id)
                next_ping_time += 600

            if cur_time >= next_save_time:
                self.save(thread_id)
                next_save_time += save_interval

            time.sleep(60)

    def save(self, thread_id):

        start_time = time.time()
        try:
            self._beacon_lock.acquire()
            beacon_data = copy.deepcopy(self._beacons)

        except Exception as err:
            log.exception(
                "Exception saving beacons: %r",
                err,
            )
            return

        finally:
            self._beacon_lock.release()

        elapsed_time = time.time() - start_time
        log.debug("beacon save copy time: %.3f", elapsed_time)
        start_time = time.time()

        try:
            data = {DB_KEY.BEACONS: beacon_data}

            data_str = json.dumps(data, indent=4)
            saver = FileSaver(prefix="beacons")
            saver.save(data_str)

        except Exception as err:
            log.exception(
                "Exception saving beacons: %r",
                err,
            )
            return

        elapsed_time = time.time() - start_time

        self._count_saves += 1
        STATS.set("Beacon Saves", self._count_saves)
        STATS.set("Beacon Save Time (sec)", int(elapsed_time))

        log.debug("Save time: %.2f", elapsed_time)

    def get_beacon_interval_ms(self, ioc_id, port, t):
        """
        Measure the beacon interval so that uptime can be computed
        """
        beacons = self._beacon_interval.get(ioc_id, {})
        beacon = beacons.get(port)

        if beacon:
            count = beacon[0]
            first_time = beacon[1]
            last_time = beacon[2]

            if (t - last_time) < 60:

                count += 1
                interval = (t - first_time) / float(count)
                beacon = (count, first_time, t)
            else:
                beacon = (0, t, t)
                interval = 0

        else:
            beacon = (0, t, t)
            interval = 0

        beacons[port] = beacon
        self._beacon_interval[ioc_id] = beacons

        return int(1000 * interval)

    def handle_gateway_alive(self, alive):
        log.debug("alive: %r", alive)
        self._gateway_alive = alive
        self._gateway_alive_time = time.time()

    def set_callback_database(self, func):
        self._callback_database_func = func

    def send_to_database(self, cmd, data):
        self._callback_database_func(cmd, data)

    def set_poll_cache(self, poll_cache):
        self._poll_cache = poll_cache

    def set_iocman(self, iocman):
        self._iocman = iocman

    def load(self):
        log.debug("called")

        saver = FileSaver(prefix="beacons")
        newest_file = saver.get_newest()

        if not newest_file:
            return

        f = None
        try:
            f = open(newest_file, "r")
        finally:
            if f:
                f.close()

        beacons = {}

        self._beacons = beacons

        log.debug("len(beacons): %d", len(self._beacons))

    def start(self):
        log.debug("called")
        self._timer_worker.start()
        self._timer_check_age.start()
        self._timer_save.start()

    def queue_heartbeat(self, data):
        self._queue.put_nowait((CMD.HEARTBEAT, data))

    def queue_beacon(self, address, data, msg_time):
        self._queue.put_nowait((CMD.BEACON, (address, data, msg_time)))

    def handle_beacon(self, ip_key, port, data, msg_time):
        """
        Called for every single beacon
        """
        self.queue_beacon((ip_key, port), data, msg_time)

    def worker_check_age(self):

        thread_id = TM.add("Beaconman: Check Beacon Age", 120)

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
                self.check_beacon_age(next_check_time)
                next_check_time += 10

            time.sleep(2)

    def worker(self):

        thread_id = TM.add("Beaconman: Process Beacons", 120)
        next_ping_time = time.time()
        next_log_time = time.time()
        max_queue_size = 0

        map = {
            CMD.BEACON: self.handle_cmd_update_beacon,
            CMD.HEARTBEAT: self.handle_cmd_heartbeat,
            CMD.BAD_BEACON: self.handle_cmd_bad_beacon,
            CMD.BAD_BEACON_ADDR: self.handle_cmd_bad_beacon_addr,
        }

        while True:
            cur_time = time.time()

            qsize = self._queue.qsize()
            if qsize > 2000:
                # This can run away on us and cause database to get flooded with
                # bogus events
                log.error(
                    "PANIC!!!!! beaconman qsize: %d TERMINATING!!!",
                    qsize,
                )
                STATS.set("PANIC: Beaconman qsize", qsize)
                break

            if qsize > max_queue_size:
                max_queue_size = qsize
                STATS.set("Max Queue Size: Beaconman", max_queue_size)

            # Ping thread monitor every 30 seconds
            if cur_time > next_ping_time:
                TM.ping(thread_id)
                next_ping_time += 30

            if cur_time > next_log_time:
                STATS.set("Queue Size: Beaconman", qsize)
                next_log_time += 30

            try:
                item = self._queue.get(block=True, timeout=2)
            except queue.Empty:
                continue

            func = map.get(item[0])
            func(item[1])

    def handle_cmd_bad_beacon_addr(self, data_in):
        """
        Got a bad beacon (i.e., invalid ip_addr).  Keep a list
        of these bad beacons with the hope of eliminating them.
        """
        address = data_in[0]
        data = data_in[1]
        log.debug("Bad beacon: %r", address)

        try:
            self._bad_beacon.set(address, data)
            ioc_id = self._bad_beacon.get_ioc_id()
            tcp_port = self._bad_beacon.get_tcp_port()
            key = "%d-%d" % (ioc_id, tcp_port)

        except Exception as err:
            log.exception(
                "Exception handling bad beacon: %r",
                err,
            )
            return

        try:
            self._beacon_lock.acquire()
            bad_beacon = self._bad_beacon_addr_dict.get(key, {})
            bad_beacon[KEY.SEQ] = self._bad_beacon.get_seq()
            bad_beacon[KEY.PORT] = address[1]
            bad_beacon[KEY.LAST_TIME] = time.time()
            bad_beacon[KEY.IOC_ID] = self._bad_beacon.get_ioc_id_beacon()
            self._bad_beacon_addr_dict[key] = bad_beacon
        finally:
            self._beacon_lock.release()

    def handle_cmd_bad_beacon(self, data_in):
        """
        Got a bad beacon (i.e., invalid ip_addr).  Keep a list
        of these bad beacons with the hope of eliminating them.
        """
        address = data_in[0]
        data = data_in[1]
        log.debug("Bad beacon: %r", address)

        try:
            self._bad_beacon.set(address, data)
            ioc_id = self._bad_beacon.get_ioc_id()
            tcp_port = self._bad_beacon.get_tcp_port()
            key = "%d-%d" % (ioc_id, tcp_port)

        except Exception as err:
            log.exception(
                "Exception handling bad beacon: %r",
                err,
            )
            return

        try:
            self._beacon_lock.acquire()
            bad_beacon = self._bad_beacon_dict.get(key, {})
            bad_beacon[KEY.SEQ] = self._bad_beacon.get_seq()
            bad_beacon[KEY.PORT] = address[1]
            bad_beacon[KEY.LAST_TIME] = time.time()
            self._bad_beacon_dict[key] = bad_beacon
        finally:
            self._beacon_lock.release()

    def handle_cmd_heartbeat(self, data):
        """
        This worker is called when there has been a change in the heartbeats.
        Check all the beacons and see if there is a beacon for this heartbeat.
        If there is (and there should be) then we can assign an accurate
        start time to the beacon.  There may also be rare cases where the beacon
        was not mapped
        """
        app_id = data[0]
        ioc_id = data[1]
        port = data[2]

        try:
            self._beacon_lock.acquire()
            beacons = self._beacons.get(ioc_id, {})
            beacon = beacons.get(port)
            if beacon:

                if app_id:
                    heartbeat = self._dataman.get_heartbeat(app_id=app_id)
                else:
                    heartbeat = self._dataman.get_heartbeat(
                        ioc_id=app_id, port=port
                    )

                if heartbeat:
                    log.info(
                        "\n%s\nMATCHED HEARTBEAT TO BEACON!!!!!!!!!!!!!\n"
                        "BEACON, %s\nHEARTBEAT %s\n%s",
                        "*" * 80,
                        beacon,
                        heartbeat,
                        "*" * 80,
                    )

        finally:
            self._beacon_lock.release()

    def get_ioc_id_list(self):

        log.debug("called")

        try:
            self._beacon_lock.acquire()
            return [k for k in self._beacons.keys()]
        finally:
            self._beacon_lock.release()

    def purge_beacons(self, ioc_id):
        """
        This could be a worker and run in the background but I want
        a forgotton IOC to disappear totally and completely ASAP.
        """
        log.debug("called; ioc_id: %d", ioc_id)

        try:
            self._beacon_lock.acquire()
            del self._beacons[ioc_id]

        except Exception:
            pass

        finally:
            self._beacon_lock.release()

    def get_beacons_bad(self):

        try:
            self._beacon_lock.acquire()

            result = copy.deepcopy(self._bad_beacon_dict)
            return result

        finally:
            self._beacon_lock.release()

    def get_beacons_addr_mismatch(self):

        try:
            self._beacon_lock.acquire()

            result = copy.deepcopy(self._bad_beacon_addr_dict)
            return result

        finally:
            self._beacon_lock.release()

    def get_beacons(self, ioc_id=None, skip_lost=True, as_dict=False):

        if ioc_id is None and as_dict is True:
            raise ValueError("as_dict must be False when ip_key is None")

        try:
            result = []
            d = {}

            self._beacon_lock.acquire()
            if ioc_id is None:
                ioc_ids = [k for k in self._beacons.keys()]
            else:
                ioc_ids = [ioc_id]

            for ioc_id in ioc_ids:
                data = self._beacons.get(ioc_id, {})
                for port, beacon in data.items():
                    if skip_lost is True and beacon.get(KEY.LOST) is not None:
                        continue

                    if as_dict:
                        d[port] = beacon
                    else:
                        result.append((ioc_id, port, beacon))

            if as_dict:
                return d

            return result

        finally:
            self._beacon_lock.release()

    def get_beacon(self, ip_key, port):
        """
        Returns the discovered beacon data for the specified ip_address and port
        """

        log.debug(
            "called; ip_key: %r port: %r",
            ip_key,
            port,
        )

        try:
            self._beacon_lock.acquire()
            beacons = self._beacons.get(ip_key)

            if not beacons:
                log.error(
                    "ERROR finding beacon for: %s", get_host_name(ip_key),
                )
                return

            beacon_data = beacons.get(port)
            if beacon_data is None:
                log.error(
                    "ERROR finding beacon for: %s port: %r",
                    get_host_name(ip_key),
                    port,
                )

                return

            return copy.copy(beacon_data)

        finally:
            self._beacon_lock.release()

    def get_beacon_ports_unmapped(self, ip_key):
        """
        Get a list of all unmapped beacons for this ip_address.  Include the
        "first_time" the beacons was seen so it can be determined if the
        netstat data from the IOC must be updated.
        """

        log.debug("called; %s", get_host_name(ip_key))

        result = []
        try:
            self._beacon_lock.acquire()

            beacons = self._beacons.get(ip_key)
            for port, value in beacons.items():
                log.debug(
                    "CONSIDER BEACON %s %r %r",
                    get_host_name(ip_key),
                    port,
                    value,
                )

                if value.get(KEY.LOST) is not None:
                    log.debug(
                        "skipping lost beacon: %s", get_host_name(ip_key),
                    )
                    continue

                if not value.get(KEY.REMAP, False):
                    cmd_have = value.get(KEY.CMD, CMD_NONE)
                    if cmd_have != CMD_NONE:
                        log.debug(
                            "already mapped: %s port: %r " "cmd: %r cwd: %r",
                            get_host_name(ip_key),
                            port,
                            value.get(KEY.CMD),
                            value.get(KEY.CWD),
                        )
                        continue

                    else:
                        log.debug(
                            "(re)map beacon: %s port %r",
                            get_host_name(ip_key),
                            port,
                        )

                else:
                    # remap flag is True; attempt to remap beacon
                    pass

                result.append(port)

        finally:
            self._beacon_lock.release()

        return result

    def map_beacon(self, ip_key, port, cwd, cmd, app_id):
        """
        Set the command and CWD for the specified beacon
        """
        log.debug(
            "called; ip_key: %d port: %d cwd: %r cmd: %r",
            ip_key,
            port,
            cwd,
            cmd,
        )

        try:
            self._beacon_lock.acquire()
            beacons = self._beacons.get(ip_key, {})

            beacon_data = beacons.get(port)
            if beacon_data is None:
                log.error(
                    "ERROR finding beacon for: %s port: %r",
                    get_host_name(ip_key),
                    port,
                )
                return

            beacon_data[KEY.CMD] = cmd
            beacon_data[KEY.CWD] = cwd
            beacon_data[KEY.APP_ID] = app_id
            beacon_data[KEY.REMAP] = False

            beacons[port] = beacon_data
            self._beacons[ip_key] = beacons

        finally:
            self._beacon_lock.release()

        # Tell iocman to update IOC state
        self._iocman.update_ioc(ip_key)

    def handle_cmd_update_beacon(self, data_in):
        """
        called for every single beacon
        """
        address = data_in[0]
        data = data_in[1]
        msg_time = data_in[2]

        result = self._beacon.set(address, data)

        if not result:
            # breem:2020_08_30: This beacon had a address mismatch. At this
            # point I am not sure if its valid or not so capture data for
            # for later examination
            self._queue.put_nowait((CMD.BAD_BEACON_ADDR, (address, data)))

        if not self._beacon.is_beacon():
            err_host_name = get_host_name((address[0]))
            log.error(
                "Invalid BEACON: (len: %d) addr: %s (%r)",
                self._bad_beacon.get_data_len(),
                address,
                err_host_name,
            )

            STATS.increment("Invalid beacons")

            return

        if self._beacon.bad_address():
            self._queue.put_nowait((CMD.BAD_BEACON, (address, data)))
            return

        beacon_key = self._beacon.get_beacon_key()
        ip_addr = self._beacon.get_ip_addr()
        ip_key = self._beacon.get_ioc_id()
        tcp_port = self._beacon.get_tcp_port()

        if SETTINGS.LOG_BEACONS:
            log.debug(
                "GOT beacon: %r (%d) (%r)",
                ip_addr,
                ip_key,
                tcp_port,
            )

        # For some reason I am getting 4 copies of all local beacons on my
        # OPI2031-006
        # But we can't just punt duplicate beacons because legacy beacons do not
        # have a sequence number and appear as duplicate beacons.  Therefore we
        # need to only check for duplicate beacons on a specific list of hosts
        if ip_key == self._my_ip_key:
            last_seq = self._beacons_duplicate.get(beacon_key, -1)
            if last_seq == self._beacon.get_seq():
                log.debug(
                    "Punt duplicate beacon: %r seq: %r",
                    beacon_key,
                    last_seq,
                )
                return
            self._beacons_duplicate[beacon_key] = self._beacon.get_seq()

        beacon_class = self._beacon_legacy.check(self._beacon)

        if beacon_class == BEACON_CHECK.UNDETERMINED:
            # Not enough information to determine beacon status
            log.debug(
                "cannot classify this beacon: %r",
                self._beacon,
            )
            return

        self._beacon_lock.acquire()

        # Note: The LEGACY CHECK could inject a sequence number into legacy
        # beacons
        seq = self._beacon.get_seq()

        ioc_beacons = self._beacons.get(ip_key, {})
        have_beacon = ioc_beacons.get(tcp_port)
        lost_beacon = self._beacons_lost.get(beacon_key)

        sql_data = {
            KEY.IP_KEY: ip_key,
            KEY.PORT: tcp_port,
            KEY.SEQ: seq,
        }

        # This is a test to see if we can record every beacon.
        # Put the actual seq_rx  into the database
        # breem: update: 2021_05_13:  Updating the database with every beacon is
        # causing significant additional processing and as far as I can tell the
        # only thing it is getting us is the "last_time_seen" for beacons that
        # are not mapped by heartbeats

        beacon_interval = None
        if seq < 100:
            beacon_interval = self.get_beacon_interval_ms(
                ip_key, tcp_port, msg_time
            )

        have_flag = False if have_beacon is None else True
        lost_flag = False if lost_beacon is None else True

        change_detected = False
        update = None

        if beacon_class == BEACON_CHECK.NEW:
            if have_flag and not lost_flag:
                age_str = beacon_estimated_age_str(have_beacon)

                # On a "restart" put the age of beacon it replaced
                sql_data[KEY.SEQ] = have_beacon.get(KEY.SEQ)

                self.send_to_database(DB_CMD.BEACON_RESTART, sql_data)
                change_detected = True
                update = (ip_key, tcp_port, KIND.RESTARTED)
                log.debug(
                    "BEACON: RESTARTED: %s:%d",
                    get_host_name(ip_key),
                    tcp_port,
                )

            else:
                age_str = "New Beacon"
                log.debug(
                    "BEACON: NEW: %s:%d",
                    get_host_name(ip_key),
                    tcp_port,
                )
                self.send_to_database(DB_CMD.BEACON_NEW, sql_data)
                change_detected = True
                update = (ip_key, tcp_port, KIND.NEW)

        else:
            if have_flag:
                if seq < have_beacon[KEY.SEQ]:
                    age_str = beacon_estimated_age_str(have_beacon)
                    sql_data[KEY.SEQ] = have_beacon.get(KEY.SEQ)
                    self.send_to_database(DB_CMD.BEACON_RESTART, sql_data)
                    change_detected = True
                    log.debug(
                        "BEACON: RESTARTED: %s:%d",
                        get_host_name(ip_key),
                        tcp_port,
                    )
                    update = (ip_key, tcp_port, KIND.RESTARTED)
                else:
                    # This is just a regular beacon update
                    pass

            else:
                age_str = beacon_estimated_age_str(None, seq=seq)
                log.debug(
                    "BEACON: DETECTED: %s:%d",
                    get_host_name(ip_key),
                    tcp_port,
                )
                self.send_to_database(DB_CMD.BEACON_DETECTED, sql_data)
                change_detected = True
                update = (ip_key, tcp_port, KIND.DETECTED)

            if lost_flag:
                if have_beacon is not None and seq < have_beacon.get(
                    KEY.SEQ, 0
                ):
                    print("what should I do here 222?")
                else:
                    age_str = beacon_estimated_age_str(None, seq=seq)
                    log.debug(
                        "BEACON: RESUMED: %s:%d",
                        get_host_name(ip_key),
                        tcp_port,
                    )
                    self.send_to_database(DB_CMD.BEACON_RESUMED, sql_data)
                    change_detected = True
                    self._iocman.update_beacon(ip_key, tcp_port, KIND.DETECTED)

                    # TODO: Ensure that poll cache is only updated when the
                    # server is actually running. We do not want a stop
                    # of the server to cause the poll cache to get cleared
                    if self._poll_cache:
                        self._poll_cache.update_beacon(
                            ip_key, tcp_port, KIND.DETECTED
                        )

        try:
            beacons_lost = self._beacons_lost.get(ip_key, {})
            del beacons_lost[tcp_port]
            self._beacons_lost[ip_key] = beacons_lost
        except:
            pass

        if have_beacon is None:
            beacon_data = {}
        else:
            beacon_data = have_beacon

        # Do not set REMAP to false here...
        if change_detected:
            beacon_data[KEY.REMAP] = change_detected

        lateness = time.time() - msg_time
        if lateness > 5.0:
            log.warning(
                "LATE PROCESSING BEACON!!!! BEACON MSG_TIME: %f LATENESS: %f",
                msg_time,
                lateness,
            )

        beacon_data[KEY.TIME] = msg_time
        beacon_data[KEY.PORT] = tcp_port
        beacon_data[KEY.SEQ] = seq
        beacon_data[KEY.LOST] = None
        if beacon_interval is not None:
            beacon_data[KEY.INTERVAL_MS] = beacon_interval

        if update:
            update_kind = update[2]
            log.info("THIS IS A BEACON UPDATE KIND---------: %s", update_kind)
            if update_kind in [KIND.NEW, KIND.RESTARTED]:
                beacon_data[KEY.TIME_START] = msg_time

        ioc_beacons = self._beacons.get(ip_key, {})
        ioc_beacons[tcp_port] = beacon_data
        self._beacons[ip_key] = ioc_beacons

        self._beacon_lock.release()

        if update:
            self._iocman.update_beacon(update[0], update[1], update[2])

            # If is safe to update the poll cache??
            if self._poll_cache:
                self._poll_cache.update_beacon(update[0], update[1], update[2])

            # See if there is a heartbeat.  This can happen if by chance
            # the heartbeat was detected before the beacon
            self.queue_heartbeat((None, ip_key, tcp_port))

    def check_beacon_age(self, desired_check_time):

        if not self._gateway_alive:
            log.debug(
                "gateway alive: FALSE; skip lost beacon check",
            )
            return

        start_time = time.time()

        # Make sure gateway has been running for a while before declaring
        # beacons lost
        if start_time - self._gateway_alive_time < 90:
            log.debug(
                "gateway alive time < 90; skip lost beacon check",
            )
            return

        try:
            cur_time = time.time()

            late = cur_time - desired_check_time
            if late > self._max_age_lateness:
                self._max_age_lateness = late
                STATS.set("Max Lateness: Beaconman", "%.2f" % late)

            if late > 5.0:
                log.debug(
                    "running; late: %.3f (max: %.3f)",
                    late,
                    self._max_age_lateness,
                )

            self._beacon_lock.acquire()

            for ip_key, ioc_data in self._beacons.items():
                for port, beacon in ioc_data.items():

                    age = cur_time - beacon[KEY.TIME]

                    if age < (60 + late):
                        continue

                    beacons_lost = self._beacons_lost.get(ip_key, {})
                    if port in beacons_lost:
                        continue

                    log.debug(
                        "BEACON: LOST: %s:%d age: %.2f ",
                        get_ip_addr(ip_key),
                        port,
                        age,
                    )

                    beacon[KEY.LOST] = int(time.time())

                    ### age_str = beacon_estimated_age_str(beacon)
                    self._iocman.update_beacon(ip_key, port, KIND.LOST)

                    if self._poll_cache:
                        self._poll_cache.update_beacon(ip_key, port, KIND.LOST)

                    sql_data = {
                        KEY.IP_KEY: ip_key,
                        KEY.PORT: port,
                        KEY.SEQ: beacon[KEY.SEQ],
                    }

                    self.send_to_database(DB_CMD.BEACON_LOST, sql_data)
                    beacons_lost[port] = True
                    self._beacons_lost[ip_key] = beacons_lost

            # Check the ages of the bad beacons
            keep = {}
            for key, value in self._bad_beacon_dict.items():
                last_time = value.get(KEY.LAST_TIME)
                age = cur_time - last_time
                if age > 60:
                    continue
                keep[key] = value
            self._bad_beacon_dict = keep

            # Check the ages of the bad beacons
            keep = {}
            for key, value in self._bad_beacon_addr_dict.items():
                last_time = value.get(KEY.LAST_TIME)
                age = cur_time - last_time
                if age > 60:
                    continue
                keep[key] = value
            self._bad_beacon_addr_dict = keep

        finally:
            self._beacon_lock.release()

        elapsed_time = time.time() - start_time

        if late > 5.0:
            log.debug(
                "late: %.2f (max: %.2f) elapsed time: %.2f",
                late,
                self._max_age_lateness,
                elapsed_time,
            )
