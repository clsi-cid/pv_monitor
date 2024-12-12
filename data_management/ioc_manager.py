# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import copy
import logging
import pickle
import queue
import threading
import time

from data_management.database import CMD as DB_CMD
from data_management.database import SQL_INSERT_LOG
from definitions import (APP_STATE, COLOR, IOC_OPTION, IOC_OPTION_LIST,
                         IOC_STATE)
from definitions import KEY
from definitions import KEY as HEARTBEAT_KEY
from definitions import KIND, LOG_KIND, PRIORITY, SETTINGS
from utilities.pvmonitor_time import PVMonitorTime
from utilities.statistics import StatisticsManager
from utilities.thread_monitor import ThreadMonitor
from utilities.utils import (beacon_est_uptime_sec, get_host_name, get_ip_addr,
                             make_host_sort_name, make_tooltip)

log = logging.getLogger("PVMLogger")

TM = ThreadMonitor()

SQL_INSERT_IOC = (
    "insert into iocs (id, status, description, create_time, update_time) "
    + "values (%s, %s, %s, %s, %s)"
)

SQL_INSERT_EXPECTED_BEACON = (
    "insert into expected_beacon_ports (ioc_id, port, create_time) "
    + "values (%s, %s, %s)"
)

STATS = StatisticsManager()


class CMD(object):
    PROCESS_IOC = 0
    MISSING_BEACONS = 1
    PING = 2


class IOCManager(object):
    """ """

    def __init__(self, dataman):

        self._ioc_cache = None
        self._summary_data = {}

        self._lock = threading.Lock()
        self._msg_id = 0
        self._msg_id_lock = threading.Lock()

        self._dataman = dataman
        self._sql = self._dataman._sql

        self._queue = queue.PriorityQueue()
        self._timer = threading.Timer(1, self.worker)
        self._mytime = PVMonitorTime()

        self._description_cache = {}
        self._tooltip_cache = {}

    def worker(self):

        thread_id = TM.add("IOCMan: Worker", 120)
        next_ping_time = time.time()
        max_queue_size = 0

        map = {
            CMD.PROCESS_IOC: self.handle_cmd_update_ioc,
            CMD.PING: self.handle_cmd_ping,
            CMD.MISSING_BEACONS: self.handle_cmd_missing_beacons,
        }

        while True:

            qsize = self._queue.qsize()
            if qsize > 10000:
                # This can run away on us and cause database to get flooded with
                # bogus events
                log.error(
                    "PANIC!!!!! IOCman qsize: %d TERMINATING!!!",
                    qsize,
                )
                STATS.set("PANIC: IOCman qsize", qsize)
                break

            if qsize > max_queue_size:
                max_queue_size = qsize
                STATS.set("Max Queue Size: IOCman", max_queue_size)

            cur_time = time.time()
            if cur_time > next_ping_time:
                TM.ping(thread_id)
                next_ping_time += 30
                STATS.set("Queue Size: IOCman", qsize)

            try:
                serialized = self._queue.get(block=True, timeout=5)
                thing = pickle.loads(serialized)
            except queue.Empty:
                continue

            func = map.get(thing[1][0])
            func(thing[1][1])

    def handle_cmd_ping(self, data):
        log.debug("called")

    def handle_cmd_missing_beacons(self, data):
        log.debug("called")

    def queue_cmd(self, cmd, data, priority=PRIORITY.NORMAL):
        serialized = pickle.dumps((priority, (cmd, data)))
        self._queue.put_nowait(serialized)

    def ping(self, ping_count):

        self.queue_cmd(CMD.PING, ping_count)
        self.queue_cmd(CMD.MISSING_BEACONS, ping_count)

    def start(self):
        self._timer.start()

    def get_tooltip(self, ioc_id):
        """
        Get the IOC description from a cache so we are not always looking it up
        """
        try:
            self._lock.acquire()
            tooltip = self._tooltip_cache.get(ioc_id)
        finally:
            self._lock.release()

        if tooltip is not None:
            return tooltip

        description = self.get_description(ioc_id)
        if description is None:
            return "Add IOC description"

        tooltip = make_tooltip(description)

        try:
            self._lock.acquire()
            self._tooltip_cache[ioc_id] = tooltip
        finally:
            self._lock.release()

        return tooltip

    def get_description(self, ioc_id):
        """
        Get the IOC description from a cache so we are not always looking it up
        """
        try:
            self._lock.acquire()
            description = self._description_cache.get(ioc_id)
        finally:
            self._lock.release()

        if description is not None:
            return description

        cmd = "select description from iocs where id=%d" % ioc_id
        description = self.sql_select_one(cmd, "Add IOC Description...")

        try:
            self._lock.acquire()
            self._description_cache[ioc_id] = description
        finally:
            self._lock.release()

        return description

    def purge_description_cache(self, ioc_id):
        try:
            self._lock.acquire()
            del self._description_cache[ioc_id]
            del self._tooltip_cache[ioc_id]

        finally:
            self._lock.release()

    def update_ioc(
        self,
        ip_key,
        beacon_data=None,
        heartbeat_data=None,
        priority=PRIORITY.NORMAL,
    ):
        self.queue_cmd(
            CMD.PROCESS_IOC,
            (ip_key, beacon_data, heartbeat_data),
            priority=priority,
        )

    def sql_select(self, cmd):
        log.debug("SQL: %s", cmd)
        return self._sql.select(cmd)

    def sql_execute(self, cmd, data, rowcount=False):
        log.debug("SQL: %s VALUES: %r", cmd, data)
        return self._sql.execute(cmd, data, rowcount=rowcount)

    def get_ioc_description(self, ioc_id, default=""):
        cmd = "select description from iocs where id=%d" % ioc_id
        return self.sql_select_one(cmd, default)

    def submit_ioc_apps(self, ioc_id, data):
        log.debug("called, ioc_id: %d", ioc_id)

        critical = []
        have = []
        forget = []
        # First, loop through all the data submitted by the form and categorize
        for key, value in data.items():
            key = int(key)
            log.debug(
                "KEY: %r value %r",
                key,
                value,
            )

            # The "value" of each key is the "port-app_id" of the beacon
            parts = value.split("-")
            try:
                port = int(parts[0])
            except:
                port = None

            try:
                app_id = int(parts[1])
            except:
                app_id = None

            # Keys from 10000 to <20000 are critical (i.e. critical checkbox checked)
            # NOTE!!! Unchecked checkboxes are NOT included in the submitted data, so we
            # must detect them by their absence when compated to "all" the data
            if key >= 10000 and key < 20000:
                critical.append((app_id, port))

            elif key >= 20000 and key < 30000:
                have.append((app_id, port))

            elif key >= 30000:
                forget.append((app_id, port))
            else:
                raise ValueError("bad key: %r" % key)

        log.debug("critical: %r", critical)
        log.debug("have    : %r", have)
        log.debug("forget  : %r", forget)

        # Get apps that are currently in database
        app_dict = {}
        app_list = self.sql_select(
            "select id, state from apps where ioc_id=%d" % ioc_id
        )
        if app_list:
            for item in app_list:
                app_id = int(item[0])
                state = int(item[1])
                app_dict[app_id] = state

        # Get ports that are currently in database
        port_dict = {}
        have_ports = self.sql_select(
            "select id,port from expected_beacon_ports where ioc_id=%d" % ioc_id
        )
        if have_ports:
            for item in have_ports:
                port_id = int(item[0])
                port = int(item[1])
                port_dict[port] = port_id

        log.debug("app_dict: %r", app_dict)
        log.debug("port_dict: %r", app_dict)

        # For all critical beacons
        for item in critical:
            app_id = item[0]
            port = item[1]

            if app_id:
                # If this app ID is not already critical, make it critical
                current_state = app_dict.get(app_id)
                log.debug(
                    "current state: %r",
                    current_state,
                )
                if current_state != APP_STATE.CRITICAL:
                    cmd = "update apps set state=%d where id=%d" % (
                        APP_STATE.CRITICAL,
                        app_id,
                    )
                    self.sql_execute(cmd, None)
                    self.add_log(ioc_id, "App ID %d marked CRITICAL" % app_id)

                port_id = port_dict.get(port)
                if port_id:
                    # If this critical app_id has a port, ensure it is NOT in
                    # the critical port list
                    cmd = (
                        "delete from expected_beacon_ports where id=%d"
                        % port_id
                    )
                    self.sql_execute(cmd, None)

            elif port:
                # There is a port but no app id...
                port_id = port_dict.get(port)
                if not port_id:
                    # Add port to expected port list if is not there
                    values = (ioc_id, int(port), int(time.time()))
                    self.sql_execute(SQL_INSERT_EXPECTED_BEACON, values)
                    self.add_log(
                        ioc_id, "Beacon port %d marked CRITICAL" % int(port)
                    )

        # For all have beacons:
        for item in have:
            app_id = item[0]
            port = item[1]

            if app_id:
                log.debug("checking app_id: %s", app_id)

                found = False
                for thing in critical:
                    critical_app_id = thing[0]
                    if critical_app_id == app_id:
                        found = True
                        break

                if not found:
                    log.debug(
                        "did not find app_id %d in critical list",
                        app_id,
                    )

                    # App id is not in list of critical, set it to ignote
                    current_state = app_dict.get(app_id)
                    log.debug(
                        "current state: %r",
                        current_state,
                    )
                    if current_state != APP_STATE.NOT_CRITICAL:
                        cmd = "update apps set state=%d where id=%d" % (
                            APP_STATE.NOT_CRITICAL,
                            app_id,
                        )
                        self.sql_execute(cmd, None)
                        self.add_log(
                            ioc_id, "App ID %d marked NOT CRITICAL" % app_id
                        )

                    port_id = port_dict.get(port)
                    if port_id:
                        # Dont care about this port
                        cmd = (
                            "delete from expected_beacon_ports where id=%d"
                            % port_id
                        )
                        self.sql_execute(cmd, None)
                        self.add_log(
                            ioc_id,
                            "Beacon port %d marked NOT CRITICAL" % port_id,
                        )

                else:
                    # The app_id is in the critical list... do nothing
                    log.debug(
                        "no need to change app_id: %d",
                        app_id,
                    )

            elif port:
                found = False

                for thing in critical:
                    critical_port = thing[1]
                    if critical_port == port:
                        found = True
                        break

                if not found:
                    port_id = port_dict.get(port)
                    if port_id:
                        # Dont care about this port
                        cmd = (
                            "delete from expected_beacon_ports where id=%d"
                            % port_id
                        )
                        self.sql_execute(cmd, None)
                        self.add_log(
                            ioc_id,
                            "Beacon port %d marked NOT CRITICAL" % port_id,
                        )

                else:
                    pass

        for item in forget:
            app_id = item[0]
            port = item[1]
            if app_id:
                cmd = "update apps set state=%d where id=%d" % (
                    APP_STATE.IGNORE,
                    app_id,
                )
                self.sql_execute(cmd, None)
                self.add_log(ioc_id, "App ID %d forgotton" % app_id)

            if port:
                cmd = "delete from expected_beacon_ports where id=%d" % port_id
                self.sql_execute(cmd, None)
                self.add_log(
                    ioc_id, "Beacon port %d marked NOT CRITICAL" % port_id
                )

        # rebuild the data for this ioc
        self.queue_cmd(CMD.PROCESS_IOC, (ioc_id, None, None))

    def sql_select_one(self, cmd, default):
        return self._dataman._sql.select_one(cmd, default)

    def delete_logs(self, ioc_id, log_id_list):

        if len(log_id_list) == 0:
            return

        id_list = [str(log_id) for log_id in log_id_list]
        id_list = ",".join(id_list)
        cmd = "update logs set deleted=1 where id in (%s)" % id_list
        rows = self.sql_execute(cmd, None, rowcount=True)

        plural = "log" if rows == 1 else "logs"
        self.add_log(ioc_id, "%d %s deleted" % (rows, plural))

        if ioc_id is not None:
            self.update_ioc(ioc_id)

    def process_all_iocs(self):
        ioc_ids = self.get_id_list()
        for ioc_id in ioc_ids:
            self.update_ioc(ioc_id, priority=PRIORITY.LOW)
        return len(ioc_ids)

    def ack_all_logs(self):
        cmd = "update logs set has_ack=1 where has_ack=0 and needs_ack=1"
        row_count = self.sql_execute(cmd, None, rowcount=True)
        return row_count

    def ack_all_ioc_logs(self, ioc_id):
        """
        Do this is current thread so that it is updated for webpage
        """
        cmd = (
            "update logs set has_ack=1 where has_ack=0 and needs_ack=1 and "
            "ioc_id=%d" % ioc_id
        )
        rows = self.sql_execute(cmd, None, rowcount=True)
        if rows > 0:
            plural = "log" if rows == 1 else "logs"
            self.add_log(ioc_id, "%d %s acknowledged" % (rows, plural))

        self.update_ioc(ioc_id)

    def add_log(
        self, ioc_id, msg, needs_ack=False, background=True, app_id=None
    ):
        # Send this is the background because logs can also be added by the
        # beacon/heartbeat processing code which as to be fast fast fast

        if msg is None:
            return

        msg = msg.strip()
        if not msg:
            return

        state = 1 if needs_ack else 0

        if background:
            data = {
                KEY.IP_KEY: ioc_id,
                KEY.MESSAGE: msg,
                KEY.STATE: state,
                KEY.KIND: LOG_KIND.IOC,
                KEY.APP_ID: app_id,
            }
            self.send_to_database(DB_CMD.LOG_ADD, data)
        else:
            values = (
                LOG_KIND.IOC,
                ioc_id,
                msg,
                int(time.time()),
                0,
                state,
                app_id,
            )
            self.sql_execute(SQL_INSERT_LOG, values)
            self.update_ioc(ioc_id)

    def forget_ioc(self, ioc_id):
        """
        Perhaps in the future it would be better to just mark the IOC as deleted
        but for now an actual delete helps to test new IOC and app detection
        """
        log.debug("called; ioc_id: %d", ioc_id)

        cmd = "delete from iocs where id=%d" % ioc_id
        self._sql.execute(cmd, None)

        cmd = "delete from expected_beacon_ports where ioc_id=%d" % ioc_id
        self._sql.execute(cmd, None)

        cmd = "delete from group_memberships where ioc_id=%d" % ioc_id
        self._sql.execute(cmd, None)

        cmd = "delete from apps where ioc_id=%d" % ioc_id
        self._sql.execute(cmd, None)

        cmd = "delete from logs where ioc_id=%d" % ioc_id
        self._sql.execute(cmd, None)

        self._dataman.purge_beacons(ioc_id)
        self._dataman.purge_heartbeats(ioc_id)
        self._dataman.purge_netstat_data(ioc_id)

        self._lock.acquire()

        try:
            del self._ioc_cache[ioc_id]
        except:
            log.error(
                "Failed to delete ioc_id %d from cache",
                ioc_id,
            )

        try:
            del self._summary_data[ioc_id]
        except:
            log.error(
                "Failed to delete ioc_id %d from _summary_data",
                ioc_id,
            )

        self._lock.release()

        ## self.purge_ioc_cache()

    def add_ioc(self, ioc_id):
        log.debug("called; ioc_id: %d", ioc_id)
        try:
            now = int(time.time())
            values = (ioc_id, IOC_STATE.ACTIVE, "", now, now)
            self._sql.execute(SQL_INSERT_IOC, values)

            try:
                self._lock.acquire()
                self._ioc_cache[ioc_id] = {
                    KEY.DESCRIPTION: "Add Host Description"
                }
            except:
                log.error(
                    "Failed to add ioc_id %d to cache",
                    ioc_id,
                )
            finally:
                self._lock.release()

        except Exception as err:
            log.exception(
                "Exception adding IOC %r: %r",
                ioc_id,
                err,
            )

    def add_if_missing(self, ioc_id):
        result = False
        cmd = "select status from iocs where id=%d" % ioc_id
        status = self.sql_select_one(cmd, None)
        if status is None:
            self.add_ioc(ioc_id)
            result = True
        return result

    def log_beacon_change(self, beacon_data):
        if beacon_data is None:
            return
        ioc_id = beacon_data[0]
        port = beacon_data[1]
        kind = beacon_data[2]

        beacon_data = self._dataman.get_beacon(ioc_id, port)
        log.debug(
            "THIS IS THE BEACON DATA: %r",
            beacon_data,
        )

        if kind == KIND.LOST and beacon_data:
            cmd = beacon_data.get(KEY.CMD, "Unknown")
            cwd = beacon_data.get(KEY.CWD, "Unknown")
            msg = "Beacon %s; port: %d CMD: %s CWD: %s" % (kind, port, cmd, cwd)
        else:
            # If the beacon is resumed or new we do not want to assign the
            # cmd and cwd.... it could have changed ports!  We could do
            # something like we do the the events ad update the log later
            # but that us tricky to get right... lets leave it for now
            msg = "Beacon %s; port: %d" % (kind, port)

        self.add_log(ioc_id, msg, needs_ack=True)

    def get_beacon_uptime_sec(self, beacon):
        if beacon is None:
            return None

        seq = beacon.get(KEY.SEQ)
        return beacon_est_uptime_sec(seq)

    def handle_cmd_update_ioc(self, data):
        """
        This spagetti logic is required to deal with:
        - beacons without app ids that may change ports
        - beacons with app ids that also may change ports
        - beacons that have not yet been identified
        - imported legacy data about critical ports
        """
        red_count = 0
        not_critical_count = 0
        flash_flag = False
        color = None
        color2 = None
        cur_time = time.time()

        if isinstance(data, int):
            log.info("FIXME ----- called with an INT")
            ioc_id = data
            beacon_data = None
            ### heartbeat_data = None
        else:
            ioc_id = data[0]
            beacon_data = data[1]
            ### heartbeat_data = data[2]

        log.debug(
            "called; ioc_id: %s (%d)",
            get_host_name(ioc_id),
            ioc_id,
        )

        if beacon_data:
            self.log_beacon_change(beacon_data)

        if self.add_if_missing(ioc_id):
            log.debug(
                "Added IOC: %s (%d)",
                get_host_name(ioc_id),
                ioc_id,
            )

        tooltip = self.get_tooltip(ioc_id)

        # Get the options for this IOC
        ioc_options = self.get_options(ioc_id)
        for item in IOC_OPTION_LIST:
            option = item[0]
            have = True if ioc_options.get(option) else False
            log.debug(
                "OPTION: %s %s",
                IOC_OPTION.name(option),
                have,
            )

        # Get the app_ids from the database (critical and otherwise)
        # What actually adds the apps to the database?
        app_list = self._sql.select(
            "select id, state from apps where ioc_id=%d" % ioc_id
        )

        # Make a dict of app_ids and critical status
        expected_critical_app_count = 0

        app_id_map = {}
        if app_list is not None:
            for item in app_list:
                app_id = int(item[0])
                state = int(item[1])

                app_id_map[app_id] = state
                if state == APP_STATE.CRITICAL:
                    expected_critical_app_count += 1

        # Get the expected ports from the database
        expect_ports = self._sql.select(
            "select port from expected_beacon_ports where ioc_id=%d" % ioc_id
        )
        if expect_ports is None:
            expect_ports = set([])
        else:
            expect_ports = set([item[0] for item in expect_ports])

        # The expected number of beacons is equal to the expected number
        # of ports and the expected number of critical app_ids.  These are
        # supposed to be exclusive

        expected_app_count = len(expect_ports) + expected_critical_app_count

        # Get the actual beacons that we see
        beacons = self._dataman.get_beacons(ioc_id=ioc_id, as_dict=True)
        have_ports = [port for port in beacons.keys()]

        # Make a dict of all running app_ids
        have_app_ids = {}
        for port, beacon in beacons.items():
            app_id = beacon.get(KEY.APP_ID)
            if app_id is not None:
                have_app_ids[app_id] = port

        log.debug(
            "THESE ARE THE EXPECTED PORTS: %r",
            expect_ports,
        )
        log.debug(
            "THESE ARE THE ACTUAL PORTS: %r",
            have_ports,
        )

        host_name = get_host_name(ioc_id, purge=True)
        host_name_sort = make_host_sort_name(host_name)

        # result 0: port
        # result 1: app_id
        # result 2: critical
        # result 3: running
        # result 4: color
        # result 5: uptime_sec
        # result 6: last_time_sec
        # result 7: measure_time

        result = []
        app_ids_assigned = []

        log.debug("expect ports: %r", expect_ports)
        for port in expect_ports:
            uptime_sec = None
            last_time_sec = None

            if port in have_ports:

                beacon = beacons.get(port)

                uptime_sec = self.get_beacon_uptime_sec(beacon)

                app_id = beacon.get(KEY.APP_ID)

                if app_id is not None:
                    # We have the beacon for this port AND it is mapped to an app
                    app_state = app_id_map.get(app_id)
                    if app_id in app_id_map:

                        cmd = (
                            "delete from expected_beacon_ports where ioc_id=%d"
                            " and port=%d" % (ioc_id, port)
                        )
                        self._sql.execute(cmd, None)
                        expected_app_count -= 1

                        if app_state != APP_STATE.CRITICAL:
                            # Since new apps are automatically marked as critical I
                            # no longer expect this to happen
                            # This app is not currently marked as critical... mark it so
                            cmd = "update apps set state=%d where id=%d" % (
                                APP_STATE.CRITICAL,
                                app_id,
                            )
                            self._sql.execute(cmd, None)
                            expected_app_count += 1

                        result.append(
                            (
                                port,
                                app_id,
                                True,
                                True,
                                COLOR.GREEN,
                                uptime_sec,
                                last_time_sec,
                                cur_time,
                            )
                        )
                        app_ids_assigned.append(app_id)
                else:
                    result.append(
                        (
                            port,
                            None,
                            True,
                            True,
                            COLOR.GREEN,
                            uptime_sec,
                            last_time_sec,
                            cur_time,
                        )
                    )

            else:
                beacon = beacons.get(port)

                red_count += 1
                result.append(
                    (
                        port,
                        None,
                        True,
                        False,
                        COLOR.RED,
                        uptime_sec,
                        last_time_sec,
                        cur_time,
                    )
                )

        ports_with_apps = []

        # Loop through all the app_ids on this IOC
        for app_id, state in app_id_map.items():
            uptime_sec = None
            last_time_sec = None

            port = have_app_ids.get(app_id)

            if state != APP_STATE.CRITICAL:
                not_critical_count += 1

            if app_id in app_ids_assigned:
                if port:
                    ports_with_apps.append(port)
                continue

            running = False

            if port:
                # Is this app on one of the beacon ports?
                running = True
                ports_with_apps.append(port)

            heartbeat = self._dataman.get_heartbeat(app_id=app_id)
            if heartbeat:
                hb_data = heartbeat.get(KEY.DATA)
                uptime_sec = hb_data.get(KEY.UP)
                last_time_sec = heartbeat.get(KEY.TIME)

                if not heartbeat.get(HEARTBEAT_KEY.LOST, False):
                    running = True
            if running:
                if state == APP_STATE.CRITICAL:
                    result.append(
                        (
                            port,
                            app_id,
                            True,
                            True,
                            COLOR.GREEN,
                            uptime_sec,
                            last_time_sec,
                            cur_time,
                        )
                    )
                else:
                    result.append(
                        (
                            port,
                            app_id,
                            False,
                            True,
                            None,
                            uptime_sec,
                            last_time_sec,
                            cur_time,
                        )
                    )
            else:

                # This app is not running
                if state == APP_STATE.CRITICAL:
                    result.append(
                        (
                            None,
                            app_id,
                            True,
                            False,
                            COLOR.RED,
                            uptime_sec,
                            last_time_sec,
                            cur_time,
                        )
                    )
                    red_count += 1

                elif state == APP_STATE.NOT_CRITICAL:
                    result.append(
                        (
                            None,
                            app_id,
                            False,
                            False,
                            None,
                            uptime_sec,
                            last_time_sec,
                            cur_time,
                        )
                    )

                elif state == APP_STATE.IGNORE:
                    log.debug(
                        "dont show ignored app %d",
                        app_id,
                    )
                else:
                    raise ValueError("Invalid state: %r" % state)

        # Now loop through all beacon ports that we see
        for port in have_ports:

            uptime_sec = None
            last_time_sec = None

            if port in ports_with_apps:
                continue

            if port in expect_ports:
                continue

            # This must be an application that is unknown
            result.append(
                (
                    port,
                    None,
                    False,
                    True,
                    None,
                    uptime_sec,
                    last_time_sec,
                    cur_time,
                )
            )

        # Set the overall color
        if ioc_options.get(IOC_OPTION.DEVELOPMENT):
            color = COLOR.GREEN
            color2 = COLOR.BLUE

        elif ioc_options.get(IOC_OPTION.MAINTENANCE):
            color = COLOR.GREEN
            color2 = COLOR.ORANGE

        else:
            if red_count > 0:
                color = COLOR.RED
            else:
                color = COLOR.GREEN

        if color == COLOR.GREEN:
            if not_critical_count > 0:
                color = COLOR.DARK_GREEN

        cmd = (
            "select count(id) from logs where ioc_id=%d and needs_ack=1 and "
            "has_ack=0 and deleted=0 limit %d"
            % (ioc_id, SETTINGS.MAX_LOGS_IOC_PAGE)
        )

        sql_result = self.sql_select(cmd)

        if sql_result:
            log_count = int(sql_result[0][0])
            if log_count:
                log.debug(
                    "iocid: %d: %d logs need ack",
                    ioc_id,
                    log_count,
                )
                # There are logs that require acknowlegment
                flash_flag = True

        if color2 is None:
            color2 = color

        try:
            self._lock.acquire()
            data = self._summary_data.get(ioc_id, {})

            # Update the hostname
            data[KEY.NAME] = host_name
            data[KEY.IP_ADDR] = get_ip_addr(ioc_id)
            data[KEY.SORT_NAME] = host_name_sort
            data[KEY.COLOR] = color
            data[KEY.COLOR2] = color2
            data[KEY.BEACONS] = result
            data[KEY.TOOLTIP] = tooltip
            data[KEY.FLASH_FLAG] = flash_flag

            # breem:2021_05_13:  Perhaps show the actual number of detected
            # apps.  Otherwise there could be unidentified beacons running
            # but the IOC screen says 0

            app_count = max(len(result), expected_app_count)
            data[KEY.APP_COUNT] = app_count
            data[KEY.SUMMARY_NAME] = "%s (%d)" % (host_name, app_count)
            self._summary_data[ioc_id] = data

        finally:
            self._lock.release()

        log.debug(
            "done=================================================",
        )

    def send_to_database(self, cmd, data):
        self._dataman.send_to_database(cmd, data)

    def get_beacons_display(self, ip_key):
        log.debug("ip_key: %d", ip_key)

        # TEST: is ip possible to update the IOC data here?
        self.handle_cmd_update_ioc((ip_key, None, None))

        try:
            self._lock.acquire()
            data = self._summary_data.get(ip_key, {})
            return data.get(KEY.BEACONS, [])
        finally:
            self._lock.release()

    def get_epics_events(self, timestamp, offset=0, count=10):

        t = PVMonitorTime()
        start_of_day = t.get_day_start(t=timestamp)
        end_of_day = t.get_day_end(t=timestamp)

        cmd = (
            "select id, type, port, ip_addr, host_name, "
            + "app_name, app_cmd, app_cwd, create_time, status, seq "
            + "from events where create_time >= %d and " % start_of_day
            + "create_time <= %d order by id desc limit %d offset %d"
            % (end_of_day, count, offset)
        )

        sql_result = self.sql_select(cmd)
        if sql_result is None:
            return []

        return sql_result

    def get_epics_event_count(self, timestamp):
        """
        Get the epics event count for a particular day
        """
        count = 0
        t = PVMonitorTime()
        start_of_day = t.get_day_start(t=timestamp)
        end_of_day = t.get_day_end(t=timestamp)

        cmd = (
            "select count(id) "
            + "from events where create_time >= %d and " % start_of_day
            + "create_time < %d" % end_of_day
        )

        log.debug("SQL: %s", cmd)

        try:
            result = self._sql.select(cmd)

            if result is None:
                count = 0
            else:
                count = result[0][0]

        except Exception as err:
            log.exception("Exception: %r", err)

        return count

    def get_ioc_log_count(self, ioc_id):

        if ioc_id is None:
            where_clause = "kind=%d and deleted=0" % LOG_KIND.IOC
        else:
            where_clause = "ioc_id=%d and kind=%d and deleted=0" % (
                ioc_id,
                LOG_KIND.IOC,
            )

        cmd = "select count(id) from logs " + "where %s " % (where_clause)

        sql_result = self.sql_select(cmd)
        if sql_result is None:
            return 0

        return sql_result[0][0]

    def get_ioc_logs(self, ioc_id, offset=0, count=100):

        if ioc_id is None:
            where_clause = "kind=%d and deleted=0" % LOG_KIND.IOC
        else:
            where_clause = "ioc_id=%d and kind=%d and deleted=0" % (
                ioc_id,
                LOG_KIND.IOC,
            )

        cmd = (
            "select id, ioc_id, create_time, needs_ack, has_ack, msg from logs "
            + "where %s " % (where_clause)
            + "order by id desc limit %d offset %d" % (count, offset)
        )

        sql_result = self.sql_select(cmd)
        if sql_result is None:
            return []

        result = []
        for log in sql_result:
            log_id = log[0]
            ioc_id = log[1]
            create_time = log[2]
            needs_ack = log[3]
            has_ack = log[4]
            msg = log[5]

            if needs_ack:
                if has_ack:
                    ack_str = "YES"
                    color = None
                else:
                    ack_str = "NO"
                    color = COLOR.YELLOW
            else:
                ack_str = ""
                color = None

            # This "color" and string formatting stuff really belongs in
            # the GUI not in this manager

            time_str = self._mytime.get_string_time_of_day(create_time)
            date_str = self._mytime.get_string_MDY(create_time)
            result.append(
                (log_id, ioc_id, time_str, date_str, ack_str, color, msg)
            )
        return result

    def load(self):
        log.debug("called")

        # Get all the iocs from the database and update them.  This will cause
        # the summary table and cache to get populated
        ioc_ids = self.get_id_list(load=True)

        for ioc_id in ioc_ids:
            self.update_ioc(ioc_id)

    def get_ioc_summary(self, group_id):

        log.debug("called; group_key: %d", group_id)

        ioc_list = self._dataman.get_memberships_by_group(group_id)

        try:
            result = []
            self._lock.acquire()
            for ioc_id in ioc_list:
                value = self._summary_data.get(ioc_id)
                if value:
                    result.append(
                        (
                            value.get(KEY.SORT_NAME),
                            ioc_id,
                            value.get(KEY.SUMMARY_NAME),
                            value.get(KEY.TOOLTIP),
                            value.get(KEY.NAME),
                            value.get(KEY.IP_ADDR),
                            value.get(KEY.COLOR),
                            value.get(KEY.COLOR2),
                            value.get(KEY.FLASH_FLAG),
                        )
                    )
                else:
                    # It is possible that we have an IOC with no summary
                    # information.
                    host_name = get_host_name(ioc_id)
                    host_name_sort = make_host_sort_name(host_name)
                    host_name_summary = "%s (0)" % host_name
                    result.append(
                        (
                            host_name_sort,
                            ioc_id,
                            host_name_summary,
                            "Add IOC Description",
                            host_name,
                            get_ip_addr(ioc_id),
                            COLOR.YELLOW,
                            COLOR.YELLOW,
                            False,
                        )
                    )

            result = copy.deepcopy(result)
        finally:
            self._lock.release()

        result.sort()
        ## print result
        return result

    def get_app_times(self, ioc_id, port, app_id):
        """
        Get the app times
        """
        log.debug(
            "called; ioc_id: %r port: %r app_id: %r",
            ioc_id,
            port,
            app_id,
        )

        if app_id:
            # The first thing to do is see if there is an active heartbeat
            # for this application
            heartbeat = self._dataman.get_heartbeat(app_id=app_id)
            if heartbeat:
                last_time = int(heartbeat.get(KEY.TIME))
                hb_data = heartbeat.get(KEY.DATA)
                uptime = int(hb_data.get(KEY.UP))
                return uptime, last_time

            else:
                # Its possible this app was running but now is stopped. Check the database.
                sql_cmd = (
                    "select time, uptime from heartbeats where app_id=%d"
                    % app_id
                )
                result = self._sql.select(sql_cmd, None)
                if len(result) == 1:
                    result = result[0]
                    last_time = result[0]
                    uptime = result[1]
                    return uptime, last_time,

                if len(result) > 1:
                    log.error(
                        "GOT UNEXPECTED RESULT: %r",
                        result,
                    )
                    return

                beacon_data = self._dataman.get_beacon(ioc_id, port)
                if beacon_data:
                    seq = beacon_data.get(KEY.SEQ)
                    last_time_seen = beacon_data.get(KEY.TIME)
                    interval_ms = beacon_data.get(KEY.INTERVAL_MS, 15000)
                    up_time = int(float(seq * interval_ms) / 1000.0)
                    return up_time, last_time_seen

        # If we made it to here, check beacons via port and ioc_id
        if port is None or ioc_id is None:
            return None, None

        beacon_data = self._dataman.get_beacon(ioc_id, port)

        if beacon_data is None:
            return None, None

        seq = beacon_data.get(KEY.SEQ)
        last_time_seen = beacon_data.get(KEY.TIME)
        interval_ms = beacon_data.get(KEY.INTERVAL_MS, 15000)

        up_time = int(float(seq * interval_ms) / 1000.0)
        return up_time, last_time_seen

    def get_app_dict(self, ioc_id):
        result = {}
        cmd = "select id,cmd,cwd,name from apps where ioc_id=%d" % ioc_id
        sql_result = self.sql_select(cmd)

        if sql_result is not None:
            for item in sql_result:
                result[item[0]] = {
                    KEY.CMD: item[1],
                    KEY.CWD: item[2],
                    KEY.NAME: item[3],
                }

        return result

    def set_option(self, ioc_id, option_id, value, locked=False):

        have_lock = False

        try:
            if not locked:
                self._lock.acquire()
                have_lock = True

            ioc_data = self._ioc_cache.get(ioc_id)
            if ioc_data is None:
                raise ValueError(
                    "Cound not find ioc_data for ioc_id: %d" % ioc_id
                )

            options = ioc_data.get(KEY.OPTIONS, {})
            options[option_id] = value
            ioc_data[KEY.OPTIONS] = options

        finally:
            if have_lock:
                self._lock.release()

    def get_id_list(self, load=False):

        log.debug("called; load: %r", load)

        if not load:
            try:
                self._lock.acquire()
                if self._ioc_cache is not None:
                    return [k for k in self._ioc_cache.keys()]

            finally:
                self._lock.release()

        # At this point we need to rebuild the cache
        cmd = (
            "select id,description,app_count,option_new_apps,option_app_count, "
            "option_maint,option_devel,option_hide,option_decom,status from iocs"
        )

        sql_result = self.sql_select(cmd)

        try:
            self._lock.acquire()

            if sql_result is None:
                self._ioc_cache = {}
                return []
            else:
                self._ioc_cache = {}
                for item in sql_result:

                    ioc_id = int(item[0])
                    description = item[1]
                    app_count = int(item[2])
                    option_new_apps = int(item[3])
                    option_app_count = int(item[4])
                    option_maint = int(item[5])
                    option_devel = int(item[6])
                    option_hide = int(item[7])
                    option_decom = int(item[8])
                    ### status = int(item[9])

                    log.debug(
                        "Loading ioc_id: %d (%s)", ioc_id, get_host_name(ioc_id)
                    )

                    ## ignore_flag = int(item[3])

                    self._ioc_cache[ioc_id] = {
                        KEY.DESCRIPTION: description,
                        KEY.APP_COUNT: app_count,
                        KEY.OPTIONS: {
                            IOC_OPTION.MAINTENANCE: option_maint,
                            IOC_OPTION.MONITOR_APP_COUNT: option_app_count,
                            IOC_OPTION.DEVELOPMENT: option_devel,
                            IOC_OPTION.NEW_APPS_CRITICAL: option_new_apps,
                            IOC_OPTION.HIDE_FROM_ACTIVE: option_hide,
                            IOC_OPTION.DECOMISSIONED: option_decom,
                        },
                    }

                id_list = iter(self._ioc_cache.keys())
                return id_list

        finally:
            self._lock.release()

    def set_options(self, ioc_id, option_list):
        log.debug(
            "called; ioc_id: %d option_list: %r",
            ioc_id,
            option_list,
        )

        settable_options = [
            (IOC_OPTION.HIDE_FROM_ACTIVE, "option_hide"),
            (IOC_OPTION.DEVELOPMENT, "option_devel"),
            (IOC_OPTION.MAINTENANCE, "option_maint"),
            (IOC_OPTION.NEW_APPS_CRITICAL, "option_new_apps"),
            (IOC_OPTION.MONITOR_APP_COUNT, "option_app_count"),
        ]

        try:
            self._lock.acquire()
            for item in settable_options:
                option_id = item[0]
                column = item[1]

                value = 1 if option_id in option_list else 0

                cmd = "update iocs set %s=%d where id=%d" % (
                    column,
                    value,
                    ioc_id,
                )
                log.debug("CMD: %s", cmd)
                self.sql_execute(cmd, None)
                self.set_option(ioc_id, option_id, value, locked=True)

            self.update_ioc(ioc_id)
        finally:
            self._lock.release()

    def get_options(self, ioc_id):
        log.debug("called; ioc_id: %d", ioc_id)

        try:
            self._lock.acquire()
            ioc_data = self._ioc_cache.get(ioc_id, {})
            options = ioc_data.get(KEY.OPTIONS, {})
            return copy.deepcopy(options)

        finally:
            self._lock.release()

    def set_description(self, ioc_id, description):

        data = (description, ioc_id)
        cmd = "update iocs set description=%s where id=%s"
        self.sql_execute(cmd, data)
        self.purge_description_cache(ioc_id)
        self.update_ioc(ioc_id)

    def get_state_dict(self):

        result = {}
        try:
            self._lock.acquire()

            for k, v in self._iocs.items():
                result[k] = {KEY.STATE: v.get(KEY.STATE, IOC_STATE)}

        finally:
            self._lock.release()
        return result

    def update_beacon(self, ioc_id, port, kind):
        log.debug(
            "called; ioc_id: %d port: %d kind: %s",
            ioc_id,
            port,
            kind,
        )

        if kind != KIND.DETECTED:
            # Only add logs for beacons that are LOST/NEW/RESUMED/RESTARTED
            beacon_data = (ioc_id, port, kind)
        else:
            beacon_data = None
        self.update_ioc(ioc_id, beacon_data=beacon_data)

    def add_ioc_warning(self, msg, value):

        if msg is None:
            return

        warnings = value.get(KEY.WARNINGS, [])

        add = True

        if len(warnings) > 0:
            most_recent_warning = warnings[-1]
            if most_recent_warning.get(KEY.MESSAGE) == msg:
                most_recent_warning[KEY.TIME] = time.time()
                add = False

        if add:
            warning = {
                KEY.ID: self.get_next_message_id(),
                KEY.TIME: time.time(),
                KEY.MESSAGE: msg,
            }
            warnings.append(warning)

        return warnings[-200:]

    def get_ioc_data(self, ip_key, ioc_data=None, skip_return=False):
        """
        Update the warnings and errors for the IOC
        """
        log.debug("called; key: %r", ip_key)

        if not isinstance(ip_key, int):
            raise ValueError("BAD KEY!!!!")

        try:
            have_lock = False
            if not self._lock.locked():
                self._lock.acquire()
                have_lock = True

            if ioc_data is None:
                ioc_data = self._iocs.get(ip_key)
                if not ioc_data:
                    log.error(
                        "ERROR, no IOC for key: %r",
                        ip_key,
                    )
                    return

            count = 0
            active_tcp_ports = ioc_data.get(KEY.ACTIVE_TCP_PORTS, [])

            if ioc_data.get(KEY.APP_COUNT) is None:
                ioc_data[KEY.APP_COUNT] = 1
                count += 1

            port_count = len(active_tcp_ports)
            app_count = int(ioc_data.get(KEY.APP_COUNT))

            if port_count == 0 and app_count > 0:
                # This is an error... the IOC could be down if there are
                # no beacons
                msg = "No Beacons detected; expect %d" % app_count

                warnings = self.add_ioc_warning(msg, ioc_data)
                if warnings:
                    ioc_data[KEY.WARNINGS] = warnings
                    count += 1

            elif port_count != app_count:
                msg = "Detected Beacons on %d TCP port(s); expect %d" % (
                    port_count,
                    app_count,
                )

                warnings = self.add_ioc_warning(msg, ioc_data)
                if warnings:
                    ioc_data[KEY.WARNINGS] = warnings
                    count += 1

            else:
                pass

            # MAB: 2018-12-19: skip this error purge by setting
            skip_purge = True

            if port_count > 0 and not skip_purge:
                # currently, the only type of error is "no beacons detected".
                # Auto-Purge this error if there is one or more beacon"
                ioc_data[KEY.ERRORS] = []
                count = 1

            self._iocs[ip_key] = ioc_data

            if not skip_return:
                return copy.deepcopy(ioc_data)

        finally:
            if have_lock:
                self._lock.release()

    def get_next_message_id(self):

        try:
            self._msg_id_lock.acquire()
            self._msg_id += 1
            return self._msg_id

        finally:
            self._msg_id_lock.release()


def get_example_data():

    data = {}
    f = open("data/example_submit.txt")
    for line in f:
        parts = line.split()

        key = parts[6].strip("'")
        value = parts[8].strip("'")

        log.debug("%s, %s", key, value)
        data[int(key)] = value
    f.close()

    return data


def test1():

    class MockDataman(object):
        def __init__(self):
            self._sql = None

    data = get_example_data()
    iocman = IOCManager(MockDataman())
    iocman.submit_ioc_apps(1000, data)


if __name__ == "__main__":

    test1()
