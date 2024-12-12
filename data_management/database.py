# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------

import datetime
import logging
import os
import queue
import threading
import time
import traceback

import mysql.connector
import psutil

from definitions import APP_STATE, EVENT, KEY, config
from password import DB_PASSWORD
from utilities.thread_monitor import ThreadMonitor
from utilities.utils import (get_host_name_for_db, get_ip_addr, make_app_id,
                             remove_leading_space)

log = logging.getLogger("PVMLogger")
TM = ThreadMonitor()


class CMD(object):
    PING = "cmd:ping"
    APP_UPDATE = "cmd:app_update"
    BEACON_APP_CLEAR_PORTS = "cmd:beacon_app:clear_ports"
    BEACON_NEW = "cmd:beacon_new"
    BEACON_LOST = "cmd:beacon_lost"
    BEACON_DETECTED = "cmd:beacon_detected"
    BEACON_RESTART = "cmd:beacon_restarted"
    BEACON_RESUMED = "cmd:beacon_resumed"
    BEACON_UPDATE = "cmd:beacon_update"
    LOG_ADD = "cmd:log_ioc"
    UPDATE_IOC = "cmd:update_ioc"
    HEARTBEAT_UPDATE = "cmd:update_heartbeat"


class EVENT_STATUS(object):

    UNIDENTIFIED = 101
    IDENTIFIED = 102


SQL_INSERT_APP = """
    INSERT INTO apps (
        id, cmd, cwd, ioc_id, ip_addr, state, create_time, update_time
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

SQL_INSERT_EVENT = """
    INSERT INTO events (
        type, ioc_id, ip_addr, port, seq, host_name, status, 
        create_time, create_date, create_hms, update_time
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

SQL_SELECT_EVENTS = """
    SELECT * 
    FROM events 
    WHERE create_time > %s 
    ORDER BY id ASC;
"""

SQL_INSERT_LOG = """
    INSERT INTO logs (
        kind, ioc_id, msg, create_time, status, needs_ack, app_id
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s);
"""


class MyConnector(object):

    def __init__(self, db_name=None):

        self._conn = None
        self._db_host = config.DB_SERVER
        self._db_name = db_name
        self._db_user = config.DB_USER
        self._db_password = DB_PASSWORD
        self._lock = threading.Lock()


    def set_db_name(self, name):
        self._db_name = name

    def database_exists(self, cursor, db_name):
        cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
        return cursor.fetchone() is not None
    
    def create_database_if_not_exists(self):
        # Connect to MySQL without specifying a database
        temp_conn = mysql.connector.connect(
            host=self._db_host,
            user=self._db_user,
            password=self._db_password,
            autocommit=True
        )
        temp_cursor = temp_conn.cursor()

        if not self.database_exists(temp_cursor, self._db_name):
            log.debug(f"Database '{self._db_name }' does not exist. Creating database...")
            temp_cursor.execute(f"CREATE DATABASE {self._db_name }")
            log.debug(f"Database '{self._db_name }' created successfully.")
        else:
            log.debug(f"Database '{self._db_name }' already exists.")

        # Close temporary connection
        temp_cursor.close()
        temp_conn.close()

    def table_exists(self, cursor, table_name):
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return cursor.fetchone() is not None

    def connect(self):

        log.debug("called")

        if self._conn is not None:
            if self._conn.is_connected():
                log.debug("Already connected")
                return
        
        # Create database if not exits
        self.create_database_if_not_exists()

        log.debug(
            "Connecting to MySQL database: %s host: %s user: %s",
            self._db_name,
            self._db_host,
            self._db_user,
        )

        # NOTE: I could NOT get connections in seperate threads to see up-to-date
        # data until I added the "autocommit=True" param.... even when I called
        # commint on the connection object after database updates
        try:
            self._conn = mysql.connector.connect(
                host=self._db_host,
                database=self._db_name,
                user=self._db_user,
                password=self._db_password,
                autocommit=True,
            )

            if self._conn.is_connected():
                log.debug(
                    "Connected to MySQL database %s",
                    self._db_name,
                    
                )
            
            temp_cursor = self._conn.cursor()
            
            tables = {
            "heartbeats": """
                CREATE TABLE heartbeats(
                    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    time            BIGINT UNSIGNED,
                    uptime          BIGINT UNSIGNED,
                    app_id          BIGINT UNSIGNED,
                    ioc_id          INT UNSIGNED,
                    port            INT UNSIGNED,
                    PRIMARY KEY     (id)
                );
            """,
            "beacons": """
                CREATE TABLE beacons(
                    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    time            BIGINT UNSIGNED,
                    app_id          BIGINT UNSIGNED,
                    ioc_id          INT UNSIGNED,
                    port            INT UNSIGNED,
                    seq             INT UNSIGNED,
                    interval_ms     INT UNSIGNED,
                    PRIMARY KEY     (id)
                );
            """,
            "apps": """
                CREATE TABLE apps(
                    id              BIGINT UNSIGNED,
                    cmd             TEXT,
                    cwd             TEXT,
                    name            TEXT,
                    ioc_id          INT UNSIGNED,
                    ip_addr         VARCHAR(64),
                    create_time     BIGINT UNSIGNED,
                    update_time     BIGINT UNSIGNED,
                    state           INT UNSIGNED,
                    PRIMARY KEY     (id)
                );
            """,
            "iocs": """
                CREATE TABLE iocs(
                    id              BIGINT UNSIGNED,
                    status          INT UNSIGNED,
                    app_count       INT UNSIGNED,
                    ignore_flag     INT UNSIGNED,
                    description     TEXT,
                    create_time     BIGINT UNSIGNED,
                    update_time     BIGINT UNSIGNED,
                    PRIMARY KEY     (id)
                );
            """,
            "system": """
                CREATE TABLE system (
                    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    init_options    INT UNSIGNED NOT NULL DEFAULT 0,
                    PRIMARY KEY     (id)
                );
            """,
            "expected_beacon_ports": """
                CREATE TABLE expected_beacon_ports(
                    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    ioc_id          BIGINT UNSIGNED,
                    port            BIGINT UNSIGNED,
                    create_time     BIGINT UNSIGNED,
                    PRIMARY KEY     (id)
                );
            """,
            "expected_app_ids": """
                CREATE TABLE expected_app_ids(
                    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    ioc_id          BIGINT UNSIGNED,
                    app_id          BIGINT UNSIGNED,
                    create_time     BIGINT UNSIGNED,
                    PRIMARY KEY     (id)
                );
            """,
            "events": """
                CREATE TABLE events(
                    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    type            INT UNSIGNED,
                    port            INT UNSIGNED,
                    ioc_id          INT UNSIGNED,
                    seq             INT UNSIGNED,
                    ip_addr         VARCHAR(64),
                    host_name       TEXT,
                    description     TEXT,
                    app_name        TEXT,
                    app_cmd         TEXT,
                    app_cwd         TEXT,
                    app_id          BIGINT UNSIGNED,
                    create_time     BIGINT UNSIGNED,
                    update_time     BIGINT UNSIGNED,
                    create_date     INT UNSIGNED,
                    create_hms      INT UNSIGNED,
                    status          INT UNSIGNED,
                    PRIMARY KEY     (id)
                );
            """,
            "host_names": """
                CREATE TABLE host_names(
                    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    name            VARCHAR(256),
                    create_time     BIGINT UNSIGNED,
                    update_time     BIGINT UNSIGNED,
                    status          INT UNSIGNED,
                    PRIMARY KEY (id)
                );
            """,
            "groups": """
                CREATE TABLE groups(
                    id              BIGINT UNSIGNED NOT NULL,
                    name            VARCHAR(256),
                    create_time     BIGINT UNSIGNED,
                    update_time     BIGINT UNSIGNED,
                    status          INT UNSIGNED,
                    PRIMARY KEY (id)
                );
            """,
            "group_memberships": """
                CREATE TABLE group_memberships(
                    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    group_id        BIGINT UNSIGNED,
                    ioc_id          BIGINT UNSIGNED,
                    create_time     BIGINT UNSIGNED,
                    update_time     BIGINT UNSIGNED,
                    status          INT UNSIGNED,
                    PRIMARY KEY (id)
                );
            """,
            "logs": """
                CREATE TABLE logs(
                    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    ioc_id          BIGINT UNSIGNED,
                    app_id          BIGINT UNSIGNED,
                    create_time     BIGINT UNSIGNED,
                    kind            INT UNSIGNED,
                    status          INT UNSIGNED,
                    deleted         INT UNSIGNED DEFAULT 0,
                    needs_ack       INT UNSIGNED DEFAULT 0,
                    has_ack         INT UNSIGNED DEFAULT 0,
                    msg             TEXT,
                    PRIMARY KEY (id)
                );
            """
        }
            
            alter_iocs_table = [
            "ALTER TABLE iocs ADD COLUMN option_hide int unsigned not null default 0 AFTER ignore_flag;",
            "ALTER TABLE iocs ADD COLUMN option_devel int unsigned not null default 0 AFTER ignore_flag;",
            "ALTER TABLE iocs ADD COLUMN option_maint int unsigned not null default 0 AFTER ignore_flag;",
            "ALTER TABLE iocs ADD COLUMN option_app_count int unsigned not null default 0 AFTER ignore_flag;",
            "ALTER TABLE iocs ADD COLUMN option_new_apps int unsigned not null default 0 AFTER ignore_flag;",
            "ALTER TABLE iocs DROP COLUMN ignore_flag;",
            "ALTER TABLE iocs ADD COLUMN option_decom int unsigned not null default 0 AFTER option_maint;",
            "ALTER TABLE iocs DROP COLUMN app_count;",
            "ALTER TABLE iocs ADD COLUMN app_count int unsigned not null default 0 AFTER option_hide;"
        ]
            for table_name, create_sql in tables.items():
                if not self.table_exists(temp_cursor, table_name):
                    log.debug(f"Creating table {table_name}...")
                    temp_cursor.execute(create_sql)
                else:
                    log.debug(f"Table {table_name} already exists, skipping.")
                    
            for sql in alter_iocs_table:
                try:
                    temp_cursor.execute(sql)
                    log.debug(f"Executed: {sql}")
                except mysql.connector.Error as err:
                    log.debug(f"Error executing: {sql}\n{err}")
            
            temp_cursor.close()

        except Exception as err:
            log.exception(
                "Exception connecting to MySQL database %s: %r",
                self._db_name,
                err,
            )

    def get_cursor(self):
        log.debug("Called")

        if self._conn is None or not self._conn.is_connected():
            self.connect()

        return self._conn.cursor(buffered=False)

    def execute(self, cmd, data, catch=True, rowcount=False):

        log.debug("Called; CMD: %r", cmd)
        log.debug("Called; DATA: %r", data)

        cursor = None
        affected_rowcount = 0
        last_row_id = 0

        if catch:
            try:
                self._lock.acquire()
                cursor = self.get_cursor()
                result = cursor.execute(cmd, data)
                affected_rowcount = cursor.rowcount
                last_row_id = cursor.lastrowid
                self._conn.commit()
                if rowcount:
                    return affected_rowcount

                if last_row_id != cursor.lastrowid:
                    log.error(
                        "LASTROWID ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
                    )

                return cursor.lastrowid

            except Exception as err:
                trace = traceback.format_exc(10)
                log.exception(
                    "Exception: %r CMD: %r\n%s",
                    err,
                    cmd,
                    trace,
                )

            finally:
                if cursor:
                    log.debug("Closing cursor")
                    cursor.close()
                self._lock.release()
        else:
            try:
                self._lock.acquire()
                cursor = self.get_cursor()
                cursor.execute(cmd, data)

                log.debug(
                    "============== CURSOR.ROWCOUNT %s",
                    cursor.rowcount,
                )
                affected_rowcount = cursor.rowcount
                last_row_id = cursor.lastrowid

                result = self._conn.commit()

                if rowcount:
                    return affected_rowcount

                if last_row_id != cursor.lastrowid:
                    log.error(
                        "LASTROWID ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
                    )
                return cursor.lastrowid

            finally:
                if cursor:
                    log.debug("Closing cursor")
                    cursor.close()
                self._lock.release()

    def select(self, cmd, catch=True):
        """
        Catch exceptions when running as background engine,
        raise exceptions when serving web pages
        """
        log.debug("called; SQL: %r", cmd)
        cursor = None

        start_time = time.time()
        if catch:
            try:
                self._lock.acquire()
                cursor = self.get_cursor()
                cursor.execute(cmd)
                result = cursor.fetchall()

                elapsed_time = time.time() - start_time
                if elapsed_time > 5:
                    log.debug(
                        "elapsed time: %.2f SQL: %r",
                        elapsed_time,
                        cmd,
                    )

                return result

            except Exception as err:
                trace = traceback.format_exc(10)
                msg = "Exception: %r CMD: %r" % (err, cmd)
                log.exception(
                    "%s\n%s",
                    msg,
                    trace,
                )
                return [(msg,)]

            finally:
                if cursor:
                    log.debug("Closing cursor")
                    cursor.close()
                self._lock.release()

        else:
            try:
                self._lock.acquire()
                cursor = self.get_cursor()
                cursor.execute(cmd)
                return cursor.fetchall()

            finally:
                if cursor:
                    log.debug("Closing cursor")
                    cursor.close()
                self._lock.release()

    def select_one(self, cmd, default, catch=True):

        sql_result = self.select(cmd, catch=catch)
        if sql_result is None:
            return default

        result = default
        for i, item in enumerate(sql_result):
            log.debug("item: %r", item)
            if i == 0:
                result = item[0]
            else:
                log.error(
                    "Unexpected result: CMD: %s index: %d",
                    result,
                    i,
                )
        return result


class DatabaseManager(object):
    """
    Some of the data is stored in a MySQL database but some of it is stored
    local dicts that are wiped when iocmon restarts
    """

    def __init__(self, queue_in, queue_out):

        self._queue_in = queue_in
        self._queue_out = queue_out
        self._sql = MyConnector()
        self._current_apps = {}
        self._my_pid = os.getpid()

    def set_db_name(self, name):
        self._sql.set_db_name(name)

    def callback_thread_monitor(self, msg, code):
        TM.print_list()

    def run(self):

        last_cmd_time = time.time()
        command_count = 0
        empty_count = 0

        while True:
            cur_time = time.time()

            try:
                item = self._queue_in.get(block=True, timeout=10)
                last_cmd_time = cur_time

            except queue.Empty:
                empty_count += 1
                log.debug("Command queue empty")
                interval = time.time() - last_cmd_time
                if interval > 600.0:
                    log.error(
                        "ELAPSED TIMEOUT; DATABASE WRITER TERMINATING!!",
                    )
                    break

                continue

            command_count += 1

            cmd = item[0]
            data = item[1]

            log.debug("CMD: %s DATA: %r", cmd, data)

            if cmd == CMD.PING:
                ping_count = data
                process = psutil.Process(self._my_pid)

                mem_usage = process.memory_info().rss

                # Added mem_usage to response to debug the SIGCHILD 137 we are getting
                d = {
                    KEY.PID: self._my_pid,
                    KEY.COUNTER: ping_count,
                    KEY.MEM: mem_usage,
                    KEY.COUNT_1: command_count,
                    KEY.COUNT_2: empty_count,
                    KEY.QSIZE: self._queue_in.qsize(),
                }
                self._queue_out.put_nowait((CMD.PING, d))
                log.debug(
                    "mem_usage: %r cmd count: %d (qsize: %d)",
                    mem_usage,
                    command_count,
                    self._queue_in.qsize(),
                )

                continue

            elif cmd == CMD.APP_UPDATE:
                self.handle_cmd_update_app(data)

            elif cmd == CMD.BEACON_APP_CLEAR_PORTS:
                self.handle_cmd_beacon_map_clear_ports(data)

            elif cmd == CMD.BEACON_NEW:
                self.handle_cmd_beacon_new(data)

            elif cmd == CMD.BEACON_DETECTED:
                self.handle_cmd_beacon_detected(data)

            elif cmd == CMD.BEACON_LOST:
                self.handle_cmd_beacon_lost(data)

            elif cmd == CMD.BEACON_RESTART:
                self.handle_cmd_beacon_restart(data)

            elif cmd == CMD.BEACON_UPDATE:
                self.handle_cmd_beacon_update(data)

            elif cmd == CMD.HEARTBEAT_UPDATE:
                self.handle_cmd_heartbeat_update(data)

            elif cmd == CMD.BEACON_RESUMED:
                log.error("FIXME")

            elif cmd == CMD.LOG_ADD:
                self.handle_cmd_add_log(data)

            else:
                raise ValueError("Don't know how to handle cmd: %r" % cmd)

    def handle_cmd_heartbeat_update(self, data):

        app_id = data[0]

        try:
            ioc_id = int(data[1])
        except:
            return

        try:
            port = int(data[2])
        except:
            return

        try:
            rx_t = int(data[3])
        except:
            return

        try:
            uptime = int(data[4])
        except:
            return

        cmd = "select id from heartbeats where app_id=%d" % app_id

        row_id = self._sql.select_one(cmd, None)

        if row_id is None:
            cmd = (
                "insert into heartbeats (ioc_id, port, time, uptime, app_id)"
                " values (%s, %s, %s, %s, %s)"
            )
            self._sql.execute(cmd, (ioc_id, port, rx_t, uptime, app_id))
        else:
            cmd = "update heartbeats set time=%d, uptime=%d where id=%s" % (
                rx_t,
                uptime,
                row_id,
            )
            self._sql.execute(cmd, None)

    def handle_cmd_beacon_update(self, data):
        """"This is an unused function kept for possible future changes"""

        try:
            ioc_id = int(data[0])
        except:
            return

        try:
            port = int(data[1])
        except:
            return

        try:
            seq = int(data[2])
        except:
            seq = 0

        try:
            t = int(data[3])
        except:
            return

        try:
            interval = int(data[4])
        except:
            interval = 0

        try:
            app_id = int(data[5])
        except:
            app_id = 0

        cmd = "select id from beacons where ioc_id=%d and port=%d" % (
            ioc_id,
            port,
        )

        log.info("DATABASE UPDATE BEACON - %s", cmd)

        row_id = self._sql.select_one(cmd, None)

        if row_id is None:
            cmd = "insert into beacons (ioc_id, port, seq, time, interval_ms, app_id) values (%s, %s, %s, %s, %s, %s)"
            self._sql.execute(cmd, (ioc_id, port, seq, t, interval, app_id))
        else:
            cmd = "update beacons set time=%d, seq=%d, app_id=%d" % (
                t,
                seq,
                app_id,
            )
            if interval:
                cmd += ", interval_ms=%d" % interval
            cmd += " where id=%d" % row_id

            self._sql.execute(cmd, None)

    def handle_cmd_add_log(self, data):
        """
        SQL_INSERT_LOG = \
            "insert into logs (kind, ioc_id, msg, create_time, status, needs_ack, app_id) " + \
            "values (%s, %s, %s, %s, %s)"
        """
        log.debug("called; data: %r", data)
        ioc_id = data.get(KEY.IP_KEY)
        values = (
            data.get(KEY.KIND),
            ioc_id,
            data.get(KEY.MESSAGE),
            int(time.time()),
            0,
            data.get(KEY.STATE),
            data.get(KEY.APP_ID),
        )
        result = self._sql.execute(SQL_INSERT_LOG, values)
        self._queue_out.put_nowait((CMD.UPDATE_IOC, {KEY.IP_KEY: ioc_id}))

    def handle_cmd_beacon_map_clear_ports(self, data):
        log.debug("called; data: %r", data)

    def handle_cmd_beacon_lost(self, data):
        event_id = self.insert_event(data, EVENT.BEACON_LOST)
        self.update_event(event_id, data, set_identified=True)

    def handle_cmd_beacon_new(self, data):
        event_id = self.insert_event(data, EVENT.BEACON_NEW)
        self.update_event(event_id, data)

    def handle_cmd_beacon_detected(self, data):
        pass
        ## self.insert_event(data, EVENT.BEACON_DETECTED)

    def handle_cmd_beacon_restart(self, data):
        event_id = self.insert_event(data, EVENT.BEACON_RESTARTED)
        self.update_event(event_id, data)

    def update_event(self, event_id, data, set_identified=False):
        port = int(data.get(KEY.PORT))
        ip_key = data.get(KEY.IP_KEY)

        current_ioc_data = self._current_apps.get(ip_key, {})
        app_data = current_ioc_data.get(port)

        if app_data is None:
            log.debug(
                "no app data for event id: %r",
                event_id,
            )
            return

        app_cwd = app_data.get(KEY.CWD, "Unknown")
        app_cmd = app_data.get(KEY.CMD, "Unknown")
        app_name = app_data.get(KEY.NAME, "Unknown")
        app_id = app_data.get(KEY.APP_ID, "Unknown")

        # Update this restarted beacon with a guess as to which app it is.
        # This is just a guess.... it might be wrong.  I think that it is
        # possible that a different app started on this port.

        sql = (
            "update events set app_cmd='%s', app_cwd='%s', app_name='%s', app_id=%d where id=%d"
            % (app_cmd, app_cwd, app_name, app_id, event_id)
        )

        log.debug("SQL: %s", sql)
        self._sql.execute(sql, None)

        if set_identified:
            sql = "update events set status=%d where id=%d" % (
                EVENT_STATUS.IDENTIFIED,
                event_id,
            )
            log.debug("SQL: %s", sql)
            self._sql.execute(sql, None)

    def insert_event(self, data, event_type):
        """
         SQL_INSERT_EVENT = \
             "insert into event (type, ip_addr, ip_addr_str, port, host_name, 
             create_time, create_date, create_hms, update_time) "
         """
        log.debug("called; data: %r", data)

        port = int(data.get(KEY.PORT))
        seq = data.get(KEY.SEQ, 0)
        ip_key = data.get(KEY.IP_KEY)
        ip_addr = get_ip_addr(ip_key)

        # Since this is an event, look up the host name as it exists in this
        # moment; do not store a reference to a host table.  In the future it
        # might be possible that the IP address is assigned to a different host_name

        status = EVENT_STATUS.UNIDENTIFIED

        host_name = get_host_name_for_db(ip_addr)

        create_time = int(time.time())
        update_time = create_time
        n = datetime.datetime.fromtimestamp(create_time)
        create_date = "%04d%02d%02d" % (n.year, n.month, n.day)
        create_hms = "%02d%02d%02d" % (n.hour, n.minute, n.second)
        data = (
            event_type,
            ip_key,
            ip_addr,
            port,
            seq,
            host_name,
            status,
            create_time,
            create_date,
            create_hms,
            update_time,
        )

        result = self._sql.execute(SQL_INSERT_EVENT, data)

        log.debug("inserted row ID: %r", result)

        return result

    def handle_cmd_update_app(self, data):

        cmd = data.get(KEY.CMD)
        cwd = data.get(KEY.CWD)

        # Must clean quotes from the command
        cmd = cmd.replace('"', "")
        cmd = cmd.replace("'", "")

        ip_key = int(data.get(KEY.IP_KEY))
        port = int(data.get(KEY.PORT))

        log.debug(
            "called; %s cmd: %r cwd: %r",
            get_host_name_for_db(ip_key),
            cmd,
            cwd,
        )

        # The app_id depends on cmd, cmd, and ip_addr BUT NOT port!
        app_id = make_app_id(cmd, cwd, ip_key)

        app_id_in = data.get(KEY.APP_ID)
        if app_id != app_id_in:
            raise ValueError("APP ID ERROR!!!!!!!!!!!!!!!!!!!!!!")

        sql = "select name,create_time from apps where id=%d" % app_id
        result = self._sql.select(sql)

        if len(result) == 1:
            item = result[0]
            log.debug(
                "This app already exists in the database; create time: %d",
                item[1],
            )
            ### app_name = item[0]
        else:
            # Add this cmd/cwd combination to the table.  The insert may fail if
            # the app is already in the database

            ip_addr = get_ip_addr(ip_key)

            # Every newly detected app is considered critical.  It must specifically be set non-critical
            state = APP_STATE.CRITICAL

            data = (
                app_id,
                cmd,
                cwd,
                ip_key,
                ip_addr,
                state,
                int(time.time()),
                int(time.time()),
            )
            self._sql.execute(SQL_INSERT_APP, data)

            # Update the IOC if an app was added
            self._queue_out.put_nowait((CMD.UPDATE_IOC, {KEY.IP_KEY: ip_key}))

        # Update the in memory data
        current_ioc_data = self._current_apps.get(ip_key, {})
        ## current_app_data = current_ioc_data.get(port, {})

        app_data = {
            KEY.CMD: cmd,
            KEY.CWD: cwd,
            KEY.NAME: "Unnamed",
            KEY.APP_ID: app_id,
        }
        current_ioc_data[port] = app_data
        self._current_apps[ip_key] = current_ioc_data

        # Next we should see if there are any unidentified events on this ip/port
        sql = (
            "select id, type from events where ioc_id=%d and " % ip_key
            + "port=%d and status=%d" % (port, EVENT_STATUS.UNIDENTIFIED)
        )

        result = self._sql.select(sql)
        if result is None:
            log.debug("There are no events to update")
            return

        # Loop through all the events and set them to IDENTIFIED so that they
        # are not processed again. Also, determine the max id for each type.
        # In the next loop we update each event

        max_kinds = {}
        for item in result:

            event_id = item[0]
            kind = item[1]
            sql = "update events set status=%d where id=%d" % (
                EVENT_STATUS.IDENTIFIED,
                event_id,
            )
            log.debug("SQL: %s", sql)
            self._sql.execute(sql, None)

            have_id = max_kinds.get(kind, 0)
            if event_id > have_id:
                max_kinds[kind] = event_id

        for kind, event_id in max_kinds.items():
            sql = (
                "update events set app_cmd='%s', app_cwd='%s', app_id=%d where id=%d"
                % (cmd, cwd, app_id, event_id)
            )
            log.debug("SQL: %s", sql)
            self._sql.execute(sql, None)


class Runner(object):

    def __init__(self):
        self._sql = MyConnector()

    def run(self):

        create_time = int(time.time())
        update_time = int(time.time())
        app_id = make_app_id("test123", "test456")
        data = (
            app_id,
            "this is the command",
            "a test dir",
            0,
            create_time,
            update_time,
        )

        self._sql.execute(SQL_INSERT_APP, data)

    def test_make_app_id(self):
        log.debug("called")
        app_id = make_app_id("tjisdlkjfsdlkj", "sjdfljkljsdlkjljsdf")
        log.debug("app_id: %d (%s)", app_id, type(app_id))

    def test1(self):

        now = int(time.time())
        one_day_before = now - (24 * 60 * 60)
        cmd = SQL_SELECT_EVENTS % one_day_before

        ### data = "%d" % one_day_before
        ##  result = self._sql.select(SQL_SELECT_EVENTS, data)
        result = self._sql.select(cmd)
        log.debug("this is the result-->%s", result)


if __name__ == "__main__":

    runner = Runner()
    ## runner.run()

    runner.test1()
    ## runner.test_make_app_id()
