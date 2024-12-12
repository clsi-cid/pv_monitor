# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
# System Imports
import copy
import datetime
import json
import logging
import multiprocessing as mp
import os
import queue
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import psutil

# Library imports
from data_management.beacon_manager import BeaconManager
from data_management.database import CMD as DB_CMD
from data_management.database import DatabaseManager, MyConnector
from data_management.group_manager import GroupManager
from data_management.heartbeat_manager import HeartbeatManager
from data_management.ioc_manager import IOCManager
from data_management.netstat import RemoteProc
from data_management.netstat_manager import NetstatManager
from data_management.plot_manager import PlotDataMgr
from data_management.poll_cache import PollCache
from data_management.pv_manager import PVManager
from definitions import KEY, PRIORITY, SORT_MODE, Constant, config
from gateway_client import NewSearcher, ProxyClient
from utilities.statistics import StatisticsManager
from utilities.thread_monitor import ThreadMonitor
from utilities.tracker import RequestTracker
from utilities.utils import make_size_str

log = logging.getLogger("PVMLogger")
TM = ThreadMonitor()
STATS = StatisticsManager()



class CMD(Constant):
    STATS = "cmd:stats"


def database_worker(queue_to_db, queue_from_db, db_name):
    """
    Multiprocess Worker.  Runs in its own dedicated python process so that
    it can acquire the GIL
    """
    writer = DatabaseManager(queue_to_db, queue_from_db)
    writer.set_db_name(db_name)
    writer.run()


class DataManager(object):
    """
    Non website code.  This class is basically a wrapper for all the individual
    datamanager classes that have organically evolved over the years.
    This class attempts to "tie" them all together in one place, although in
    many cases the call each other directly.
    """

    def __init__(
        self,
        my_hostname,
        db_name,
        gateway,
        gateway_listen_port,
        client_to_gateway_port,
    ):

        self._launch_time = time.time()

        self._count_rx_gw = 0
        self._count_req_gw_packets = 0
        self._count_req_gw_pvs = 0
        self._count_req_ar_packets = 0
        self._count_req_ar_pvs = 0
        self._count_writes = 0

        self._pid_me = os.getpid()
        self._pid_gateway = None
        self._pid_sql_mon = None

        self._gateway = gateway
        self._gateway_alive_flag = False
        self._my_hostname = my_hostname
        self._my_ip_addr = socket.gethostbyname(self._my_hostname)

        self._urlfetch_lock = threading.Lock()
        self._urlfetch_cainfo = "http://%s:%d/cainfo/" % (
            config.CAINFO_HOST,
            config.CAINFO_SERVER_PORT,
        )
        self._urlfetch_caget = "http://%s:%d/caget/" % (
            config.CAINFO_HOST,
            config.CAINFO_SERVER_PORT,
        )

        self._sql = MyConnector(db_name)
        self._q_to_db = mp.Queue()
        self._q_from_db = mp.Queue()
        self._process_db = mp.Process(
            target=database_worker,
            args=(self._q_to_db, self._q_from_db, db_name),
        )

        # Stats for monitoring and debugging
        self._db_q_max = 0
        self._db_q_counter = 0
        self._db_q_count_last_interval = 0
        self._db_q_check_time = time.time()

        self._queue_cmd = queue.Queue()

        self._tracker = RequestTracker()
        self._tracker.set_server_name(self._my_hostname)

        self._iocman = IOCManager(self)
        self._searcher = NewSearcher(gateway, client_to_gateway_port)

        self._new_cache = PollCache(self)
        self._new_cache.set_searcher(self._searcher)

        self._netman = NetstatManager(self)
        self._netman.set_callback_database(self.send_to_database)

        self._pvman = PVManager(self._my_hostname, self._gateway)
        self._pvman.set_tracker(self._tracker)
        self._pvman.set_searcher(self._searcher)
        self._pvman.set_poll_cache(self._new_cache)
        self._pvman.set_dataman(self)

        self._beaconman = BeaconManager(self, self._my_hostname)
        self._beaconman.set_poll_cache(self._new_cache)
        self._beaconman.set_iocman(self._iocman)
        self._beaconman.set_callback_database(self.send_to_database)

        self._procman = RemoteProc()

        self._heartman = HeartbeatManager(self)
        self._heartman.set_searcher(self._searcher)
        self._heartman.set_pvman(self._pvman)
        self._heartman.set_callback_database(self.send_to_database)

        self._plotman = PlotDataMgr(self, self._my_hostname)
        self._plotman.set_pvman(self._pvman)

        # ProxyClient receives data from the gateway
        self._gateway_client = ProxyClient(self)
        self._gateway_client.set_listen_port(gateway_listen_port)
        self._gateway_client.set_callback_beacon(self._beaconman.handle_beacon)
        self._gateway_client.set_callback_heartbeat(
            self._heartman.handle_heartbeat
        )
        self._gateway_client.set_callback_pv_request(
            self._pvman.handle_pv_request
        )
        self._gateway_client.set_callback_pv_response(
            self._pvman.handle_pv_response
        )
        self._gateway_client.set_callback_stats(self.handle_stats)

        self._groupman = GroupManager(self)
        self._groupman.set_iocman(self._iocman)

        self._gateway_client.add_callback_alive(
            self._beaconman.handle_gateway_alive
        )
        self._gateway_client.add_callback_alive(
            self._heartman.handle_gateway_alive
        )
        self._gateway_client.add_callback_alive(
            self._netman.handle_gateway_alive
        )
        self._gateway_client.add_callback_alive(
            self._new_cache.handle_gateway_alive
        )
        self._gateway_client.add_callback_alive(
            self._pvman.handle_gateway_alive
        )
        self._gateway_client.add_callback_alive(self.handle_gateway_alive)

        self._timer_monitor_db = threading.Timer(1, self.worker_monitor_db)
        self._timer_cmd = threading.Timer(1, self.worker_cmd)
        self._timer_keyboard = threading.Timer(1, self.worker_keyboard)

    def handle_gateway_alive(self, alive):
        self._gateway_alive_flag = alive

    def is_gateway_alive(self):
        return self._gateway_alive_flag

    def load(self):
        self._pvman.load()
        self._beaconman.load()
        self._heartman.read_app_config()
        self._iocman.load()
        self._groupman.load()

    def start(self):
        log.debug(
            "called; host: %s ip_addr: %s",
            self._my_hostname,
            self._my_ip_addr,
        )

        self.load()

        # Does order matter here? Ideally no but I am not sure.
        self._searcher.start()
        self._tracker.start()
        self._process_db.start()
        self._netman.start()
        self._heartman.start()
        self._gateway_client.start()
        self._timer_monitor_db.start()
        self._pvman.start()
        self._iocman.start()
        self._beaconman.start()
        self._timer_cmd.start()
        self._plotman.start()
        self._timer_keyboard.start()
        self._new_cache.start()
        STATS.start()

        # TM callback required to start TM... there are many instances with
        # shared data but only one does the monitoring
        TM.set_callback(self.callback_thread_monitor)

    def callback_thread_monitor(self, msg, code):
        log.debug("called")
        TM.print_list()

    def make_leader(self, pv_name):
        return self._pvman.make_leader(pv_name)

    def forget_ioc(self, ioc_id):
        return self._iocman.forget_ioc(ioc_id)

    def forget_pv(self, pv_name):
        return self._pvman.forget_pv(pv_name)

    def add_ioc_log(self, ioc_id, msg, needs_ack=False, app_id=None):
        self._iocman.add_log(ioc_id, msg, needs_ack=needs_ack, app_id=app_id)

    def group_create(self, group_name):
        return self._groupman.create(group_name)

    def group_delete(self, group_id):
        return self._groupman.delete(group_id)

    def clear_ioc_msg(self, ioc_key, msg_id):
        return self._iocman.clear_ioc_msg(ioc_key, msg_id)

    def clear_ioc_logs(self, ioc_key, kind, pattern=None):
        return self._iocman.clear_ioc_logs(ioc_key, kind, pattern=pattern)

    def get_ioc_ports(self, ip_key):
        return self._iocman.get_ports(ip_key)

    def get_ioc_beacons_display(self, ip_key):
        return self._iocman.get_beacons_display(ip_key)

    def get_epics_event_count(self, timestamp):
        return self._iocman.get_epics_event_count(timestamp)

    def get_epics_events(self, timestamp, offset=0, count=10):
        return self._iocman.get_epics_events(
            timestamp, offset=offset, count=count
        )

    def get_ioc_log_count(self, ioc_id):
        return self._iocman.get_ioc_log_count(ioc_id)

    def get_ioc_logs(self, ioc_id, offset=0, count=10):
        return self._iocman.get_ioc_logs(ioc_id, offset=offset, count=count)

    def get_ioc_port_pvs(
        self, ioc_id, port, offset=0, count=None, sort=SORT_MODE.NAME
    ):
        return self._pvman.get_ioc_port_pvs(
            ioc_id, port, offset=offset, count=count, sort=sort
        )

    def get_ioc_mapped_ports(self, key):
        return self._pvman.get_ioc_ports(key)

    def get_ioc_app_dict(self, ioc_id):
        return self._iocman.get_app_dict(ioc_id)

    def get_beacon_count(self, key):
        return self._iocman.get_beacon_count(key)

    def set_beacon_count(self, key, count):
        return self._iocman.set_beacon_count(key, count)

    def set_ioc_description(self, ioc_id, description):
        return self._iocman.set_description(ioc_id, description)

    def set_ioc_options(self, ioc_id, options_list):
        return self._iocman.set_options(ioc_id, options_list)

    def get_ioc_options(self, ioc_id):
        return self._iocman.get_options(ioc_id)

    def get_beacon_ioc_id_list(self):
        return self._beaconman.get_ioc_id_list()

    def get_heartbeat_ioc_id_list(self):
        return self._heartman.get_ioc_id_list()

    def get_beacon(self, ioc_id, port):
        return self._beaconman.get_beacon(ioc_id, port)

    def get_beacon_ports_unmapped(self, ip_key):
        return self._beaconman.get_beacon_ports_unmapped(ip_key)

    def map_beacon(self, ip_key, port, cwd, cmd, app_id):
        return self._beaconman.map_beacon(ip_key, port, cwd, cmd, app_id)

    def get_linuxmon_dict(self):
        return self._pvman.get_linuxmon_dict()

    def get_heartbeat_for_netstat(self, ioc_id):
        return self._heartman.get_heartbeat_for_netstat(ioc_id)

    def get_heartbeat(self, app_id=None, ioc_id=None, port=None):
        return self._heartman.get_heartbeat(
            app_id=app_id, ioc_id=ioc_id, port=port
        )

    def get_heartbeats_sorted(self, offset, count, sort):
        return self._heartman.get_list(offset, count, sort)

    def get_heartbeat_list_for_ioc(self, ioc_id):
        return self._heartman.get_list_for_ioc(ioc_id)

    def get_heartbeat_pvs(self, ip_key, port):
        return self._heartman.get_pvs(ip_key, port)

    def get_heartbeat_history(self, ip_key, port):
        return self._heartman.get_history(ip_key, port)

    def get_heartbeat_history_string(self, event):
        return self._heartman.get_history_string(event)

    def get_pv_heartbeat(self, pv_name):
        return self._heartman.get_pv_heartbeat(pv_name)

    def get_pv_fetch_counts(self, sort=False):
        return self._heartman.get_pv_fetch_counts(sort=sort)

    def get_iocapps_data(self, pv_name):
        return self._pvman.get_iocapps_data(pv_name)

    def get_thread_data_gateway(self):
        gateway_data_raw = self._gateway_client.get_thread_data()
        if gateway_data_raw:
            gateway_thread = gateway_data_raw
            return gateway_thread
        else:
            return None
    
    def get_thread_data_pvm(self):
        return TM.get_list()

    def get_nestat_fetch_counts(self):
        return self._netman.get_netstat_fetch_counts()

    def get_netstat_unsupported(self):
        return self._netman.get_netstat_unsupported()

    def has_linuxmon(self, name):
        return self._pvman.has_linuxmon(name)

    def get_memberships_by_ioc(self, ioc_id, sort=True, not_member=False):
        return self._groupman.get_memberships_by_ioc(
            ioc_id, sort=sort, not_member=not_member
        )

    def get_memberships_by_group(self, group_id):
        return self._groupman.get_memberships_by_group(group_id)

    def get_group_name(self, group_key):
        return self._groupman.get_group_name(group_key)

    def get_app_times(self, ioc_id, port, app_id):
        return self._iocman.get_app_times(ioc_id, port, app_id)

    def get_group_dict(self):
        return self._groupman.get_group_dict()

    def get_group_list(self, counts=False):
        return self._groupman.get_group_list(counts=counts)

    def get_pids(self):
        return (self._pid_me, self._pid_sql_mon, self._pid_gateway)

    def ack_all_logs(self):
        return self._iocman.ack_all_logs()

    def process_all_iocs(self):
        return self._iocman.process_all_iocs()

    def get_ioc_summary(self, group_key):
        return self._iocman.get_ioc_summary(group_key)

    def get_ioc_id_list(self):
        return self._iocman.get_id_list()

    def get_ioc_description(self, ioc_id, default=""):
        return self._iocman.get_ioc_description(ioc_id, default=default)

    def check_ioc(self, key, ioc=None):
        return self._iocman.get_ioc_data(key, ioc_data=ioc)

    def submit_ioc_apps(self, ip_key, data):
        self._iocman.submit_ioc_apps(ip_key, data)

    def get_procserv_data(self, ip_addr):
        return self._netman.get_procserv_data(ip_addr)

    def search_pvs(
        self,
        search_string,
        key,
        offset,
        count,
        ignore_case=True,
        sort=SORT_MODE.NAME,
    ):
        return self._pvman.search_pvs(
            search_string,
            key,
            offset,
            count,
            ignore_case=ignore_case,
            sort=sort,
        )

    def get_pvs_all(self, offset, count, sort=SORT_MODE.NAME):
        return self._pvman.get_pvs_all(offset, count, sort=sort)

    def get_pv_stats(self):
        return self._pvman.get_stats()

    def get_pvs_gone(self):
        return self._pvman.get_pvs_gone()

    def get_pv_data(self, pv_name, log=False):
        return self._pvman.get_pv_data(pv_name, log=log)

    def get_pv_latest_info(self, data):
        return self._pvman.get_pv_latest_info(data)

    def get_pv_latest_request(self, data):
        return self._pvman.get_pv_latest_request(data)

    def get_pvs_lost(self, offset, count=None, sort=SORT_MODE.LAST_SEEN):
        return self._pvman.get_pvs_lost(offset, count=count, sort=sort)

    def get_pvs_duplicate(self, offset, count=None, sort=SORT_MODE.NAME):
        return self._pvman.get_pvs_duplicate(offset, count=count, sort=sort)

    def get_pvs_latest_requests_ioc(
        self, ioc_id, offset, count=None, sort=SORT_MODE.REQ_COUNT
    ):
        return self._pvman.get_pvs_latest_requests_ioc(
            ioc_id, offset, count=count, sort=sort
        )

    def get_pvs_invalid(self, offset, count=None, sort=SORT_MODE.REQ_TIME):
        return self._pvman.get_pvs_invalid(offset, count=count, sort=sort)

    def forget_heartbeat(self, key):
        return self._heartman.forget(key)

    def forget_linuxmon(self, ioc_name):
        return self._pvman.forget_linuxmon(ioc_name)

    def group_member_add(self, group_id, ioc_id):
        return self._groupman.membership_add(group_id, ioc_id)

    def group_member_remove(self, group_id, ioc_id):
        return self._groupman.membership_remove(group_id, ioc_id)

    def get_beacons(self, ioc_id=None, skip_lost=True, as_dict=False):
        return self._beaconman.get_beacons(
            ioc_id=ioc_id, skip_lost=skip_lost, as_dict=as_dict
        )

    def get_beacons_bad(self):
        return self._beaconman.get_beacons_bad()

    def get_beacons_addr_mismatch(self):
        return self._beaconman.get_beacons_addr_mismatch()

    def get_dead_thread_count(self):
        return TM.get_dead_thread_count()

    def ack_ioc_logs(self, ioc_id):
        return self._iocman.ack_all_ioc_logs(ioc_id)

    def purge_beacons(self, ioc_id):
        return self._beaconman.purge_beacons(ioc_id)

    def purge_heartbeats(self, ioc_id):
        return self._heartman.purge_heartbeats(ioc_id)

    def purge_netstat_data(self, ioc_id):
        return self._netman.purge_netstat_data(ioc_id)

    def delete_logs(self, ioc_id, log_id_list):
        return self._iocman.delete_logs(ioc_id, log_id_list)

    def heartbeat_to_beaconman(self, data):
        return self._beaconman.queue_heartbeat(data)

    def is_ready(self):
        count = self._netman.get_beacon_map_count()
        if count < 2:
            return False
        return True

    def fetch_cainfo(self, pv_name):
        """
        Fetch ca info from the stand alone multithreaded server.
        Of course this could stall... should there be a queue?
        """
        self._urlfetch_lock.acquire()

        try:
            url = self._urlfetch_cainfo + pv_name

            request = urllib.request.Request(url)
            response = urllib.request.urlopen(request)

            if response.code != 200:
                raise ValueError(
                    "urlfetch cainfo %s response code %d"
                    % (pv_name, response.code)
                )

            lines = json.loads(response.read().decode("utf-8"))

        except Exception as err:
            msg = "urlfetch: %s exception %s" % (pv_name, str(err))
            log.exception(msg)
            lines = [msg]

        finally:
            self._urlfetch_lock.release()

        return lines

    def fetch_caget(self, pv_name):
        """
        Do a caget from the stand alone multithreaded server.
        Of course this could stall... should there be a queue?
        """
        self._urlfetch_lock.acquire()

        try:
            url = self._urlfetch_caget + pv_name

            request = urllib.request.Request(url)
            response = urllib.request.urlopen(request)

            if response.code != 200:
                raise ValueError(
                    "urlfetch cainfo %s response code %d"
                    % (pv_name, response.code)
                )
            result = response.read().decode("utf-8")

        except Exception as err:
            msg = "urlfetch: %s exception %s" % (pv_name, str(err))
            log.exception("%s\n%s", msg, url)
            result = "EXCEPTION: %r" % err

        finally:
            self._urlfetch_lock.release()
        return result

    def ca_proto_search(self, pv_name, priority=PRIORITY.NORMAL):
        self._searcher.search(pv_name, priority=priority)

    def send_to_database(self, cmd, data):
        """
        Send the command to a seperate process that updates the database
        """
        qsize = self._q_to_db.qsize()
        if qsize > 100:
            log.debug("DBQ size: %d cmd: %s", qsize, cmd)

        if qsize > 2000:
            msg = "PANIC!!!!! database qsize: %d" % qsize
            log.error(msg)
            STATS.set("PANIC: Database qsize", qsize)
            raise ValueError(msg)

        self._db_q_counter += 1
        self._db_q_count_last_interval += 1

        if qsize > self._db_q_max:
            self._db_q_max = qsize

        cur_time = time.time()
        if cur_time > self._db_q_check_time + 60:
            interval = cur_time - self._db_q_check_time

            qrate = self._db_q_count_last_interval / interval
            qrate_str = "%.2f" % qrate
            STATS.set("Queue Rate/min: Database", qrate_str)
            self._db_q_check_time += 60
            self._db_q_count_last_interval = 0

            STATS.set("Max Queue Size: Database", self._db_q_max)
            STATS.set("Queue Size: Database", qsize)
            qrate = self._db_q_counter / (time.time() - self._launch_time)
            qrate_str = "%.2f" % qrate
            STATS.set("Queue Rate: Database", qrate_str)
            STATS.set("Database TX", self._db_q_counter)
            log.debug(
                "database qsize: %d (max: %d)",
                qsize,
                self._db_q_max,
            )

        c = copy.deepcopy(cmd)
        d = copy.deepcopy(data)
        self._q_to_db.put_nowait((c, d))

    def update_ioc(self, ioc_id, beacon_data=None, heartbeat_data=None):
        return self._iocman.update_ioc(
            ioc_id, beacon_data=beacon_data, heartbeat_data=heartbeat_data
        )

    def handle_stats(self, ip_key, port, data_str, gateway_pid):
        log.debug(
            "Called; ip_key: %d port: %d len data: %d",
            ip_key,
            port,
            len(data_str),
        )
        self._queue_cmd.put_nowait(
            (CMD.STATS, (ip_key, port, data_str, gateway_pid))
        )

    def worker_cmd(self):

        thread_id = TM.add("Dataman: Save counts", 60)
        next_write_time = int(int(time.time()) / 60) * 60
        next_ping_time = time.time()

        while True:
            cur_time = time.time()

            if cur_time > next_ping_time:
                TM.ping(thread_id)
                next_ping_time += 20

            if cur_time >= next_write_time:
                next_write_time += 60
                self.handle_cmd_save_counts(cur_time)

            try:
                item = self._queue_cmd.get(block=True, timeout=1)
            except queue.Empty:
                continue

            cmd = item[0]
            data = item[1]

            if cmd == CMD.STATS:
                self.handle_cmd_stats(data)

            else:
                raise ValueError("unhandled cmd")

    def handle_cmd_stats(self, data_in):

        ### ioc_id = data_in[0]
        ### port = data_in[1]
        data_str = data_in[2]
        gateway_pid = data_in[3]

        data = json.loads(data_str)

        log.debug("stats from client: %r", data)

        self._pid_gateway = gateway_pid

        self._count_req_gw_packets += data.get("gw")
        self._count_req_gw_pvs += data.get("gw_pv")

        self._count_req_ar_packets += data.get("ar")
        self._count_req_ar_pvs += data.get("ar_pv")

        gateway_q_size = data.get("rx_q")
        self._pvman.set_gateway_queue_size(gateway_q_size)

        gateway_rx = data.get("rx_count")
        self._count_rx_gw += gateway_rx
        STATS.set("RX count (gateway)", self._count_rx_gw)

    def handle_cmd_save_counts(self, cur_time):

        # Get the total number of pvs
        test, pv_count_all = self.get_pvs_all(0, 0)
        if len(test):
            raise ValueError("dont want data")

        test, pv_count_lost = self.get_pvs_lost(0, count=0)
        if len(test):
            raise ValueError("dont want data")

        # Don't save counts if no data ready yet
        if pv_count_all == 0:
            log.debug("no data yet")
            return

        if self._pvman.get_mapping_run_count() == 0:
            log.debug("PVs not mapped yet")
            return

        test, pv_count_duplicate = self.get_pvs_duplicate(0, count=0)
        if len(test):
            raise ValueError("dont want data")

        test, pv_count_invalid = self.get_pvs_invalid(0, count=0)
        if len(test):
            raise ValueError("dont want data")

        user_count_packets, user_count_pvs = self._pvman.get_req_counts()

        d = datetime.datetime.fromtimestamp(cur_time)

        file_name = "data/%s/stats/%04d_%02d/counts_%04d_%02d_%02d.csv" % (
            self._my_hostname,
            d.year,
            d.month,
            d.year,
            d.month,
            d.day,
        )

        try:
            path = os.path.dirname(file_name)
            if not os.path.isdir(path):
                log.debug("creating directory '%s'", path)
                os.makedirs(path)

            f = open(file_name, "a+")
            log.debug("filename: %s", file_name)

        except Exception as e:
            log.exception(
                "Error opening file '%s': %s",
                file_name,
                str(e),
            )
            return

        result = "%s" % d
        parts = result.split(".")
        time_str = parts[0]

        """
        The old format was as follows
         0) Time string
         1) packets
         2) non-gw requests
         3) gateway requests
         4) total requests (gw+non gw)
         5) new pvs (invremental)
         6) new pvs (total)
         7) total number of PVs
         8) lost PV  count
         9) duplicate PV count
        10) invalid PV count
        
        The new format is:
        
        0 time string
        1 user packets
        2 user pvs
        3 gw packets
        4 gw pvs
        5 archiver packets
        6 archiver pvs
        7 pvs total
        8 pvs lost
        9 pvs duplicate
        10 pvs invalid
        11 mem usage (RSS)
        12 cpu usage
        13 mem usage (VMS)
        14 free bytes (srv_unix)
        """
        cpu_percent = psutil.cpu_percent() * 10
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        mem_usage_rss = memory_info.rss
        mem_usage_vms = memory_info.vms

        d = psutil.disk_usage("/iocApps")
        srv_unix_free_iocapps = d.free

        d = psutil.disk_usage("/home/control")
        srv_unix_free_home = d.free

        if f:  #  0   1   2   3   4   5   6   7   8   9  10  11  12  13  14, 15
            line = (
                "%s, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d\n"
                % (
                    time_str,
                    user_count_packets,
                    user_count_pvs,
                    self._count_req_gw_packets,
                    self._count_req_gw_pvs,
                    self._count_req_ar_packets,
                    self._count_req_ar_pvs,
                    pv_count_all,
                    pv_count_lost,
                    pv_count_duplicate,
                    pv_count_invalid,
                    mem_usage_rss,
                    cpu_percent,
                    mem_usage_vms,
                    srv_unix_free_home,
                    srv_unix_free_iocapps,
                )
            )

            if self._count_writes > 0:
                f.write(line)
            f.close()

        self._count_writes += 1
        self._count_req_gw_packets = 0
        self._count_req_gw_pvs = 0
        self._count_req_ar_packets = 0
        self._count_req_ar_pvs = 0

    def get_auth(self):

        remote = ""

        print("Enter Auth Code:")

        # Need to clean out blocking buffer.  There should be a 'CR'
        # in the buffer
        while True:
            c = sys.stdin.read(1)
            if str(c) == str("\n"):
                break

        while True:
            c = sys.stdin.read(1)

            if str(c) == str("\n"):
                self._procman.set_password(remote)
                return
            else:
                remote += c

    def worker_keyboard(self):

        while True:
            c = sys.stdin.read(1)
            print("CHAR-->", c, len(c))

            if c == str("\n"):
                print("Commands: ")
                print("a: authenticate")

            elif c == "a":
                self.get_auth()
                print("Auth Code Entered")

    def worker_monitor_db(self):
        """
        Ensures that the database writer is alive
        """
        thread_id = TM.add("Dataman: Monitor DB", 120)
        last_response_time = time.time()
        next_ping_time = time.time()
        next_object_list_time = time.time()
        next_log_time = time.time()
        ping_count = 0

        while True:
            cur_time = time.time()

            if cur_time > next_ping_time:
                TM.ping(thread_id)

                ping_count += 1
                self.send_to_database(DB_CMD.PING, ping_count)

                self._iocman.ping(ping_count)
                next_ping_time += 20

            if cur_time > next_object_list_time:
                next_object_list_time += 30

            if cur_time > next_log_time:
                vm = psutil.virtual_memory()
                total_str = make_size_str(vm.total)
                used_str = make_size_str(vm.used)
                free_str = make_size_str(vm.free)
                process = psutil.Process(os.getpid())
                mem_info = process.memory_info()
                mem_rss_str = make_size_str(mem_info.rss)
                mem_vms_str = make_size_str(mem_info.vms)

                log.debug(
                    "MEM: total: %s used: %s free: %s RSS: %s VMS: %s",
                    total_str,
                    used_str,
                    free_str,
                    mem_rss_str,
                    mem_vms_str,
                )

                next_log_time += 30

            try:
                item = self._q_from_db.get(block=True, timeout=2.0)

            except queue.Empty:
                #  Time since last response
                age = time.time() - last_response_time
                if age > 600:
                    log.error(
                        "no ping response from database; "
                        "monitor thread terminating",
                    )
                    return
                continue

            last_response_time = time.time()

            cmd = item[0]
            data = item[1]

            if cmd == DB_CMD.PING:
                self._pid_sql_mon = data.get(KEY.PID)

            elif cmd == DB_CMD.UPDATE_IOC:
                ioc_id = data.get(KEY.IP_KEY)
                self.update_ioc(ioc_id)
