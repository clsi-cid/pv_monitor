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
import pickle
import queue
import random
import re
import socket
import struct
import threading
import time
import traceback

from data_management.ioc_apps import IocApps
from definitions import (CMD, EPICS, KEY, LINUXMON_PV_SIGNATURE,
                         LINUXMON_PV_SUFFIX_POLL, LINUXMON_PV_SUFFIX_POLL_ALL,
                         PRIORITY, PV_KEY, SETTINGS, SORT_MODE, config)
from utilities.file_saver import FileSaver
from utilities.print_binary import print_binary
from utilities.pvmonitor_lock import PVMonitorLock
from utilities.statistics import StatisticsManager
from utilities.thread_monitor import ThreadMonitor
from utilities.utils import (ExceptionReturn, get_host_name, get_ip_from_key,
                             get_ip_key, linuxmon_date_to_int,
                             linuxmon_uptime_to_int, make_last_seen_str,
                             make_server_key, validate_pv_name_new)

log = logging.getLogger("PVMLogger")

TM = ThreadMonitor()
STATS = StatisticsManager()


class DB_KEY(object):
    PVS = "PVS"
    LINUXMON_PVS = "LINUXMON_PVS"
    LINUXMON_BACKOFF = "LINUXMON_BACKOFF"


class PVManager(object):

    def __init__(self, my_hostname, gateway):
        log.debug("called")

        self._forget_lock = PVMonitorLock()
        self._forget_dict = {}

        self._pv_lock = PVMonitorLock()
        self._pvs_dict = {}

        self._pvs_invalid_lock = PVMonitorLock()
        self._pvs_invalid = {}
        self._pvs_sorted_invalid_name = []
        self._pvs_sorted_invalid_req_count = []
        self._pvs_sorted_invalid_req_time = []

        self._pvs_lost_lock = PVMonitorLock()
        self._pvs_sorted_lost_req_time = []
        self._pvs_sorted_lost_req_count = []
        self._pvs_sorted_lost_last_seen = []
        self._pvs_sorted_lost_name = []

        self._pvs_duplicate_lock = PVMonitorLock()
        self._pvs_duplicate = []

        self._pvs_all_lock = PVMonitorLock()
        self._pvs_sorted_all_name = []
        self._pvs_sorted_all_req_count = []
        self._pvs_sorted_all_req_time = []

        self._pvs_search_cache_lock = PVMonitorLock()
        self._pvs_search_cache = {}

        self._pv_to_ioc_map_lock = PVMonitorLock()
        self._pv_to_ioc_map = {}

        # for keeping track of the latest requests
        self._latest_requests_lock = PVMonitorLock()
        self._latest_lock = PVMonitorLock()
        self._latest_requests = []
        self._latest_requests_by_ioc = {}
        self._latest_data_pvs = {}
        self._latest_data_iocs = {}

        self._poll_follower_lock = PVMonitorLock()
        self._poll_all_list = []
        self._poll_follower_list = []
        self._poll_leader_dict = {}

        self._linuxmon_lock = PVMonitorLock()
        self._linuxmon_dict = {}
        self._linuxmon_backoff = {}
        self._linuxmon_backoff_last = {}
        self._linuxmon_check_dict = {}
        self._linuxmon_check_list = []

        self._pvs_search_lock = PVMonitorLock()
        self._pvs_search_set = []

        # For debugging
        self._dbg_possible_gone_lock = PVMonitorLock()
        self._dbg_possible_gone = {}

        self._count_pvs_new = 0
        self._count_saves = 0
        self._count_cache_hit = 0
        self._count_cache_miss = 0
        self._count_polls = 0
        self._count_poll_leaders = 0
        self._count_poll_followers = 0
        self._count_poll_random = 0
        self._count_poll_never_seen = 0
        self._count_poll_to_old = 0
        self._count_poll_remaining = 0
        self._count_poll_size = 0
        self._count_req_pvs = 0
        self._count_req_packets = 0
        self._count_mapping_runs = 0
        self._poll_start_time = 0

        self._test_pv_req_count = 0
        self._test_pv_req_size = 0

        self._gateway = gateway
        self._my_hostname = my_hostname

        self._followers = []
        self._followers_lock = PVMonitorLock()

        # The gateway reports its queue size so that we do not flood it with
        # CA_PROTO_SEARCH requests
        self._gateway_queue_size = 0
        self._gateway_alive = False

        gateway = self._gateway.lower()

        if gateway.startswith(config.GW_HOST):
            self._gateway_ids = [
                get_ip_key(addr) for addr in config.ADDRESS_GW
            ]

        else:
            raise ValueError("gateway not supported: %r" % gateway)

        self._ioc_apps = IocApps(self._my_hostname)
        self._tracker = None
        self._searcher = None
        self._poll_cache = None
        self._dataman = None
        self._new_cache = None
        self._queue_cmd = queue.PriorityQueue()

        self._timer_save = threading.Timer(60, self.worker_save)
        self._timer_poll = threading.Timer(60, self.worker_poll)
        self._timer_map_to_ioc = threading.Timer(60, self.worker_map_to_ioc)
        self._timer_process_data = threading.Timer(60, self.worker_process_data)
        self._timer_linuxmon_pvs_read = threading.Timer(
            10, self.worker_linuxmon_pvs_read
        )
        self._timer_cmd = threading.Timer(1, self.worker_cmd)

    def handle_cmd_pv_forget(self, data):
        """Remove pv from all the place which it must be deleted
        where must PV be deleted? It cound be
            - in a poll cache;
            - mapped to an IOC;
            - queued for polling.
            - invalid list
            - lost list
            - duplicate list
            - others?

        Args:
            data (str): name of the PV which will be deleted
        """
        pv_name = str(data)

        log.debug("forget pv: %s", pv_name)

        if self._poll_cache:
            self._poll_cache.delete_pv(pv_name)

        self._forget_lock.acquire()
        try:
            del self._forget_dict[pv_name]
        except:
            pass
        self._forget_lock.release()

        # ----------------------------------------------------------------------
        self._pv_lock.acquire()
        try:
            del self._pvs_dict[pv_name]
        except:
            pass
        self._pv_lock.release()
        # ----------------------------------------------------------------------
        self._pvs_invalid_lock.acquire()

        try:
            del self._pvs_invalid[pv_name]
        except:
            pass

        lists = [
            self._pvs_sorted_invalid_name,
            self._pvs_sorted_invalid_req_count,
            self._pvs_sorted_invalid_req_time,
        ]
        for l in lists:
            try:
                l.remove(pv_name)
            except:
                pass

        self._pvs_invalid_lock.release()
        # ----------------------------------------------------------------------
        self._pvs_lost_lock.acquire()
        lists = [
            self._pvs_sorted_lost_req_time,
            self._pvs_sorted_lost_req_count,
            self._pvs_sorted_lost_last_seen,
            self._pvs_sorted_lost_name,
        ]
        for l in lists:
            try:
                l.remove(pv_name)
            except:
                pass
        self._pvs_lost_lock.release()
        # ----------------------------------------------------------------------
        self._pvs_all_lock.acquire()
        lists = [
            self._pvs_sorted_all_name,
            self._pvs_sorted_all_req_count,
            self._pvs_sorted_all_req_time,
        ]
        for l in lists:
            try:
                l.remove(pv_name)
            except:
                pass
        self._pvs_all_lock.release()
        # ----------------------------------------------------------------------
        self._poll_follower_lock.acquire()
        try:
            self._poll_all_list.remove(pv_name)
        except:
            pass
        try:
            self._poll_follower_list.remove(pv_name)
        except:
            pass
        try:
            del self._poll_leader_dict[pv_name]
        except:
            pass
        self._poll_follower_lock.release()

    def handle_gateway_alive(self, alive):
        """One of the handle_gateway_alive function(there are others defined in
        other manager files), takes in a boolean value to changed the status of
        _gateway_alive, if _gateway_alive is False, the PV Monitor will wait for
        5 seconds before continue to process

        Args:
            alive (Boolean): Indicates whether gateway is alive and polling PVs
        """
        log.debug("alive: %r", alive)
        self._gateway_alive = alive

    def queue_cmd(self, cmd, data, priority=PRIORITY.NORMAL):
        """using pickle to serialise cmd, data, and priority and put them into
        a priorityQueue _queue_cmd.

        Reason to use pickle.dumps here: a TypeError will occur when a tuple or
        dict are put into a PriorityQueue with same priority, there are many
        will to solve this, I choose to pass a serialized object and deserialize
        it when we need it.

        Args:
            cmd (int): a integer defined in CMD class in the definition.py file
            represents operations(can be find in worker_cmd())
            data (str/tuple): data format is (ip_key, port, data) if cmd is PV_RESPONSE(1100)
                or PV_REQUEST(1200), the data is a string PV name if cmd is PV_FORGET(1400)

            priority (int, optional): an integer value defined in PRIORITY class
            in the definition.py file. Higher priority item servered first in a
            PriorityQueue.
                Defaults to PRIORITY.NORMAL.
        """
        serialized = pickle.dumps((priority, (cmd, data)))
        self._queue_cmd.put_nowait(serialized)

    def set_poll_cache(self, new_cache):
        """Set a new PollCache object

        Args:
            poll_cache (PollCache): a PollCache object defined in poll_cache.py
        """
        self._new_cache = new_cache

    def set_tracker(self, tracker):
        """Set self._tracker to tracker, dataman.py calls this function

        Args:
            tracker (RequestTracker): RequestTracker object defined in tacker
            file. tracker moves captured data from the capture queue into the
            longer term storage queue.
        """
        self._tracker = tracker

    def set_searcher(self, searcher):
        """Set self._searcher to the searcher, dataman.py calls this function

        Args:
            searcher (NewSearcher): object defined in gateway_client, search PV
            from the network
        """
        self._searcher = searcher

    def set_dataman(self, dataman):
        """Set data manager

        Args:
            dataman (DataManager): object defined in the dataman.py
        """
        self._dataman = dataman

    def start(self):
        """Start all Timer"""
        log.debug("called")
        self._timer_save.start()
        self._timer_poll.start()
        self._timer_map_to_ioc.start()
        self._timer_process_data.start()
        self._timer_linuxmon_pvs_read.start()
        self._timer_cmd.start()

    def load(self):
        """load PVs from _ioc_apps and load_pvs: this means load PVs from both
        file names with "find_pv" and "pvs"
        """
        self._ioc_apps.load()
        self.load_pvs()

    def load_pvs(self):
        """load PVs into _pvs_dict"""

        saver = FileSaver(prefix="pvs")
        newest_file = saver.get_newest()

        linuxmon_dict = {}
        pv_dict = {}

        if newest_file:
            f = None

            try:
                start_time = time.time()
                f = open(newest_file, "r")

                data = json.load(f)

                elapsed_time = time.time() - start_time
                log.debug(
                    "%s read time: %.2f sec %s",
                    newest_file,
                    elapsed_time,
                    str(data),
                )

                # Legacy files (i.e., from the old PV monitor) keep the list of PVs in
                # sub-dict called 'PVS'
                if DB_KEY.PVS in data:
                    pv_dict = data.get(DB_KEY.PVS)

                linuxmon_dict = data.get(DB_KEY.LINUXMON_PVS)
                self._linuxmon_backoff = data.get(DB_KEY.LINUXMON_BACKOFF, {})

            finally:
                if f:
                    f.close()
        else:
            pv_dict = {}

        dropped_list = self.load_linuxmon(linuxmon_dict)

        purge_count = 0
        for pv_name_raw, data in pv_dict.items():
            pv_name, valid = validate_pv_name_new(pv_name_raw)

            if not valid:
                log.warning("BAD PV DETECTED!!!!!! %s", pv_name)
                print_binary(pv_name)
                continue

            if PV_KEY.SERVER_KEY not in data:
                server_key = make_server_key(data)
                log.debug(
                    "PV: %s server_key: %s",
                    pv_name,
                    server_key,
                )
                data[PV_KEY.SERVER_KEY] = server_key

            if pv_name in self._pvs_dict:
                # This is happening because the validate_pv function is
                # stripping .C from PV names.  With PVs like:
                #
                # breem:BL1605-ID2-1:constants.C
                # breem:BL1605-ID2-1:constants
                #
                # there are duplicates!  I am not sure how the PV with the
                # .C extension got into the database to startwith
                log.warning(
                    "WARNING!! pvs dict already has PV: %r",
                    pv_name,
                )
                continue

            if self.purge_pv(pv_name, purge_count, pv_data=data):
                purge_count += 1
                continue

            self._pvs_dict[pv_name] = data

        self.purge_old_linuxmon(dropped_list)

    def load_linuxmon(self, linuxmon_dict):
        """load linux monitor list from linuxmon_dict

        Args:
            linuxmon_dict (dict): a dictionary stores data from the file with
            prefix "pvs". Called in load_pvs()

        Returns:
            list: return a list of formatted linuxmon(a dictionary of dictionary)
        """
        # ----------------------------------------------------------------------
        #  Loop to load linuxmon data
        # ----------------------------------------------------------------------

        if linuxmon_dict is None:
            return []
        dropped_list = []
        temp = {}
        for k, v in linuxmon_dict.items():
            if len(k) < 5:
                log.error(
                    "BAD LINUXMON PV DETECTED: %r",
                    k,
                )
                continue

            prefix = k.strip()
            if prefix.lower() == "unknown":
                log.error(
                    "BAD LINUXMON PV DETECTED: %r",
                    k,
                )
                continue

            vk = [k for k in v.keys()]

            if len(vk) < 2:
                log.error(
                    "Dropping never seen LinuxMon entry %d for %s",
                    len(dropped_list),
                    prefix,
                )
                dropped_list.append(prefix)
                continue

            temp[prefix] = v

        self._linuxmon_dict = temp

        return dropped_list

    def purge_old_linuxmon(self, dropped_list):
        """_summary_

        Args:
            dropped_list (_type_): _description_
        """

        delete_count = 0
        for prefix in dropped_list:
            log.info("PURGE LinuxMon prefix: %s", prefix)
            for suffix in LINUXMON_PV_SUFFIX_POLL_ALL:
                pv_name = "%s:%s" % (prefix, suffix)

                try:
                    del self._pvs_dict[pv_name]
                    delete_count += 1
                    log.info(
                        "Deleted PV: %s (delete count: %d)",
                        pv_name,
                        delete_count,
                    )
                except:
                    pass

    def get_pvs_new_count(self):
        return self._count_pvs_new

    def get_req_counts(self):
        result = (self._count_req_packets, self._count_req_pvs)
        self._count_req_packets = 0
        self._count_req_pvs = 0
        return result

    def get_pvs_lost(self, offset, count=None, sort=SORT_MODE.LAST_SEEN):
        log.debug(
            "called; offset: %r count: %r sort: %r",
            offset,
            count,
            sort,
        )

        try:
            self._pvs_lost_lock.acquire()

            if sort == SORT_MODE.LAST_SEEN:
                dataset = self._pvs_sorted_lost_last_seen
            elif sort == SORT_MODE.REQ_TIME:
                dataset = self._pvs_sorted_lost_req_time
            elif sort == SORT_MODE.REQ_COUNT:
                dataset = self._pvs_sorted_lost_req_count
            elif sort == SORT_MODE.NAME:
                dataset = self._pvs_sorted_lost_name
            else:
                log.warning("sort mode not supported: %s", sort)
                dataset = []

            total_len = len(dataset)

            if count is None:
                result = copy.deepcopy(dataset[offset:])
            else:
                result = copy.deepcopy(dataset[offset : offset + count])

            return result, total_len

        finally:
            self._pvs_lost_lock.release()

    def get_pvs_duplicate(self, offset, count=None, sort=SORT_MODE.NAME):
        log.debug(
            "called; offset: %r count: %r sort: %r",
            offset,
            count,
            sort,
        )

        try:
            self._pvs_duplicate_lock.acquire()

            if sort == SORT_MODE.NAME:
                dataset = self._pvs_duplicate
            else:
                log.warning("sort mode not supported: %s", sort)
                dataset = []

            total_len = len(dataset)

            if count is None:
                result = copy.deepcopy(dataset[offset:])
            else:
                result = copy.deepcopy(dataset[offset : offset + count])

            return result, total_len

        finally:
            self._pvs_duplicate_lock.release()

    def get_pvs_invalid(self, offset, count=None, sort=SORT_MODE.REQ_TIME):
        log.debug(
            "called; offset: %r count: %r sort: %r",
            offset,
            count,
            sort,
        )

        try:
            self._pvs_invalid_lock.acquire()

            if sort == SORT_MODE.REQ_TIME:
                dataset = self._pvs_sorted_invalid_req_time
            elif sort == SORT_MODE.REQ_COUNT:
                dataset = self._pvs_sorted_invalid_req_count
            elif sort == SORT_MODE.NAME:
                dataset = self._pvs_sorted_invalid_name
            else:
                log.warning("sort mode not supported: %s", sort)
                dataset = []

            total_len = len(dataset)
            if count is None:
                result = copy.deepcopy(dataset[offset:])
            else:
                result = copy.deepcopy(dataset[offset : offset + count])

            return result, total_len

        finally:
            self._pvs_invalid_lock.release()

    def get_iocapps_data(self, pv_name):
        return self._ioc_apps.get_data(pv_name)

    def worker_save(self):
        thread_id = TM.add("PVMan: Data saver", 1200)

        last_time = time.time()

        while True:
            log.debug("running")
            TM.ping(thread_id)
            time.sleep(10)

            cur_time = time.time()
            if (cur_time - last_time) > config.SAVE_INTERVAL: 
                self.save(thread_id)
                last_time = cur_time

    def save(self, thread_id):

        log.debug("called")

        start_time = time.time()

        # ----------------------------------------------------------------------
        # Get the PVs dict -- release the lock periodically
        # ----------------------------------------------------------------------
        pvs_dict = {}
        try:
            self._pv_lock.acquire()
            keys = [pv_name for pv_name in self._pvs_dict.keys()]
        finally:
            self._pv_lock.release()

        log.debug("Want to save keys: %d", len(keys))
        time.sleep(0.1)

        self._pv_lock.acquire()
        count = 0
        for pv_name in keys:
            count += 1
            if count == 50:
                # pausing .2 seconds after every 50 PVs takes a long time
                # but if this is not done the CPU becomes very busy and
                # it seems like some of the message queues start to fall
                # behind
                count = 0
                self._pv_lock.release()
                time.sleep(0.2)
                TM.ping(thread_id)
                self._pv_lock.acquire()

            data = self._pvs_dict.get(pv_name)
            if not data:
                continue

            pvs_dict[pv_name] = copy.deepcopy(data)

        self._pv_lock.release()

        # ----------------------------------------------------------------------
        # Get the PVs dict -- release the lock periodically
        # ----------------------------------------------------------------------
        self._linuxmon_lock.acquire()
        linuxmon_pvs = copy.deepcopy(self._linuxmon_dict)
        backoff = copy.deepcopy(self._linuxmon_backoff)
        self._linuxmon_lock.release()

        data = {
            DB_KEY.PVS: pvs_dict,
            DB_KEY.LINUXMON_PVS: linuxmon_pvs,
            DB_KEY.LINUXMON_BACKOFF: backoff,
        }

        data_str = json.dumps(data)
        saver = FileSaver(prefix="pvs")
        saver.save(data_str)

        elapsed_time = time.time() - start_time

        self._count_saves += 1
        STATS.set("PV Saves", self._count_saves)
        STATS.set("PV Save Time (sec)", int(elapsed_time))

        log.debug("Save time: %.2f", elapsed_time)

    def get_next_follower(self):
        try:
            self._poll_follower_lock.acquire()

            while True:
                if not self._poll_follower_list:
                    return None

                pv_name = self._poll_follower_list.pop(0)

                log.debug("Poll PV follower: %r", pv_name)
                return pv_name

        finally:
            self._poll_follower_lock.release()

    def get_leader_pv(self):
        pv_name = self._poll_cache.get_next_leader()

        log.debug("Poll PV leader: %r", pv_name)

        return pv_name

    def worker_poll(self):
        """
        This worker polls the PVs to detect hosts (and LOST PVs)
        """
        thread_id = TM.add("PVMan: PVs Poll", 60)

        next_log_time = time.time()
        next_ping_time = time.time()
        pv_list = []

        polls = 0
        start_time = 0
        purge_count = 0
        skip_invalid = 0

        rate_max = 50
        temp_count = 0

        invalid_pvs = []

        while True:
            cur_time = time.time()

            if cur_time > next_ping_time:
                TM.ping(thread_id)
                next_ping_time += 20

            if cur_time >= next_log_time:
                log.debug(
                    "pv_list: %d gw_q: %d",
                    len(pv_list),
                    self._gateway_queue_size,
                )
                next_log_time += 30
                STATS.set("Poll Remaining", len(pv_list))
                STATS.set("Queue Size: GW Rx", self._gateway_queue_size)

            if len(pv_list) == 0:
                self._new_cache.reset()

                self._pv_lock.acquire()
                pv_list = copy.deepcopy([k for k in self._pvs_dict.keys()])
                self._pv_lock.release()

                temp = [
                    (random.randint(0, 10000000), pv_name)
                    for pv_name in pv_list
                ]
                temp.sort()
                pv_list = [item[1] for item in temp]

                invalid_pvs = self.get_pvs_invalid(0)

                log.info("POLL DONE ")

                if start_time:
                    elapsed_time = cur_time - start_time
                    STATS.set("Poll Time", int(elapsed_time))

                STATS.set("Poll Size", len(pv_list))
                STATS.set("Poll Remaining", len(pv_list))
                STATS.set("Poll Invalid", skip_invalid)
                STATS.set("Poll Purge", purge_count)
                STATS.set("Invalid PVs", len(invalid_pvs))
                STATS.set("Polls complete", polls)

                polls += 1
                skip_invalid = 0
                purge_count = 0

                start_time = cur_time

            if not self._gateway_alive:
                log.debug("gateway not alive, not polling PVs")
                time.sleep(5)
                continue

            if self._gateway_queue_size > 200:
                log.debug(
                    "gw_q:% d, not polling",
                    self._gateway_queue_size,
                )
                time.sleep(1)
                continue

            ###################################################################
            # Check for followers
            ###################################################################
            len_before = None
            try:
                self._followers_lock.acquire()
                if len(self._followers) > 0:
                    len_before = len(pv_list)
                    pv_list.extend(self._followers)
                    self._followers = []
            finally:
                self._followers_lock.release()

            if len_before:
                len_after = len(pv_list)
                log.debug(
                    "Added poll followers: %d --------------------> %d",
                    len_before,
                    len_after,
                )

            # Get the next PV fol polling
            pv_name = pv_list.pop()

            # Check if this PV is invalid
            if pv_name in invalid_pvs:
                skip_invalid += 1
                continue

            if self._new_cache.check(pv_name):
                continue

            self._searcher.search(pv_name, PRIORITY.NORMAL)

            temp_count += 1
            if temp_count >= rate_max:
                time.sleep(1)
                temp_count = 0

    def linuxmon_pvs_find(self, pv_list):
        """
        This utility looks for linuxmon PVs in the list of PVs and if one
        is found that we don't know about, it is added to a list for checking
        """
        log.debug("called, len(pv_list): %d", len(pv_list))

        ioc_name_list = []
        for pv_name in pv_list:
            if not pv_name.endswith(LINUXMON_PV_SIGNATURE):
                continue

            parts = pv_name.split(":")
            if len(parts) != 2:
                log.warning(
                    "Cannot process linuxmon PV: %s",
                    pv_name,
                )
                continue

            ioc_name = parts[0].strip()
            ioc_name_list.append(ioc_name)

        try:
            self._linuxmon_lock.acquire()
            for ioc_name in ioc_name_list:
                if ioc_name in self._linuxmon_dict:
                    continue
                self._linuxmon_check_dict[ioc_name] = True
        finally:
            self._linuxmon_lock.release()

    def purge_pv(self, pv_name, purge_count, pv_data=None):
        if pv_data is None:
            pv_data = self.get_pv_data(pv_name)
        if pv_data is None:
            return False

        cur_time = time.time()
        pv_purge_age = config.PV_PURGE_AGE
        lost, host, port, last_time_seen = self.get_pv_latest_info(pv_data)
        if last_time_seen:
            age_sec = cur_time - last_time_seen
            age_str = make_last_seen_str(age_sec)
            if age_sec < pv_purge_age:
                return False

        requests = self.get_requests_sorted(pv_data)

        last_time_sec = None
        last_ioc = None
        for request in requests:
            last_ioc = request[1]
            last_time_sec = request[0]
            break

        if last_time_sec:
            age_sec = cur_time - last_time_sec

            if age_sec < pv_purge_age:
                return False

            last_name = get_host_name(last_ioc)
            age_str = make_last_seen_str(age_sec)

            log.info(
                "PURGE: %d %s: LAST REQUEST: %s (%s)",
                purge_count,
                pv_name,
                age_str,
                last_name,
            )

        return True

    def worker_map_to_ioc(self):
        thread_id = TM.add("PVMan: PVs Map to IOCs", 60)
        last_ping_time = time.time()

        while True:
            cur_time = time.time()
            if cur_time - last_ping_time > 30:
                TM.ping(thread_id)
                last_ping_time = cur_time

            # Get a list of all PVs
            start_time = time.time()
            self._pv_lock.acquire()
            local_list = [k for k in self._pvs_dict.keys()]
            self._pv_lock.release()
            total_len = len(local_list)
            elapsed_time = time.time() - start_time

            if elapsed_time > 5:
                log.debug(
                    "%.2f seconds to get %d pv names for mapping",
                    elapsed_time,
                    total_len,
                )

            self.linuxmon_pvs_find(local_list)

            # Sort the pvs for display
            temp = [(name.lower(), name) for name in local_list]
            temp.sort()
            temp = [item[1] for item in temp]

            # The PVS sorted list is used to display "All" the PVs
            self._pvs_all_lock.acquire()
            self._pvs_sorted_all_name = temp
            self._pvs_all_lock.release()

            # Add the invalid PVs to the temp list for searching
            try:
                self._pvs_invalid_lock.acquire()
                pv_names_invalid = [k for k in self._pvs_invalid.keys()]
            finally:
                self._pvs_invalid_lock.release()

            pv_names_invalid.sort()
            temp.extend(pv_names_invalid)
            temp = set(temp)

            self._pvs_search_lock.acquire()
            self._pvs_search_set = temp
            self._pvs_search_lock.release()

            i = 0
            j = 0

            result = {}
            pvs_all_req_count = []
            pvs_all_req_time = []

            pvs_lost_name = []
            pvs_lost_req_count = []
            pvs_lost_req_time = []
            pvs_lost_last_seen = []

            pvs_duplicate = []

            # Loop through all the PVs in the local list
            for pv_name in local_list:
                # Ping the thread monitor if required
                cur_time = time.time()
                if cur_time - last_ping_time > 30:
                    TM.ping(thread_id)
                    last_ping_time = cur_time

                # Get the PV's data.  It is possible that the PV was deleted underfoot
                data = self.get_pv_data(pv_name)
                if not data:
                    log.debug(
                        "Map: No data for PV %s... forgotten?",
                        pv_name,
                    )
                    continue

                lost, host, port, last_time_seen = self.get_pv_latest_info(data)

                log.debug(
                    "pv_name: %r lost: %r host: %r port: %r last_time_seen: %r",
                    pv_name,
                    lost,
                    host,
                    port,
                    last_time_seen,
                )

                # Keep track of duplicate PVs
                if PV_KEY.DUPLICATES in data:
                    pvs_duplicate.append(pv_name)

                # List to find top requested PVs
                client_count = data.get(PV_KEY.CLIENT_REQ, 0)
                pvs_all_req_count.append((client_count, pv_name))

                # List to find the most recently requested PVs
                last_req = self.get_pv_latest_request(data)
                if last_req is None:
                    last_req_time = 0
                else:
                    last_req_time = last_req[3]

                pvs_all_req_time.append((last_req_time, pv_name))

                if lost:
                    pvs_lost_name.append(pv_name)
                    pvs_lost_req_count.append((client_count, pv_name))
                    pvs_lost_req_time.append((last_req_time, pv_name))
                    pvs_lost_last_seen.append((last_time_seen, pv_name))

                if host is None or port is None:
                    log.debug("No port for PV: %s", pv_name)
                    continue

                ioc = result.get(host, {})
                pv_list = ioc.get(port, [])
                pv_list.append((pv_name, lost, last_time_seen))
                ioc[port] = pv_list
                result[host] = ioc

                i += 1
                j += 1

                if j == 1000:
                    log.debug(
                        "Map PVs to IOCs: processed %d of %d PVs",
                        i,
                        total_len,
                    )
                    time.sleep(0.1)
                    j = 0

            # Sort the PV names in place
            for key, value in result.items():
                for port, pv_list in value.items():
                    pv_list.sort()

            # It looks like we are just updating the old list with a new one
            self._pv_to_ioc_map_lock.acquire()
            self._pv_to_ioc_map = result
            self._pv_to_ioc_map_lock.release()

            # Lost PVs sorted by name
            pvs_lost_name.sort()
            self._pvs_lost_lock.acquire()
            self._pvs_sorted_lost_name = pvs_lost_name
            self._pvs_lost_lock.release()

            # Lost PVs sorted by request time
            pvs_lost_req_time.sort()
            s = [item[1] for item in pvs_lost_req_time]
            s.reverse()
            self._pvs_lost_lock.acquire()
            self._pvs_sorted_lost_req_time = s
            self._pvs_lost_lock.release()

            # Lost PVs sorted by request count
            pvs_lost_req_count.sort()
            s = [item[1] for item in pvs_lost_req_count]
            s.reverse()
            self._pvs_lost_lock.acquire()
            self._pvs_sorted_lost_req_count = s
            self._pvs_lost_lock.release()

            pvs_lost_last_seen.sort()
            s = [item[1] for item in pvs_lost_last_seen]
            s.reverse()
            self._pvs_lost_lock.acquire()
            self._pvs_sorted_lost_last_seen = s
            self._pvs_lost_lock.release()

            pvs_duplicate.sort()
            self._pvs_duplicate_lock.acquire()
            self._pvs_duplicate = pvs_duplicate
            self._pvs_duplicate_lock.release()

            # All PVS sorted by request count
            pvs_all_req_count.sort()
            s = [item[1] for item in pvs_all_req_count]
            s.reverse()
            self._pvs_all_lock.acquire()
            self._pvs_sorted_all_req_count = s
            self._pvs_all_lock.release()

            # All PVS sorted by request time
            pvs_all_req_time.sort()
            s = [item[1] for item in pvs_all_req_time]
            s.reverse()
            self._pvs_all_lock.acquire()
            self._pvs_sorted_all_req_time = s
            self._pvs_all_lock.release()

            elapsed_time = time.time() - start_time

            log.debug(
                "time to map %s PVs to IOCs: %.3f",
                total_len,
                elapsed_time,
            )

            STATS.set("PV Sort Time", int(elapsed_time))

            self._count_mapping_runs += 1
            time.sleep(10)

    def get_ioc_port_pvs(
        self, ioc_id, port, offset=0, count=None, sort=SORT_MODE.NAME
    ):
        log.debug(
            "called; ioc_id: %d port: %d offset: %d count: %r",
            ioc_id,
            port,
            offset,
            count,
        )

        try:
            self._pv_to_ioc_map_lock.acquire()
            if sort == SORT_MODE.NAME:
                data = self._pv_to_ioc_map.get(ioc_id, {})
            else:
                raise ValueError("sort mode %s not supported" % sort)

            result = copy.deepcopy(data.get(port, []))

            total_len = len(result)
            if count:
                return result[offset : offset + count], total_len

            return result[offset:], total_len

        finally:
            self._pv_to_ioc_map_lock.release()

    def get_ioc_ports(self, ioc_key):

        log.debug("called; ioc_key: %d", ioc_key)

        try:
            self._pv_to_ioc_map_lock.acquire()
            data = self._pv_to_ioc_map.get(ioc_key, {})
            ports = [(port, len(pv_list)) for port, pv_list in data.items()]
            return ports

        finally:
            self._pv_to_ioc_map_lock.release()

    def worker_process_data(self):
        thread_id = TM.add("PVMan: Process Data", 120)
        while True:
            TM.ping(thread_id)
            time.sleep(60)
            self.sort_pvs_invalid()

    def forget_pv(self, pv_name):
        try:
            pv_name = str(pv_name)
        except:
            pass

        try:
            self._forget_lock.acquire()
            if pv_name in self._forget_dict:
                log.info("PV ALREADY QUEUED FOR DELETE: %s", pv_name)
                return
            self._forget_dict[pv_name] = 1
        finally:
            self._forget_lock.release()

        self.queue_cmd(CMD.PV_FORGET, pv_name)

    def load_test_data(self, file_name):
        log.debug("load test file: %s", file_name)

        f = open(file_name, "r")
        test_data = json.load(f)
        f.close()

        self._latest_requests_lock.acquire()
        self._latest_requests = test_data
        self._latest_requests_lock.release()

    def get_pvs_latest_requests_ioc(
        self, ioc_id, offset, count=0, sort=SORT_MODE.REQ_COUNT
    ):
        log.debug(
            "called; ioc_id: %d offset: %d count: %d, sort: %s",
            ioc_id,
            offset,
            count,
            sort,
        )
        start_time = time.time()

        try:
            self._latest_requests_lock.acquire()
            requests = self._latest_requests_by_ioc.get(ioc_id, [])
            requests = copy.deepcopy(requests)
            total_requests = len(requests)
            elapsed_time = time.time() - start_time
        finally:
            self._latest_requests_lock.release()

        log.debug("request count: %d", total_requests)

        pv_dict = {}
        pv_time = {}
        for request in requests:
            pv_name = request[0]
            req_time = request[2]
            pv_dict[pv_name] = pv_dict.get(pv_name, 0) + 1

            t = pv_time.get(pv_name, 0)
            if req_time > t:
                pv_time[pv_name] = req_time

        if sort == SORT_MODE.REQ_COUNT:
            temp = [
                (req_count, pv_name) for pv_name, req_count in pv_dict.items()
            ]
            temp.sort()
            temp.reverse()

            pv_list = [item[1] for item in temp]

        elif sort == SORT_MODE.NAME:
            pv_list = [pv_name for pv_name in pv_dict.keys()]
            pv_list.sort()
            log.info("THIS IS THE LENGTH OF THE PV LIST: %s", len(pv_list))
        elif sort == SORT_MODE.REQ_TIME:
            temp = [(t, pv_name) for pv_name, t in pv_time.items()]
            temp.sort()
            temp.reverse()
            pv_list = [item[1] for item in temp]

        else:
            raise ValueError("sort mode %r not supported" % sort)

        pvs = pv_list[offset : offset + count]

        counts = {}
        for pv_name in pvs:
            count = pv_dict.get(pv_name)
            percentage = 100.0 * float(count) / float(total_requests)
            counts[pv_name] = (count, percentage)

        log.debug(
            "elapsed_time: %.2f (len: %d)",
            elapsed_time,
            len(pvs),
        )
        return pvs, len(pv_list), counts

    def get_latest_requests(self, offset=0):
        start_time = time.time()

        try:
            self._latest_requests_lock.acquire()
            requests = copy.deepcopy(self._latest_requests)
            elapsed_time = time.time() - start_time
        finally:
            self._latest_requests_lock.release()

        log.debug(
            "elapsed_time: %.2f (len: %d)",
            elapsed_time,
            len(requests),
        )
        return requests

    def sort_pvs_invalid(self):
        self._pvs_invalid_lock.acquire()
        pvs = copy.deepcopy([k for k in self._pvs_invalid.keys()])
        self._pvs_invalid_lock.release()

        pvs_name = []
        pvs_req_count = []
        pvs_req_time = []

        for pv in pvs:
            pv_data = self.get_pv_invalid_data(pv)
            if pv_data is None:
                log.debug("no data for pv: %s", pv)
                continue

            result = self.get_pv_latest_request(pv_data)
            if result is None:
                t = 0
            else:
                t = result[3]
            pvs_req_time.append((t, pv))

            req_count = pv_data.get(PV_KEY.CLIENT_REQ, 0)
            pvs_req_count.append((req_count, pv))
            pvs_name.append(pv)

        pvs_req_time.sort()
        s = [item[1] for item in pvs_req_time]
        s.reverse()
        self._pvs_invalid_lock.acquire()
        self._pvs_sorted_invalid_req_time = s
        self._pvs_invalid_lock.release()

        pvs_req_count.sort()
        s = [item[1] for item in pvs_req_count]
        s.reverse()
        self._pvs_invalid_lock.acquire()
        self._pvs_sorted_invalid_req_count = s
        self._pvs_invalid_lock.release()

        pvs_name.sort()
        self._pvs_invalid_lock.acquire()
        self._pvs_sorted_invalid_name = pvs_name
        self._pvs_invalid_lock.release()

    def forget_linuxmon(self, ioc_name):
        try:
            self._linuxmon_lock.acquire()

            try:
                del self._linuxmon_dict[ioc_name]
                return True
            except:
                pass

            return False

        finally:
            self._linuxmon_lock.release()

    def has_linuxmon(self, name):
        try:
            self._linuxmon_lock.acquire()
            if name in self._linuxmon_dict:
                return True
            return False

        finally:
            self._linuxmon_lock.release()

    def get_linuxmon_dict(self):
        try:
            self._linuxmon_lock.acquire()
            return copy.deepcopy(self._linuxmon_dict)

        finally:
            self._linuxmon_lock.release()

    def linuxmon_set(self, prefix, key, value, clear_time=False):
        try:
            self._linuxmon_lock.acquire()
            data = self._linuxmon_dict.get(prefix, {})
            data[key] = value
            self._linuxmon_dict[prefix] = data

            if clear_time:
                try:
                    del self._linuxmon_dict["TIME"]
                except:
                    pass

        finally:
            self._linuxmon_lock.release()

    def caget(self, pv_name):
        log.debug("FETCH PV: %s", pv_name)

        value = self._dataman.fetch_caget(pv_name)

        if value is None:
            raise ValueError("value is none")

        if value.startswith("EPICS"):
            raise ValueError(repr(value))

        if value.startswith("EXCEPTION"):
            raise ValueError(repr(value))

        return value

    def linuxmon_backoff_clear(self, prefix):
        try:
            self._linuxmon_lock.acquire()
            self._linuxmon_backoff[prefix] = 0
            self._linuxmon_backoff_last[prefix] = 0
            data = self._linuxmon_dict.get(prefix, {})
            data["T"] = int(time.time())
            self._linuxmon_dict[prefix] = data
        finally:
            self._linuxmon_lock.release()

    def linuxmon_backoff_get(self, prefix):
        try:
            self._linuxmon_lock.acquire()
            backoff = self._linuxmon_backoff.get(prefix, 0)
            if backoff > 0:
                backoff -= 1
            self._linuxmon_backoff[prefix] = backoff
            return backoff
        finally:
            self._linuxmon_lock.release()

    def linuxmon_backoff_set(self, prefix):
        try:
            self._linuxmon_lock.acquire()
            last_backoff = self._linuxmon_backoff_last.get(prefix, 0)
            self._linuxmon_backoff[prefix] = last_backoff
            last_backoff += random.randint(1, 2)
            if last_backoff > 20:
                last_backoff = 20
            self._linuxmon_backoff_last[prefix] = last_backoff
        finally:
            self._linuxmon_lock.release()

    def worker_linuxmon_pvs_read(self):
        thread_id = TM.add("PVMan: LinuxMonitor PVs Read", 60)
        last_ping_time = time.time()

        while True:
            time.sleep(10)
            TM.ping(thread_id)

            start_time = time.time()
            # Get the list of linuxmon PV prefixes to poll
            self._linuxmon_lock.acquire()
            prefix_list = [key for key in self._linuxmon_dict.keys()]

            # Make a list of ioc_names to check... just check one each time through this list
            if not self._linuxmon_check_list:
                self._linuxmon_check_list = [
                    key for key in self._linuxmon_check_dict.keys()
                ]
            self._linuxmon_lock.release()

            if len(self._linuxmon_check_list):
                ioc_name = self._linuxmon_check_list.pop(0)

                log.debug(
                    "checking linuxmon ioc_name %s (lan: %d)",
                    ioc_name,
                    len(self._linuxmon_check_list),
                )

                pv_name = "%s:%s" % (ioc_name, LINUXMON_PV_SIGNATURE)
                try:
                    value = self.caget(pv_name)

                    log.debug(
                        "adding %s to linuxmon data",
                        pv_name,
                    )
                    self._linuxmon_lock.acquire()

                    if ioc_name not in self._linuxmon_dict:
                        self._linuxmon_dict[ioc_name] = {}

                    try:
                        del self._linuxmon_check_dict[ioc_name]
                    except:
                        pass
                    self._linuxmon_lock.release()

                except Exception as err:
                    log.exception(
                        "failed to get PV %s: %r",
                        pv_name,
                        err,
                    )

            # Sorting just helps with debugging
            prefix_list.sort()
            for prefix in prefix_list:
                try:
                    self._linuxmon_lock.acquire()
                    data = self._linuxmon_dict.get(prefix)
                    last_time = data.get("T", 0)
                    if (time.time() - last_time) > config.MAX_LINUXMON_AGE:
                        log.debug(
                            "%s too old; skip poll",
                            prefix,
                        )
                        continue
                finally:
                    self._linuxmon_lock.release()

                log.debug(
                    "prefix: %s (len list: %d)",
                    prefix,
                    len(prefix_list),
                )
                backoff = self.linuxmon_backoff_get(prefix)
                if backoff:
                    log.debug(
                        "skipping poll for prefix %s (backoff: %d)",
                        prefix,
                        backoff,
                    )
                    continue

                for suffix in LINUXMON_PV_SUFFIX_POLL:
                    cur_time = time.time()
                    if cur_time - last_ping_time:
                        TM.ping(thread_id)
                        last_ping_time = cur_time

                    # Treat this suffix as special.  It must be first in the list
                    if suffix == LINUXMON_PV_SIGNATURE:
                        try:
                            ip_addr = socket.gethostbyname(prefix)
                            ioc_id = get_ip_key(ip_addr)
                        except Exception as err:
                            log.exception("Exception: %r: %r", prefix, err)
                            ioc_id = 0

                        self.linuxmon_set(prefix, KEY.IOC_ID, ioc_id)

                    pv_name = "%s:%s" % (prefix, suffix)

                    value = None
                    success = False
                    for _ in range(3):
                        try:
                            value = str(self.caget(pv_name))
                            success = True
                            break
                        except Exception as err:
                            log.exception(
                                "PV %s fetch failed: %r",
                                pv_name,
                                err,
                            )
                            time.sleep(0.1)
                            continue

                    if not success:
                        if suffix == LINUXMON_PV_SIGNATURE:
                            self.linuxmon_backoff_set(prefix)
                            break
                        else:
                            continue

                    log.debug(
                        "SUCCESS: %s: %r (%s)",
                        suffix,
                        value,
                        type(value),
                    )

                    if suffix == "UPTIME":
                        value = linuxmon_uptime_to_int(value)
                    elif suffix == "TIME":
                        value = linuxmon_date_to_int(value)

                    self.linuxmon_set(prefix, suffix, value)

                    if suffix == LINUXMON_PV_SIGNATURE:
                        self.linuxmon_set(
                            prefix, "T", cur_time, clear_time=True
                        )
                        self.linuxmon_backoff_clear(prefix)

                    time.sleep(0.1)

            elapsed_time = int(time.time() - start_time)
            STATS.set("Linuxmon Loop (sec)", elapsed_time)

    def worker_cmd(self):
        """
        This worker handles commands from other managers
        """
        thread_id = TM.add("PVMan: CMD worker", 300)
        next_ping_time = time.time()
        next_log_time = time.time()
        max_queue_size = 0

        map = {
            CMD.HEARTBEAT_PV: self.handle_cmd_heartbeat_pv,
            CMD.PV_RESPONSE: self.handle_cmd_pv_response,
            CMD.PV_REQUEST: self.handle_cmd_pv_request,
            CMD.PV_FORGET: self.handle_cmd_pv_forget,
        }

        while True:
            qsize = self._queue_cmd.qsize()
            if qsize > 1000000:
                # This can run away on us and cause database to get flooded with bogus events
                log.error(
                    "PANIC!!!!! PVman qsize: %d TERMINATING!!!",
                    qsize,
                )
                STATS.set("PANIC: PVman qsize", qsize)
                break

            if qsize > max_queue_size:
                max_queue_size = qsize
                STATS.set("Max Queue Size: PVman", max_queue_size)

            cur_time = time.time()
            if cur_time > next_ping_time:
                TM.ping(thread_id)
                next_ping_time += 30

            if cur_time > next_log_time:
                STATS.set("Queue Size: PVman", qsize)
                next_log_time += 30

            try:
                serialized = self._queue_cmd.get(block=True, timeout=10)
                thing = pickle.loads(serialized)

            except queue.Empty:
                continue

            ### priority = thing[0]
            item = thing[1]
            cmd = item[0]
            data = item[1]
            func = map.get(cmd)
            func(data)

    def handle_cmd_heartbeat_pv(self, data):
        """
        Called when a PV fetched from a heartbeat.  This replaces the old
        method in which the heartbeat code did a CA_PROTO_SEARCH on the PV
        so that it would be found via the response
        """

        linuxmon_flag = False

        try:
            self._pv_lock.acquire()

            pv_name = data.get(KEY.NAME)

            if not isinstance(pv_name, str):
                pv_name = str(pv_name)

            pv_data = self._pvs_dict.get(pv_name)

            # Look for LinuxMonitor PVs.  This is how new LinuxMonitor
            # instances are detected
            if pv_name.endswith(LINUXMON_PV_SIGNATURE):
                linuxmon_flag = True

            if not pv_data:
                log.debug("HEARTBEAT PV: %s", pv_name)

                # If the PV has never been seen before, we must add it to the
                # list of PVs with a known "seen" time.  It is unlikely to be
                # a duplicate, and if it is, it should be marked as such when it
                # is eventually polled
                host = data.get(KEY.IOC_ID)
                port = data.get(KEY.PORT)

                item = {
                    PV_KEY.HOST: host,
                    PV_KEY.LAST_TIME: data.get(KEY.LAST_TIME),
                    PV_KEY.FIRST_TIME: data.get(KEY.LAST_TIME),
                    PV_KEY.PORT: port,
                }

                server_key = make_server_key([[host, port]])

                pv_data = {
                    PV_KEY.REQUESTS: [],
                    PV_KEY.CLIENT_REQ: 0,
                    PV_KEY.REQ_COUNT: 0,
                    PV_KEY.INFO: [item],
                    PV_KEY.SERVER_KEY: server_key,
                }

                # If the PV was reported via a heartbeat then there is no need
                # to confirm that it is a valid EPICS PV name
                self._pvs_dict[pv_name] = pv_data
                if self._poll_cache:
                    self._poll_cache.update_pv(pv_name, server_key)

        finally:
            self._pv_lock.release()

        if linuxmon_flag:
            ioc_name = pv_name[: -len(LINUXMON_PV_SIGNATURE)].strip(":")
            log.debug(
                "Found linuxmon PV in heartbeat: %s",
                pv_name,
            )
            self.linuxmon_backoff_clear(ioc_name)

            try:
                self._searcher.search(
                    "%s:%s" % (ioc_name, LINUXMON_PV_SIGNATURE),
                    priority=PRIORITY.HIGH,
                )

                self._linuxmon_lock.acquire()
                # This should be the only place new entries are added.
                # Because this is a heartbeat detected PV, it should be "alive"

                if ioc_name not in self._linuxmon_dict:
                    self._linuxmon_dict[ioc_name] = {"T": int(time.time())}
            finally:
                self._linuxmon_lock.release()

    def add_pv_request(self, input, ip, port, cur_time):
        found = None
        result = []
        for item in input:
            if item[0] == ip and item[1] == port:
                found = item
            else:
                result.append(item)

        if found:
            new = [found[0], found[1], found[2] + 1, cur_time]
        else:
            new = [ip, port, 1, cur_time]

        result.append(new)

        # Only let list grow to length 10
        return result[-10:]

    def pv_name_strip_nulls(self, pv_name_raw):
        pv_name = ""

        for i, c in enumerate(pv_name_raw):
            d = ord(c)
            if d == 0:
                continue
            pv_name += c

        return pv_name

    def add_bad_pv(self, pv_name_raw, ip_key, port):
        cur_time = int(time.time())
        try:
            self._pvs_invalid_lock.acquire()
            pv_name = self.pv_name_strip_nulls(pv_name_raw)

            # Test that this key will export to a JSON file
            try:
                pv_name = str(pv_name)
                json.dumps({"n": pv_name})
            except Exception as e:
                log.exception(
                    "ERROR: %s Exception json encoding PV: storing as binary",
                    str(e),
                )
                print_binary(pv_name, prnt=False)

                x = []
                for c in pv_name:
                    x.append("%02X" % ord(c))
                pv_name = " ".join(x)

            if pv_name == "":
                pv_name = str("NULL")

            pv = self._pvs_invalid.get(pv_name, {})

            # The request count was supposed to be "all" requests. gateway
            # and client.  But gateway requests are no longer being reported
            req_count = pv.get(PV_KEY.REQ_COUNT, 0)
            req_count += 1
            pv[PV_KEY.REQ_COUNT] = req_count
            pv[PV_KEY.CLIENT_REQ] = req_count
            pv[KEY.PV_INVALID] = True

            requests = pv.get(PV_KEY.REQUESTS, [])
            requests = self.add_pv_request(requests, ip_key, port, cur_time)
            pv[PV_KEY.REQUESTS] = requests

            self._pvs_invalid[pv_name] = pv

        finally:
            self._pvs_invalid_lock.release()

    def add_latest_request(self, pv_name, ioc_id, port, cur_time):
        self._latest_requests_lock.acquire()
        self._latest_requests.append((pv_name, ioc_id, port, cur_time))
        self._latest_requests = self._latest_requests[
            -SETTINGS.LATEST_REQUEST_PVS :
        ]

        ioc_requests = self._latest_requests_by_ioc.get(ioc_id, [])
        ioc_requests.append((pv_name, port, cur_time))
        ioc_requests = ioc_requests[-SETTINGS.LATEST_REQUEST_IOCS :]
        self._latest_requests_by_ioc[ioc_id] = ioc_requests
        self._latest_requests_lock.release()

    def handle_cmd_pv_request(self, data_in):
        """ """
        ip_key = data_in[0]
        port = data_in[1]
        data = data_in[2]

        log.debug(
            "called; ip_key: %d port: %d len data: %d",
            ip_key,
            port,
            len(data),
        )

        log_request = False
        self._count_req_packets += 1

        search_list = []
        duplicate = []

        # Use ints not floats to keep JSON smaller
        cur_time = int(time.time())
        try:
            self._tracker.add_lock()
            self._pv_lock.acquire()
            total_length = len(data)

            while True:
                # Unpack the message header
                try:
                    c = struct.unpack_from("HHHHII", data)
                except Exception as err:
                    log.exception(
                        "Exception in header unpack: %s",
                        str(err),
                    )
                    return

                command = socket.ntohs(c[0])
                payload_len = socket.ntohs(c[1])
                msg_len = EPICS.HDR_LEN + payload_len

                if command == 6:
                    if payload_len == 0:
                        log.error(
                            "ERROR: no payload in request",
                        )
                        break

                    self._test_pv_req_count += 1
                    self._test_pv_req_size += msg_len

                    # The PV requests definitely have different lengths so
                    # we cannot compute the number of requests in a msg
                    # based on its length

                    pv_name_raw = data[EPICS.HDR_LEN : msg_len]

                    pv_name, valid = validate_pv_name_new(pv_name_raw)

                    if pv_name in duplicate:
                        if log_request:
                            # This could be from trimming things like .PROC etc
                            log.warning(
                                "DUPLICATE PV IN REQUEST: %s",
                                pv_name,
                            )
                    else:
                        duplicate.append(pv_name)

                        if not valid:
                            self.add_bad_pv(pv_name, ip_key, port)
                            self.add_latest_request(
                                pv_name, ip_key, port, cur_time
                            )
                        else:
                            if not isinstance(pv_name, str):
                                raise ValueError("PV %r not unicode" % pv_name)

                            self.add_latest_request(
                                pv_name, ip_key, port, cur_time
                            )

                            if log_request:
                                host_name = get_host_name(
                                    get_ip_from_key(ip_key)
                                )
                                log.info(
                                    "%s %d (%r) WANTS PV ------> %s",
                                    host_name,
                                    port,
                                    ip_key,
                                    pv_name,
                                )

                            pv = self._pvs_dict.get(pv_name)

                            if pv is None:
                                pv = {}
                                self._count_pvs_new += 1

                            req_count = pv.get(PV_KEY.REQ_COUNT, 0)
                            req_count += 1
                            pv[PV_KEY.REQ_COUNT] = req_count

                            if SETTINGS.PV_SEARCH_ON_FIRST_REQUEST:
                                if req_count == 1:
                                    search_list.append(pv_name)

                            # The gateway should no longer send requests from itself
                            requests = pv.get(PV_KEY.REQUESTS, [])
                            requests = self.add_pv_request(
                                requests, ip_key, port, cur_time
                            )
                            pv[PV_KEY.REQUESTS] = requests

                            self._tracker.add_new(ip_key, port, pv_name)

                            pv[PV_KEY.CLIENT_REQ] = (
                                pv.get(PV_KEY.CLIENT_REQ, 0) + 1
                            )

                            self._count_req_pvs += 1
                            self._pvs_dict[pv_name] = pv

                else:
                    pass

                total_length -= msg_len

                if total_length < 1:
                    break

                # Advance to next CA_PROTO_SEARCH in message
                data = data[msg_len:]

        except Exception as err:
            trace = traceback.format_exc(10)
            log.exception(
                "Exception decoding CA_PROTO_SEARCH: %r\n%s",
                err,
                trace,
            )

        finally:
            self._pv_lock.release()
            self._tracker.add_unlock()

        for pv_name in search_list:
            log.debug("search new PV: %s", pv_name)
            self._searcher.search(pv_name, PRIORITY.HIGH)

    def get_pv_invalid_data(self, pv_name, return_none=False):
        self._pvs_invalid_lock.acquire()
        data = copy.deepcopy(self._pvs_invalid.get(pv_name))
        self._pvs_invalid_lock.release()
        return data

    def get_pv_data(self, pv_name, return_none=False, log=False):
        self._pv_lock.acquire()
        data = copy.deepcopy(self._pvs_dict.get(pv_name))
        self._pv_lock.release()

        if data is not None:
            if log:
                log.info("GOT PV DATA FROM PVS DICT for %r", pv_name)
            return data

        data = self.get_pv_invalid_data(pv_name)
        if data is not None:
            if log:
                log.warning("GOT INVALID PV DATA FOR %r", pv_name)
            data[KEY.PV_INVALID] = True
            return data

        if log:
            log.warning("DID NOT FIND PV DATA FOR: %r", pv_name)

        if return_none:
            return None

        return {}

    def get_stats(self):
        try:
            self._pv_lock.acquire()

            total_pvs = len(self._pvs_dict)

            result = {"pv_count": total_pvs}

            return result

        finally:
            self._pv_lock.release()

    def search_pvs(
        self,
        search_string,
        key,
        offset,
        count,
        ignore_case=True,
        sort=SORT_MODE.NAME,
    ):
        log.debug(
            "called; search_string: %r key: %d offset: %d count: %d",
            search_string,
            key,
            offset,
            count,
        )

        if search_string:
            start_time = time.time()

            self._pvs_search_lock.acquire()
            pv_names = copy.deepcopy(self._pvs_search_set)
            self._pvs_search_lock.release()

            elapsed_time = time.time() - start_time
            log.debug(
                "called; search_string: %r ignore_case: %s (copy time: %.2f)",
                search_string,
                ignore_case,
                elapsed_time,
            )

            result = []
            start_time = time.time()

            if ignore_case:
                r = re.compile(search_string, re.IGNORECASE)
            else:
                r = re.compile(search_string)

            result = list(filter(r.search, pv_names))
            result = result[0 : SETTINGS.MAX_SEARCH_RESULTS]

            if sort and sort == SORT_MODE.NAME:
                result.sort()

            elapsed_time = time.time() - start_time

            log.debug(
                "found %d PVs (search time: %.2f)",
                len(result),
                elapsed_time,
            )
            if key:
                log.debug(
                    "======= saving search result to cache: %r",
                    key,
                )
                self._pvs_search_cache_lock.acquire()
                self._pvs_search_cache[key] = result
                self._pvs_search_cache_lock.release()

        else:
            log.debug(
                "======= getting search result from cache: %r",
                key,
            )

            self._pvs_search_cache_lock.acquire()
            result = self._pvs_search_cache.get(key, [])
            self._pvs_search_cache_lock.release()

        return result[offset : offset + count], len(result)

    def get_pvs_gone(self):
        try:
            self._dbg_possible_gone_lock.acquire()
            return copy.deepcopy(self._dbg_possible_gone)
        finally:
            self._dbg_possible_gone_lock.release()

    def get_pvs_all(self, offset, count, sort=SORT_MODE.NAME):
        try:
            self._pvs_all_lock.acquire()

            if sort == SORT_MODE.NAME:
                dataset = self._pvs_sorted_all_name
            elif sort == SORT_MODE.REQ_COUNT:
                dataset = self._pvs_sorted_all_req_count
            elif sort == SORT_MODE.REQ_TIME:
                dataset = self._pvs_sorted_all_req_time
            else:
                raise ValueError("sort mode %s not supported" % sort)

            result = dataset[offset : offset + count]
            return copy.deepcopy(result), len(dataset)
        finally:
            self._pvs_all_lock.release()

    def make_leader(self, pv_name):
        """
        Record PVs that were polled as "leader" pvs
        """
        try:
            self._poll_follower_lock.acquire()
            self._poll_leader_dict[pv_name] = True
        finally:
            self._poll_follower_lock.release()

    def is_leader(self, pv_name):
        """
        Return True if the PV was polled as a leader, False otherwise
        """
        try:
            self._poll_follower_lock.acquire()
            del self._poll_leader_dict[pv_name]
            result = True

        except Exception:
            result = False

        finally:
            self._poll_follower_lock.release()

        return result

    def handle_pv_response(self, ip_key, port, data):
        # This is called in the context of the gateway client so queue for processing
        self.queue_cmd(CMD.PV_RESPONSE, (ip_key, port, data))

    def handle_pv_request(self, ip_key, port, data):
        # This is called in the context of the gateway client so queue for processing
        self.queue_cmd(CMD.PV_REQUEST, (ip_key, port, data))

    def set_gateway_queue_size(self, value):
        self._gateway_queue_size = value

    def handle_cmd_pv_response(self, data_in):
        ip_key = data_in[0]
        port = data_in[1]
        data = data_in[2]

        # The CA_PROTO_SEARCH response messages from the gateway contains the
        # server queue size in the ip_key field so that this client does
        # not swamp the gateway with requests
        self._gateway_queue_size = ip_key

        log.debug(
            "called; ioc_id: %d port: %d len data: %d",
            ip_key,
            port,
            len(data),
        )
        item = json.loads(data)

        pv_name = str(item.get("p"))

        if pv_name.startswith("breem") or pv_name.startswith("$(PRE)"):
            log.info("------  PV DATA: %r", data)

        # Get the existing data for this PV extremely early on in
        # case it is a never seen PV.  If never seen, the name must
        # be validated.  Finding the PV data means the name need not
        # be validated

        pv_data = self.get_pv_data(pv_name, return_none=True)

        if pv_data is None:
            pv_name, valid = validate_pv_name_new(pv_name)
            if not valid:
                print_binary(pv_name)
                log.warning("Invalid PV detected")
                self.add_bad_pv(pv_name, ip_key, port)
                return

            else:
                log.debug("Detected new PV: %s", pv_name)

        servers = item.get("s")

        # Convert server IP addresses to ints (keys)
        temp = []
        for server in servers:
            k = get_ip_key(server[0])
            p = server[1]
            temp.append([k, p])

        temp.sort()
        servers = temp

        # This is the server(s) where the PV was just seen
        server_key_new = make_server_key(servers)

        if len(servers) > 1:
            duplicates = servers
        else:
            duplicates = None

        if len(servers) == 0:
            # TODO: Does this happen (e.g., when there is no response?
            server_id = 0
            server_port = 0
        else:
            server = servers[0]
            server_id = server[0]
            server_port = server[1]

        server_key_old = 0
        if pv_data:
            server_key_old = pv_data.get(PV_KEY.SERVER_KEY, 0)

        # Tell the cache about this PV.  It might respond with other PVs that
        # should be set to the same value
        followers = self._new_cache.update_pv(
            pv_name, server_key_old, server_key_new, servers
        )
        if followers:
            # These followers will have been removed from the cache so we want
            # to poll them again soon.  Add them to this followrs list
            try:
                self._followers_lock.acquire()
                self._followers.extend(followers)
            finally:
                self._followers_lock.release()

            followers.append(pv_name)
        else:
            followers = [pv_name]

        # Update the PV
        t = int(time.time())
        for i, pv_name in enumerate(followers):
            info = {
                PV_KEY.HOST: server_id,
                PV_KEY.PORT: server_port,
                PV_KEY.FIRST_TIME: t,
                PV_KEY.LAST_TIME: t,
            }

            self.add_info_dict(pv_name, info, server_key_new, duplicates)

        return

    def get_mapping_run_count(self):
        return self._count_mapping_runs

    def get_requests_sorted(self, data):
        result = []
        requests = data.get(PV_KEY.REQUESTS, [])
        for request in requests:
            t = request[3]
            ioc_id = request[0]
            result.append((t, ioc_id))

        result.sort()
        result.reverse()
        return result

    def get_pv_latest_request(self, data):
        # These *should* be sorted by latest time
        requests = data.get(PV_KEY.REQUESTS, [])

        try:
            result = requests[-1]

            # Sanity check
            last_time = result[3]
            for item in requests[:-1]:
                if last_time < item[3]:
                    raise ValueError("Bad last time!!!!")

            return result
        except:
            return None

    def get_pv_latest_info(self, data):
        """
        Returns a tuple:  Lost (True/False), host_key, port, last_seen_time
        Lost == True means that the last time it was checked it was not
        found.  The host and port are the last host/port on which the PV
        was found. These may be set even if the PV is currently "Lost"
        """
        lost = True
        host = None
        port = None
        last_time_seen = None

        try:
            if data is None:
                raise ExceptionReturn

            info = data.get(PV_KEY.INFO, [])
            if len(info) == 0:
                # I think this is a case of never seen
                host = None
                port = None
                lost = False

            elif len(info) == 1:
                last_item = info[-1]
                host = last_item.get(PV_KEY.HOST)
                if not host:
                    # This is a case of "never found"
                    host = None
                    port = None
                    lost = False
                else:
                    port = last_item.get(PV_KEY.PORT)
                    last_time_seen = last_item.get(PV_KEY.LAST_TIME)
                    lost = False
            else:
                last_item = info[-1]
                second_last_item = info[-2]
                host = last_item.get(PV_KEY.HOST)
                if host == 0:
                    lost = True
                    host = second_last_item.get(PV_KEY.HOST)
                    port = second_last_item.get(PV_KEY.PORT)
                    last_time_seen = second_last_item.get(PV_KEY.LAST_TIME)
                else:
                    lost = False
                    host = last_item.get(PV_KEY.HOST)
                    port = last_item.get(PV_KEY.PORT)
                    last_time_seen = last_item.get(PV_KEY.LAST_TIME)

        except ExceptionReturn:
            pass

        return lost, host, port, last_time_seen

    def add_info_dict(self, pv_name, info_dict, server_key, duplicates):
        """
        Update information about the specified PV
        """
        log.debug(
            "called; pv_name: %s info_dict: %r duplicates: %r",
            pv_name,
            info_dict,
            duplicates,
        )

        try:
            self._pv_lock.acquire()

            pv_data = self._pvs_dict.get(pv_name)
            if not pv_data:
                # Lock released in finally
                log.debug(
                    "Adding PV detected by SEARCH RESPONSE: %s",
                    pv_name,
                )
                pv_data = {
                    PV_KEY.REQUESTS: [],
                    PV_KEY.CLIENT_REQ: 0,
                    PV_KEY.REQ_COUNT: 0,
                }

            info = pv_data.get(PV_KEY.INFO, [])

            # Add new info_item to "fixed" info
            found = False
            for index, item in enumerate(info):
                if item[PV_KEY.HOST] == info_dict[PV_KEY.HOST]:
                    if item[PV_KEY.PORT] == info_dict[PV_KEY.PORT]:
                        found = True
                        item[PV_KEY.LAST_TIME] = info_dict[PV_KEY.LAST_TIME]
                        break

            if found:
                # Delete the item then append it.  This is required so that
                # the info items are sorted by last time seen
                info_dict = info[index]
                del info[index]

            info.append(info_dict)

            # Only keep last few checks so data does not grow without bound
            pv_data[PV_KEY.INFO] = info[-10:]
            pv_data[PV_KEY.SERVER_KEY] = server_key

            if duplicates:
                pv_data[PV_KEY.DUPLICATES] = duplicates
            else:
                # Get rid of duplicate information
                try:
                    del pv_data[PV_KEY.DUPLICATES]
                except:
                    pass

            self._pvs_dict[pv_name] = pv_data

        finally:
            self._pv_lock.release()

        return
