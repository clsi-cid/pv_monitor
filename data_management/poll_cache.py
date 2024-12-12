# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import json
import logging
import queue
import threading
import time

from definitions import KIND, PRIORITY
from utilities.file_saver import FileSaver
from utilities.pvmonitor_lock import PVMonitorLock
from utilities.statistics import StatisticsManager
from utilities.thread_monitor import ThreadMonitor

log = logging.getLogger("PVMLogger")

TM = ThreadMonitor()

STATS = StatisticsManager()


class CMD(object):
    UPDATE_BEACON = 100
    UPDATE_PV = 101
    DELETE_PV = 102


class PollCache(object):

    def __init__(self, dataman):

        self._dataman = dataman
        self._searcher = None
        self._gateway_alive = False

        self._heartbeat_pv_count = 0
        self._heartbeat_skipped = 0
        self._heartbeat_seen_dict = {}
        self._server_key_seen_dict = {}

        self._count_cache_hit = 0
        self._count_cache_miss = 0

        self._cache = {}
        self._cache_lock = PVMonitorLock()

        self._server_to_pv_map = {}
        self._server_to_pv_lock = PVMonitorLock()

        self._timer = threading.Timer(1, self.worker)
        self._queue = queue.Queue()

        self._pv_checked = {}

    def set_searcher(self, searcher):
        self._searcher = searcher

    def handle_gateway_alive(self, alive):
        log.debug("alive: %r", alive)
        self._gateway_alive = alive

    def start(self):
        log.debug("called")
        self._timer.start()

    def reset(self):
        log.debug("called")

        STATS.set("Cache Heartbeats", self._heartbeat_pv_count)
        STATS.set("Cache Hits HB", self._heartbeat_skipped)

        STATS.set("Cache Hits", self._count_cache_hit)
        STATS.set("Cache Misses", self._count_cache_miss)

        self._count_cache_hit = 0
        self._count_cache_miss = 0

        self._heartbeat_pv_count = 0
        self._heartbeat_skipped = 0

        self._heartbeat_seen_dict = {}
        self._server_key_seen_dict = {}

        self._cache_lock.acquire()
        self._pv_checked = {}
        self._cache_lock.release()

    def check(self, pv_name):
        log.debug("called; PV Name: %s", pv_name)

        heartbeat = self._dataman.get_pv_heartbeat(pv_name)

        ###################################################################
        # Check if this PV is part of a heartbeat group
        #
        # TODO: What happens to the pv->heatbeat map if a heartbeat is
        # TODO: restarted with a different set of PVs? We must ensure that
        # TODO: the database is purged properly
        ###################################################################

        if heartbeat:
            log.debug("found heartbeat PV: %s", pv_name)

            self._heartbeat_pv_count += 1
            ioc_id = heartbeat[0]
            port = heartbeat[1]
            have_ports = self._heartbeat_seen_dict.get(ioc_id, [])

            if port in have_ports:
                self._heartbeat_skipped += 1
                return True

            have_ports.append(port)
            self._heartbeat_seen_dict[ioc_id] = have_ports
            return False

        try:
            self._cache_lock.acquire()
            # The PV does not have a heartbeat
            have_key = self._cache.get(pv_name)

            if not have_key:
                # PV is not in the cache
                self._count_cache_miss += 1
                return False

            # PV is in the cache, has this key been checked?
            if have_key in self._server_key_seen_dict:
                self._count_cache_hit += 1
                return True

            self._count_cache_miss += 1
            return False
        finally:
            self._cache_lock.release()

    def update_beacon(self, ip_key, port, kind):
        log.debug(
            "called; key: %r port: %r kind: %r",
            ip_key,
            port,
            kind,
        )
        self._queue.put_nowait((CMD.UPDATE_BEACON, (ip_key, port, kind)))

    def handle_cmd_update_beacon(self, data):

        ioc_id = data[0]
        port = data[1]
        kind = data[2]

        if kind not in [KIND.RESTARTED, KIND.NEW, KIND.LOST]:
            return

        key = "%d:%d" % (ioc_id, port)
        try:
            self._server_to_pv_lock.acquire()
            pv_name = self._server_to_pv_map.get(key)
            if pv_name:

                log.info(
                    "%s\nSEARCH PV %s on beacon change!!!!!", "*" * 80, pv_name
                )
                self._searcher.search(pv_name, PRIORITY.HIGH)
        finally:
            self._server_to_pv_lock.release()

    def update_pv(self, pv_name, old_key, new_key, servers):
        """
        This needs to be called in the context of the client
        because it returns a list of pvs
        """

        # ----------------------------------------------------------------------
        # Keep a map of which PVs were last seen on which IOCs/ports.  When a
        # a beacon changes (LOST/RESTARTED/NEW) the cache will do a priority
        # search on that PV
        # ----------------------------------------------------------------------
        try:
            self._server_to_pv_lock.acquire()
            for server in servers:
                if server[0] == 0:
                    continue

                key = "%d:%d" % (server[0], server[1])
                self._server_to_pv_map[key] = pv_name
        finally:
            self._server_to_pv_lock.release()

        # ----------------------------------------------------------------------

        heartbeat = self._dataman.get_pv_heartbeat(pv_name)
        if heartbeat:
            if old_key == new_key:
                return

            ioc_id = heartbeat[0]
            port = heartbeat[1]

            # Something changed... update all associated PVs
            heartbeat_pvs = self._dataman.get_heartbeat_pvs(ioc_id, port)
            return heartbeat_pvs

        try:
            self._cache_lock.acquire()
            self._pv_checked[pv_name] = True

            if new_key:
                self._server_key_seen_dict[new_key] = pv_name

                # Add this PV to the cache
                self._cache[pv_name] = new_key

            else:
                # Remove this PV from the cache
                try:
                    del self._cache[pv_name]
                except:
                    pass

            if old_key and old_key != new_key:
                pv_list = []
                temp = {}
                for p, key in self._cache.items():
                    if key == old_key:
                        if p not in self._pv_checked:
                            self._pv_checked[pv_name] = True
                            pv_list.append(p)
                    else:
                        temp[p] = key
                self._cache = temp

                return pv_list

        finally:
            self._cache_lock.release()

    def handle_cmd_save(self):
        """
        Save the cache so that on restart it will not take days to rebuild
        """

        start_time = time.time()
        data_str = None

        try:
            self._cache_lock.acquire()
            self._server_to_pv_lock.acquire()

            data = {}
            data["pvs"] = self._cache
            data["ioc_map"] = self._server_to_pv_map
            data_str = json.dumps(data, indent=4)

        except Exception as err:
            log.exception("Error saving poll_cache: %r", err)

        finally:
            self._server_to_pv_lock.release()
            self._cache_lock.release()

        try:
            if data_str:
                saver = FileSaver(prefix="poll_cache_new")
                saver.save(data_str)

        except Exception as err:
            log.exception("Error saving poll_cache: %r", err)

        elapsed_time = time.time() - start_time
        log.debug("elapsed_time: %.2f", elapsed_time)

    def worker(self):

        thread_id = TM.add("PollCache: Worker", 600)

        self._save_sec = 60 * 60
        next_save_time = time.time() + self._save_sec
        next_ping_time = time.time()

        while True:
            cur_time = time.time()

            if cur_time > next_ping_time:
                TM.ping(thread_id)
                next_ping_time += 30
                STATS.set("Queue Size: Poll Cache", self._queue._qsize())
                self._cache_lock.acquire()
                size = len(self._cache)
                self._cache_lock.release()
                STATS.set("Cache PVs:", size)

            if cur_time > next_save_time:
                self.handle_cmd_save()
                next_save_time += self._save_sec

            try:
                item = self._queue.get(block=True, timeout=10)
            except queue.Empty:
                continue

            if not self._gateway_alive:
                log.debug(
                    "gateway not alive (%d)",
                    self._queue._qsize(),
                )
                continue

            cmd = item[0]
            data = item[1]

            if cmd == CMD.UPDATE_PV:
                self.handle_cmd_update_pv(data)

            elif cmd == CMD.UPDATE_BEACON:
                self.handle_cmd_update_beacon(data)
