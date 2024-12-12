# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import copy
import logging
import queue
import threading
import time

log = logging.getLogger("GatewayLogger")

DELETE_INACTIVE = True


class ThreadMonitor(object):
    """
    A simple class to monitor thread health
    """

    TERMINATE = "terminate"

    # Shared between all instances of this class
    _TM_lock = threading.Lock()
    _TM_thread_list = {}
    _TM_queue = queue.Queue()
    _TM_shared_data = {"id": 0}

    def __init__(self):

        self._timer = None
        self._callback_error_func = None
        self._check_time = 0
        self._dead_thread_count = 0

    def get_dead_thread_count(self):
        return self._dead_thread_count

    def set_callback(self, func):
        """
        Only thread monitors with callback functions actually monitor threads.
        Should only be one per process.
        """
        if self._callback_error_func is not None:
            raise ValueError("callback already set")

        self._callback_error_func = func
        self._timer = threading.Timer(1, self.worker)
        self._timer.start()

    def add(self, description, max_age):

        try:
            self._TM_lock.acquire()
            new_id = self._TM_shared_data.get("id")

            self._TM_shared_data["id"] = new_id + 1
            self._TM_thread_list[new_id] = {
                "desc": description,
                "msg": None,
                "max_age": float(max_age),
                "last_time": time.time(),
                "ping_count": 0,
                "active": True,
                "lost_count": 0,
                "lost": False,
            }

            for k, v in list(self._TM_thread_list.items()):
                log.debug(
                    "MONITORED THREAD: %d: %s",
                    k,
                    v.get("desc"),
                )

        finally:
            self._TM_lock.release()

        return new_id

    def delete(self, thread_id):

        try:
            self._TM_lock.acquire()
            if DELETE_INACTIVE:
                del self._TM_thread_list[thread_id]
            else:
                data = self._TM_thread_list[thread_id]
                data["active"] = False
        except Exception as err:
            log.error(
                "Exception deleting thread: %r: %r",
                thread_id,
                err,
            )

        finally:
            self._TM_lock.release()

    def get_list(self):

        try:
            result = {}
            self._TM_lock.acquire()
            for t, v in list(self._TM_thread_list.items()):
                result[t] = copy.deepcopy(v)

            return result
        finally:
            self._TM_lock.release()

    def print_list(self):

        now = time.time()
        try:
            print(
                "------------------------------------------------------------"
            )
            self._TM_lock.acquire()
            for t, v in list(self._TM_thread_list.items()):
                age = now - v.get("last_time")
                active = v.get("active")
                print(
                    (
                        "THREAD: %2d: AGE: %.2f Active: %s %s"
                        % (t, age, repr(active), repr(v))
                    )
                )
        finally:
            self._TM_lock.release()
            print(
                "------------------------------------------------------------"
            )

    def ping(self, thread_id, msg=None):
        log.debug("called; thread_id: %r", thread_id)
        if thread_id is None:
            raise ValueError("Thread ID is none")
        self._TM_queue.put_nowait((thread_id, time.time(), msg))

    def terminate(self):
        self._TM_queue.put_nowait((self.TERMINATE, None))

    def worker(self):

        while True:
            try:
                ping = self._TM_queue.get(block=True, timeout=10)
            except queue.Empty:
                ping = None

            if self._TM_queue.qsize() > 50:
                log.debug(
                    "WARNING ---> ThreadMonitor QSize: %d",
                    self._TM_queue.qsize(),
                )

            self.handle_check()
            if ping is None:
                continue
            if ping[0] == self.TERMINATE:
                break
            self.handle_ping(ping)

    def handle_check(self):

        dead_threads = []
        now = time.time()

        # Limit check frequency (a fast pinger causes this
        # method to get called frequently)
        if now - self._check_time < 10:
            return

        self._check_time = now

        try:
            self._TM_lock.acquire()
            for id, data in list(self._TM_thread_list.items()):
                if not data.get("active", False):
                    log.debug("skip inactive thread: %d", id)
                    continue

                age = now - data.get("last_time")
                if age > data.get("max_age_seen", 0):
                    data["max_age_seen"] = age

                if age > data.get("max_age"):
                    # NOTE: putting the actual age in the message causes
                    # many dialogs to appear because they are all unique.
                    # Dialogs that are the same are not reshown.
                    msg = "Thread '%s' unresponsive (age: %f max: %f)" % (
                        data.get("desc"),
                        age,
                        data.get("max_age"),
                    )

                    data["lost"] = True
                    ping_msg = data.get("msg")
                    if ping_msg:
                        msg += "<br>Last msg: %s)" % repr(ping_msg)

                    dead_threads.append(msg)
                    data["lost_count"] = data.get("lost_count", 0) + 1

        finally:
            self._TM_lock.release()

        self._dead_thread_count = len(dead_threads)

        for msg in dead_threads:
            log.error(msg)
            self._callback_error_func(None, None)

    def handle_ping(self, ping):

        try:
            self._TM_lock.acquire()

            data = self._TM_thread_list.get(ping[0])

            if data is None:
                log.error(
                    "No thread with id: %r",
                    ping[0],
                )
            else:
                data["last_time"] = ping[1]
                data["msg"] = ping[2]
                ping_count = data["ping_count"] + 1
                data["ping_count"] = ping_count
                data["lost"] = False
                if not data["active"]:
                    log.debug(
                        "ping of inactive thread: %r",
                        ping[0],
                    )

        except Exception as err:
            # It is possible to handle a ping after thread has been removed
            # from the list
            print("handle_ping() EXCEPTION:", repr(err))
            print("---> PING:", repr(ping))
            for k, v in list(self._TM_thread_list.items()):
                print("Monitored thread: %d: %s" % (k, repr(v)))

        finally:
            self._TM_lock.release()


if __name__ == "__main__":

    def callback(msg):
        print("callback got", msg)

    tm1 = ThreadMonitor()
    tm2 = ThreadMonitor()

    tm1.add("thing1-A", 30)
    tm2.add("thing2-A", 40)
    tm1.add("thing1-B", 30)
    tm2.add("thing2-B", 40)
    tm1.set_callback(callback)
