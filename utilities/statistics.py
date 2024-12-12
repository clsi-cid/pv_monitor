#---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
#---------------------------------------------------------------------
import threading
import queue
import copy
import time

from utilities.thread_monitor import ThreadMonitor

TM = ThreadMonitor()


class CMD(object):
    INCREMENT = 1
    SET = 2


class StatisticsManager(object):
    """
    A Simple class that allows for various statistics to be gathered
    """

    _data = {}
    _lock = threading.Lock()
    _timer = None
    _queue = queue.Queue()

    def __init__(self):
        pass

    def start(self):

        self._lock.acquire()
        if self._timer is None:
            self._timer = threading.Timer(1, self.worker)
            self._timer.start()
        self._lock.release()

    def increment(self, name):
        self._queue.put_nowait((CMD.INCREMENT, name))

    def set(self, name, value):
        self._queue.put_nowait((CMD.SET, (name, value)))

    def get(self):
        try:
            self._lock.acquire()
            return copy.deepcopy(self._data)
        finally:
            self._lock.release()

    def worker(self):

        thread_id = TM.add("Statistics: worker", 60)
        last_time = time.time()

        while True:
            try:
                item = self._queue.get(block=True, timeout=10)

            except queue.Empty:
                TM.ping(thread_id)
                last_time = time.time()
                continue

            cmd = item[0]
            data = item[1]

            cur_time = time.time()
            if cur_time - last_time > 20:
                TM.ping(thread_id)
                last_time = cur_time

            try:
                self._lock.acquire()

                if cmd == CMD.INCREMENT:
                    name = data
                    count = self._data.get(name, 0)
                    count += 1
                    self._data[name] = count

                elif cmd == CMD.SET:

                    name = data[0]
                    value = data[1]
                    self._data[name] = value
            finally:
                self._lock.release()
