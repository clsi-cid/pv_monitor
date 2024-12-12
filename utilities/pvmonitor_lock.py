#---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
#---------------------------------------------------------------------
import threading
import time
import traceback


def get_details(index):

    x = traceback.extract_stack()
    try:
        item = x[index]

        file = item[0]
        line = item[1]
        func = item[2]
        details = "%s (%s:%d)" % (func, file, line)
    except:
        details = "Unknown"

    return details


class PVMonitorLock(object):

    def __init__(self):

        self._name = get_details(-3)
        self._lock = threading.Lock()
        self._wait_time = 0
        self._acquire_time = 0

    def acquire(self):
        start_time = time.time()
        self._lock.acquire()
        self._acquire_time = time.time()
        self._wait_time = self._acquire_time - start_time

        if self._wait_time > 2:
            print(
                "################################### %s WAIT TIME: %f CALLER: %s"
                % (self._name, self._wait_time, get_details(-3))
            )

    def release(self):

        hold_time = time.time() - self._acquire_time
        self._lock.release()

        if hold_time > 2:
            print(
                "################################### %s HOLD TIME: %f CALLER: %s"
                % (self._name, hold_time, get_details(-3))
            )
