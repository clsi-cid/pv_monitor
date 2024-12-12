# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
# System imports
import datetime
import json
import logging
import os
import threading
import time

# Library imports
from definitions import PV_KEY as KEY
from utilities.file_saver import FileSaver
from utilities.thread_monitor import ThreadMonitor

log = logging.getLogger("PVMLogger")

TM = ThreadMonitor()

MAX_DATA_LENGTH = 50000


class RequestTracker(object):

    def __init__(self):

        self._lock = threading.Lock()
        self.data = []
        self.lock_count = 0

        self._add_lock = threading.Lock()
        self._add_queue = []
        self._add_lock_count = 0

        self._xfer_timer = threading.Timer(10, self.handle_timer_xfer)

        self._save_timer = threading.Timer(60, self.handle_timer_save)
        self._save_time_last = 0

        self._server_name = "testing"
        # The following are for testing
        self._oldest = None
        self._newest = None

    def start(self):
        self._xfer_timer.start()
        self._save_timer.start()

    def set_server_name(self, value):
        self._server_name = value

    def load_test_file(self):
        """
        Load the test file
        """
        saver = FileSaver(prefix="TRACKER_TEST")

        file_name = saver.get_newest()

        if not file_name:
            print("No test datafile detected")
            return

        opened = False
        try:
            f = open(file_name)
            opened = True
            data = json.load(f)
            print("Loaded File: '%s'" % file_name)

        except Exception as err:
            log.error(
                "Exception loading data file: %s",
                str(err),
            )
            data = None
        finally:
            if opened:
                f.close()

        if data:

            self.lock()
            self.data = data.get("TRACKER")
            self.unlock()

            print("TRACKER: Loaded %d Requests" % len(self.data))

            # The oldest item is at the beginning of the queue and the
            # newest item is at the end of the queue
            self._oldest = self.data[0]
            self._newest = self.data[-1]

    def write_test_file(self):
        print("TRACKER: writing a test file")

        self.lock()
        try:
            data = {"TRACKER": self.data}
            data_str = json.dumps(data)

        except Exception as err:
            data_str = None
            print("Exception generating TRACKER json: %s" % str(err))

        finally:
            self.unlock()

        if data_str:
            saver = FileSaver(prefix="TRACKER_TEST")
            saver.save(data_str)
            print("TRACKER saved to ", saver.get_newest())

    def test_save_request_data(self):

        start_sec = self._oldest[2]
        end_sec = self._newest[2]

        self.save_request_data(start_sec, end_sec)

    def handle_timer_save(self):

        thread_id = TM.add("Tracker SAVE", 120)

        while True:
            TM.ping(thread_id)

            cur_time = int(time.time())
            minutes = int(float(cur_time) / 60.0)

            end_sec = minutes * 60
            start_sec = (minutes - 1) * 60

            if start_sec != self._save_time_last:
                self._save_time_last = start_sec

                self.save_request_data(start_sec, end_sec)

            time.sleep(50)

    def check_directory(self, file_name):

        path = os.path.dirname(file_name)
        if not os.path.isdir(path):
            log.debug("TRACKER: creating directory '%s'", path)
            os.makedirs(path)

    def save_request_data(self, start_sec, end_sec):

        # Construct the filename
        d = datetime.datetime.fromtimestamp(end_sec)

        # Data file location should somehow be more integrated with FileSaver
        file_name = (
            "data/%s/stats/%04d_%02d/request_counts_%04d_%02d_%02d.csv"
            % (
                self._server_name,
                d.year,
                d.month,
                d.year,
                d.month,
                d.day,
            )
        )

        try:

            self.check_directory(file_name)
            f = open(file_name, "a+")

        except Exception as err:
            log.error(
                "Error opening file '%s': %s",
                file_name,
                str(err),
            )
            return

        try:
            self.lock()

            data = {}

            index_start = None

            for i, item in enumerate(self.data):
                t = item[2]
                if t <= end_sec and t > start_sec:
                    if index_start is None:
                        index_start = i
                    count = data.get(item[0], 0)
                    data[item[0]] = count + 1

            # Convert to a list of items (ioc_key, request_count)
            lines = [(key, value) for key, value in data.items()]

            # Sort by request_count
            lines = sorted(lines, key=lambda item: item[1])
            lines.reverse()

            line = "%d,%d" % (start_sec, end_sec)
            f.write(line)

            # Only save the top 50 IOCs
            lines = lines[:50]
            for line in lines:
                f.write(",%d,%d" % (line[0], line[1]))
            f.write("\n")

        finally:
            self.unlock()
            f.close()

    def handle_timer_xfer(self):
        """
        This timer handler periodically moves the captured data from the
        capture queue into the longer term storage queue.  The idea is to
        miniminze the time duration during which the capture queue lock
        is held so that the actual traffic scanning code is not blocked
        """
        thread_id = TM.add("Tracker XFER", 60)

        while True:
            time.sleep(10)
            TM.ping(thread_id)

            self._lock.acquire()
            self._add_lock.acquire()

            add_len = len(self._add_queue)
            self.data.extend(self._add_queue)
            self._add_queue = []
            self._add_lock.release()

            self.data = self.data[-MAX_DATA_LENGTH:]
            data_len = len(self.data)

            # Get the oldest data point
            try:
                oldest_data_point = self.data[0]
                n = int(time.time())
                age_seconds = n - oldest_data_point[2]

            except:
                oldest_data_point = None
                age_seconds = 0

            log.debug(
                "oldest_data_point: %r",
                oldest_data_point,
            )

            self._lock.release()

            log.debug(
                "TRACKER: xfer %d -> Total len: %d (max_age: %d)",
                add_len,
                data_len,
                age_seconds,
            )

    def add_lock(self):
        if self._add_lock_count == 0:
            self._add_lock.acquire()
            self._add_lock_count += 1

    def add_unlock(self):
        if self._add_lock_count > 0:
            self._add_lock.release()
            self._add_lock_count -= 1

    def lock(self):
        if self.lock_count == 0:
            self._lock.acquire()
            self.lock_count += 1

    def unlock(self):
        if self.lock_count > 0:
            self._lock.release()
            self.lock_count -= 1

    def add_new(self, host_key, port, pv_name):

        t = int(time.time())
        if self._add_lock_count == 0:
            raise ValueError("expected _add_lock_count")
        self._add_queue.append((host_key, port, t, pv_name))

    def add(self, host, port, pv_name):

        t = int(time.time())
        host_key = host.get(KEY.HOST_KEY)
        self.lock()
        self.data.append((host_key, t, pv_name))
        if len(self.data) > MAX_DATA_LENGTH:
            self.data = self.data[-MAX_DATA_LENGTH:]
        self.unlock()


if __name__ == "__main__":

    print("tracker testing")

    tracker = RequestTracker()
    tracker.load_test_file()
    tracker.test_save_request_data()
