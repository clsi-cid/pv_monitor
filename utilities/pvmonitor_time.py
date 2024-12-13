# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import datetime
import time


class PVMonitorTime(object):

    def __init__(self):
        pass

    def get_string_MDY(self, t=None):

        if t is None:
            t = int(time.time())

        x = datetime.datetime.fromtimestamp(t)
        return x.strftime(("%B %-d, %Y"))

    def get_string_time_of_day(self, t=None):

        if t is None:
            t = int(time.time())

        x = datetime.datetime.fromtimestamp(t)
        return x.strftime("%-I:%M:%S %p")

    def get_day_end(self, t=None):
        day_start = self.get_day_start(t=t)
        return day_start - 1 + 24 * 60 * 60

    def get_day_start(self, t=None):

        if t is None:
            t = int(time.time())

        n = datetime.datetime.fromtimestamp(t)
        start = datetime.datetime(n.year, n.month, n.day)
        result = int(time.mktime(start.utctimetuple()))
        return result

    def is_today(self, t):

        start_of_today = self.get_day_start()

        if t < start_of_today:
            return False

        end_of_today = start_of_today + (24 * 60 * 60)

        if t > end_of_today:
            return False

        return True


def my_time(timestamp):
    result = "%s" % datetime.datetime.fromtimestamp(timestamp)
    parts = result.split(".")
    return parts[0]
