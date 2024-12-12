# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import calendar
import copy
import datetime
import logging
import threading
import time
import traceback

from definitions import PLOT_INTERVAL, PLOT_TYPE, SETTINGS
from utilities.thread_monitor import ThreadMonitor
from utilities.utils import get_host_name

TOP_LATEST_PV_COUNT = 24

TOP_IOC_COUNT = 20
BIN_COUNT = 600

log = logging.getLogger("PVMLogger")

TM = ThreadMonitor()


class KEY(object):
    COLORS = "ioc_colors"
    BIN_NAME = "bin_name"
    SECONDS = "seconds"
    COUNTS = "counts"
    IOC_NAMES = "ioc_names"


FILE_PREFIX_COUNTS = "counts"
FILE_PREFIX_REQ_COUNTS = "request_counts"

INTERVAL_DATA = {
    PLOT_INTERVAL.HOUR: {
        KEY.SECONDS: 60 * 60,
        KEY.COLORS: [2, 3, 4, 5, 6, 7],
        KEY.BIN_NAME: "minute",
    },
    PLOT_INTERVAL.DAY: {
        KEY.SECONDS: 60 * 60 * 24,
        KEY.COLORS: [
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
        ],
        KEY.BIN_NAME: "hour",
    },
    PLOT_INTERVAL.WEEK: {
        KEY.SECONDS: 60 * 60 * 24 * 7,
        KEY.COLORS: [1, 2, 3, 4, 5, 6, 7],
        KEY.BIN_NAME: "day",
    },
    PLOT_INTERVAL.MONTH: {
        KEY.SECONDS: 60 * 60 * 24 * 30,
        KEY.COLORS: [1, 3, 5, 7],
        KEY.BIN_NAME: "week",
    },
    PLOT_INTERVAL.MONTH2: {
        KEY.SECONDS: 60 * 60 * 24 * 30 * 2,
        KEY.COLORS: [4, 7],
        KEY.BIN_NAME: "month",
    },
    PLOT_INTERVAL.MONTH6: {
        KEY.SECONDS: 60 * 60 * 24 * 30 * 6,
        KEY.COLORS: [2, 3, 4, 5, 6, 7],
        KEY.BIN_NAME: "month",
    },
    PLOT_INTERVAL.YEAR: {
        KEY.SECONDS: 60 * 60 * 24 * 365,
        KEY.COLORS: [2, 3, 4, 5, 6, 7, 2, 3, 4, 5, 6, 7],
        KEY.BIN_NAME: "month",
    },
    PLOT_INTERVAL.YEAR2: {
        KEY.SECONDS: 60 * 60 * 24 * 365 * 2,
        KEY.COLORS: [4, 7],
        KEY.BIN_NAME: "year",
    },
}

COLOR_GREEN = [
    "#99FF99",
    "#66FF66",
    "#33FF33",
    "#00EE00",
    "#00CC00",
    "#009900",
    "#005500",
    "#002200",
]

Y_AXIS = {
    PLOT_TYPE.REQ_USER: "CA_PROTO_SEARCH or Packets / min",
    PLOT_TYPE.REQ_GATEWAY: "CA_PROTO_SEARCH or Packets / min",
    PLOT_TYPE.PKT_GW: "CA_PROTO_SEARCH or Packets / min",
    PLOT_TYPE.PKT_USER: "CA_PROTO_SEARCH or Packets / min",
    PLOT_TYPE.REQ_IOC: "CA_PROTO_SEARCH",
    PLOT_TYPE.PVS_TOTAL: "Process Variables (PVs)",
    PLOT_TYPE.PVS_LOST: "Process Variables (PVs)",
    PLOT_TYPE.PVS_DUPLICATE: "Process Variables (PVs)",
    PLOT_TYPE.PVS_INVALID: "Process Variables (PVs)",
    PLOT_TYPE.MEM_RSS_USAGE: "Gigabytes (GB)",
    PLOT_TYPE.MEM_VMS_USAGE: "Gigabytes (GB)",
    PLOT_TYPE.CPU_USAGE: "CPU Usage (%)",
    PLOT_TYPE.MOST_ACTIVE_PVS: "CA_PROTO_SEARCH",
    PLOT_TYPE.MOST_ACTIVE_IOCS: "CA_PROTO_SEARCH",
    PLOT_TYPE.SRV_UNIX_FREE_HOME: "Disk Space Free (GB)",
    PLOT_TYPE.SRV_UNIX_FREE_IOCAPPS: "Disk Space Free (GB)",
}

NAME = {
    PLOT_TYPE.REQ_USER: "User - PV Req",
    PLOT_TYPE.PKT_USER: "User - Pkts",
    PLOT_TYPE.REQ_GATEWAY: "Gateway - PV Req",
    PLOT_TYPE.PKT_GW: "Gateway - Pkts",
    PLOT_TYPE.PVS_TOTAL: "PVs - Total",
    PLOT_TYPE.PVS_LOST: "PVs - Lost",
    PLOT_TYPE.PVS_DUPLICATE: "PVs - Duplicates",
    PLOT_TYPE.PVS_INVALID: "PVs - Invalid",
    PLOT_TYPE.MEM_RSS_USAGE: "RSS Memory Usage (GB)",
    PLOT_TYPE.MEM_VMS_USAGE: "VM Memory Usage (GB)",
    PLOT_TYPE.CPU_USAGE: "CPU Usage (%)",
    PLOT_TYPE.SRV_UNIX_FREE_HOME: "SRV-UNIX free /home (GB)",
    PLOT_TYPE.SRV_UNIX_FREE_IOCAPPS: "SRV-UNIX free /iocApps (GB)",
}


class PlotTimer(object):

    def __init__(self, value=None):
        self._datetime = None
        t1 = calendar.timegm(time.gmtime())
        t2 = calendar.timegm(time.localtime())
        self._utc_offset_sec = t1 - t2
        self._epoch = datetime.datetime(1970, 1, 1)

        if isinstance(value, float):
            self.from_timestamp(value)

    def now(self):
        self._datetime = datetime.datetime.now()

    def get_utc_timestamp(self):
        time_delta = self._datetime - self._epoch
        return time_delta.total_seconds() + self._utc_offset_sec

    def get_datetime(self):
        return self._datetime

    def replicate(self, my_time):
        self._datetime = copy.copy(my_time.get_datetime())

    def from_timestamp(self, t):
        self._datetime = datetime.datetime.fromtimestamp(t)

    def from_time_arg(self, time_arg):
        self._datetime = datetime.datetime.strptime(
            time_arg, "%Y-%m-%d %H:%M:%S"
        )

    def get_yyyy_mm_dd(self):
        return self._datetime.year, self._datetime.month, self._datetime.day

    def increment_day(self):
        self._datetime += datetime.timedelta(days=1)

    def check(self, my_time):
        """
        If the passed in time is newer return True, otherwise return False
        """
        check_year, check_month, check_day = my_time.get_yyyy_mm_dd()

        if check_year >= self._datetime.year:
            if check_month >= self._datetime.month:
                if check_day >= self._datetime.day:
                    return True

        return False

    def check_line_newer(self, line):
        parts = line.split(",")

        try:
            line_datetime = datetime.datetime.strptime(
                parts[0].strip(), "%Y-%m-%d %H:%M:%S"
            )
        except:
            line_datetime = datetime.datetime.fromtimestamp(int(parts[1]))

        if line_datetime > self._datetime:
            return True
        return False


class PlotDataMgr(object):

    def __init__(self, dataman, server):
        self._timer = threading.Timer(1, self.worker)

        server_parts = server.split(".")
        self._server = server_parts[0]

        self._lock = threading.Lock()
        self._data_time = {}
        self._data = {}
        self._data_ioc_req = {}

        self._pvman = None
        self._dataman = dataman

        # For most active IOCs and PVs
        self._latest_requests_lock = threading.Lock()
        self._latest_requests_time = 0
        self._latest_requests = None

        self._latest_lock = threading.Lock()

        self._latest_pv_names_str = ""
        self._latest_pv_iocs = []
        self._latest_pv_data = {}

        self._latest_ioc_names_str = ""
        self._latest_ioc_pvs = []
        self._latest_ioc_data = {}

    def set_pvman(self, value):
        self._pvman = value

    def get_name_pv_ioc(self, index):
        try:
            self._latest_requests_lock.acquire()
            ioc = self._latest_pv_iocs[index]
            if ioc == 0:
                return "Other"
            return get_host_name(ioc)

        finally:
            self._latest_requests_lock.release()

    def get_name_ioc_pv(self, index):
        try:
            self._latest_requests_lock.acquire()
            pv_name = self._latest_ioc_pvs[index]
            return pv_name

        finally:
            self._latest_requests_lock.release()

    def get_name(self, kind, interval, index):
        # Not that the bigger the index the more recent the data!!!!
        if kind == PLOT_TYPE.REQ_IOC:
            return self.get_name_ioc_req(interval, index)
        elif kind == PLOT_TYPE.MOST_ACTIVE_PVS:
            return self.get_name_pv_ioc(index)
        elif kind == PLOT_TYPE.MOST_ACTIVE_IOCS:
            return self.get_name_ioc_pv(index)
        elif kind == PLOT_TYPE.MEM_RSS_USAGE:
            return "RSS Mem Usage"
        elif kind == PLOT_TYPE.MEM_VMS_USAGE:
            return "VMS Mem Usage"

        return NAME.get(kind, "%r" % kind)

    def get_name_ioc_req(self, interval, index):

        interval_data = INTERVAL_DATA[interval]
        bin_count = len(interval_data[KEY.COLORS])
        bin_units = interval_data[KEY.BIN_NAME]

        index_reversed = bin_count - index
        index_reversed -= 1

        if interval == PLOT_INTERVAL.HOUR:
            index_reversed *= 10

        if index_reversed != 1:
            bin_units += "s"

        return "T - %d %s" % (index_reversed, bin_units)

    def get_y_axis_label(self, kind):
        return Y_AXIS.get(kind, "%r" % kind)

    def get_color(self, kind, interval, index):
        log.debug(
            "called; kind: %s interval: %s, index: %d",
            kind,
            interval,
            index,
        )

        if kind != PLOT_TYPE.REQ_IOC:
            return ""

        interval_data = INTERVAL_DATA[interval]
        colors = interval_data[KEY.COLORS]
        color_index = colors[index]
        color = COLOR_GREEN[color_index]
        log.debug(
            "index: %d color_index: %d color: %s",
            index,
            color_index,
            color,
        )
        return color

    def get_x_iocs(self, interval):
        try:
            self._lock.acquire()
            data = self._data_ioc_req.get(interval, {})
            return data.get(KEY.IOC_NAMES)
        finally:
            self._lock.release()

    def get_y_iocs(self, interval, index):
        try:
            self._lock.acquire()
            data = self._data_ioc_req.get(interval, {})
            var_dict = data.get(KEY.COUNTS)
            return var_dict.get(index)

        finally:
            self._lock.release()

    def fetch_latest_results(self):
        """
        Crazy complicated code to cross reference active PVs with active IOCs.
        I am not sure how userfut this will be,,, I just did it as a challenge
        """

        # NOTE: by commenting out the two lines below the plot is regenerated
        # on demand from live data
        # if time.time() - self._latest_requests_time < 60:
        #     return

        self._latest_requests_time = time.time()
        latest_requests = self._pvman.get_latest_requests()

        pv_dict = {}
        ioc_dict = {}

        for request in latest_requests:
            pv_name = request[0]
            ioc_id = request[1]
            ### port = request[2]

            pv_data = pv_dict.get(pv_name, {})
            pv_data[ioc_id] = pv_data.get(ioc_id, 0) + 1
            pv_dict[pv_name] = pv_data

            ioc_data = ioc_dict.get(ioc_id, {})
            ioc_data[pv_name] = ioc_data.get(pv_name, 0) + 1
            ioc_dict[ioc_id] = ioc_data

        pv_list = []
        for pv_name, pv_data in pv_dict.items():
            total_count = 0
            for ioc_id, requests in pv_data.items():
                total_count += requests

            pv_list.append((total_count, pv_name))

        pv_list.sort()
        pv_list.reverse()

        ioc_list = []
        for ioc_id, ioc_data in ioc_dict.items():
            total_count = 0
            for pv_name, requests in ioc_data.items():
                total_count += requests

            ioc_list.append((total_count, ioc_id))

        ioc_list.sort()
        ioc_list.reverse()

        last_pvs = pv_list[:TOP_LATEST_PV_COUNT]
        last_iocs = ioc_list[:TOP_IOC_COUNT]

        # ----------------------------------------------------------------------
        # Get the IOCS that contribute to the active PVs (lots of work)
        # ---------------------------------------------------------------------

        temp_ioc_dict = {}
        # Loop through the top PVs and figure out which IOCs contribute...
        for item in last_pvs:
            pv_name = item[1]
            pv_data = pv_dict.get(pv_name)
            for ioc, count in pv_data.items():
                temp_ioc_dict[ioc] = temp_ioc_dict.get(ioc, 0) + count

        temp = [(count, ioc) for ioc, count in temp_ioc_dict.items()]
        temp.sort()
        temp.reverse()

        # Trim maximum contributing IOCs
        contributing_iocs = temp[:23]

        contributing_iocs = [item[1] for item in contributing_iocs]

        # We need to reverse here becase it looks like plot adds line legend in reverse order
        contributing_iocs.reverse()
        # Sort the contributing IOCS

        # Build a matrix like this:
        #
        #      PV1  PV2
        #   +-----+-----+
        #   |     |     |  ioc 1
        #   +-----+-----+
        #   |     |     |  ioc 2
        #   +-----+-----+
        #   |     |     |  other
        #   +-----+-----+

        row_count = len(contributing_iocs)
        result = {}

        accounted_for = {}

        for index in range(row_count):
            row = []
            ioc = contributing_iocs[index]
            for item in last_pvs:
                pv_name = item[1]
                pv_data = pv_dict.get(pv_name)
                count = pv_data.get(ioc, 0)
                row.append(count)

                accounted_for[pv_name] = accounted_for.get(pv_name, 0) + count

            result[ioc] = row

        # Now must add a final row to the result for "other"
        # This is all the counts that are NOT accounted for by the IOCs
        row = []
        pv_name_list = []

        for item in last_pvs:
            count_want = item[0]
            pv_name = item[1]
            pv_name_list.append("'%s'" % pv_name)
            count_have = accounted_for[pv_name]
            row.append(count_want - count_have)

        for count in row:
            if count != 0:
                result[0] = row
                contributing_iocs.append(0)
                break

        # Make a string of the PV names
        pv_name_str = ",".join(pv_name_list)

        self._latest_lock.acquire()
        self._latest_pv_names_str = pv_name_str
        self._latest_pv_iocs = contributing_iocs
        self._latest_pv_data = result
        self._latest_lock.release()

        # ----------------------------------------------------------------------
        # Get the PVs that contribute to the active IOCs (lots of work)
        # ---------------------------------------------------------------------

        temp_pv_dict = {}
        # Loop through the top PVs and figure out which IOCs contribute...
        for item in last_iocs:
            ### ioc_name = item[1]
            ioc_data = ioc_dict.get(ioc)
            for pv, count in ioc_data.items():
                temp_pv_dict[pv] = temp_pv_dict.get(pv, 0) + count

        temp = [(count, pv) for pv, count in temp_pv_dict.items()]
        temp.sort()
        temp.reverse()

        # Trim maximum contributing IOCs
        contributing_pvs = temp[:16]

        contributing_pvs = [item[1] for item in contributing_pvs]

        # We need to reverse here becase it looks like plot adds line legend in reverse order
        contributing_pvs.reverse()
        # Sort the contributing IOCS

        # Build a matrix like this:
        #
        #     IOC1  IOC2
        #   +-----+-----+
        #   |     |     |  PV 1
        #   +-----+-----+
        #   |     |     |  PV 2
        #   +-----+-----+
        #   |     |     |  other
        #   +-----+-----+

        row_count = len(contributing_pvs)
        result = {}

        accounted_for = {}

        for index in range(row_count):
            row = []
            pv_name = contributing_pvs[index]
            for item in last_iocs:
                ioc = item[1]
                ioc_data = ioc_dict.get(ioc)
                count = ioc_data.get(pv_name, 0)
                row.append(count)

                accounted_for[ioc] = accounted_for.get(ioc, 0) + count

            result[pv_name] = row

        # Now must add a final row to the result for "other"
        # This is all the counts that are NOT accounted for by the IOCs
        row = []
        ioc_name_list = []

        for item in last_iocs:
            count_want = item[0]
            ioc = item[1]
            ioc_name_list.append("'%s'" % get_host_name(ioc))
            count_have = accounted_for[ioc]
            row.append(count_want - count_have)

        for count in row:
            if count != 0:
                result["Other"] = row
                contributing_pvs.append("Other")
                break

        # Make a string of the PV names
        ioc_name_str = ",".join(ioc_name_list)

        self._latest_lock.acquire()
        self._latest_ioc_names_str = ioc_name_str
        self._latest_ioc_pvs = contributing_pvs
        self._latest_ioc_data = result
        self._latest_lock.release()

    def get_latest_var_count(self, kind):
        self.fetch_latest_results()

        if kind == PLOT_TYPE.MOST_ACTIVE_PVS:
            vars = self._latest_pv_iocs
        elif kind == PLOT_TYPE.MOST_ACTIVE_IOCS:
            vars = self._latest_ioc_pvs
        else:
            raise ValueError("kind not supported: %s" % kind)

        try:
            self._latest_requests_lock.acquire()
            return len(vars)
        finally:
            self._latest_requests_lock.release()

    def get_var_count(self, kind, interval):

        if kind == PLOT_TYPE.REQ_IOC:
            try:
                self._lock.acquire()
                data = self._data_ioc_req.get(interval, {})
                var_dict = data.get(KEY.COUNTS, {})
                return len(var_dict)

            finally:
                self._lock.release()

        elif kind in [PLOT_TYPE.MOST_ACTIVE_PVS, PLOT_TYPE.MOST_ACTIVE_IOCS]:
            return self.get_latest_var_count(kind)

    def get_x_latest(self, kind):

        try:
            self._latest_requests_lock.acquire()

            if kind == PLOT_TYPE.MOST_ACTIVE_PVS:
                latest = self._latest_pv_names_str
            else:
                latest = self._latest_ioc_names_str

            return latest

        finally:
            self._latest_requests_lock.release()

    def get_y_latest(self, kind, index):

        try:
            self._latest_requests_lock.acquire()

            if kind == PLOT_TYPE.MOST_ACTIVE_PVS:
                index = self._latest_pv_iocs[index]
                row = self._latest_pv_data.get(index)
            else:
                index = self._latest_ioc_pvs[index]
                row = self._latest_ioc_data.get(index)

            counts = []
            for count in row:
                counts.append("%d" % count)

            return ",".join(counts)

        finally:
            self._latest_requests_lock.release()

    def get_x(self, kind, interval):

        log.debug(
            "called; kind: %r interval: %r",
            kind,
            interval,
        )
        if kind == PLOT_TYPE.REQ_IOC:
            return self.get_x_iocs(interval)

        elif kind in [PLOT_TYPE.MOST_ACTIVE_PVS, PLOT_TYPE.MOST_ACTIVE_IOCS]:
            return self.get_x_latest(kind)

        try:
            self._lock.acquire()
            return self._data_time[interval]
        finally:
            self._lock.release()

    def get_y(self, kind, interval, index=0):
        log.debug(
            "called; kind: %r interval: %r index: %d",
            kind,
            interval,
            index,
        )

        if kind == PLOT_TYPE.REQ_IOC:
            return self.get_y_iocs(interval, index)
        elif kind in [PLOT_TYPE.MOST_ACTIVE_PVS, PLOT_TYPE.MOST_ACTIVE_IOCS]:
            return self.get_y_latest(kind, index)

        try:
            self._lock.acquire()
            data = self._data[interval]
            return data[kind]
        finally:
            self._lock.release()

    def start(self):
        self._timer.start()

    def worker(self):

        # This worker can take a long time to compute the year plots
        thread_id = TM.add("Plotman: Worker", 1200)
        last_ping_time = time.time()

        interval_hour = 60  # generate hour plot every minute
        interval_day = 60 * 10  # generate day plot every 10 minutes
        interval_week = 60 * 60  # generate week plot every 60 minutes
        interval_month = 60 * 60 * 24  # generate month plot every day
        interval_year = 60 * 60 * 24  # generate year plot every day

        last_time_hour = 0
        last_time_day = 0
        last_time_week = 0
        last_time_month = 0
        last_time_year = 0

        while True:
            time.sleep(1)

            cur_time = time.time()
            if cur_time - last_ping_time > 60:
                TM.ping(thread_id)
                last_ping_time = cur_time

            if cur_time - last_time_hour > interval_hour:
                self.compute_plots(PLOT_INTERVAL.HOUR)
                last_time_hour = cur_time

            if cur_time - last_time_day > interval_day:
                self.compute_plots(PLOT_INTERVAL.DAY)
                last_time_day = cur_time

            if cur_time - last_time_week > interval_week:
                self.compute_plots(PLOT_INTERVAL.WEEK)
                last_time_week = cur_time

            if cur_time - last_time_month > interval_month:
                self.compute_plots(PLOT_INTERVAL.MONTH)
                self.compute_plots(PLOT_INTERVAL.MONTH2)
                self.compute_plots(PLOT_INTERVAL.MONTH6)
                last_time_month = cur_time

            if cur_time - last_time_year > interval_year:
                self.compute_plots(PLOT_INTERVAL.YEAR)
                self.compute_plots(PLOT_INTERVAL.YEAR2)
                last_time_year = cur_time

    def process_lines(self, lines, interval):
        """ """
        log.debug("called, len(lines): %d", len(lines))

        time_list = []
        user_req_list = []
        gw_req_list = []
        mem_usage_rss_list = []
        mem_usage_vms_list = []
        cpu_usage_list = []
        srv_unix_free_home_list = []
        srv_unix_free_iocapps_list = []

        pvs_invalid_list = []
        pvs_lost_list = []
        pvs_total_list = []
        pvs_duplicate_list = []

        pkts_gw_list = []
        pkts_user_list = []

        for item in lines:
            parts = item.split(",")

            parts_count = len(parts)

            user_requests = 0
            gateway_requests = 0
            pvs_lost = 0
            pvs_invalid = 0
            mem_usage_rss = 0
            mem_usage_vms = 0
            cpu_usage = 0
            pkts_user = 0
            srv_unix_free_home = 0
            srv_unix_free_iocapps = 0

            if parts_count == 11:
                user_requests = float(parts[2])
                gateway_requests = float(parts[3])

                pvs_lost = float(parts[8])
                pvs_total = float(parts[7])
                pvs_duplicate = float(parts[9])
                pvs_invalid = float(parts[10])
                pkts_gw = float(parts[1])

            elif parts_count in [13, 14, 15, 16]:
                user_requests = float(parts[2])
                gateway_requests = float(parts[4])
                pvs_lost = float(parts[8])
                pvs_total = float(parts[7])
                pvs_duplicate = float(parts[9])
                pvs_invalid = float(parts[10])
                pkts_gw = float(parts[3])
                pkts_user = float(parts[1])

                # This is mem usage in bytes
                mem_usage_rss = parts[11]
                try:
                    mem_usage_rss = float(mem_usage_rss) / float(
                        1024 * 1024 * 1024
                    )
                except:
                    mem_usage_rss = 0

                try:
                    cpu_usage = float(parts[12]) / 10.0
                except:
                    cpu_usage = 0

                # Added mem_vms to saved stats on Feb. 9, 2021
                if parts_count >= 14:

                    mem_usage_vms = parts[13]
                    try:
                        mem_usage_vms = float(mem_usage_vms) / float(
                            1024 * 1024 * 1024
                        )
                    except:
                        mem_usage_vms = 0

                if parts_count >= 15:
                    srv_unix_free_home = float(parts[14]) / float(
                        1024 * 1024 * 1024
                    )

                if parts_count >= 16:
                    srv_unix_free_iocapps = float(parts[15]) / float(
                        1024 * 1024 * 1024
                    )

            elif parts_count == 8:
                continue

            else:
                raise ValueError("unsupported parts count: %d" % parts_count)

            t = parts[0]

            time_list.append(t)
            user_req_list.append(user_requests)
            gw_req_list.append(gateway_requests)
            mem_usage_rss_list.append(mem_usage_rss)
            mem_usage_vms_list.append(mem_usage_vms)
            pvs_lost_list.append(pvs_lost)
            pvs_total_list.append(pvs_total)
            pvs_duplicate_list.append(pvs_duplicate)
            pvs_invalid_list.append(pvs_invalid)

            pkts_gw_list.append(pkts_gw)
            pkts_user_list.append(pkts_user)
            cpu_usage_list.append(cpu_usage)
            srv_unix_free_home_list.append(srv_unix_free_home)
            srv_unix_free_iocapps_list.append(srv_unix_free_iocapps)

        temp = ["'%s'" % t for t in time_list]
        time_str = ",".join(temp)

        temp = ["%.2f" % v for v in user_req_list]
        user_req_str = ",".join(temp)

        temp = ["%.2f" % v for v in gw_req_list]
        gw_req_str = ",".join(temp)

        temp = ["%.2f" % v for v in pkts_gw_list]
        pkts_gw_str = ",".join(temp)

        temp = ["%.2f" % v for v in pkts_user_list]
        pkts_user_str = ",".join(temp)

        temp = ["%.2f" % v for v in pvs_lost_list]
        pvs_lost_str = ",".join(temp)

        temp = ["%.2f" % v for v in pvs_total_list]
        pvs_total_str = ",".join(temp)

        temp = ["%.2f" % v for v in pvs_duplicate_list]
        pvs_duplicate_str = ",".join(temp)

        temp = ["%.2f" % v for v in pvs_invalid_list]
        pvs_invalid_str = ",".join(temp)

        temp = ["%.3f" % v for v in mem_usage_rss_list]
        mem_rss_usage_str = ",".join(temp)

        temp = ["%.3f" % v for v in mem_usage_vms_list]
        mem_vms_usage_str = ",".join(temp)

        temp = ["%.2f" % v for v in cpu_usage_list]
        cpu_usage_str = ",".join(temp)

        temp = ["%.2f" % v for v in srv_unix_free_home_list]
        srv_unix_free_home_str = ",".join(temp)

        temp = ["%.2f" % v for v in srv_unix_free_iocapps_list]
        srv_unix_free_iocapps_str = ",".join(temp)

        self._lock.acquire()
        self._data_time[interval] = time_str
        data = self._data.get(interval, {})
        data[PLOT_TYPE.REQ_USER] = user_req_str
        data[PLOT_TYPE.REQ_GATEWAY] = gw_req_str
        data[PLOT_TYPE.PVS_LOST] = pvs_lost_str
        data[PLOT_TYPE.PVS_TOTAL] = pvs_total_str
        data[PLOT_TYPE.PVS_DUPLICATE] = pvs_duplicate_str
        data[PLOT_TYPE.PVS_INVALID] = pvs_invalid_str
        data[PLOT_TYPE.MEM_RSS_USAGE] = mem_rss_usage_str
        data[PLOT_TYPE.MEM_VMS_USAGE] = mem_vms_usage_str
        data[PLOT_TYPE.CPU_USAGE] = cpu_usage_str
        data[PLOT_TYPE.PKT_GW] = pkts_gw_str
        data[PLOT_TYPE.PKT_USER] = pkts_user_str
        data[PLOT_TYPE.SRV_UNIX_FREE_HOME] = srv_unix_free_home_str
        data[PLOT_TYPE.SRV_UNIX_FREE_IOCAPPS] = srv_unix_free_iocapps_str
        self._data[interval] = data

        self._lock.release()

    def sum_lines(self, lines):
        """
        Account for the possibility that new data may be added to the lines.
        So do not assume the number of columns.  Because there can be missing
        data, misleading data can result unless the sum is divided by the
        number of samples, which keeps the overall rate at packets/requests
        per minute
        """

        use_lines = []
        max_parts = None
        for line in lines:
            parts = line.split(",")
            if max_parts is None:
                max_parts = len(parts)
                use_lines.append(line)
            else:
                if len(parts) == max_parts:
                    use_lines.append(line)
                else:
                    ## This can happen when we try to bin the lines
                    ## from the old system and the new system in one bin
                    ## print("CANNOT USE THIS LINE IN THIS BIN!!!!!")
                    pass

        lines = use_lines

        # Make a list with max_parts elements
        sum = [0] * max_parts

        for line in lines:
            parts = line.split(",")
            for i in range(max_parts):
                if i == 0:
                    continue  # this skips part[0] ... the date
                try:
                    sum[i] += float(parts[i].strip())
                except Exception as err:
                    log.exception("exception: %r", err)

            line_time = parts[0].strip()

        # Because the lines are sorted youngest to oldest, use time of last time as bin time
        result = line_time
        len_lines = float(len(lines))

        for i in range(max_parts):
            if i == 0:
                continue  # This skips part[0] .. the date

            result += ", %.4f" % (sum[i] / len_lines)

        return result

    def get_line_time(self, line):
        parts = line.split(",")

        t = PlotTimer()
        t.from_time_arg(parts[0].strip())
        return t.get_utc_timestamp()

    def bin_data(self, lines, st, et):
        """
        This method collapses the data from minutes to hours if the interval
        is greater that 3 days, and from minutes to days in the interval
        is greater than 60 days
        """
        log.debug("called")

        binned_lines = {}
        # Get the seconds between the start time and the end time

        start_time = st.get_utc_timestamp()
        end_time = et.get_utc_timestamp()
        interval = end_time - start_time

        bins = BIN_COUNT
        # Aim for 1200
        bin_size = float(interval) / float(bins)

        for line_item in lines:
            line = line_item[0]
            line_timestamp = self.get_line_time(line)
            interval_from_start = line_timestamp - start_time
            bin_index = int(interval_from_start / bin_size)
            if bin_index < 0 or bin_index >= bins:
                raise ValueError("invalid bin index: %d" % bin_index)

            bin = binned_lines.get(bin_index, [])
            bin.append(line)
            binned_lines[bin_index] = bin

        result = []
        for bin_index in range(bins):
            bin = binned_lines.get(bin_index)
            if bin is None:
                log.debug("no lines in bin: %d", bin_index)
                continue

            if len(bin) == 1:
                result.append(bin[0])
                continue

            line = self.sum_lines(bin)
            if line is None:
                continue

            result.append(line)

        return result

    def get_lines(self, file_list, st, et):
        """
        Get all the lines of data, potentially from multiple data files,
        between the specified start time and end time.
        """
        log.debug(
            "called: st: %s et: %s",
            st.get_datetime(),
            et.get_datetime(),
        )

        lines = []

        for file_name in file_list:
            log.debug(
                "opening file -------------------------------> %s",
                file_name,
            )
            try:
                f = open(file_name, "r")
                for line in f:
                    line = line.strip()
                    keep = False
                    if st.check_line_newer(line):
                        if not et.check_line_newer(line):
                            keep = True

                    if keep:
                        lines.append((line, file_name))
                    else:
                        pass

            except Exception as err:
                # log.exception(
                #     "Exception processing file: %s: %s",
                #     file_name, str(err)
                # )
                f = None

            finally:
                if f:
                    f.close()

        return lines

    def make_file_list(self, st_in, et, file_prefix):

        log.debug("called")
        # Copy the input start time as it will be modified herein
        st = PlotTimer()
        st.replicate(st_in)

        file_list = []

        while True:

            y, m, d = st.get_yyyy_mm_dd()

            file_name = (
                "data/%s.clsi.ca/stats/%04d_%02d/%s_%04d_%02d_%02d.csv"
                % (
                    self._server,
                    y,
                    m,
                    file_prefix,
                    y,
                    m,
                    d,
                )
            )

            log.debug("Adding file: %s", file_name)

            file_list.append(file_name)

            if et.check(st):
                break

            st.increment_day()

        file_list = sorted(file_list)

        for file in file_list:
            log.debug("returning file: %s", file)

        return file_list

    def get_top_users_bin_count(self, interval):
        """ """
        interval_data = INTERVAL_DATA[interval]
        return len(interval_data[KEY.COLORS])

    def process_lines_top_iocs(self, lines, st, et, interval):
        """
        Convert the raw data as saved in multiple files into data to plot
        top users histogram
        """

        def total_requests(data):
            result = 0
            for value in data.values():
                result += value
            return result

        request_dict = {}

        log.debug(
            "called; lines: %d interval: %s",
            len(lines),
            interval,
        )

        # Get the start and end seconds in UTC
        ts = st.get_utc_timestamp()
        te = et.get_utc_timestamp()

        # How many bins should there be?
        bin_count = self.get_top_users_bin_count(interval)
        bin_sec = int((te - ts) / bin_count)

        log.debug(
            "bin_count: %d bin_sec: %d",
            bin_count,
            bin_sec,
        )

        for line_item in lines:
            line = line_item[0]
            ### file_name = line_item[1]
            parts = line.split(",")
            if len(parts) < 4:
                # There are a bunch request lines that have no data.  There are
                # some in 2019_06 and 2019_07 as an example.  I don't know why this is
                # but the following line floods the logs
                # log.warning("SKIPPING LINE: %s (%s)", line, file_name)
                continue

            # Compute the "bin" index for this line
            end_time = int(parts[1])

            # Ensure that this data is not past the end time
            if end_time > te:
                raise ValueError("end time %d > %d" % (end_time, te))

            # Ensure that this data is not before the start time
            if end_time < ts:
                raise ValueError("start time %d < %d" % (end_time, ts))

            # Compute the bin index.
            # The larger the bin index the more recent the request!
            diff_seconds = end_time - ts
            bin_index = int(float(diff_seconds) / float(bin_sec))

            # Sanity check the bin index
            if bin_index < 0 or bin_index >= bin_count:
                bin_index = bin_count - 1

            i = 2
            while True:
                try:
                    ip = int(parts[i])
                    request_count = int(parts[i + 1])
                    i += 2

                    d = request_dict.get(ip, {})
                    r = d.get(bin_index, 0)
                    r += request_count
                    d[bin_index] = r
                    request_dict[ip] = d

                except:
                    break

        # Sort list by total number of requests
        request_list = [
            (total_requests(value), key) for key, value in request_dict.items()
        ]
        request_list = sorted(request_list)
        request_list.reverse()

        # Plot only top 30 users
        plot_list = request_list[:TOP_IOC_COUNT]

        host_name_list = []
        var_dict = {}

        for item in plot_list:

            host_key = item[1]
            data = request_dict.get(host_key)
            host_name = get_host_name(host_key)
            host_name_list.append("'%s'" % host_name)

            for i in range(bin_count):
                requests = data.get(i, 0)
                var_list = var_dict.get(i, [])
                var_list.append(requests)
                var_dict[i] = var_list

        host_str = ",".join(host_name_list)

        var_str = {}
        for k, v in var_dict.items():
            result = []
            for item in v:
                result.append("%d" % item)

            temp_str = ",".join(result)
            var_str[k] = temp_str

        self._lock.acquire()
        interval_data = self._data_ioc_req.get(interval, {})
        interval_data[KEY.IOC_NAMES] = host_str
        interval_data[KEY.COUNTS] = var_str
        self._data_ioc_req[interval] = interval_data
        self._lock.release()

    def compute_plots_ioc(self, st, et, interval):
        log.debug("called; interval: %r", interval)
        file_list = self.make_file_list(st, et, FILE_PREFIX_REQ_COUNTS)
        lines = self.get_lines(file_list, st, et)
        self.process_lines_top_iocs(lines, st, et, interval)

    def compute_plots(self, interval):
        log.debug("called; interval: %r", interval)

        end_time = time.time()
        d = INTERVAL_DATA[interval]
        start_time = end_time - d[KEY.SECONDS]
        st = PlotTimer(start_time)
        et = PlotTimer(end_time)

        log.debug(
            "start time: %s end time: %s",
            st.get_datetime(),
            et.get_datetime(),
        )
        # Compute the IOC plots which are histograms
        self.compute_plots_ioc(st, et, interval)

        file_list = self.make_file_list(st, et, FILE_PREFIX_COUNTS)
        lines = self.get_lines(file_list, st, et)
        lines = self.bin_data(lines, st, et)
        self.process_lines(lines, interval)


def add_traceback(err):

    result = "<h2>Error Processing Request</h2>\n"
    result += (
        "<b><em>Please report this error to the maintainers of "
        "PV Monitor</em></b><br><br>\n"
    )
    result += "<span>\n"
    t = traceback.format_exc(10)
    lines = t.split("\n")
    for line in lines:
        print(line)
        result += "%s<br>" % line
    result += "</span>\n"
    return result


def make_page_header(script=None):
    page = (
        "<!DOCTYPE html><html><head>\n"
        '<meta http-equiv="Cache-Control" '
        'content="no-cache, no-store, must-revalidate"/>\n'
        '<meta http-equiv="Pragma" content="no-cache"/>\n'
        '<meta http-equiv="Expires" content="0"/>\n'
        "<title>PLOT TEST</title>\n"
        "</head>\n"
    )

    if script:
        page += script
    page += "<body>\n"
    return page


def test_line_plot():

    from pv_manager import PVManager

    try:
        kind = PLOT_TYPE.MOST_ACTIVE_IOCS
        interval = PLOT_INTERVAL.DAY

        pvman = PVManager("opi2031-006", "opi2031-006")
        pvman.load_test_data("data/temp_request_list.json")
        plotman = PlotDataMgr("opi2031-006.clsi.ca")
        plotman.set_pvman(pvman)

        plotman.compute_plots(interval)

        page = make_page_header(script=SETTINGS.SCRIPT_PLOTLY_JS)
        page += "<h2>Plot Test: %s %s</h2>\n" % (kind, interval)
        page += '<div id="myDiv" style="width:1200px;height:700px;"></div>\n'
        page += "<script>\n"

        # CUT HERE ============================================================

        var_names = []

        if kind in [
            PLOT_TYPE.REQ_IOC,
            PLOT_TYPE.MOST_ACTIVE_IOCS,
            PLOT_TYPE.MOST_ACTIVE_PVS,
        ]:
            var_count = plotman.get_var_count(kind, interval)

            for index in range(var_count):
                var_name = "trace_%d" % index
                var_names.append(var_name)
                var_str = ""
                var_str += "var %s = {\n" % var_name
                var_str += "    x: [%s],\n" % plotman.get_x(kind, interval)
                var_str += "    y: [%s],\n" % plotman.get_y(
                    kind, interval, index=index
                )
                var_str += '    type: "bar",\n'
                var_str += '    name: "%s",\n' % plotman.get_name(
                    kind, interval, index
                )
                var_str += '    marker: { color: "%s" },\n' % plotman.get_color(
                    kind, interval, index
                )
                var_str += "};\n"
                page += var_str

            var_name_str = ",".join(var_names)
            page += "var data = [%s];\n" % var_name_str
            page += "var layout = {\n"
            page += 'barmode: "stack",\n'
            page += 'yaxis: {title: "%s"},\n' % plotman.get_y_axis_label(kind)
            page += "};\n"

        elif kind in [PLOT_TYPE.REQ_GATEWAY, PLOT_TYPE.REQ_USER]:
            lines = [
                PLOT_TYPE.REQ_USER,
                PLOT_TYPE.PKT_USER,
                PLOT_TYPE.REQ_GATEWAY,
                PLOT_TYPE.PKT_GW,
            ]
            for index, kind in enumerate(lines):
                var_name = "trace_%d" % index
                var_names.append(var_name)
                var_str = "var %s = {\n" % var_name
                var_str += '    type: "scatter",\n'
                var_str += '    mode: "lines",\n'
                var_str += '    name: "%s",\n' % plotman.get_name(
                    kind, interval, index
                )
                var_str += "    x: [%s],\n" % plotman.get_x(kind, interval)
                var_str += "    y: [%s],\n" % plotman.get_y(kind, interval)
                var_str += "};\n"

                page += var_str
            var_name_str = ",".join(var_names)
            page += "var data = [%s];\n" % var_name_str

            page += "var layout = {\n"
            page += 'yaxis: {title: "%s"},\n' % plotman.get_y_axis_label(kind)
            page += "};\n"

        else:
            # This is a simple line plot
            page += "var trace1 = {\n"
            page += '    type: "scatter",\n'
            page += '    mode: "lines",\n'
            page += '    name: "User Requests",\n'
            page += "    x: [%s],\n" % plotman.get_x(kind, interval)
            page += "    y: [%s],\n" % plotman.get_y(kind, interval)
            page += '    line: {color: "#17BECF"}\n'
            page += "};\n"
            page += "var data = [trace1];\n"

            page += "var layout = {\n"
            page += 'yaxis: {title: "%s"},\n' % plotman.get_y_axis_label(kind)
            page += "};\n"

        page += 'Plotly.newPlot("myDiv", data, layout);\n'

        # CUT HERE ============================================================
        page += "</script>\n"

    except Exception as err:
        page = make_page_header()
        page += add_traceback(err)

    page += "</body></html>\n"

    f = open("plot_test.html", "w")
    f.write(page)
    f.close()


if __name__ == "__main__":

    test_line_plot()
