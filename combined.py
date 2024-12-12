# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
# System Imports
import logging

from utilities.pvm_logger import PVMonLogger

logging.setLoggerClass(PVMonLogger)
import argparse
import datetime
import functools
import json
import os
import signal
import socket
import time
import traceback
import threading
import urllib.error
import urllib.parse
import urllib.request

import bottle
import psutil
import git
from bottle import PasteServer

from css import CSS
from data_management.database import EVENT_STATUS
from data_management.dataman import DataManager
from definitions import (COLOR, EVENT, EVENT_DISPLAY, GROUP, IOC_OPTION,
                         IOC_OPTION_BUTTON, IOC_OPTION_BUTTON_LIST,
                         IOC_OPTION_LIST, KEY, LINUXMON_PV_SUFFIX_POLL_ALL,
                         PLOT_INTERVAL, PLOT_TYPE, PRIORITY, PV_KEY, SETTINGS,
                         SORT_MODE, Constant, config_log, config)
from descriptions import DESCRIPTION
from data_management.heartbeat_manager import KEY as HEARTBEAT_KEY
from jsmin import jsmin
from utilities.print_binary import print_binary
from utilities.pvmonitor_time import PVMonitorTime, my_time
from utilities.statistics import StatisticsManager
from utilities.utils import (beacon_estimated_age_str, get_ip_addr,
                             get_ip_from_key, get_ip_key, make_app_id,
                             make_duration_string, make_last_seen_str,
                             make_size_str, my_uptime_str, get_host_name, remove_leading_space)

log = logging.getLogger("PVMLogger")

deploy_dir = os.getcwd()
repo = git.Repo(deploy_dir)

STATS = StatisticsManager()

# APP_NAME = "PV Monitor 2"
APP_NAME = "PV Monitor Dev Version"

STATUS_STR_LOST = '<span style="background-color:red">- LOST -</span>'
STATUS_STR_NEVER_SEEN = (
    '<span style="background-color:yellow">NEVER SEEN</span>'
)
STATUS_STR_ONLINE = '<span style="background-color:lawngreen">- ONLINE -</span>'

LOST_STR = "*** LOST ***"

DIR_DEC = "dec"
DIR_INC = "inc"

ITEMS_PER_PAGE = 40
MAX_FORGET_PVS = 500


class REQUESTS(Constant):
    PVS = "pvs"
    CLIENTS = "clients"


class PV_LIST(Constant):
    ALL = "all"
    LOST = "lost"
    SEARCH = "search"
    PORT = "port"
    DUPLICATE = "duplicate"
    INVALID = "invalid"
    LATEST_IOC = "latest_ioc"


# These are the sort options in the pv list pick list
PV_LIST_SORT_MODE = {
    PV_LIST.ALL: [
        (SORT_MODE.REQ_TIME, "Last Requested"),
        (SORT_MODE.REQ_COUNT, "Total Requests"),
        (SORT_MODE.NAME, "Name"),
    ],
    PV_LIST.LOST: [
        (SORT_MODE.LAST_SEEN, "Last Seen"),
        (SORT_MODE.REQ_TIME, "Last Requested"),
        (SORT_MODE.REQ_COUNT, "Total Requests"),
        (SORT_MODE.NAME, "Name"),
    ],
    PV_LIST.SEARCH: [
        (SORT_MODE.NAME, "Name"),
    ],
    PV_LIST.LATEST_IOC: [
        (SORT_MODE.NAME, "Name"),
        (SORT_MODE.REQ_COUNT, "Recent Request Count"),
        (SORT_MODE.REQ_TIME, "Last Requested"),
    ],
    PV_LIST.DUPLICATE: [(SORT_MODE.NAME, "Name")],
    PV_LIST.INVALID: [
        (SORT_MODE.REQ_TIME, "Last Requested"),
        (SORT_MODE.REQ_COUNT, "Total Requests"),
        (SORT_MODE.NAME, "Name"),
    ],
    PV_LIST.PORT: [(SORT_MODE.NAME, "Name")],
}

COMMON_KINDS = [
    (PV_LIST.ALL, "All"),
    (PV_LIST.LOST, "Lost"),
    (PV_LIST.DUPLICATE, "Duplicates"),
    (PV_LIST.INVALID, "Invalid"),
]

# These are the "kinds" that go to
PV_LIST_KIND_SELECT = {
    PV_LIST.ALL: COMMON_KINDS,
    PV_LIST.LOST: COMMON_KINDS,
    PV_LIST.DUPLICATE: COMMON_KINDS,
    PV_LIST.INVALID: COMMON_KINDS,
    PV_LIST.LATEST_IOC: COMMON_KINDS
    + [(PV_LIST.LATEST_IOC, "Latest Requests")],
    PV_LIST.SEARCH: COMMON_KINDS + [(PV_LIST.SEARCH, "Search")],
    PV_LIST.PORT: COMMON_KINDS + [(PV_LIST.PORT, "Port")],
}

PLOT_TYPE_SELECT = [
    (PLOT_TYPE.REQ_USER, "CA_PROTO_SEARCH"),
    (PLOT_TYPE.REQ_IOC, "CA_PROTO_SEARCH by Client"),
    (PLOT_TYPE.PVS_TOTAL, "PVs - Total"),
    (PLOT_TYPE.PVS_LOST, "PVs - Lost"),
    (PLOT_TYPE.PVS_DUPLICATE, "PVs - Duplicate"),
    (PLOT_TYPE.PVS_INVALID, "PVs - Invalid"),
    (PLOT_TYPE.MOST_ACTIVE_PVS, "Latest Requests by PV"),
    (PLOT_TYPE.MOST_ACTIVE_IOCS, "Latest Requests by Client"),
    (PLOT_TYPE.MEM_RSS_USAGE, "Memory Usage"),
    (PLOT_TYPE.CPU_USAGE, "CPU Usage"),
    (PLOT_TYPE.SRV_UNIX_FREE_HOME, "SRV-UNIX Disk Space Free"),
]

PLOT_TYPE_SELECT_DICT = dict(PLOT_TYPE_SELECT)

PLOT_INTERVAL_SELECT = [
    (PLOT_INTERVAL.HOUR, "Last Hour"),
    (PLOT_INTERVAL.DAY, "Last Day"),
    (PLOT_INTERVAL.WEEK, "Last Week"),
    (PLOT_INTERVAL.MONTH, "Last Month"),
    (PLOT_INTERVAL.MONTH2, "Last 2 Months"),
    (PLOT_INTERVAL.MONTH6, "Last 6 Months"),
    (PLOT_INTERVAL.YEAR, "Last Year"),
    (PLOT_INTERVAL.YEAR2, "Last 2 Years"),
]

PLOT_INTERVAL_SELECT_DICT = dict(PLOT_INTERVAL_SELECT)

HEARTBEAT_SORT = [
    (SORT_MODE.UPTIME, "Uptime"),
    (SORT_MODE.LAST_SEEN, "Last Seen"),
    (SORT_MODE.IOC, "IOC Name"),
]


class GotoExit(Exception):
    pass


class MyApp(bottle.Bottle):

    def __init__(self, name, gateway, mode):

        super(MyApp, self).__init__()

        self._start_time = time.time()
        self._name = name

        self._gateway = gateway
        self._mode = mode

        self._pv_search_string_cache = {}

        # javascript cache
        self._js_cache = {}
        log.info("%s", gateway)
        log.debug(
            "gateway: %s (%s)",
            gateway,
            socket.gethostbyname(gateway),
        )

        hostname = socket.getfqdn().lower()

        self._my_hostname = hostname

        if self._mode.startswith("p"):
            log.debug("%s: Production Mode", self._my_hostname)
        else:
            log.debug("%s: Test Mode", self._my_hostname)
        db_name = config.DB_NAME
        port_to_client = config.TO_CLIENT_PORT
        port_to_gateway = config.TO_GATEWAY_PORT
        self._web_server_port = config.HTTP_PORT

        signal.signal(signal.SIGCHLD, self.handle_signal)

        # The DataManager must be accessible by the web pages
        self._dataman = DataManager(
            self._my_hostname,
            db_name,
            self._gateway,
            port_to_client,
            port_to_gateway,
        )

        # Set up the routes
        self.route("/", callback=self.page_epics_events)
        self.route("/index.html", callback=self.page_epics_events)

        self.route("/admin", callback=self.page_admin)
        self.route("/admin/threads", callback=self.page_admin_threads)
        self.route("/admin/netstat", callback=self.page_admin_netstat)
        self.route("/admin/no_netstat", callback=self.page_admin_netstat_none)
        self.route("/admin/pv_fetch", callback=self.page_admin_pv_fetch)
        self.route("/admin/counts", callback=self.page_admin_counts)
        self.route("/admin/logs", callback=self.page_admin_logs)
        self.route("/admin/ack_all_logs", callback=self.page_admin_ack_all_logs)
        self.route(
            "/admin/process_all_iocs", callback=self.page_admin_process_all_iocs
        )
        self.route("/admin/beacons_bad", callback=self.page_admin_beacons_bad)

        self.route("/ack_msg/<msg_id>", callback=self.page_ack_msg)

        self.route(
            "/set_beacon_count/<key>", callback=self.page_set_beacon_count
        )
        self.route("/beacons", callback=self.page_beacons)
        self.route("/beacons/<key>", callback=self.page_beacons_ioc)
        self.route("/client_port", callback=self.page_client_port)

        self.route("/epics_events", callback=self.page_epics_events)

        self.route("/group_grid", callback=self.page_ioc_grid)
        self.route("/groups", callback=self.page_groups)
        self.route("/group_select", callback=self.page_group_select)
        self.route("/group_create", callback=self.page_group_create)
        self.route("/group_edit", callback=self.page_group_edit)
        self.route("/group_delete_ask", callback=self.page_group_delete_ask)
        self.route("/group_delete_yes", callback=self.page_group_delete_yes)

        self.route("/heartbeat", callback=self.page_heartbeat)
        self.route("/heartbeats", callback=self.page_heartbeats)
        self.route(
            "/heartbeat_forget/<key>", callback=self.page_heartbeat_forget
        )

        self.route("/ioc/<key>", callback=self.page_ioc)
        self.route(
            "/ioc_clear_all_warnings/<key>",
            callback=self.page_ioc_clear_all_warnings,
        )
        self.route("/ioc_desc_edit/<key>", callback=self.page_ioc_desc_edit)
        self.route(
            "/ioc_desc_submit/<key>",
            callback=self.page_ioc_desc_submit,
            method="POST",
        )

        self.route(
            "/ioc_apps_submit/<key>",
            callback=self.page_ioc_apps_submit,
            method="POST",
        )

        self.route(
            "/ioc_settings_submit/<key>",
            callback=self.page_ioc_settings_submit,
            method="POST",
        )

        self.route(
            "/ioc_settings_help/<key>", callback=self.page_ioc_settings_help
        )
        self.route("/ioc_redirect/<key>", callback=self.page_ioc_redirect)

        self.route("/ioc_log_add/<key>", callback=self.page_ioc_log_add)
        self.route(
            "/ioc_log_submit/<key>",
            callback=self.page_ioc_log_submit,
            method="POST",
        )
        self.route(
            "/ioc_logs_ack/<key>",
            callback=self.page_ioc_logs_ack,
            method="POST",
        )
        self.route("/ioc_forget/<key>", callback=self.page_ioc_forget)
        self.route(
            "/ioc_forget_submit/<key>",
            callback=self.page_ioc_forget_submit,
            method="POST",
        )

        self.route("/linuxmon", callback=self.page_linuxmon)
        self.route("/linuxmon_ioc", callback=self.page_linuxmon_ioc)
        self.route("/linuxmon_forget", callback=self.page_linuxmon_forget)

        self.route("/pv_list", callback=self.page_pv_list)
        self.route("/pv/<quoted>", callback=self.page_pv_info)
        self.route("/pv_cainfo/<quoted>", callback=self.page_pv_cainfo)
        self.route("/pv_update/<quoted>", callback=self.page_pv_update)
        self.route("/pv_forget", callback=self.page_pvs_forget)
        self.route("/plots", callback=self.page_plots)

        self.route("/requests", callback=self.page_requests)
        self.route("/requests_ioc", callback=self.page_requests_ioc)
        self.route(
            "/requests_latest_client_pvs",
            callback=self.page_requests_client_pvs,
        )

        self.route("/submit_pvs", callback=self.page_submit_pvs, method="POST")
        self.route(
            "/submit_plot", callback=self.page_submit_plot, method="POST"
        )
        self.route(
            "/submit_requests",
            callback=self.page_submit_requests,
            method="POST",
        )
        self.route(
            "/submit_pvs_forget",
            callback=self.page_submit_pvs_forget,
            method="POST",
        )
        self.route(
            "/submit_pvs_forget_ok",
            callback=self.page_submit_pvs_forget_ok,
            method="POST",
        )
        self.route(
            "/submit_forget_pv",
            callback=self.page_submit_forget_one_pv,
            method="POST",
        )
        self.route(
            "/submit_forget_button",
            callback=self.page_submit_forget_button,
            method="POST",
        )
        self.route(
            "/submit_heartbeats",
            callback=self.page_submit_heartbeats,
            method="POST",
        )

        self.route("/api/v1.0/heartbeats", callback=self.page_api_heartbeats)
        self.route("/api/v1.0/beacons", callback=self.page_api_beacons)

    def start(self):
        self._dataman.start()
        myApp.run(
            host="0.0.0.0", port=self._web_server_port, server=PasteServer
        )

    def handle_signal(self, signum):
        log.info(
            "%s\nhandle_signal called: signum: %r\n %s",
            "*" * 80,
            signum,
            "*" * 80,
        )

    def make_host_sort_link(self, key=None):
        if key:
            return "Host"
        return '<a href="%s/sort/host">Host</a>' % self.make_link_base()

    def make_addr_sort_link(self, key=None):
        if key:
            return "IP Address"
        return '<a href="%s/sort/addr">IP Address</a>' % self.make_link_base()

    def make_uptime_sort_link(self, length=None):
        if length is not None and length < 2:
            return "Uptime (est.)"
        return (
            '<a href="%s/sort/uptime">Uptime (est.)</a>' % self.make_link_base()
        )

    def make_restarts_sort_link(self, length=None):
        if length is not None and length < 2:
            return "Restarts"
        return '<a href="%s/sort/restarts">Restarts</a>' % self.make_link_base()

    def make_link_base(self, port=None):
        if port is None:
            port = self._web_server_port
        return "http://%s:%d" % (self._my_hostname, port)

    def make_link(self, url, display):
        return '<a href="%s%s">%s</a>' % (self.make_link_base(), url, display)

    def make_link_admin_log_list(self, offset=0, name="Logs"):
        items = "offset=%d" % offset
        return '<a href="%s/admin/logs?%s">%s</a>' % (
            self.make_link_base(),
            items,
            name,
        )

    def make_link_requests(
        self, kind=REQUESTS.PVS, offset=0, name="Requests", url_only=False
    ):
        items = "kind=%s&offset=%d" % (kind, offset)
        url = "%s/requests?%s" % (self.make_link_base(), items)
        if url_only:
            return url

        return '<a href="%s">%s</a>' % (url, name)

    def make_link_requests_latest_client_pvs(
        self, ioc_id=None, offset=0, display="pvs"
    ):
        return (
            '<a href="%s/requests_latest_client_pvs?ioc_id=%d&offset=%d">%s</a>'
            % (
                self.make_link_base(),
                ioc_id,
                offset,
                display,
            )
        )

    def make_link_plots(
        self,
        kind=PLOT_TYPE.REQ_USER,
        interval=PLOT_INTERVAL.DAY,
        name="Plots",
        url_only=False,
    ):
        items = "kind=%s&interval=%s" % (kind, interval)
        url = "%s/plots?%s" % (self.make_link_base(), items)
        if url_only:
            return url

        return '<a href="%s">%s</a>' % (url, name)

    def make_link_pv_list(
        self,
        offset=0,
        kind=None,
        key=None,
        ioc_id=None,
        sort=None,
        forget=None,
        search_string=None,
        ignore_case=1,
        url_only=False,
        name="PVs",
    ):

        items = "offset=%d" % offset
        if kind is not None:
            items += "&kind=%s" % kind
        if key is not None:
            items += "&key=%s" % str(key)
        if ioc_id is not None:
            items += "&ioc_id=%d" % ioc_id
        if sort is not None:
            items += "&sort=%s" % sort
        if forget:
            items += "&forget=1"

        if search_string:
            s = urllib.parse.quote_plus(urllib.parse.quote_plus(search_string))
            items += "&search_string=%s" % s

        url = "%s/pv_list?%s" % (self.make_link_base(), items)
        if url_only:
            return url
        return '<a href="%s">%s</a>' % (url, name)

    def make_link_linuxmon(
        self, name="LinuxMonitor", sort=SORT_MODE.NAME, direction=DIR_DEC
    ):
        return '<a href="%s/linuxmon?sort=%s&direction=%s">%s</a>' % (
            self.make_link_base(),
            sort,
            direction,
            name,
        )

    def make_link_epics_events(
        self, display="EPICS Events", timestamp=None, offset=None
    ):

        if timestamp is None:
            timestamp = int(time.time())
        url = "%s/epics_events?timestamp=%d" % (
            self.make_link_base(),
            timestamp,
        )

        if offset is not None:
            url += "&offset=%d" % offset

        return '<a href="%s">%s</a>' % (url, display)

    def make_link_ioc(self, ioc_id, display, url_only=False):
        url = "%s/ioc/%s" % (self.make_link_base(), ioc_id)
        if url_only:
            return url
        return '<a href="%s">%s</a>' % (url, display)

    def make_link_admin(self, name="Admin"):
        dead_threads = self._dataman.get_dead_thread_count()
        if dead_threads:
            display = '<span style="background-color:%s">%s</span>' % (
                COLOR.RED,
                name,
            )
        else:
            display = name

        return '<a href="%s/admin">%s</a>' % (self.make_link_base(), display)

    def make_link_pv_update(self, key, name):
        return '<a href="%s/pv_update/%s">%s</a>' % (
            self.make_link_base(),
            urllib.parse.quote_plus(urllib.parse.quote_plus(key)),
            name,
        )

    def make_link_linuxmon_ioc(self, ioc_id, ioc_name, display="LinuxMonitor"):
        return '<a href="%s/linuxmon_ioc?ioc_id=%d&ioc_name=%s">%s</a>' % (
            self.make_link_base(),
            ioc_id,
            ioc_name,
            display,
        )

    def make_link_linuxmon_forget(self, ioc_name, display):
        return '<a href="%s/linuxmon_forget?ioc_name=%s">%s</a>' % (
            self.make_link_base(),
            ioc_name,
            display,
        )

    def make_link_forget_beacon(self, ip_key, port):
        return '<a href="%s/beacon_forget?ip_key=%s&port=%d">X</a>' % (
            self.make_link_base(),
            ip_key,
            port,
        )

    def make_link_forget_heartbeat(self, ip_addr, port, display="X"):
        return '<a href="%s/heartbeat_forget?ip_key=%s&port=%d">%s</a>' % (
            self.make_link_base(),
            ip_addr,
            port,
            display,
        )

    def make_link_heartbeat(
        self, ioc_id=None, port=None, heartbeat_id=None, display=None
    ):
        if display is None and ioc_id is not None:
            display = get_host_name(ioc_id)
        parms = []
        if ioc_id is not None:
            parms.append("ioc_id=%d" % ioc_id)
        if port is not None:
            parms.append("port=%d" % port)
        if heartbeat_id is not None:
            parms.append("heartbeat_id=%d" % heartbeat_id)
        url = "%s/heartbeat?%s" % (self.make_link_base(), "&".join(parms))

        return '<a href="%s">%s</a>' % (url, display)

    def make_link_group_ioc_remove(self, group_id, ioc_key, redirect, display):
        return (
            '<a href="%s/group_edit?group_id=%s'
            '&ioc_id=%s&redirect=%s&remove=1">%s</a>'
            % (self.make_link_base(), group_id, ioc_key, redirect, display)
        )

    def make_link_forget_ioc(
        self, ioc_id, display="Forget IOC", url_only=False
    ):
        url = "%s/ioc_forget/%s" % (self.make_link_base(), ioc_id)
        if url_only:
            return url
        return '<a href="%s">%s</a>' % (url, display)

    def make_link_heartbeats(
        self, sort=SORT_MODE.IOC, offset=0, display="Heartbeats", url_only=False
    ):
        url = "%s/heartbeats?offset=%d&sort=%s" % (
            self.make_link_base(),
            offset,
            sort,
        )
        if url_only:
            return url
        return '<a href="%s">%s</a>' % (url, display)

    def make_link_pv_cainfo(self, pv_name, display):
        return '<a href="%s/pv_cainfo/%s">%s</a>' % (
            self.make_link_base(),
            urllib.parse.quote_plus(urllib.parse.quote_plus(pv_name)),
            display,
        )

    def make_link_ping_ioc(self, ip_addr):
        display = "Ping IOC"
        quoted = urllib.parse.quote_plus(urllib.parse.quote_plus(ip_addr))
        return '<a href="%s/ioc/%s?ping=1">%s</a>' % (
            self.make_link_base(),
            quoted,
            display,
        )

    def make_link_group_delete_yes(self, group_id, name):
        return '<a href="%s/group_delete_yes?group_id=%d">%s</a>' % (
            self.make_link_base(),
            int(group_id),
            name,
        )

    def make_link_groups(self, name, url_only=False):
        url = "%s/groups" % self.make_link_base()
        if url_only:
            return url
        return '<a href="%s">%s</a>' % (url, name)

    def make_link_group_grid(self, group_id, name, url_only=False):
        url = "%s/group_grid?group_id=%d" % (
            self.make_link_base(),
            int(group_id),
        )
        if url_only:
            return url
        return '<a href="%s">%s</a>' % (url, name)

    def make_link_pv_info(self, name, display=None):
        if display is None:
            display = name
        return '<a href="%s/pv/%s">%s</a>' % (
            self.make_link_base(),
            urllib.parse.quote_plus(urllib.parse.quote_plus(name)),
            display,
        )

    def make_link_client_port(self, ioc_id, port, display):
        return '<a href="%s/client_port?ioc_id=%d&port=%d">%s</a>' % (
            self.make_link_base(),
            ioc_id,
            port,
            display,
        )

    def make_page_header(
        self,
        css=None,
        tooltip=False,
        auto_refresh_sec=None,
        script=None,
        blinker=False,
        jquery=False,
    ):

        page = (
            "<!DOCTYPE html><html><head>\n"
            "<title>%s</title>\n"
            '<meta http-equiv="Cache-Control" content="no-cache, no-store, '
            'must-revalidate"/>\n'
            '<meta http-equiv="Pragma" content="no-cache"/>\n'
            '<meta http-equiv="Expires" content="0"/>\n' % APP_NAME
        )

        if auto_refresh_sec:
            page += (
                '<meta http-equiv="refresh" content="%d">' % auto_refresh_sec
            )

        if css:
            page += css

        if blinker:
            tooltip = True

        if tooltip:
            page += CSS.TOOLTIP

        if blinker:
            jquery = True

        if jquery:
            page += (
                "<script src="
                '"https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js">'
                "</script>\n"
            )

        if blinker:
            page += """
            <script>
                function blinker() {
                    $('.blink').fadeOut(200); 
                    $('.blink').fadeIn(500); 
                }
                setInterval(blinker, 1500);
            </script>
            """

        if script:
            page += script

        if blinker:
            page += '</head><body onload="blinker();">\n'

        page += self.make_link_pv_list()
        page += "&nbsp &nbsp\n"
        page += self.make_link("/group_grid", "IOCs")
        page += "&nbsp &nbsp\n"
        page += self.make_link_plots()
        page += "&nbsp &nbsp\n"
        page += self.make_link_groups("Groups")
        page += "&nbsp &nbsp\n"
        page += self.make_link("/beacons", "Beacons")
        page += "&nbsp &nbsp\n"
        page += self.make_link("/heartbeats", "Heartbeats")
        page += "&nbsp &nbsp\n"
        page += self.make_link("/admin/logs", "Latest Logs")
        page += "&nbsp &nbsp\n"
        page += self.make_link_requests(name="Latest Requests")
        page += "&nbsp &nbsp\n"
        page += self.make_link_linuxmon()
        page += "&nbsp &nbsp\n"
        page += self.make_link_epics_events()
        page += "&nbsp &nbsp\n"
        page += self.make_link_admin()
        page += "<p>\n"

        return page

    def table_add_row(self, page, string_1, string_2):
        page += "<tr>\n"
        page += "<td>%s</td>\n" % string_1
        page += "<td>%s</td>\n" % string_2
        page += "</tr>\n"
        return page

    def page_heartbeat(self):
        """
        Shows the details of one HEARTBEAT
        """
        page = self.make_page_header(css=CSS.IOC)

        try:

            cur_time = int(time.time())
            ioc_id = port = None
            for k, v in bottle.request.query.items():
                if k == "ioc_id":
                    ioc_id = int(v)
                elif k == "port":
                    port = int(v)

            if ioc_id is None or port is None:
                raise GotoExit("No heartbeat key specified")

            ip_addr = get_ip_addr(ioc_id)

            host_name = get_host_name(ioc_id)
            host_name_link = self.make_link_ioc(ip_addr, host_name)

            # Get the actual heartbeat object
            heartbeat = self._dataman.get_heartbeat(ioc_id=ioc_id, port=port)

            data = heartbeat.get(KEY.DATA)

            page += "<h2>Heartbeat: %s (%s) Port %d</h2>" % (
                host_name_link,
                ip_addr,
                port,
            )

            page += "<p><table>"
            page += "<tr>"
            page += "<th>Parameter</th>"
            page += "<th>Value</th>"
            page += "</tr>"

            read_time = int(heartbeat.get(KEY.TIME))
            server_time = int(data.get(KEY.SERVER_TIME))

            offset_seconds = cur_time - read_time
            offset_server = server_time - read_time

            if abs(offset_server) < 5:
                offset_server_string = "None"
            elif offset_server > 0:
                offset_server_string = "Ahead %s" % datetime.timedelta(
                    seconds=offset_server
                )
            else:
                offset_server_string = "Behind %s" % datetime.timedelta(
                    seconds=abs(offset_server)
                )

            if heartbeat.get(KEY.LOST, False):
                uptime = LOST_STR
            else:
                uptime_seconds = int(data.get(KEY.UP, 0)) + offset_seconds
                uptime = datetime.timedelta(seconds=uptime_seconds)

            page = self.table_add_row(page, "Uptime", uptime)

            page = self.table_add_row(
                page, "Sequence number", "%d" % int(data.get(KEY.SEQ))
            )

            page = self.table_add_row(
                page, "Heartbeat Version", data.get(KEY.VERSION)
            )

            page = self.table_add_row(page, "Image", data.get(KEY.IMAGE))

            page = self.table_add_row(
                page, "Working Directory", data.get(KEY.DIRECTORY)
            )

            page = self.table_add_row(
                page, "Start Command", data.get(KEY.START_CMD)
            )

            page = self.table_add_row(page, "Process ID", data.get(KEY.PID))

            page = self.table_add_row(
                page, "Server Time Offset", offset_server_string
            )

            page += "</table>"

            # Display the history -------------------------------------------------
            history = self._dataman.get_heartbeat_history(ioc_id, port)

            page += "<h2>History</h2>"

            if not history:
                page += "No history available"
            else:
                page += (
                    "<table>"
                    "<tr>"
                    "<th>Event Time</th>"
                    "<th>Event</th>"
                    "<th>Heartbeat Uptime</th>"
                    "</tr>"
                )

                history.reverse()

                for event in history:
                    uptime = datetime.timedelta(seconds=int(event[1]))
                    event_time = datetime.datetime.fromtimestamp(int(event[2]))
                    page += (
                        "<tr>"
                        "<td>%s</td>"
                        "<td>%s</td>"
                        "<td>%s</td>"
                        "</tr>"
                        % (
                            str(event_time),
                            self._dataman.get_heartbeat_history_string(
                                event[0]
                            ),
                            uptime,
                        )
                    )

                page += "</table>"

            page += "<h3>%s</h3>" % self.make_link_forget_heartbeat(
                ioc_id, port, display="Forget Heartbeat"
            )

            # Display the PVs -----------------------------------------------------

            pvs = self._dataman.get_heartbeat_pvs(ioc_id, port)

            if pvs is None:
                raise GotoExit("No PV data available")

            pv_count = len(pvs)
            if pv_count == 1:
                page += "<h3>This app hosts 1 PV</h3>"
            else:
                page += "<h3>This app hosts %d PVs</h3>" % pv_count

            for pv in pvs:
                page += "%s<br>" % self.make_link_pv_info(pv)

        except GotoExit as err:
            page += "<h2>Exception: %s</h2>" % str(err)

        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def add_traceback(self, err):

        result = (
            "<h2>Error Processing Request</h2>\n"
            "<b><em>Please report this error to the "
            "maintainers of PV Monitor</em></b><br><br>\n"
            '<span class="my_monospace">\n'
        )
        t = traceback.format_exc(10)
        lines = t.split("\n")
        for line in lines:
            result += "%s<br>" % line
        result += "</span>\n"
        return result

    def page_heartbeat_forget(self, key):

        page = self.make_page_header(css=CSS.IOC)
        try:

            parts = key.split(":")
            ip_addr = parts[0]
            port = int(parts[1])
            host_name = get_host_name(ip_addr)
            host_name_link = self.make_link_ioc(ip_addr, host_name)

            self._dataman.forget_heartbeat(key)

            page += "<h2>Deleted heartbeat from %s port %s</h2>" % (
                host_name_link,
                port,
            )
            page += "Heartbeat will be re-added to database if re-detected"

        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_ioc_clear_all_warnings(self, key):
        return self.page_ioc_clear_all(key, KEY.WARNINGS)

    def page_ioc_clear_all(self, key, kind):
        ip_key, ip_addr = self.get_ioc_id_and_addr(key)
        host_name = get_host_name(ip_key)
        page = self.make_page_header(css=CSS.IOC)

        self._dataman.clear_ioc_logs(ip_key, kind)

        if kind == KEY.ERRORS:
            display_kind = "errors"
        else:
            display_kind = "warnings"

        page += "<h2>IOC: %s (%s) All %s cleared</h2>" % (
            host_name,
            ip_addr,
            display_kind,
        )

        page += (
            "Note: the warnings and/or errors may reappear if still warranted."
        )
        display = "Back to IOC %s Status Page" % host_name
        page += "<p>%s" % self.make_link_ioc(ip_key, display)
        page += "</body></html>"
        return page

    def page_ack_msg(self, msg_id):

        parts = msg_id.split("_")
        ip_key = int(parts[0])
        msg_id = int(parts[1])
        ip_addr = get_ip_from_key(ip_key)
        host_name = get_host_name(ip_key)
        self._dataman.clear_ioc_msg(ip_key, msg_id)
        page = self.make_page_header(css=CSS.IOC)

        page += "<h2>IOC: %s (%s) Acknowledged (cleared) MSG_ID: %d</h2>" % (
            host_name,
            ip_addr,
            msg_id,
        )

        page += "<p>Note: The warning or error may reappear if still warranted."
        display = "Back to IOC %s Status Page" % host_name
        page += "<p>%s" % self.make_link_ioc(ip_addr, display)
        page += "</body></html>"
        return page

    def page_pv_cainfo(self, quoted):

        pv_name = urllib.parse.unquote_plus(urllib.parse.unquote_plus(quoted))
        page = self.make_page_header(css=CSS.PV)

        try:
            page += "<h2>%s</h2>" % self.make_link_pv_info(pv_name)
            result = self._dataman.fetch_cainfo(pv_name)

            page += "<p><TABLE>"

            for line in result:
                if line.startswith("===="):
                    continue
                page += "<tr>"
                page += "<td>%s</td>" % line
                page += "</tr>"

            page += "</TABLE>"

        except Exception as err:
            page += self.add_traceback(err)

        page += "</BODY></HTML>"
        return page

    def my_range(self, offset, max_items, total_items):

        l = offset + 1
        if l > total_items:
            l = total_items

        m = offset + max_items
        if m > total_items:
            m = total_items

        return l, m

    def page_submit_heartbeats(self):

        sort = SORT_MODE.IOC
        for k, v in bottle.request.forms.items():
            log.debug("submit KEY: %s VALUE: %s", k, v)
            if k == "sort":
                sort = v

        url = self.make_link_heartbeats(sort=sort, url_only=True)
        bottle.redirect(url)

    def page_submit_pvs(self):
        """
        This is called when the user selects a sort mode on the PV list screen
        """

        kind = PV_LIST.ALL
        kind_prev = PV_LIST.ALL
        sort = None
        ioc_id = None
        key = None
        offset = 0
        search_string = None
        ignore_case = None
        forget = 0

        for k, v in bottle.request.forms.items():
            log.debug("submit KEY: %s VALUE: %s", k, v)
            if k == "sort":
                sort = v

            elif k == "ioc_id":
                ioc_id = int(v)

            elif k == "kind":
                kind = v

            elif k == "kind_prev":
                kind_prev = v

            elif k == "key":
                key = v

            elif k == "search_string":
                search_string = v

            elif k == "forget":
                forget = 1

            elif k == "ignore_case":
                ignore_case = v

        if kind != kind_prev and kind != PV_LIST.SEARCH:
            search_string = None
        else:
            # The kind has not changed and there is a search string so
            # the user must be trying to do a search
            if search_string:
                kind = PV_LIST.SEARCH

        if kind != kind_prev:
            sort_modes = PV_LIST_SORT_MODE.get(kind)
            modes = [mode[0] for mode in sort_modes]
            sort = modes[0]
            log.debug(
                "kind %s->%s (setting sort to %s)",
                kind_prev,
                kind,
                sort,
            )

        url = self.make_link_pv_list(
            kind=kind,
            offset=offset,
            key=key,
            sort=sort,
            ioc_id=ioc_id,
            search_string=search_string,
            ignore_case=ignore_case,
            forget=forget,
            url_only=True,
        )

        bottle.redirect(url)

    def page_submit_requests(self):

        log.debug("called")

        kind = REQUESTS.PVS
        offset = 0
        for k, v in bottle.request.forms.items():
            log.debug("submit KEY: %s VALUE: %s", k, v)
            if k == "kind":
                kind = v
            elif k == "offset":
                offset = int(0)

            else:
                log.error("arg not supported: %r", k)

        url = self.make_link_requests(kind=kind, offset=offset, url_only=True)
        bottle.redirect(url)

    def page_submit_plot(self):
        """
        This is called when the user selects a sort mode on the PV list screen
        """
        log.debug("called")

        kind = PLOT_TYPE.REQ_USER
        interval = PLOT_INTERVAL.DAY

        for k, v in bottle.request.forms.items():
            log.debug("submit KEY: %s VALUE: %s", k, v)
            if k == "kind":
                kind = v
            elif k == "interval":
                interval = v
            else:
                log.error("arg not supported: %r", k)

        url = self.make_link_plots(kind=kind, interval=interval, url_only=True)
        bottle.redirect(url)

    def make_pv_table(self, pvs, offset=0, counts=None, forget_box=False):

        page = "<p><table><tr>\n" "<th>Index</th>\n"
        if forget_box:
            page += "<th>Forget</th>\n"

        page += '<th>PV Name</th>\n <th class="right">Requests</th>\n'
        if counts is not None:
            page += "<th>Percent</th>\n"
        page += (
            '<th class="right">Last Req.</th>\n'
            '<th class="right">Port</th>\n'
            '<th class="right">Last Time Req.</th>\n'
            "<th>Server</th>\n"
            '<th class="right">Port</th>\n'
            '<th class="right">Last Time Seen</th>\n'
            '<th class="center">Status</th>\n'
            "</tr>\n"
        )

        for index, pv_name in enumerate(pvs):
            log.debug(
                "index %d pvname: %r (POLLING)",
                index,
                pv_name,
            )

            d = self._dataman.get_pv_data(pv_name)
            if d is None:
                page += "<tr>\n" "<td>%d</td>\n" % (offset + index + 1)
                if forget_box:
                    page += "<td></td>"
                page += (
                    "<td>%s - NO DATA</td>\n <td>0</td>\n" % pv_name
                )  # Requests
                if counts is not None:
                    page += "<td></td>\n"
                page += (
                    "<td></td>\n"
                    "<td></td>\n"
                    "<td></td>\n"
                    "<td></td>\n"
                    "<td></td>\n"
                    "<td></td>\n"
                    "<td></td>\n"
                    "</tr>\n"
                    "</div>"
                )
                continue

            if KEY.PV_INVALID not in d:
                # Rather that poll a PV when it is viewed, it would be preferrable if
                # the URL that the javascript hits updates the PV, but it can't
                # because it is a seperate service based on tornado
                self._dataman._searcher.search(pv_name, PRIORITY.HIGH)

            info_link = self.make_link_pv_info(pv_name)
            index_link = self.make_link_pv_info(
                pv_name, display="%d" % (offset + index + 1)
            )

            status_str = ""
            port_link = ""
            server_link = ""
            last_client = ""
            last_time = ""
            last_seen_str = ""
            client_port_link = ""
            last_client_link = ""
            host_name = ""

            # The last_time_seen will return the last time
            lost, ioc_id, port, last_time_seen = (
                self._dataman.get_pv_latest_info(d)
            )

            if port and ioc_id:
                port_link = self.make_link_pv_list(
                    kind="port", key=port, ioc_id=ioc_id, name="%d" % port
                )
            else:
                port = 0

            if last_time_seen:
                last_seen_str = my_time(last_time_seen)
            else:
                last_time_seen = 0

            if lost:
                status_str = STATUS_STR_LOST

            if ioc_id:
                if not lost:
                    status_str = STATUS_STR_ONLINE
                host_name = get_host_name(ioc_id)

                server_link = self.make_link_ioc(ioc_id, host_name)

                if PV_KEY.DUPLICATES in d:
                    server_link = (
                        '<span style="background-color:%s">%s</span>'
                        % (
                            COLOR.RED,
                            server_link,
                        )
                    )
            else:
                ioc_id = 0
                status_str = STATUS_STR_NEVER_SEEN

            last_request = self._dataman.get_pv_latest_request(d)
            if last_request is not None:
                ip_addr = last_request[0]
                last_client = get_host_name(ip_addr)
                client_ioc_id = get_ip_key(ip_addr)
                last_client_link = self.make_link_ioc(
                    client_ioc_id, last_client
                )
                last_port = int(last_request[1])
                last_time = my_time(last_request[3])
                client_port_link = self.make_link_client_port(
                    last_request[0], last_port, "%d" % last_port
                )

            page += "<tr>\n"

            page += '<td class="right">%s</td>\n' % index_link
            if forget_box:
                forget_check = (
                    '<input type="checkbox" id=%s name=%s value=%s checked>'
                    % (pv_name, pv_name, pv_name)
                )
                page += "<td>%s</td>\n" % forget_check

            page += "<td>%s</td>\n" % info_link
            if counts is None:
                page += '<td class="right">%d</td>\n' % (
                    d.get(PV_KEY.CLIENT_REQ, 0)
                )
            else:
                count_data = counts.get(pv_name, (0, 0))
                page += '<td class="right">%d</td>\n' % (
                    count_data[0]
                )  # latest counts
                page += '<td class="right">%.2f</td>\n' % (
                    count_data[1]
                )  # percentage

            # Embed data into the page for the javascript
            page += (
                '<td class="right">%s</td>\n' % last_client_link
                + '<td class="right">%s</td>\n' % client_port_link
                + '<td class="right">%s</td>\n' % last_time
                + '<td class="right"><div id="%d_server">%s</div></td>\n'
                % (index, server_link)
                + '<td class="right"><div id="%d_port">%s</div></td>\n'
                % (index, port_link)
                + '<td class="right"><div id="%d_last_seen">%s</div></td>\n'
                % (index, last_seen_str)
                + '<td class="center"><div id="%d_status">%s</div></td>\n'
                % (index, status_str)
                + '<div hidden class="pv_index" value="%d"></div>\n' % index
                + '<div hidden id="%d_pv_name_id">%s</div>\n' % (index, pv_name)
                + '<div hidden id="%d_ioc_id">%d"></div>\n' % (index, ioc_id)
                + '<div hidden id="%d_port_id">%d"></div>\n' % (index, port)
                + '<div hidden id="%d_last_seen_id">%d"></div>\n'
                % (index, last_time_seen)
                + "</tr>\n"
            )

        page += "</table><p>\n"

        return page

    def get_js(self, file_name, *args):

        script = self._js_cache.get(file_name)
        if script is None:
            f = None
            try:
                f = open(file_name, "r")
                script = f.read()
            except:
                log.error("Error opening file %s", file_name)
            finally:
                if f:
                    f.close()

            script = script % args
            script = jsmin(script)
            self._js_cache[file_name] = script

        return script

    def page_submit_pvs_forget_ok(self):
        log.debug("called")
        url = self.make_link_pv_list(url_only=True)
        bottle.redirect(url)

    def page_submit_pvs_forget(self):
        log.debug("called")

        forget = False
        pv_list = []

        try:
            for k, v in bottle.request.forms.items():
                log.debug("KEY: %s VALUE: %s", k, v)
                if k == "__ok__":
                    forget = True
                else:
                    pv_list.append(k)

            page = self.make_page_header()

            if forget:
                for pv_name in pv_list:
                    self._dataman.forget_pv(pv_name)
                if len(pv_list) == 1:
                    page += "<h2>1 PV queued for deletion</h2>"
                else:
                    page += "<h2>%d PVs queued for deletion</h2>" % len(pv_list)
            else:
                page += "<h2>PV Forget Canceled<h2>"

            url = "%s/submit_pvs_forget_ok" % self.make_link_base()
            page += (
                '<p><FORM action="%s" method="POST">\n'
                '<button type="submit" name="__ok__">OK</button>\n'
                "<form>" % url
            )

        except Exception as err:
            page = self.make_page_header(css=CSS.IOC)
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_submit_forget_one_pv(self):

        pv_list = []
        for k, v in bottle.request.forms.items():
            log.debug("KEY: %s VALUE: %s", k, v)
            if k == "__ok__":
                pass
            else:
                pv_list.append(k)

        return self.page_pvs_forget(pv_list)

    def page_submit_forget_button(self):
        """
        page += '<input type="hidden" name="kind" value="%s">\n' % kind
        page += '<input type="hidden" name="key" value="%s">\n' % key
        page += '<input type="hidden" name="ioc_id" value="%s">\n' % ioc_id
        page += '<input type="hidden" name="search_string" value="%s">\n' % search_string
        """
        kind = None
        search_string = None
        ioc_id = 0
        key = 0
        for k, v in bottle.request.forms.items():
            log.debug("KEY: %s VALUE: %s", k, v)
            if k == "kind":
                kind = v
            elif k == "key":
                key = v
            elif k == "ioc_id":
                ioc_id = int(v)
            elif k == "search_string":
                search_string = v

        url = self.make_link_pv_list(
            kind=kind,
            ioc_id=ioc_id,
            key=key,
            search_string=search_string,
            forget=1,
            url_only=True,
        )
        bottle.redirect(url)

    def page_pvs_forget(self, pvs_in):

        try:
            page = self.make_page_header(css=CSS.IOC)

            pvs = [str(pv) for pv in pvs_in]

            if len(pvs) == 1:
                page += "<h2>Forget PV?</h2>"
            else:
                page += "<h2>Forget %d PVs?</h2>" % len(pvs)

            page += "<p>"
            if len(pvs) >= MAX_FORGET_PVS:
                page += (
                    "<b>NOTE:</b> A maximum of %d PVs "
                    "can be deleted at one time.<br>\n" % MAX_FORGET_PVS
                )
            url = "%s/submit_pvs_forget" % self.make_link_base()
            page += (
                "Due to caching, it may take time for a deleted PV to "
                "completely disappear from the database.<br>"
                "PVs will be automatically re-added to the database if "
                "they are subsequently detected or requested.\n"
                '<p><FORM action="%s" method="POST">\n'
                '<button type="submit" name="__ok__">OK</button>\n'
                '<button type="submit" name="__cancel__">Cancel</button>\n'
                % url
            )

            page += self.make_pv_table(pvs, forget_box=True)
            page += (
                '<button type="submit" name="__ok__">OK</button>\n'
                '<button type="submit" name="__cancel__">Cancel</button>\n'
                "</form>"
                '<br><span class="my_monospace">'
            )
            for pv_name in pvs:
                page += "-----------------------------------<br>"
                lines = print_binary(pv_name, prnt=False)
                for line in lines:
                    if line:
                        page += "%s<br>" % repr(line)
            page += "</span>"

        except Exception as err:
            page = self.make_page_header(css=CSS.IOC)
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_pv_list(self):
        """
        This page displays all types of PV lists in the system.  It should become
        the only page for displaying pv lists
        """
        page = self.make_page_header(css=CSS.IOC, jquery=True)

        kind = PV_LIST.ALL
        offset = 0
        pvs_per_page = 25
        link_fwd = None
        link_back = None
        count_dict = None
        ioc_id = 0
        search_string = None
        search_string_display = ""
        ignore_case = True
        key = None
        sort = SORT_MODE.REQ_TIME
        forget = False

        try:
            # Look at the request params
            for k, v in bottle.request.query.items():
                log.debug(
                    "PV LIST KEY: %r VALUE: %r",
                    k,
                    v,
                )

                if k == "offset":
                    offset = int(v)

                elif k == "kind":
                    kind = v

                elif k == "key":
                    key = v

                elif k == "ioc_id":
                    ioc_id = int(v)

                elif k == "search_string":
                    search_string = urllib.parse.unquote_plus(
                        urllib.parse.unquote_plus(v)
                    )

                elif k == "sort":
                    sort = v

                elif k == "forget":
                    forget = True

            log.debug(
                "kind: %s key: %s ioc_id: %d sort: %s",
                kind,
                key,
                ioc_id,
                sort,
            )

            # Get sort modes supported for this kind
            sort_mode_picklist = PV_LIST_SORT_MODE.get(kind)

            # Must check that the sort mode is supported
            sort_modes = [item[0] for item in sort_mode_picklist]
            if sort not in sort_modes:
                log.debug(
                    "switching to supported sort mode (%s -> %s)",
                    sort,
                    sort_modes[0],
                )

                # The first mode in the list is the default for this kind
                sort = sort_modes[0]

            # Get the kind list for this kind (it differs, eg, show 'search' for search)
            kind_select_picklist = PV_LIST_KIND_SELECT.get(kind)

            if kind == PV_LIST.ALL:
                # Display all PVs
                pvs, total_len = self._dataman.get_pvs_all(
                    offset, pvs_per_page, sort=sort
                )
                l, u = self.my_range(offset, pvs_per_page, total_len)

                page += "<h2>Displaying %d to %d of %d Detected PVs</h2>\n" % (
                    l,
                    u,
                    total_len,
                )

            elif kind == PV_LIST.LATEST_IOC:
                pvs, total_len, count_dict = (
                    self._dataman.get_pvs_latest_requests_ioc(
                        ioc_id, offset, pvs_per_page, sort
                    )
                )
                l, u = self.my_range(offset, pvs_per_page, total_len)
                host_name = get_host_name(ioc_id)
                server_link = self.make_link_ioc(ioc_id, host_name)
                page += (
                    "<h2>Displaying %d to %d of the Latest %d PVs Requested "
                    "(CA_PROTO_SEARCH) by %s</h2>"
                    % (l, u, total_len, server_link)
                )

            elif kind == PV_LIST.PORT:
                port = int(key)

                if forget:
                    pvs, _ = self._dataman.get_ioc_port_pvs(
                        ioc_id, port, offset, MAX_FORGET_PVS, sort=sort
                    )
                    pvs = [item[0] for item in pvs]
                    return self.page_pvs_forget(pvs)

                pvs, total_len = self._dataman.get_ioc_port_pvs(
                    ioc_id, port, offset, pvs_per_page, sort=sort
                )
                l, u = self.my_range(offset, pvs_per_page, total_len)

                host_name = get_host_name(ioc_id)
                server_link = self.make_link_ioc(ioc_id, host_name)

                pvs = [item[0] for item in pvs]
                page += (
                    "<h2>Displaying %d to %d of %d PVs on %s Port %d</h2>\n"
                    % (
                        l,
                        u,
                        total_len,
                        server_link,
                        port,
                    )
                )

            elif kind == PV_LIST.SEARCH:
                # Display search results
                if search_string is not None:

                    # Force the sort mode to NAME for new searches
                    sort = SORT_MODE.NAME
                    # Make a key for this search
                    key = hash(str(search_string)) & 0xFFFFFFFFFFFFFFFF
                    self._pv_search_string_cache[key] = (
                        search_string,
                        ignore_case,
                    )
                    search_string_display = search_string
                else:
                    key = int(key)
                    item = self._pv_search_string_cache.get(
                        key, "Error getting search string"
                    )
                    if item:
                        search_string_display = item[0]
                        ignore_case = item[1]
                    else:
                        search_string_display = ""

                # If the forget flag was specified on a search, then do a PV forget!
                if forget:
                    pvs, _ = self._dataman.search_pvs(
                        search_string,
                        key,
                        0,
                        MAX_FORGET_PVS,
                        ignore_case=ignore_case,
                    )
                    return self.page_pvs_forget(pvs)

                pvs, total_len = self._dataman.search_pvs(
                    search_string,
                    key,
                    offset,
                    pvs_per_page,
                    ignore_case=ignore_case,
                )
                l, u = self.my_range(offset, pvs_per_page, total_len)

                ess = "" if len(pvs) == 1 else "s"

                page += (
                    "<h2>Displaying %d to %d of %d PV Search Result%s (%s)</h2>\n"
                    % (l, u, total_len, ess, search_string_display)
                )

            elif kind == PV_LIST.LOST:

                # If the forget flag was specified on a search, then do a PV forget!
                if forget:
                    pvs, _ = self._dataman.get_pvs_lost(
                        offset, count=MAX_FORGET_PVS, sort=sort
                    )
                    return self.page_pvs_forget(pvs)

                pvs, total_len = self._dataman.get_pvs_lost(
                    offset, count=pvs_per_page, sort=sort
                )
                l, u = self.my_range(offset, pvs_per_page, total_len)

                page += "<h2>Displaying %d to %d of %d Lost PVs</h2>\n" % (
                    l,
                    u,
                    total_len,
                )

            elif kind == PV_LIST.DUPLICATE:

                pvs, total_len = self._dataman.get_pvs_duplicate(
                    offset, count=pvs_per_page, sort=sort
                )
                l, u = self.my_range(offset, pvs_per_page, total_len)

                page += "<h2>Displaying %d to %d of %d Duplicate PVs</h2>\n" % (
                    l,
                    u,
                    total_len,
                )

            elif kind == PV_LIST.INVALID:

                if forget:
                    pvs, _ = self._dataman.get_pvs_invalid(
                        offset, count=MAX_FORGET_PVS, sort=sort
                    )
                    return self.page_pvs_forget(pvs)

                pvs, total_len = self._dataman.get_pvs_invalid(
                    offset, count=pvs_per_page, sort=sort
                )
                l, u = self.my_range(offset, pvs_per_page, total_len)

                page += "<h2>Displaying %d to %d of %d Invalid PVs</h2>\n" % (
                    l,
                    u,
                    total_len,
                )

            else:
                raise ValueError("kind: %s not supported" % kind)

            # ------------------------------------------------------------------
            # Form for widgets
            # ------------------------------------------------------------------
            sort_submit_url = "%s/submit_pvs" % self.make_link_base()
            page += (
                '<FORM style="display: inline;" action="%s" method="POST">\n'
                # It seems that this form is going on a new lint
                '<input type="hidden" name="kind_prev" value="%s">\n'
                '<input type="hidden" name="key" value="%s">\n'
                '<input type="hidden" name="ioc_id" value="%s">\n'
                'Kind: <select name="kind" onchange="this.form.submit()">\n'
                % (sort_submit_url, kind, key, ioc_id)
            )

            # Set up the pick list for the kinds that can be chosen
            for item in kind_select_picklist:
                k = item[0]
                display = item[1]
                selected = "selected" if k == kind else ""
                page += '<option value="%s" %s>%s</option>\n' % (
                    k,
                    selected,
                    display,
                )
            page += "</select>\n"

            page += "&nbspSearch:\n"
            page += (
                '<input type="text" id="search_string" name="search_string" value="%s">\n'
                % search_string_display
            )

            page += '&nbspSort: <select name="sort" onchange="this.form.submit()">\n'  # this.form.submit() is inline JavaScript

            # Construct the pick list setting the selected item to the current sort mode
            for item in sort_mode_picklist:
                mode = item[0]
                display = item[1]
                selected = "selected" if mode == sort else ""
                page += '<option value="%s" %s>%s</option>\n' % (
                    mode,
                    selected,
                    display,
                )
            page += "</select>\n"

            page += "</form>\n"

            if len(pvs) and kind in [
                PV_LIST.INVALID,
                PV_LIST.PORT,
                PV_LIST.SEARCH,
                PV_LIST.LOST,
            ]:
                # I had to put the forget button on a differnt form becasae I could not figure out a
                # simple way to determine which widget caused the submission in the above form
                other_url = "%s/submit_forget_button" % self.make_link_base()
                page += (
                    '&nbsp<form style="display: inline;" action="%s" method="POST">\n'
                    '<input type="hidden" name="kind" value="%s">\n'
                    '<input type="hidden" name="key" value=%s>\n'
                    '<input type="hidden" name="ioc_id" value=%s>\n'
                    % (
                        other_url,
                        kind,
                        repr(key),
                        repr(ioc_id),
                    )
                )

                if search_string:
                    page += (
                        '<input type="hidden" name="search_string" value="%s">\n'
                        % search_string
                    )

                page += (
                    '&nbsp<button name="forget" type="submit">'
                    "Forget PVs</button>\n</form>\n"
                )

            # Add << PREV NEXT >> links if required
            if total_len > pvs_per_page:

                offset_prev = offset - pvs_per_page
                if offset_prev < 0:
                    offset_prev = total_len - pvs_per_page

                offset_next = offset + pvs_per_page
                if offset_next >= total_len:
                    offset_next = 0

                link_fwd = self.make_link_pv_list(
                    offset=offset_next,
                    kind=kind,
                    key=key,
                    ioc_id=ioc_id,
                    sort=sort,
                    name="NEXT >>",
                )
                link_back = self.make_link_pv_list(
                    offset=offset_prev,
                    kind=kind,
                    key=key,
                    ioc_id=ioc_id,
                    sort=sort,
                    name="<< PREV",
                )

                page += "<p>"
                page += "%s &nbsp %s" % (link_back, link_fwd)

            # -----------------------------------------------------------------
            # Table of PVs
            # ------------------------------------------------------------------
            page += self.make_pv_table(pvs, offset=offset, counts=count_dict)

            if link_fwd:
                page += "%s &nbsp %s" % (link_back, link_fwd)

        except Exception as err:
            page += self.add_traceback(err)

        page += (
            "\n<p><b>NOTE:</b> The latest requesting clients and times shown are determined using\n"
            "real time data, which may have been updated since the list of PVs\n"
            "for this page was assembled.<br>For example, request times my not be in precise\n"
            "order, and a page showing requests from a particular client may show\n"
            "other requesting clients if they requested PVs after the client of interest.\n"
            "</body>\n"
            # '<script type="text/javascript" language="javascript">\n'
            # self.get_js('javascript/ca_proto_search.js',  "opi2031-006.clsi.ca", 5068, self._my_hostname, self._web_server_port)
            # self.get_js('javascript/ca_proto_search.js',  configuration.CAINFO_HOST, configuration.CAINFO_SERVER_PORT, self._my_hostname, self._web_server_port)
            # '</script><body onload="caProtoSearch()">\n'
            "</html>"
        )

        return page

    def page_api_beacons(self):
        log.debug("called")

        from bottle import response

        response.content_type = "application/json"

        handle = None
        ### lost_flag = False
        ### lost_count = 0
        ### result_count = 0
        host = None
        ioc_id = None
        ### raw_flag = False
        try:

            for key, value in bottle.request.query.items():
                log.debug(
                    "URL KEY: %r VAL: %r",
                    key,
                    value,
                )

                if key == "host":
                    host = value

                if key == "handle":
                    handle = value

                ### if key == "lost":
                ###     lost_flag = True

                ### if key == "raw":
                ###     raw_flag = True

            if host is not None:
                ioc_id, ip_addr = self.get_ioc_id_and_addr(host)

            try:
                handle = int(handle)
            except:
                handle = None

            beacons = self._dataman.get_beacons(ioc_id)

            result = {
                "version": "1.0",
                "status": "OK",
                ###       'lost_flag'     : lost_flag,
                ###      'raw_flag'      : raw_flag,
                ###       'lost_count'    : lost_count,
                ###       'result_count'  : result_count,
                "beacons": beacons,
            }

            if handle is not None:
                result["handle"] = handle

            return_sting = json.dumps(result)
            return return_sting

        except Exception as err:
            return self.api_error(err)

    def api_error(self, err):
        t = traceback.format_exc(10)
        lines = t.split("\n")
        result = {
            "api_version": "1.0",
            "status": "ERROR",
            "msg": "%s" % repr(err),
            "traceback": lines,
        }
        return_sting = json.dumps(result)
        return return_sting

    def page_api_heartbeats(self):
        log.debug("called")

        from bottle import response

        response.content_type = "application/json"

        cur_time = int(time.time())
        handle = None
        lost_flag = False
        lost_count = 0
        result_count = 0
        host = None
        ioc_id = None
        raw_flag = False
        try:

            for key, value in bottle.request.query.items():
                log.debug(
                    "URL KEY: %r VAL: %r",
                    key,
                    value,
                )

                if key == "host":
                    host = value

                if key == "handle":
                    handle = value

                if key == "lost":
                    lost_flag = True

                if key == "raw":
                    raw_flag = True

            if host is not None:
                ioc_id, ip_addr = self.get_ioc_id_and_addr(host)

            try:
                handle = int(handle)
            except:
                handle = None

            # Get the actual heartbeats
            heartbeats = self._dataman.get_heartbeat_list_for_ioc(ioc_id)

            heartbeat_list = []
            for heartbeat in heartbeats:
                beacon_port = heartbeat[1]
                raw_data = heartbeat[2]
                pv_count = raw_data.get(KEY.PV_COUNT)
                hb_data = raw_data.get(KEY.DATA, {})
                last_time = int(raw_data.get(KEY.TIME))

                lost = hb_data.get(KEY.LOST, False)

                if lost:
                    lost_count += 1
                    if not lost_flag:
                        log.debug("skipping lost heartbeat")

                result_count += 1

                version = hb_data.get(KEY.VERSION)
                pid = int(hb_data.get(KEY.PID))
                ioc_id = int(hb_data.get(KEY.IP_KEY))
                image = hb_data.get(KEY.IMAGE)
                uptime = int(hb_data.get(KEY.UP))
                command = hb_data.get(KEY.START_CMD)
                directory = hb_data.get(KEY.DIRECTORY)
                heartbeat_port = int(hb_data.get(KEY.PORT))

                if lost:
                    extra_time = 0
                else:
                    extra_time = cur_time - last_time
                
                image = remove_leading_space(image)

                cmd = "%s %s" % (image, command)
                
                app_id = make_app_id(cmd, directory, ioc_id)

                data = {
                    "heartbeat_port": heartbeat_port,
                    "app_port": beacon_port,
                    "version": version,
                    "pvs": pv_count,
                    "pid": pid,
                    "image": image,
                    "command": command,
                    "directory": directory,
                    "lost": lost,
                    "up_time": uptime + extra_time,
                    "last_time": last_time,
                    # JSON cannot handle the large app_id ints!!! Must use strings!
                    "app_id": str(app_id),
                    "ioc_id": ioc_id,
                }

                if raw_flag:
                    data["raw"] = hb_data

                heartbeat_list.append(data)

            result = {
                "version": "1.0",
                "status": "OK",
                "lost_flag": lost_flag,
                "raw_flag": raw_flag,
                "lost_count": lost_count,
                "result_count": result_count,
                "heartbeats": heartbeat_list,
            }

            if handle is not None:
                result["handle"] = handle

            return_sting = json.dumps(result, indent=4)
            return return_sting

        except Exception as err:
            return self.api_error(err)

    def page_heartbeats(self):
        """
        Show a list of all heartbeats
        """

        try:
            page = self.make_page_header(css=CSS.IOC)

            link_fwd = None
            link_back = None
            cur_time = time.time()

            items_per_page = ITEMS_PER_PAGE
            offset = 0
            sort = SORT_MODE.IOC

            for key, value in bottle.request.query.items():
                log.debug("KEY: %s VAL: %s", key, value)
                if key == "offset":
                    offset = int(value)
                if key == "sort":
                    sort = value

            heartbeats, total_len = self._dataman.get_heartbeats_sorted(
                offset, items_per_page, sort
            )

            l, u = self.my_range(offset, items_per_page, total_len)
            page += "<h2>Displaying %d to %d of %d Heartbeats</h2>" % (
                l,
                u,
                total_len,
            )

            submit_url = "%s/submit_heartbeats" % self.make_link_base()
            page += (
                '<form style="display: inline;" action="%s" method="POST">\n'
                % submit_url
            )  # It seems that this form is going on a new lint
            page += 'Sort: <select name="sort" onchange="this.form.submit()">\n'
            for item in HEARTBEAT_SORT:
                k = item[0]
                display = item[1]
                selected = "selected" if sort == k else ""
                page += '<option value="%s" %s>%s</option>\n' % (
                    k,
                    selected,
                    display,
                )
            page += "</select>\n"
            page += "</form>\n"

            if total_len > items_per_page:

                offset_prev = offset - items_per_page
                if offset_prev < 0:
                    offset_prev = total_len - items_per_page

                offset_next = offset + items_per_page
                if offset_next >= total_len:
                    offset_next = 0

                page += "<p>"

                link_back = self.make_link_heartbeats(
                    offset=offset_prev, sort=sort, display="<< PREV"
                )
                link_fwd = self.make_link_heartbeats(
                    offset=offset_next, sort=sort, display="NEXT >>"
                )

                page += "%s &nbsp %s" % (link_back, link_fwd)
            page += (
                "<p>"
                "<table>\n"
                "<tr>\n"
                '<th class="right">Index</th>\n'
                "<th>Host</th>\n"
                '<th class="right">Port</th>\n'
                "<th>Vers.</th>"
                '<th class="right">PVs</th>\n'
                '<th class="center">Last Seen</th>\n'
                '<th class="center">Lost</th>\n'
                '<th class="center">Uptime</th>\n'
                '<th class="center">Time Offset</th>\n'
                "<th>Working Directory</th>\n"
                "<th>Image</th>\n"
                "<th>Command</th>\n"
                "</tr>"
            )

            # This section re-sorts by uptime (it can change since cache time)
            temp = []
            for heartbeat in heartbeats:
                heartbeat_data = self._dataman.get_heartbeat(heartbeat)

                if sort == SORT_MODE.LAST_SEEN:
                    last_seen = 0
                    if heartbeat_data:
                        beat_time = heartbeat_data.get(KEY.TIME)
                        last_seen = cur_time - beat_time
                    temp.append((last_seen, heartbeat, heartbeat_data))
                else:
                    temp.append((heartbeat, heartbeat_data))

            if sort == SORT_MODE.LAST_SEEN:
                temp.sort()
                temp = [(item[1], item[2]) for item in temp]
            for index, item in enumerate(temp):
                heartbeat = item[0]
                heartbeat_data = item[1]

                port_str = ""
                pv_count_link = ""
                index_link = "%d" % (index + 1 + offset)
                host_link = ""

                image_str = ""
                cwd_str = ""
                cmd_str = ""
                version_str = ""
                last_seen_str = ""
                offset_server_string = ""
                lost_str = ""
                uptime_str = ""

                if heartbeat_data:
                    data = heartbeat_data.get(KEY.DATA, {})
                    pv_count = heartbeat_data.get(KEY.PV_COUNT)
                    beat_time = heartbeat_data.get(KEY.TIME)
                    lost = data.get(KEY.LOST)
                    last_seen = cur_time - beat_time
                    last_seen_str = make_last_seen_str(last_seen)

                    ioc_id = data.get(KEY.IP_KEY, 0)
                    ### ip_addr = data.get(KEY.IP_ADDR)
                    host_name = get_host_name(ioc_id)
                    host_link = self.make_link_ioc(ioc_id, display=host_name)
                    port = data.get(KEY.SERVER_PORT, 0)
                    port_str = "%d" % port
                    pv_count_link = self.make_link_heartbeat(
                        ioc_id=ioc_id, port=port, display="%d" % pv_count
                    )
                    index_link = self.make_link_heartbeat(
                        ioc_id=ioc_id, port=port, display=index_link
                    )

                    if lost:
                        lost_str = LOST_STR

                    uptime = int(data.get(KEY.UP, 0)) + cur_time - beat_time
                    uptime_str = my_uptime_str(uptime)

                    read_time = int(data.get(KEY.TIME, 0))
                    server_time = int(data.get(KEY.SERVER_TIME, 0))

                    offset_server = server_time - read_time

                    if abs(offset_server) < 5:
                        offset_server_string = "0"
                    elif offset_server > 0:
                        offset_server_string = "Ahead %s" % datetime.timedelta(
                            seconds=offset_server
                        )
                    else:
                        offset_server_string = "Behind %s" % datetime.timedelta(
                            seconds=abs(offset_server)
                        )

                    image_str = data.get(KEY.IMAGE)
                    cmd_str = data.get(KEY.START_CMD)
                    cwd_str = data.get(KEY.DIRECTORY)
                    version_str = data.get(KEY.VERSION)

                page += (
                    "<tr>"
                    '<td class="right">%s</td>' % index_link
                    + "<td>%s</td>" % host_link
                    + '<td class="right">%s</td>' % port_str
                    + '<td class="right">%s</td>' % version_str
                    + '<td class="right">%s</td>' % pv_count_link
                    + '<td class="right">%s</td>' % last_seen_str
                    + '<td class="right">%s</td>' % lost_str
                    + '<td class="right">%s</td>' % uptime_str
                    + '<td class="right">%s</td>' % offset_server_string
                    + "<td>%s</td>" % cwd_str
                    + "<td>%s</td>" % image_str
                    + "<td>%s</td>" % cmd_str
                    + "</tr>"
                )

            page += "</table>\n"

            page += "<p>"
            if link_back:
                page += "%s &nbsp" % link_back

            if link_fwd:
                page += link_fwd

        except Exception as err:
            page = self.make_page_header(css=CSS.IOC)
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_pv_update(self, quoted):

        pv_name = urllib.parse.unquote_plus(urllib.parse.unquote_plus(quoted))

        # This used to be synchronous; now its asynchronous
        if not isinstance(pv_name, str):
            log.debug("PV %s --> unicode", pv_name)
            pv_name = str(pv_name)

        self._dataman.make_leader(pv_name)
        self._dataman.ca_proto_search(pv_name, priority=PRIORITY.HIGH)
        time.sleep(2)
        return self.page_pv_info(quoted)

    def page_linuxmon(self):

        def linuxmon_time(ioc_time, age_sec, active):

            if not active:
                return ""

            if not ioc_time:
                return ""

            n = int(time.time())

            diff = (ioc_time + age_sec) - n

            # LinuxMon only reports to within 1 minute... ugh
            if abs(diff) > 120:
                result = linuxmon_age(abs(diff))
                if diff > 0:
                    return "+ " + result
                return "- " + result

            return ""

        def linuxmon_age(uptime_sec):

            if uptime_sec == 0:
                return "0"

            days = int(uptime_sec / 86400)
            uptime_sec = uptime_sec - days * 86400
            hours = int(uptime_sec / 3600)
            uptime_sec = uptime_sec - hours * 3600
            min = int(uptime_sec / 60)
            uptime_sec = uptime_sec - min * 60

            if days > 0:
                if days > 5000:
                    return ""

                if days == 1:
                    return "1 day"
                else:
                    return "%d days" % days

            if hours:
                if hours == 1:
                    h = "1 hour"
                else:
                    h = "%d hours" % hours
                return h

            if min > 0:
                return "%d min" % min

            return "%d sec" % uptime_sec

        def format_linuxmon_float(value):

            if isinstance(value, float):
                return "%.2f" % value

            if not isinstance(value, str):
                return "Not a string or unicode"
            else:
                if value.startswith("EPIC"):
                    return value

            if value == "":
                return ""
            if value == "nan":
                return "NaN"

            try:
                v = float(value)
            except Exception as err:
                return str(err)

            return "%.2f" % v

        def sort_linuxmon_name(a, b):

            if a > b:
                return 1
            else:
                return -1

        def sort_linuxmon_machine(a, b):

            if a[10] > b[10]:
                return 1
            else:
                return -1

        def sort_linuxmon_cpu_user(a, b):

            try:
                load_a = float(a[11])
            except:
                load_a = 0.0

            try:
                load_b = float(b[11])
            except:
                load_b = 0.0

            return int(10000.0 * load_b - 10000.0 * load_a)

        def sort_linuxmon_cpu_sys(a, b):

            try:
                load_a = float(a[12])
            except:
                load_a = 0.0

            try:
                load_b = float(b[12])
            except:
                load_b = 0.0

            return int(10000.0 * load_b - 10000.0 * load_a)

        def sort_linuxmon_cpu_idle(a, b):

            try:
                load_a = float(a[13])
            except:
                load_a = 0.0

            try:
                load_b = float(b[13])
            except:
                load_b = 0.0

            return int(10000.0 * load_b - 10000.0 * load_a)

        def sort_linuxmon_load(a, b):

            try:
                load_a = float(a[5])
            except:
                load_a = 0.0

            try:
                load_b = float(b[5])
            except:
                load_b = 0.0

            return int(10000.0 * load_b - 10000.0 * load_a)

        def sort_linuxmon_mem(a, b):

            try:
                load_a = float(a[1])
            except:
                load_a = 0.0

            try:
                load_b = float(b[1])
            except:
                load_b = 0.0

            return int(10000.0 * load_b - 10000.0 * load_a)

        def sort_linuxmon_memfree(a, b):

            try:
                load_a = float(a[2])
            except:
                load_a = 0.0

            try:
                load_b = float(b[2])
            except:
                load_b = 0.0

            return int(10000.0 * load_b - 10000.0 * load_a)

        def sort_linuxmon_swap(a, b):

            try:
                load_a = float(a[3])
            except:
                load_a = 0.0

            try:
                load_b = float(b[3])
            except:
                load_b = 0.0

            return int(10000.0 * load_b - 10000.0 * load_a)

        def sort_linuxmon_uptime(a, b):

            # Should NEVER not be an int
            load_a = a[7]
            load_b = b[7]
            return int(load_b - load_a)

        def sort_linuxmon_last_seen(a, b):
            try:
                load_a = float(a[16])
            except:
                load_a = 0.0

            try:
                load_b = float(b[16])
            except:
                load_b = 0.0

            return int(10000.0 * load_b - 10000.0 * load_a)

        def sort_linuxmon_swapfree(a, b):

            try:
                if a[4] == "nan":
                    load_a = -1
                else:
                    load_a = float(a[4])
            except:
                load_a = 0.0

            try:
                if b[4] == "nan":
                    load_b = -1
                else:
                    load_b = float(b[4])
            except:
                load_b = 0.0

            return int(10000.0 * load_b - 10000.0 * load_a)

        # ----------------------------------------------------------------------
        # Page start
        # ----------------------------------------------------------------------

        sort_mode = SORT_MODE.NAME
        direction = DIR_INC

        try:
            page = self.make_page_header(css=CSS.IOC)

            for key, value in bottle.request.query.items():
                if key == "sort":
                    sort_mode = value
                if key == "direction":
                    if value in [DIR_INC, DIR_DEC]:
                        direction = value

            log.debug(
                "called; sort: %s dir: %s",
                sort_mode,
                direction,
            )
            linuxmon_dict = self._dataman.get_linuxmon_dict()
            if direction == DIR_DEC:
                next_direction = DIR_INC
            else:
                next_direction = DIR_DEC

            cur_time = int(time.time())

            active_count = 0
            inactive_count = 0
            lost_count = 0

            display_list = []
            for ioc_name, data in linuxmon_dict.items():

                mem = data.get("MEMAVPCT", "")
                memfree = data.get("MEMFREEPCT", "")
                swap = data.get("SWAPACTIVEPCT", "")
                swapfree = data.get("SWAPFREEPCT", "")
                load = data.get("LOAD15MIN", "")
                cpu_user = data.get("CPUUSER", "")
                cpu_system = data.get("CPUSYSTEM", "")
                cpu_idle = data.get("CPUIDLE", "")
                arch = data.get("MACHINE", "")
                uptime_sec = data.get("UPTIME", 0)  # This is a string
                ioc_time = data.get("TIME", 0)  # This is a string

                t = data.get("T", 0)
                ioc_id = data.get(KEY.IOC_ID, 0)

                active = False
                age_sec = cur_time - t
                last_seen_str = linuxmon_age(age_sec)
                # Declare active if seen in last 20 mins.
                if age_sec < 30 * 60:
                    active = True

                if active:
                    active_count += 1
                else:
                    inactive_count += 1

                lost = False
                if t > 0:
                    lost = True

                if isinstance(uptime_sec, int):
                    if active:
                        uptime_sec += age_sec
                        lost = False
                    uptime_display = my_uptime_str(uptime_sec)
                else:
                    # The uptime could be a string... display verbatim
                    uptime_display = uptime_sec
                    uptime_sec = 0

                time_display = linuxmon_time(ioc_time, age_sec, active)

                if lost:
                    lost_count += 1

                # DO NOT CHANGE TUPLE ORDER!!
                display_list.append(
                    (
                        ioc_name,  # 0
                        mem,  # 1
                        memfree,  # 2
                        swap,  # 3
                        swapfree,  # 4
                        load,  # 5
                        last_seen_str,  # 6
                        uptime_sec,  # 7
                        uptime_display,  # 8
                        active,  # 9
                        arch,  # 10
                        cpu_user,  # 11
                        cpu_system,  # 12
                        cpu_idle,  # 13
                        lost,  # 14
                        time_display,  # 15
                        age_sec,  # 16
                        ioc_id,  # 17
                    )
                )

            page += "<h2>LinuxMonitor Processes: Active: %d Lost: %d</h2>" % (
                active_count,
                lost_count,
            )

            if sort_mode == SORT_MODE.NAME:
                display_list = sorted(
                    display_list, key=functools.cmp_to_key(sort_linuxmon_name)
                )
                if direction == DIR_INC:
                    display_list.reverse()

            elif sort_mode == SORT_MODE.UPTIME:
                list1 = []
                list2 = []
                for item in display_list:
                    # index 7 is the uptime in seconds
                    if item[7] == 0:
                        list2.append(item)
                    else:
                        list1.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_uptime)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1

            elif sort_mode == SORT_MODE.MEM:
                list1 = []
                list2 = []
                for item in display_list:
                    if item[1] == "":
                        list2.append(item)
                    else:
                        list1.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_mem)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1

            elif sort_mode == SORT_MODE.MEM_FREE:
                list1 = []
                list2 = []
                for item in display_list:
                    if item[2] == "":
                        list2.append(item)
                    else:
                        list1.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_memfree)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1
            elif sort_mode == SORT_MODE.SWAP:
                list1 = []
                list2 = []
                for item in display_list:
                    if item[3] == "":
                        list2.append(item)
                    else:
                        list1.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_swap)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1
            elif sort_mode == SORT_MODE.SWAP_FREE:
                list1 = []
                list2 = []
                for item in display_list:
                    if item[4] == "" or item[4] == "nan":
                        list2.append(item)
                    else:
                        list1.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_swapfree)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1

            elif sort_mode == SORT_MODE.MACHINE:
                list1 = []
                list2 = []
                for item in display_list:
                    if item[10] == "":
                        list2.append(item)
                    else:
                        list1.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_machine)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1

            elif sort_mode == SORT_MODE.CPU_USER:
                list1 = []
                list2 = []
                for item in display_list:
                    if item[11] == "":
                        list2.append(item)
                    else:
                        list1.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_cpu_user)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1

            elif sort_mode == SORT_MODE.CPU_SYS:
                list1 = []
                list2 = []
                for item in display_list:
                    if item[12] == "":
                        list2.append(item)
                    else:
                        list1.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_cpu_sys)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1

            elif sort_mode == SORT_MODE.CPU_IDLE:
                list1 = []
                list2 = []
                for item in display_list:
                    if item[13] == "":
                        list2.append(item)
                    else:
                        list1.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_cpu_idle)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1

            elif sort_mode == SORT_MODE.LOST:
                list1 = []
                list2 = []

                for item in display_list:
                    if item[14]:
                        list1.append(item)
                    else:
                        list2.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_last_seen)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1

            elif sort_mode == SORT_MODE.LAST_SEEN:
                list1 = []
                list2 = []

                for item in display_list:
                    if item[16] < cur_time:
                        list1.append(item)
                    else:
                        list2.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_last_seen)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1

            else:
                # By default, sort mode is by load average
                list1 = []
                list2 = []
                for item in display_list:
                    if item[5] == "":
                        list2.append(item)
                    else:
                        list1.append(item)

                list1 = sorted(
                    list1, key=functools.cmp_to_key(sort_linuxmon_load)
                )
                list2 = sorted(
                    list2, key=functools.cmp_to_key(sort_linuxmon_name)
                )

                if direction == DIR_INC:
                    list1.reverse()

                list1.extend(list2)
                display_list = list1

            page += (
                "<p><TABLE>\n"
                "<tr>\n"
                "<th>Index\n</th>"
                "<th>Forget</th>\n"
                "<th>%s</th>\n"
                % self.make_link_linuxmon(
                    name="IOC", sort=SORT_MODE.NAME, direction=next_direction
                )
                + '<th class="right">%s</th>\n'
                % self.make_link_linuxmon(
                    name="Last Seen",
                    sort=SORT_MODE.LAST_SEEN,
                    direction=next_direction,
                )
                + '<th class="right">%s</th>\n'
                % self.make_link_linuxmon(
                    name="Arch.",
                    sort=SORT_MODE.MACHINE,
                    direction=next_direction,
                )
                + '<th class="right">%s</th>\n'
                % self.make_link_linuxmon(
                    name="Uptime",
                    sort=SORT_MODE.UPTIME,
                    direction=next_direction,
                )
                + '<th class="right">%s</th>\n'
                % self.make_link_linuxmon(
                    name="Lost", sort=SORT_MODE.LOST, direction=next_direction
                )
                + '<th class="right">IOC<br>Time</th>\n'
                + '<th class="right">%s</th>\n'
                % self.make_link_linuxmon(
                    name="% Mem<br>Used",
                    sort=SORT_MODE.MEM,
                    direction=next_direction,
                )
                + '<th class="right">%s</th>\n'
                % self.make_link_linuxmon(
                    name="% Mem<br>Free",
                    sort=SORT_MODE.MEM_FREE,
                    direction=next_direction,
                )
                + '<th class="right">%s</th>\n'
                % self.make_link_linuxmon(
                    name="% CPU<br>User",
                    sort=SORT_MODE.CPU_USER,
                    direction=next_direction,
                )
                + '<th class="right">%s</th>\n'
                % self.make_link_linuxmon(
                    name="% CPU<br>Sys",
                    sort=SORT_MODE.CPU_SYS,
                    direction=next_direction,
                )
                + '<th class="right">%s</th>\n'
                % self.make_link_linuxmon(
                    name="% CPU<br>Idle",
                    sort=SORT_MODE.CPU_IDLE,
                    direction=next_direction,
                )
                + '<th class="right">%s</th>\n'
                % self.make_link_linuxmon(
                    name="Load Avg.<br>(15 min)",
                    sort=SORT_MODE.LOAD,
                    direction=next_direction,
                )
                + "</tr>"
            )

            for index, item in enumerate(display_list):

                ioc_name = item[0]
                ioc_id = item[17]
                linuxmon_pvs_link = self.make_link_linuxmon_ioc(
                    ioc_id, ioc_name, display=ioc_name
                )

                lost_str = ""
                if item[14]:
                    lost_str = "** LOST **"

                forget_link = self.make_link_linuxmon_forget(ioc_name, "X")
                page += (
                    "<tr>\n"
                    '<td class="right">%d</td>\n' % (index + 1)
                    + "<td>%s</td>\n" % forget_link
                    + "<td>%s</td>\n" % linuxmon_pvs_link
                    + '<td class="right">%s</td>\n' % item[6]  # Last Seen
                    + '<td class="right">%s</td>\n' % item[10]  # Arch
                    + '<td class="right">%s</td>\n' % item[8]
                    + '<td class="right">%s</td>\n' % lost_str
                    + '<td class="right">%s</td>\n' % item[15]
                    + '<td class="right">%s</td>\n'
                    % format_linuxmon_float(item[1])
                    + '<td class="right">%s</td>\n'
                    % format_linuxmon_float(item[2])
                    + '<td class="right">%s</td>\n'
                    % format_linuxmon_float(item[11])
                    + '<td class="right">%s</td>\n'
                    % format_linuxmon_float(item[12])
                    + '<td class="right">%s</td>\n'
                    % format_linuxmon_float(item[13])
                    + '<td class="right">%s</td>\n'
                    % format_linuxmon_float(item[5])
                    + "</tr>\n"
                )
            page += "</table>"

        except Exception as err:
            page = self.make_page_header(css=CSS.IOC)
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_linuxmon_forget(self):
        """
        This is mostly for debugging... to ensure that LinuxMon instances
        are detected.  Thats why its not fancy.  Maybe add a checkbox to
        delete multiple imtens at once if we find its used a lot.
        """
        try:
            page = self.make_page_header(css=CSS.IOC)

            ioc_name = None
            for key, value in bottle.request.query.items():
                log.info("KEY: %r VALUE: %r", key, value)

                if key == "ioc_name":
                    ioc_name = value
                ### if key == "sort":
                ###     sort_mode = value
                ### if key == "direction":
                ###     if value in [DIR_INC, DIR_DEC]:
                ###         direction = value

            log.debug("called; ioc_name: %s", ioc_name)

            if ioc_name:
                result = self._dataman.forget_linuxmon(ioc_name)
                if result:
                    page += (
                        "<h2>LinuxMonitor entry for %s deleted</h2>"
                        "<p>If LinuxMonitor is detected on this IOC it will be"
                        " automatically re-added to the database." % ioc_name
                    )
                else:
                    page += (
                        "<h2>LinuxMonitor entry for %s not found</h2>"
                        % ioc_name
                    )

        except Exception as err:
            page = self.make_page_header(css=CSS.IOC)
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_linuxmon_ioc(self):

        try:
            page = self.make_page_header(css=CSS.IOC, jquery=True)

            ioc_name = None
            ioc_id = None

            for k, v in bottle.request.query.items():
                if k == "ioc_name":
                    ioc_name = v
                elif k == "ioc_id":
                    ioc_id = int(v)

            server_link = self.make_link_ioc(ioc_id, ioc_name)

            page += (
                "<h2>%s LinuxMonitor</h2>\n"
                "<p><table>\n"
                "<tr>\n"
                "<th>PV Name</th>\n"
                "<th>Value</th>\n"
                "</tr>\n"
            ) % server_link

            items = sorted(LINUXMON_PV_SUFFIX_POLL_ALL)
            for item in items:
                pv_name = ioc_name + ":" + item
                pv_name_link = self.make_link_pv_info(pv_name)
                page += (
                    "<tr>\n"
                    "<td>%s</td>\n"
                    '<td><div class="pv_value" item_id="%s">Unknown</div></td>\n'
                    "</tr>\n"
                ) % (pv_name_link, pv_name)

            page += (
                "</table></body>"
                '<script type="text/javascript" language="javascript">'
                "%s"
                "</script>"
            ) % self.get_js(
                "javascript/linuxmon.js",
                config.CAINFO_HOST,
                config.CAINFO_SERVER_PORT,
            )

        except Exception as err:
            page = self.make_page_header(css=CSS.IOC)
            page += self.add_traceback(err)
            page += "</body>"

        page += "</html>"
        return page


    def page_client_port(self):

        page = self.make_page_header(css=CSS.IOC)

        try:

            ioc_id = None
            port = None

            for key, value in bottle.request.query.items():
                log.debug("key: %s value: %s", key, value)
                if key == "ioc_id":
                    ioc_id = int(value)
                if key == "port":
                    port = int(value)

            page += "<h2>Client Port %d %d</h2>" % (ioc_id, port)

            ip_addr = get_ip_addr(ioc_id)
            data = self._dataman._procman.get_procs(ip_addr, port)

            log.info("This is the data we got", data)

            if data is None:
                page += "Not Authorized"
            elif isinstance(data, str):
                page += data

            elif isinstance(data, list):
                data = data[0]
                if len(data):

                    page += (
                        "<p><table>"
                        + "<tr>"
                        + "<th>Parameter</th>"
                        + "<th>Value</th>"
                        + "</tr>"
                    )

                    for key, value in data.items():
                        page += (
                            "<tr>"
                            + "<td>%s</td>" % key
                            + "<td>%s</td>" % value
                            + "</tr>"
                        )

                    page += "</table>"
            else:
                log.error(
                    "Don't know how to handle data: %s",
                    data,
                )

        except Exception as err:
            page = self.make_page_header(css=CSS.IOC)
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_admin(self):
        page = self.make_page_header(css=CSS.PV)

        uptime = time.time() - self._start_time
        uptime_str = make_duration_string(int(uptime))

        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        size_str_rss = make_size_str(mem_info.rss)
        size_str_vms = make_size_str(mem_info.vms)

        page += (
            "<h2>Admin &nbsp Uptime: %s &nbsp Mem Usage: %s (RSS) %s (VMS)</h2>\n"
            % (uptime_str, size_str_rss, size_str_vms)
        )

        dead_threads = self._dataman.get_dead_thread_count()
        if dead_threads:
            threads = '<span style="background-color:%s">%s</span>' % (
                COLOR.RED,
                "Threads",
            )
        else:
            threads = "Threads"

        pid_list = self._dataman.get_pids()

        page += (
            "%s<br>\n" % self.make_link("/admin/threads", threads)
            + "%s<br>\n" % self.make_link("/admin/netstat", "Netstat Fetch")
            + "%s<br>\n"
            % self.make_link("/admin/no_netstat", "Netstat Unsupported")
            + "%s<br>\n" % self.make_link("/admin/pv_fetch", "PV Fetch")
            + "%s<br>\n" % self.make_link("/admin/counts", "System Counts")
            + "%s<br>\n" % self.make_link("/admin/logs", "Latest Logs")
            + "%s<br>\n" % self.make_link("/admin/dbg_pv_gone", "Possibly Gone")
            + "%s<br>\n"
            % self.make_link("/admin/beacons_bad", "Invalid Beacons")
            + "%s<br>\n" % self.make_link("/admin/ack_all_logs", "ACK all logs")
            + "%s<br>\n"
            % self.make_link("/admin/process_all_iocs", "Process all IOCs")
            + "%s<br>\n"
            % self.make_link("/api/v1.0/beacons", "API Test: beacons")
            + "%s<br>\n"
            % self.make_link("/api/v1.0/heartbeats", "API Test: heartbeats")
            + "<h2>Process IDs:</h2>"
            + "<b>Main:</b> %s<br>" % repr(pid_list[0])
            + "<b>SQL Manager:</b> %s<br>" % repr(pid_list[1])
            + "<b>Gateway Client:</b> %s<br>" % repr(pid_list[2])
            + "<h2>Deployment/Git:</h2>"
            + "<b>Deployment Directory:</b> %s<br>" % deploy_dir
            + "<b>Git Remote URL:</b> %s<br>" % repo.remotes.origin.url
            + "<b>Git Branch:</b> %s<br>" % repo.active_branch.name
            + "<b>Git Commit Hash:</b> %s<br>" % repo.head.commit.hexsha
            + "</body></html>"
        )

        return page

    def page_admin_counts(self):
        page = self.make_page_header(css=CSS.PV)
        page += "<h2>System Counters</h2>"
        page += '<span class="my_monospace">'
        data = STATS.get()

        items = [(key.lower(), key, value) for key, value in data.items()]
        items.sort()

        for item in items:
            key = item[1]
            value = item[2]
            if isinstance(value, int):
                page += "%-40s %15d<br>" % (key, value)

        for item in items:
            key = item[1]
            value = item[2]

            if isinstance(value, bytes) or isinstance(value, str):
                page += "%-40s %s<br>" % (key, value)

        page += "</span></body></html>"
        return page

    def page_admin_logs(self):

        page = self.make_page_header(css=CSS.PV)
        offset = 0
        items_per_page = 50
        try:
            # Look at the request params
            for k, v in bottle.request.query.items():
                if k == "offset":
                    offset = int(v)

            total_log_count = self._dataman.get_ioc_log_count(None)
            l, u = self.my_range(offset, items_per_page, total_log_count)
            page += "<h2>Displaying %d to %d of %d Logs</h2>" % (
                l,
                u,
                total_log_count,
            )
            logs = self._dataman.get_ioc_logs(
                None, offset=offset, count=items_per_page
            )

            offset_prev = offset - items_per_page
            if offset_prev < 0:
                offset_prev = total_log_count - items_per_page

            offset_next = offset + items_per_page
            if offset_next >= total_log_count:
                offset_next = 0

            link_fwd = self.make_link_admin_log_list(
                offset=offset_next, name="NEXT >>"
            )
            link_back = self.make_link_admin_log_list(
                offset=offset_prev, name="<< PREV"
            )

            page += (
                "<p>%s &nbsp %s"
                "<p><table>\n"
                "<tr>\n"
                "<th>Index</th>\n"
                "<th>ACKed</th>\n"
                "<th>Date</th>\n"
                "<th>Time</th>\n"
                "<th>IOC</th>\n"
                "<th>Log Message</th>\n"
                "</tr>" % (link_back, link_fwd)
            )

            for index, log in enumerate(logs):
                ### log_id = log[0]
                ioc_id = log[1]
                time_str = log[2]
                date_str = log[3]
                ack_str = log[4]
                color = log[5]
                msg = log[6]

                if ioc_id:
                    ioc_name = get_host_name(ioc_id)
                    ioc_link = self.make_link_ioc(ioc_id, ioc_name)
                else:
                    ioc_link = ""

                if color is not None:
                    ack_str = '<span style="background-color:%s">%s</span>' % (
                        color,
                        ack_str,
                    )

                page += (
                    "<tr>"
                    + "<td>%d</td>\n" % (index + offset + 1)
                    + "<td>%s</td>\n" % ack_str
                    + "<td>%s</td>\n" % date_str
                    + "<td>%s</td>\n" % time_str
                    + "<td>%s</td>\n" % ioc_link
                    + "<td>%s</td>\n" % msg
                    + "</tr>\n"
                )

            page += "</table><p>\n<p>%s &nbsp %s" % (link_back, link_fwd)

        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_admin_ack_all_logs(self):
        page = self.make_page_header(css=CSS.PV)
        try:
            log_count = self._dataman.ack_all_logs()
            page += "<h2>ACK All logs: Count: %d</h2>" % log_count

        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_admin_process_all_iocs(self):
        page = self.make_page_header(css=CSS.PV)
        try:
            ioc_count = self._dataman.process_all_iocs()
            page += "<h2>Process All IOCs: Count: %d</h2>" % ioc_count

        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_requests_client_pvs(self):

        ioc_id = None
        offset = 0
        pvs_per_page = 25

        for k, v in bottle.request.query.items():
            log.debug("KEY: %r VALUE: %r", k, v)
            if k == "ioc_id":
                ioc_id = int(v)

            if k == "offset":
                offset = int(v)

        requests = self._dataman._pvman.get_latest_requests()
        request_count = len(requests)

        pv_dict = {}
        request_count = 0
        for request in requests:
            pv_name = request[0]
            ioc_id_req = request[1]
            ### port = request[2]
            if ioc_id_req != ioc_id:
                continue

            request_count += 1
            pv_dict[pv_name] = pv_dict.get(pv_name, 0) + 1

        pv_list = [(v, k) for k, v in pv_dict.items()]
        pv_list.sort()
        pv_list.reverse()
        pv_names = [item[1] for item in pv_list]
        length = len(pv_names)

        pv_names_display = pv_names[offset : offset + pvs_per_page]

        counts = {}
        for pv_name in pv_names_display:
            count = pv_dict.get(pv_name)
            percentage = 100.0 * float(count) / float(request_count)
            counts[pv_name] = (count, percentage)

        host_name = get_host_name(ioc_id)
        ioc_link = self.make_link_ioc(ioc_id, host_name)

        try:
            page = self.make_page_header(css=CSS.IOC)
            page += "<h2>Latest %d Real Time Requests from %s</h2>" % (
                request_count,
                ioc_link,
            )

            offset_prev = offset - pvs_per_page
            if offset_prev < 0:
                offset_prev = length - pvs_per_page

            offset_next = offset + pvs_per_page
            if offset_next >= length:
                offset_next = 0

            link_fwd = self.make_link_requests_latest_client_pvs(
                offset=offset_next, ioc_id=ioc_id, display="NEXT >>"
            )
            link_back = self.make_link_requests_latest_client_pvs(
                offset=offset_prev, ioc_id=ioc_id, display="<< PREV"
            )

            page += (
                "<p>%s &nbsp %s" % (link_back, link_fwd)
                + "<p>"
                + self.make_pv_table(
                    pv_names_display,
                    counts=counts,
                    offset=offset,
                )
                + "<p>%s &nbsp %s" % (link_back, link_fwd)
                + "<p><b>Note:</b>: "
                "This is real-time data, therefore the last listed request "
                + "may not be from %s. " % ioc_link
                + "The PV may have been requested by another client while this"
                " page was being compiled."
            )

        except Exception as err:
            page = self.make_page_header(css=CSS.IOC)
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_requests_ioc(self):

        page = self.make_page_header(css=CSS.IOC)

        ### ioc_id = None
        ### offset = 0

        for k, v in bottle.request.query.items():
            log.debug("KEY: %r VALUE: %r", k, v)

        ### if k == "ioc_id":
        ###     ioc_id = int(v)
        ### elif k == "offset":
        ###     offset = int(v)

        try:
            page += "IOC requests"

        except Exception as err:
            page = self.make_page_header(css=CSS.IOC)
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_requests(self):
        """
        This is totally live data so processing happens in this code
        """

        pvs_per_page = 25
        kind = REQUESTS.PVS
        offset = 0

        for k, v in bottle.request.query.items():
            log.debug("KEY: %r VALUE: %r", k, v)

            if k == "kind":
                kind = v
            elif k == "offset":
                offset = int(v)

        page = self.make_page_header(css=CSS.IOC)

        try:
            requests = self._dataman._pvman.get_latest_requests(offset=offset)
            request_count = len(requests)
            submit_requests_url = "%s/submit_requests" % self.make_link_base()
            selected_pvs = "selected" if kind == REQUESTS.PVS else ""
            selected_clients = "selected" if kind == REQUESTS.CLIENTS else ""
            page += (
                "<h2>Real Time Display of Last %d Requests (CA_PROTO_SEARCH)</h2>"
                % request_count
                + '<FORM action="%s" method="POST">\n' % submit_requests_url
                + '<input type="hidden" name="offset" value=%d>\n' % offset
                + 'Display: <select name="kind" onchange="this.form.submit()">\n'
                + '<option value="%s" %s>%s</option>\n'
                % (REQUESTS.PVS, selected_pvs, "By PV")
                + '<option value="%s" %s>%s</option>\n'
                % (REQUESTS.CLIENTS, selected_clients, "By Client")
                + "</select>\n"
                + "</form>\n"
            )
            log.debug("kink: %r", kind)
            if kind == REQUESTS.PVS:
                html, length = self.page_requests_pvs(
                    requests, offset, pvs_per_page
                )
            else:
                html, length = self.page_requests_clients(
                    requests, offset, pvs_per_page
                )

            offset_prev = offset - pvs_per_page
            if offset_prev < 0:
                offset_prev = 0

            offset_next = offset + pvs_per_page
            if offset_next > length:
                offset_next = offset

            link_fwd = self.make_link_requests(
                offset=offset_next, kind=kind, name="NEXT >>"
            )
            link_back = self.make_link_requests(
                offset=offset_prev, kind=kind, name="<< PREV"
            )

            page += (
                "<p>"
                + "%s &nbsp %s" % (link_back, link_fwd)
                + "<p>"
                + html
                + "<p>"
                + "%s &nbsp %s" % (link_back, link_fwd)
            )

        except Exception as err:
            page = self.make_page_header(css=CSS.IOC)
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_requests_pvs(self, request_list, offset, count):
        log.debug("called; offset: %d count: %d", offset, count)

        pv_dict = {}

        for index, request in enumerate(request_list):
            pv_name = request[0]
            ioc_id = request[1]
            ### port = request[2]
            pv_data = pv_dict.get(pv_name, {})
            pv_data[ioc_id] = pv_data.get(ioc_id, 0) + 1
            pv_dict[pv_name] = pv_data

        pv_list = []
        for pv_name, pv_data in pv_dict.items():
            total_count = 0
            for ioc_id, requests in pv_data.items():
                total_count += requests

            pv_list.append((total_count, pv_name))

        request_len = float(len(request_list))
        counts = {}
        for item in pv_list:
            request_count = item[0]
            percentage = 100.0 * float(request_count) / request_len
            counts[item[1]] = (request_count, percentage)

        pv_list.sort()
        pv_list.reverse()
        pv_names = [item[1] for item in pv_list]

        length = len(pv_names)

        pv_names = pv_names[offset : offset + count]

        page = self.make_pv_table(pv_names, offset=offset, counts=counts)
        return page, length

    def page_requests_clients(self, request_list, offset, count):
        log.debug("called; offset: %d count: %d", offset, count)

        page = ""
        ioc_dict = {}

        for index, request in enumerate(request_list):
            pv_name = request[0]
            ioc_id = request[1]
            ### port = request[2]
            ioc_data = ioc_dict.get(ioc_id, {})
            ioc_data[pv_name] = ioc_data.get(pv_name, 0) + 1
            ioc_dict[ioc_id] = ioc_data

        ioc_list = []
        for ioc_id, ioc_data in ioc_dict.items():
            total_count = 0
            for pv_name, requests in ioc_data.items():
                total_count += requests

            ioc_list.append((total_count, ioc_id))

        ioc_list.sort()
        ioc_list.reverse()

        length = len(ioc_list)

        page = (
            "<p><table><tr>\n"
            "<th>Index</th>\n"
            "<th>Client</th>\n"
            "<th>Requests</th>\n"
            "<th>Percent</th>\n"
            "</tr>\n"
        )

        ioc_list = ioc_list[offset : offset + count]

        for index, item in enumerate(ioc_list):
            request_count = item[0]
            ioc_id = item[1]
            count_link = self.make_link_requests_latest_client_pvs(
                ioc_id=ioc_id, display="%d" % request_count
            )

            host_name = get_host_name(ioc_id)

            percentage = 100.0 * float(request_count) / float(len(request_list))

            ioc_link = self.make_link_ioc(ioc_id, host_name)

            page += (
                "<tr>"
                "<td>%d</td>" % (index + offset + 1)
                + "<td>%s</td>" % ioc_link
                + "<td>%s</td>" % count_link
                + "<td>%0.2f</td>" % percentage
                + "</tr>"
            )

        page += "</table>"
        return page, length

    def page_admin_beacons_bad(self):

        page = self.make_page_header(css=CSS.PV)

        # This section gets "bad beaons" .i.e., from 192.168.X.X
        try:

            data = self._dataman.get_beacons_bad()
            data_len = len(data)

            if data_len == 1:
                page += "<h2>1 Bad Beacon</h2>"
            else:
                page += "<h2>%d Bad Beacons</h2>" % data_len

            page += DESCRIPTION.BAD_BEACON

            page += "<p><table>"
            page += (
                "<tr>"
                "<th>Index</th>"
                "<th>IP Address</th>"
                "<th>TCP Port</th>"
                "<th>Beacon Port</th>"
                "<th>Sequence</th>"
                "<th>Uptime (est.)</th>"
                "</tr>\n"
            )

            sorted_list = []
            for key, value in data.items():
                parts = key.split("-")
                ioc_key = int(parts[0])
                tcp_port = int(parts[1])
                sorted_list.append((ioc_key, tcp_port, value))

            sorted_list.sort()

            for index, item in enumerate(sorted_list):
                ip_addr = get_ip_addr(item[0])
                tcp_port = item[1]
                data = item[2]
                beacon_port = data.get(KEY.PORT)
                seq = data.get(KEY.SEQ, 0)
                est_age_str = beacon_estimated_age_str(None, seq=seq)
                page += (
                    "<tr>"
                    + "<td>%d</td>" % index
                    + "<td>%s</td>" % ip_addr
                    + "<td>%d</td>" % tcp_port
                    + "<td>%d</td>" % beacon_port
                    + "<td>%d</td>" % seq
                    + "<td>%s</td>" % est_age_str
                    + "</tr>\n"
                )

            page += "</table>\n"

            # This section gets "address mismatches"

            data = self._dataman.get_beacons_addr_mismatch()
            data_len = len(data)

            if data_len == 1:
                page += "<h2>1 Mismatched Beacon Address</h2>"
            else:
                page += "<h2>%d Mismatched Beacon Addresses</h2>" % data_len

            page += DESCRIPTION.BEACON_ADDR_MISMATCH

            page += "<p><table>"
            page += (
                "<tr>"
                "<th>Index</th>"
                "<th>IOC Name</th>"
                "<th>IP Address (Rx)</th>"
                "<th>IP Address (beacon)</th>"
                "<th>TCP Port</th>"
                "<th>Beacon Port</th>"
                "<th>Sequence</th>"
                "<th>Uptime (est.)</th>"
                "</tr>\n"
            )

            sorted_list = []
            for key, value in data.items():
                parts = key.split("-")
                ioc_key = int(parts[0])
                tcp_port = int(parts[1])
                sorted_list.append((ioc_key, tcp_port, value))

            sorted_list.sort()

            for index, item in enumerate(sorted_list):
                ip_addr = get_ip_addr(item[0])
                tcp_port = item[1]
                data = item[2]
                beacon_port = data.get(KEY.PORT)
                seq = data.get(KEY.SEQ, 0)
                host_name = get_host_name(item[0])

                ioc_id_beacon = data.get(KEY.IOC_ID)
                ip_addr_beacon = get_ip_addr(ioc_id_beacon)

                est_age_str = beacon_estimated_age_str(None, seq=seq)

                page += (
                    "<tr>"
                    + "<td>%d</td>" % index
                    + "<td>%s</td>" % host_name
                    + "<td>%s</td>" % ip_addr
                    + "<td>%s</td>" % ip_addr_beacon
                    + "<td>%d</td>" % tcp_port
                    + "<td>%d</td>" % beacon_port
                    + "<td>%d</td>" % seq
                    + "<td>%s</td>" % est_age_str
                    + "</tr>\n"
                )

            page += "</table>\n"

        except Exception as err:
            page += "</table>\n"
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_admin_threads(self):
        page = self.make_page_header(css=CSS.PV)
        page += "<h2>Threads</h2>"

        try:
            page += '<span class="my_monospace">'
            page += "<h3>PV Monitor Server threads:</h3>"
            thread_data_pvm = self._dataman.get_thread_data_pvm()
            page = self.thread_page_helper(thread_data_pvm, page)
            
            page += "<h3>PV Monitor Gateway threads:</h3>"
            thread_data_gateway = self._dataman.get_thread_data_gateway()
            page = self.thread_page_helper(thread_data_gateway, page)

            page += "</span>"
        except Exception as err:
            page += self.add_traceback(err)

        page += "</BODY></HTML>"
        return page
    
    def thread_page_helper(self, thread_list, page):
        if thread_list:
            thread_list = [(v.get("desc"), v) for k, v in thread_list.items()]
            thread_list.sort()
            page += (
                "<p><table>\n"
                "<tr>\n"
                "<th>Thread</th>\n"
                "<th>Age</th>\n"
                "<th>Max</th>\n"
                "<th>Seen</th>\n"
                "<th>Pings</th>\n"
                "<th>Lost count</th>\n"
                "<th>Msg</th>\n"
                "</tr>" 
                )
            for item in thread_list:
                name = item[0]
                v = item[1]
                
                cur_time = time.time()
                age = cur_time - v.get("last_time")
                red_span = v.get("lost")

                if red_span:
                    page += '<tr style="background-color:%s">\n' % COLOR.RED
                else:
                    page += "<tr>\n"

                page += (
                    "<td>%-30s</td>\n" % name
                    + "<td>%8.1f</td>\n" % age
                    + "<td>%6.1f</td>\n" % v.get("max_age")
                    + "<td>%6.1f</td>\n" % v.get("max_age_seen", 0)
                    + "<td>%10d</td>\n" % v.get("ping_count")
                    + "<td>%7d</td>\n" % v.get("lost_count")
                    + "<td>%s</td>\n" % repr(v.get("msg"))
                    + "</tr>\n"
                )

            page += "</table>"
        else:
            page += "<h3>Gateway thread is not available</h3>"
        return page


    def page_set_beacon_count(self, ip_addr):

        ip_key = get_ip_key(ip_addr)
        host_name = get_host_name(ip_key)

        page = self.make_page_header(css=CSS.IOC)

        # Get the list of active ports
        ports = self._dataman.get_ioc_ports(ip_key)
        port_count = len(ports)

        beacon_count = self._dataman.get_beacon_count(ip_key)
        self._dataman.set_beacon_count(ip_key, port_count)

        page += (
            "<h2>IOC: %s (%s): EPICS App Count changed from %d to %d</h2>"
            % (
                host_name,
                ip_addr,
                beacon_count,
                port_count,
            )
        )

        look_for = "Detected Beacons on %d TCP port(s);" % port_count
        self._dataman.clear_ioc_logs(ip_key, KEY.WARNINGS, pattern=look_for)

        display = "Back to IOC %s Status Page" % host_name
        ioc_link = self.make_link_ioc(ip_addr, display)
        page += ioc_link

        page += "</body></html>"
        return page

    def pad(self, input, length):
        return " " * (length - len(input))

    def page_plots(self, kind=PLOT_TYPE.REQ_USER, interval=PLOT_INTERVAL.DAY):
        """
        handle all system plots
        """
        page = self.make_page_header(
            css=CSS.PV, script=SETTINGS.SCRIPT_PLOTLY_JS
        )

        try:

            # Look at the request params
            for k, v in bottle.request.query.items():
                log.debug("KEY: %r VALUE: %r", k, v)

                if k == "interval":
                    interval = v

                elif k == "kind":
                    kind = v

            log.debug("kind: %s interval: %s", kind, interval)

            kind_str = PLOT_TYPE_SELECT_DICT.get(kind)
            if kind in [PLOT_TYPE.MOST_ACTIVE_PVS, PLOT_TYPE.MOST_ACTIVE_IOCS]:
                page += "<h2>%s (Last %d Requests)</h2>" % (
                    kind_str,
                    SETTINGS.LATEST_REQUEST_PVS,
                )
            else:
                interval_str = PLOT_INTERVAL_SELECT_DICT.get(interval)
                page += "<h2>%s (%s)</h2>" % (kind_str, interval_str)

            plot_submit_link = "%s/submit_plot" % self.make_link_base()

            page += '<FORM action="%s" method="POST">\n' % plot_submit_link

            # Pick list for plot kind
            page += 'Kind: <select name="kind" onchange="this.form.submit()">\n'
            for item in PLOT_TYPE_SELECT:
                k = item[0]
                display = item[1]
                selected = "selected" if k == kind else ""
                page += '<option value="%s" %s>%s</option>\n' % (
                    k,
                    selected,
                    display,
                )
            page += "</select>\n"

            # Pick list for plot interval
            if kind not in [
                PLOT_TYPE.MOST_ACTIVE_IOCS,
                PLOT_TYPE.MOST_ACTIVE_PVS,
            ]:
                page += (
                    '&nbspInterval: <select name="interval" '
                    'onchange="this.form.submit()">\n'
                )
                for item in PLOT_INTERVAL_SELECT:
                    k = item[0]
                    display = item[1]
                    selected = "selected" if k == interval else ""
                    page += '<option value="%s" %s>%s</option>\n' % (
                        k,
                        selected,
                        display,
                    )
                page += "</select>\n"
            page += "</form>\n"

            # Needed so code from test file can be pasted directly
            plotman = self._dataman._plotman
            ### pvman = self._dataman._pvman

            page += (
                '<div id="myDiv" style="width:1000px;height:600px;"></div>\n'
                "<script>\n"
            )

            # INSERT START ====================================================

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
                    var_str += (
                        '    marker: { color: "%s" },\n'
                        % plotman.get_color(kind, interval, index)
                    )
                    var_str += "};\n"
                    page += var_str

                var_name_str = ",".join(var_names)
                page += (
                    "var data = [%s];\n"
                    "var layout = {\n"
                    'barmode: "stack",\n'
                    'yaxis: {title: "%s"},\n'
                    "};\n" % (var_name_str, plotman.get_y_axis_label(kind))
                )
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
                    page += (
                        "var %s = {\n" % var_name
                        + '    type: "scatter",\n'
                        + '    mode: "lines",\n'
                        + '    name: "%s",\n'
                        % plotman.get_name(kind, interval, index)
                        + "    x: [%s],\n" % plotman.get_x(kind, interval)
                        + "    y: [%s],\n" % plotman.get_y(kind, interval)
                        + "};\n"
                    )
                var_name_str = ",".join(var_names)
                page += (
                    "var data = [%s];\n"
                    "var layout = {\n"
                    'yaxis: {title: "%s"},\n'
                    "};\n" % (var_name_str, plotman.get_y_axis_label(kind))
                )

            elif kind in [PLOT_TYPE.MEM_RSS_USAGE, PLOT_TYPE.MEM_VMS_USAGE]:
                lines = [PLOT_TYPE.MEM_RSS_USAGE, PLOT_TYPE.MEM_VMS_USAGE]
                for index, kind in enumerate(lines):
                    var_name = "trace_%d" % index
                    var_names.append(var_name)
                    page += (
                        "var %s = {\n" % var_name
                        + '    type: "scatter",\n'
                        + '    mode: "lines",\n'
                        + '    name: "%s",\n'
                        % plotman.get_name(kind, interval, index)
                        + "    x: [%s],\n" % plotman.get_x(kind, interval)
                        + "    y: [%s],\n" % plotman.get_y(kind, interval)
                        + "};\n"
                    )
                var_name_str = ",".join(var_names)
                page += (
                    "var data = [%s];\n"
                    "var layout = {\n"
                    'yaxis: {title: "%s"},\n'
                    "};\n" % (var_name_str, plotman.get_y_axis_label(kind))
                )
            elif kind in [
                PLOT_TYPE.SRV_UNIX_FREE_HOME,
                PLOT_TYPE.SRV_UNIX_FREE_IOCAPPS,
            ]:
                lines = [
                    PLOT_TYPE.SRV_UNIX_FREE_HOME,
                    PLOT_TYPE.SRV_UNIX_FREE_IOCAPPS,
                ]
                for index, kind in enumerate(lines):
                    var_name = "trace_%d" % index
                    var_names.append(var_name)
                    page += (
                        "var %s = {\n" % var_name
                        + '    type: "scatter",\n'
                        + '    mode: "lines",\n'
                        + '    name: "%s",\n'
                        % plotman.get_name(kind, interval, index)
                        + "    x: [%s],\n" % plotman.get_x(kind, interval)
                        + "    y: [%s],\n" % plotman.get_y(kind, interval)
                        + "};\n"
                    )

                var_name_str = ",".join(var_names)
                page += (
                    "var data = [%s];\n"
                    "var layout = {\n"
                    'yaxis: {title: "%s"},\n'
                    "};\n" % (var_name_str, plotman.get_y_axis_label(kind))
                )

            else:
                # This is a simple line plot
                page += (
                    "var trace1 = {\n"
                    + '    type: "scatter",\n'
                    + '    mode: "lines",\n'
                    + '    name: "User Requests",\n'
                    + "    x: [%s],\n" % plotman.get_x(kind, interval)
                    + "    y: [%s],\n" % plotman.get_y(kind, interval)
                    + '    line: {color: "#17BECF"}\n'
                    + "};\n"
                    + "var data = [trace1];\n"
                    + "var layout = {\n"
                    + 'yaxis: {title: "%s"},\n' % plotman.get_y_axis_label(kind)
                    + "};\n"
                )

            # INSERT END ======================================================
            page += 'Plotly.newPlot("myDiv", data, layout);\n</script>\n'
        except Exception as err:
            page = self.make_page_header(css=CSS.PV)
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_admin_pv_fetch(self):

        t = PVMonitorTime()

        page = self.make_page_header(css=CSS.PV)
        try:
            page += "<h2>PV Fetches (Heartbeat V1/V2)</h2>"
            data = self._dataman.get_pv_fetch_counts(sort=True)
            page += '<span class="my_monospace">'

            for item in data:
                ip_key = item[0]
                port = item[1]
                app_data = item[2]
                host_name = get_host_name(ip_key)

                host_link = self.make_link_ioc(get_ip_addr(ip_key), host_name)
                port_link = self.make_link_heartbeat(
                    ip_key, port, display="%d" % port
                )
                fetch_time = app_data.get(KEY.TIME)
                fetch_count = app_data.get(KEY.COUNTER)
                last_date_str = t.get_string_MDY(fetch_time)
                last_time_str = t.get_string_time_of_day(fetch_time)
                heartbeat_version = app_data.get(KEY.VERSION)
                got_pvs = app_data.get(KEY.PV_COUNT)
                expect_pvs = app_data.get(KEY.EXPECT)

                page += (
                    "%s%s Port: %s%s Ver: %5s Count: %5d Last time: %10s %15s "
                    "PVs Expected: <b>%6d</b> Got <b>%6d</b><br>"
                    % (
                        host_link,
                        self.pad(host_name, 20),
                        self.pad("%d" % port, 6),
                        port_link,
                        heartbeat_version,
                        fetch_count,
                        last_time_str,
                        last_date_str,
                        got_pvs,
                        expect_pvs,
                    )
                )

        except Exception as err:
            page += self.add_traceback(err)

        page += "</BODY></HTML>"
        return page

    def page_admin_netstat_none(self):

        page = self.make_page_header(css=CSS.PV)
        try:
            page += (
                "<h2>Netstat Unsupported</h2>"
                "The following hosts do not support netstat (Heartbeat V2)<br><br>"
                '<span class="my_monospace">'
            )

            data = self._dataman.get_netstat_unsupported()

            for ip_key in data.keys():
                ip_addr = get_ip_addr(ip_key)
                host_name = get_host_name(ip_key)
                host_link = self.make_link_ioc(ip_addr, host_name)
                page += "%s<br>" % host_link

        except Exception as err:
            page += self.add_traceback(err)

        page += "</BODY></HTML>"
        return page

    def page_admin_netstat(self):

        t = PVMonitorTime()

        page = self.make_page_header(css=CSS.PV)
        try:
            page += "<h2>Netstat Fetches (Heartbeat V2)</h2>"
            data = self._dataman.get_nestat_fetch_counts()
            page += '<span class="my_monospace">'

            fetch_list = []
            for ip_addr, v in data.items():
                host_name = get_host_name(ip_addr)
                fetch_list.append((host_name, v, ip_addr))

            fetch_list.sort()

            for item in fetch_list:
                host_name = item[0]
                v = item[1]
                ip_addr = item[2]

                host_link = self.make_link_ioc(ip_addr, host_name)

                count = v.get("c")

                try:
                    start_time = v.get("s")
                    end_time = v.get("e")
                    duration_str = "%10.2f" % (end_time - start_time)
                except:
                    duration_str = "Unknown"

                lines = v.get("l")
                if lines is None:
                    lines = 0

                last_date_str = t.get_string_MDY(start_time)
                last_time_str = t.get_string_time_of_day(start_time)

                page += (
                    "%s%s Count: %5d Last time: %10s %15s "
                    "Duration (sec): %s Lines: %8d<br>"
                    % (
                        host_link,
                        self.pad(host_name, 20),
                        count,
                        last_time_str,
                        last_date_str,
                        duration_str,
                        lines,
                    )
                )

            page += "</span>"

        except Exception as err:
            page += self.add_traceback(err)

        page += "</BODY></HTML>"
        return page

    def page_pv_info(self, quoted):

        page = self.make_page_header(css=CSS.PV)

        try:
            pv_name = urllib.parse.unquote_plus(
                urllib.parse.unquote_plus(quoted)
            )
            page += "<h2>%s</h2>" % pv_name

            lines = print_binary(pv_name, prnt=False)
            page += '<br><span class="my_monospace">'
            for line in lines:
                page += "%s<br>" % line
            page += "</span>"

            d = self._dataman.get_pv_data(pv_name)

            if not d:
                page += "<p>PV not found in database</BODY></HTML>"
                return page

            requests = d.get(PV_KEY.REQUESTS, ())

            page += (
                "<h3>Most Recent Client Requests:</h3>\n"
                "<p><TABLE>\n"
                "<tr>\n"
                "<th>Time</th>\n"
                "<th>Host</th>\n"
                "<th>Port</th>\n"
                "<th>Count</th>\n"
                "</tr>\n"
            )

            for item in reversed(requests):
                name = get_host_name(item[0])
                port_link = self.make_link_client_port(
                    item[0], int(item[1]), str(item[1])
                )

                page += (
                    "<tr>\n"
                    + "<td>%s</td>\n" % my_time(item[3])
                    + "<td>%s</td>\n" % name
                    + "<td>%s</td>\n" % port_link
                    + "<td>%d</td>\n" % item[2]
                    + "</tr>\n"
                )

            page += "</TABLE>\n"

            # ---- gateway request info ----
            last_gw_time = d.get(PV_KEY.LAST_TIME_GW, 0)
            total_requests = d.get(PV_KEY.REQ_COUNT, 0)
            client_requests = d.get(PV_KEY.CLIENT_REQ, 0)

            try:
                gw_request_count = total_requests - client_requests
                msg = "%d Gateway Requests" % gw_request_count
            except Exception as err:
                msg = "Exception generating GW request count: %s" % str(err)

            if last_gw_time:
                msg += " - Latest: %s" % my_time(last_gw_time)

            page += "<p><b>%s</b>" % msg

            page += "<p><h3>%s</h3><p>\n" % self.make_link_pv_update(
                pv_name, "Update Server Info"
            )

            info = d.get(PV_KEY.INFO, [])

            page += (
                "<p><TABLE>\n"
                "<tr>\n"
                "<th>First Time</th>\n"
                "<th>Last Time</th>\n"
                "<th>Host</th>\n"
                "<th>Port</th>\n"
                "</tr>\n"
            )

            server_key = None

            for item in reversed(info):

                host_key = item.get(PV_KEY.HOST)
                name = get_host_name(host_key)
                port = item.get(PV_KEY.PORT)

                if server_key is None and host_key is not None:
                    server_key = host_key

                port_link = self.make_link_client_port(
                    host_key, int(port), str(port)
                )
                server_link = self.make_link_ioc(host_key, name)

                page += (
                    "<tr>\n"
                    + "<td>%s</td>\n" % my_time(item.get(PV_KEY.FIRST_TIME))
                    + "<td>%s</td>\n" % my_time(item.get(PV_KEY.LAST_TIME))
                    + "<td>%s</td>\n" % server_link
                    + "<td>%s</td>\n" % port_link
                    + "</tr>\n"
                )

            page += "</TABLE>\n"

            duplicates = d.get(PV_KEY.DUPLICATES)

            if duplicates:
                page += (
                    '<p><h2><span style="background-color:%s">'
                    "WARNING! Multiple Servers Host this PV:"
                    "</span></h2></p>"
                    "<TABLE>"
                    "<tr><th>Host</th><th>Port</th></tr>" % COLOR.RED
                )

                for server in duplicates:
                    port = server[1]
                    host_key = server[0]
                    port_link = self.make_link_client_port(
                        host_key, int(port), str(port)
                    )

                    page += (
                        "<tr>"
                        + "<td>%s</td>" % get_host_name(server[0])
                        + "<td>%s</td>" % port_link
                        + "</tr>"
                    )

                page += "</TABLE>"

            if server_key:
                page += "<h3>%s</h3>" % self.make_link_pv_cainfo(
                    pv_name, display="CAINFO"
                )

            heartbeat = self._dataman.get_pv_heartbeat(pv_name)
            if not heartbeat:
                page += "<h3>No Heartbeat</h3>"
            else:
                page += "<h3>HEARTBEAT: %s</h3>" % repr(heartbeat)

            page += "<h3>iocApps Command Files</h3>"

            paths = self._dataman.get_iocapps_data(pv_name)
            if len(paths) == 0:
                page += "No command files found"
            else:
                page += "<TABLE>\n"

                for line in paths:
                    page += "<tr><td>%s</td></tr>\n" % line

                page += "</TABLE>\n"

            url = "%s/submit_forget_pv" % self.make_link_base()

            page += (
                '<p><FORM action="%s" method="POST">\n' % url
                + '<input type="hidden" id="%s" name="%s" value="%s">\n'
                % (
                    # Wondering why 3 same variables? - Leo Jun 5 2024
                    pv_name,
                    pv_name,
                    pv_name,
                )
                + '<button type="submit" name="__ok__">Forget this PV</button>\n'
                + "</form>"
            )

        except Exception as err:
            page += self.add_traceback(err)

        page += "</BODY></HTML>"
        return page

    def page_epics_events(self):

        log.debug("called")

        items_per_page = 25
        offset = 0

        t = PVMonitorTime()

        # First, look at the query items
        timestamp = None
        for key, value in bottle.request.query.items():
            if key == "timestamp":
                timestamp = int(value)
            elif key == "offset":
                offset = int(value)

        if timestamp is None:
            timestamp = int(time.time())

        sec_per_day = 24 * 60 * 60
        page = self.make_page_header(css=CSS.EPICS_EVENTS)

        total_event_count = self._dataman.get_epics_event_count(timestamp)
        l, u = self.my_range(offset, items_per_page, total_event_count)

        link_older = None
        link_newer = None

        if total_event_count == 1:
            page += "<h2>1 EPICS Event</h2>"
        elif total_event_count <= items_per_page:
            page += "<h2>%d EPICS Events</h2>" % total_event_count
        else:
            page += "<h2>Displaying %d to %d of %d EPICS Events</h2>" % (
                l,
                u,
                total_event_count,
            )

            if total_event_count > items_per_page:
                if offset == 0:
                    offset_newer = None
                else:
                    offset_newer = offset - items_per_page
                    if offset_newer < 0:
                        offset_newer = 0

                offset_older = offset + items_per_page
                if offset_older >= total_event_count:
                    offset_older = None

            if offset_older is not None:
                link_older = self.make_link_epics_events(
                    display="<< Older", timestamp=timestamp, offset=offset_older
                )
            if offset_newer is not None:
                link_newer = self.make_link_epics_events(
                    display="Newer >>", timestamp=timestamp, offset=offset_newer
                )

        try:
            result = self._dataman.get_epics_events(
                timestamp, offset=offset, count=items_per_page
            )

            if total_event_count == 1:
                event_count = "1 Event"
            elif total_event_count <= items_per_page:
                event_count = "%d Events" % total_event_count
            else:
                event_count = "%d to %d of %d Events" % (
                    l,
                    u,
                    total_event_count,
                )

            page += "<p><hr>%s &nbsp &nbsp %s &nbsp &nbsp &nbsp &nbsp" % (
                t.get_string_MDY(t=timestamp),
                event_count,
            )

            page += self.make_link_epics_events(
                display="<< Previous Day", timestamp=timestamp - sec_per_day
            )
            if not t.is_today(timestamp):
                page += (
                    "&nbsp &nbsp &nbsp"
                    + self.make_link_epics_events(display="Today")
                    + "&nbsp &nbsp &nbsp"
                    + self.make_link_epics_events(
                        display="Next Day >> ",
                        timestamp=timestamp + sec_per_day,
                    )
                )

            page += "<hr><p>"

            if link_older or link_newer:
                if link_older:
                    page += "%s &nbsp" % link_older
                else:
                    page += "<< Older &nbsp"

                if link_newer:
                    page += "%s" % link_newer
                else:
                    page += "Newer >>"

            EVENT_BEACON_HTML_BLOCK = """
            <div class="%s">
                <div><span class="eventname">%s: &nbsp &nbsp </span>
                     <span class="iocname">%s</span> 
                     &nbsp &nbsp <tt>(%s port %s)</tt>
                </div><p>
                <div class="indent50">
                    <span class="column_title">Name:    </span><span class="app_detail">%s<br></span>
                    <span class="column_title">Cmd:     </span><span class="app_detail">%s<br></span>
                    <span class="column_title">Dir:     </span><span class="app_detail">%s<br></span>
                    <span class="column_title">Age (est.): </span><span class="app_detail">%s<br></span>
                </div>
                <p>
                <div class="indent50">
                    <span class="timestamp">%s &nbsp &nbsp &nbsp (ID: %d)
                     
                    </span>
                </div>
            </div>
            """

            for item in result:
                event_id = int(item[0])
                type = item[1]
                port = item[2]
                ip_addr = item[3]
                ioc_name = item[4]
                app_name = item[5]
                app_cmd = item[6]
                app_cwd = item[7]
                event_time = item[8]
                status = item[9]
                seq = int(item[10])

                ioc_name = ioc_name.replace(".clsi.ca", "")
                ioc_name = ioc_name.replace(".CLSI.CA", "")

                ioc_link = self.make_link_ioc(ip_addr, ioc_name)

                event_name = EVENT_DISPLAY.get(type, "UNKNOWN EVENT")

                est_age = "%s (seq: %d)" % (
                    beacon_estimated_age_str(None, seq=seq),
                    seq,
                )

                if type in [
                    EVENT.BEACON_NEW,
                    EVENT.BEACON_RESUMED,
                    EVENT.BEACON_DETECTED,
                ]:
                    color = "good"
                else:
                    color = "bad"

                if app_name is None:
                    app_name = "Unknown"
                if app_cwd is None:
                    app_cwd = "Unknown"
                if app_cmd is None:
                    app_cmd = "Unknown"

                if status != EVENT_STATUS.IDENTIFIED:
                    if app_cwd == "Unknown":
                        app_cwd = (
                            '<span style="color:red">Lookup Pending: </span>%s  (heartbeat active?)'
                            % app_cwd
                        )
                    else:
                        app_cwd = (
                            '<span style="color:red">Confirmation Pending: </span>%s'
                            % app_cwd
                        )

                    if app_cmd == "Unknown":
                        app_cmd = (
                            '<span style="color:red">Lookup Pending: </span>%s  (heartbeat active?)'
                            % app_cmd
                        )
                    else:
                        app_cmd = (
                            '<span style="color:red">Confirmation Pending: </span>%s'
                            % app_cmd
                        )

                    if app_name == "Unknown":
                        app_name = (
                            '<span style="color:red">Lookup Pending: </span>%s  (heartbeat active?)'
                            % app_name
                        )
                    else:
                        app_name = (
                            '<span style="color:red">Confirmation Pending: </span>%s'
                            % app_name
                        )

                time_str = t.get_string_time_of_day(t=event_time)
                page += EVENT_BEACON_HTML_BLOCK % (
                    color,
                    event_name,
                    ioc_link,
                    ip_addr,
                    port,
                    app_name,
                    app_cmd,
                    app_cwd,
                    est_age,
                    time_str,
                    event_id,
                )

            if link_older or link_newer:
                if link_older:
                    page += "%s &nbsp" % link_older
                else:
                    page += "<< Older &nbsp"

                if link_newer:
                    page += "%s" % link_newer
                else:
                    page += "Newer >>"

        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_groups(self):

        log.debug("called")
        page = self.make_page_header(css=CSS.IOC)

        try:
            page += (
                "<h2>IOC Groups</h2>"
                "<p><TABLE>"
                "<tr>"
                "<th>Name</th>"
                '<th class="center">IOCs</th>'
                "</tr>\n"
            )

            group_list = self._dataman.get_group_list(counts=True)

            for item in group_list:
                group_id = item[0]
                group_name = item[1]
                ioc_count = item[2]
                group_link = self.make_link_group_grid(group_id, group_name)

                page += (
                    "<tr>\n"
                    + "<td>%s</td>\n" % group_link
                    + '<td class="center">%d</td>\n' % ioc_count
                    + "</tr>\n"
                )

            page += (
                "</TABLE>\n"
                '<FORM action="group_create" method="GET">\n'
                "<h2>Create new Group</h2><p>\n"
                '<input type="text" name="group_name"><p>\n'
                '<button type="submit">Create</button>\n'
                "</FORM>\n"
            )

        except Exception as err:
            page += self.add_traceback(err)

        page += "</BODY></HTML>"
        return page

    def page_group_edit(self):

        log.debug("called")
        try:
            group_id = int(bottle.request.query.get("group_id"))
            log.debug("called; group_id: %r", group_id)

            cmd = {}
            for key, value in bottle.request.query.items():
                cmd[key] = value
                log.debug("key: %s value: %r", key, value)

            redirect_to_group = True

            if "ioc_id" in cmd:
                ioc_id, _ = self.get_ioc_id_and_addr(cmd.get("ioc_id"))
            else:
                ioc_id = None

            if "add" in cmd:
                self._dataman.group_member_add(group_id, ioc_id)

            elif "remove" in cmd:
                self._dataman.group_member_remove(group_id, ioc_id)

            elif "delete" in cmd:
                redirect_to_group = False
                log.debug("Want to delete")

            if cmd.get("redirect", "") == "redirect_ioc":
                url = self.make_link_ioc(ioc_id, None, url_only=True)

            elif redirect_to_group:
                url = "%s/group?group_id=%d" % (self.make_link_base(), group_id)
            else:
                url = "%s/group_delete_ask?group_id=%d" % (
                    self.make_link_base(),
                    group_id,
                )

        except Exception as err:
            page = (
                self.make_page_header(css=CSS.IOC)
                + self.add_traceback(err)
                + "</body></html>"
            )
            return page

        bottle.redirect(url)

    def page_group_delete_ask(self):
        page = self.make_page_header(css=CSS.IOC)

        try:
            group_id = int(bottle.request.query.get("group_id"))
            group_name = self._dataman.get_group_name(group_id)
            page += "<h2>Delete group: %s?</h2>" % group_name

            link_no = self.make_link_groups("NO")
            link_yes = self.make_link_group_delete_yes(group_id, "YES")
            page += "<p>%s &nbsp &nbsp %s" % (link_yes, link_no)

        except Exception:
            page += self.add_traceback()

        page += "</body></html>"
        return page

    def page_group_delete_yes(self):

        group_id = int(bottle.request.query.get("group_id"))
        log.debug("called; group_id: %d", group_id)

        self._dataman.group_delete(group_id)
        url = self.make_link_groups(None, url_only=True)
        bottle.redirect(url)

    def page_group_create(self):

        try:
            group_name = bottle.request.query.group_name
            group_id = self._dataman.group_create(group_name)

            # URL to the new group
            url = self.make_link_group_grid(group_id, None, url_only=True)

        except Exception as err:
            page = (
                self.make_page_header(css=CSS.IOC)
                + self.add_traceback(err)
                + "</body></html>"
            )
            return page

        bottle.redirect(url)

    def page_group_select(self):
        group_id = bottle.request.query.get("group_id")
        url = self.make_link_group_grid(group_id, None, url_only=True)
        bottle.redirect(url)

    def page_ioc_grid(self):

        page = self.make_page_header(
            css=CSS.IOC_GRID, auto_refresh_sec=30, blinker=True
        )

        try:

            group_id = 0

            for k, v in bottle.request.query.items():
                if k == "group_id":
                    group_id = int(v)

            # Get the IOCs for this page
            iocs = self._dataman.get_ioc_summary(group_id)

            if len(iocs) == 1:
                msg = "1 IOC"
            else:
                msg = "%d IOCs" % len(iocs)

            group_name = self._dataman.get_group_name(group_id)
            group_list = self._dataman.get_group_list()

            url_group_select = "%s/group_select" % self.make_link_base()

            try:
                page += (
                    '<form action="%s" method="GET">' % url_group_select
                    + '<span class="title">%s</span>' % group_name
                    + "&nbsp &nbsp &nbsp%s" % msg
                )
                try:
                    page += '&nbsp &nbsp &nbsp <select name="group_id" onchange="this.form.submit()">'
                    for item in group_list:
                        gid = item[0]
                        name = item[1]
                        selected = "selected" if gid == group_id else ""
                        page += '<option value="%s" %s>%s</option>' % (
                            gid,
                            selected,
                            name,
                        )
                finally:
                    page += "</select>"
            finally:
                page += "</form>"

            page += "<table><p>\n"
            max_cols = 12
            col_count = 0
            row_count = 0

            for item in iocs:
                ioc_id = item[1]
                display_name = item[2]
                tooltip = item[3]
                host_name = item[4]
                ip_addr = item[5]
                color = item[6]
                color2 = item[7]
                flash = item[8]

                if col_count == 0:
                    if row_count > 0:
                        page += "</tr>\n"
                    page += "<tr>\n"
                    row_count += 1

                col_count += 1
                if col_count == max_cols:
                    col_count = 0

                if flash:
                    display_name = '<div class="blink">%s</div>' % display_name

                display_name = (
                    '<div class="tooltip">%s<span class="tooltiptext">%s</span></div>'
                    % (
                        display_name,
                        "<b>%s</b> (%s)<br>%s" % (host_name, ip_addr, tooltip),
                    )
                )

                # Override the color if the gateway is not active so that we notice it.
                if not self._dataman.is_gateway_alive():
                    color = COLOR.WHITE

                ioc_link = self.make_link_ioc(ioc_id, display_name)
                page += '<td bgcolor="%s">%s</td>\n' % (color, ioc_link)
                page += '<td bgcolor="%s"></td>\n' % (color2)

            if col_count:
                page += "<tr>\n"

            page += "</table><p>\n"

            if group_id == GROUP.ACTIVE:
                page += (
                    '<span class="key_width_wide" style="background-color:%s;">OK</span>\n'
                    % COLOR.GREEN
                    + "&nbsp"
                    + '<span class="key_width_wide" style="background-color:%s;">OK (some APPS ignored)</span>\n'
                    % COLOR.DARK_GREEN
                    + "&nbsp"
                    + '<span class="key_width_wide" style="background-color:%s;"><span class="blink">Un-ACK\'d Logs</span></span>\n'
                    % COLOR.DARK_GREEN
                    + "&nbsp"
                    + '<span class="key_width_wide" style="background-color:%s">ERRORS</span>\n'
                    % COLOR.RED
                    + "&nbsp"
                )
            else:
                page += (
                    '<span class="key_width" style="background-color:%s;">OK</span>\n'
                    % COLOR.GREEN
                    + "&nbsp"
                    + '<span class="key_width" style="background-color:%s">WARNINGS</span>\n'
                    % COLOR.YELLOW
                    + "&nbsp"
                    + '<span class="key_width" style="background-color:%s">ERRORS</span>\n'
                    % COLOR.RED
                    + "&nbsp"
                    + '<span class="key_width" style="background-color:%s">MAINT.</span>\n'
                    % COLOR.ORANGE
                    + "&nbsp"
                    + '<span class="key_width" style="background-color:%s">TEST/DEV</span>\n'
                    % COLOR.BLUE
                    + "&nbsp"
                    + '<span class="key_width" style="background-color:%s">DECOM.</span>\n'
                    % COLOR.GRAY
                )

            if group_id >= 1000:

                all_iocs = self._dataman.get_ioc_id_list()

                ioc_add_list = []
                ioc_del_list = []

                have_ioc_list = [item[0] for item in iocs]

                for ioc_id in all_iocs:
                    if ioc_id in have_ioc_list:
                        ioc_del_list.append()

                url_edit_ioc = "%s/group_edit" % self.make_link_base()

                page += '<p><FORM action="%s" method="GET">' % url_edit_ioc
                page += (
                    '<input type="hidden" name="group_id" value="%d">'
                    % group_id
                )
                if len(ioc_add_list):
                    page += 'Add IOC: <select name="add_ioc_key">'
                    for item in ioc_add_list:
                        page += '<option value="%s">%s</option>' % (
                            item[1],
                            item[3],
                        )
                    page += (
                        "</select>"
                        '&nbsp <button type="submit" name="add">Add</button>'
                        "&nbsp &nbsp &nbsp"
                    )
                if len(ioc_del_list):
                    page += 'Remove IOC: <select name="del_ioc_key">'
                    for item in ioc_del_list:
                        page += '<option value="%s">%s</option>' % (
                            item[1],
                            item[3],
                        )
                    page += (
                        "</select>"
                        '&nbsp <button type="submit" name="remove">Remove</button>'
                        "&nbsp &nbsp &nbsp"
                    )
                page += 'Delete Group: <button type="submit" name="delete">Delete</button>'
                page += "</FORM>"

        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"

        return page

    def get_ioc_id_and_addr(self, key):
        """
        Allow the key to be the ioc_id, ip_addr, or hostname
        """
        log.debug("called; kay: %r", key)
        try:
            ioc_id = int(key)
            ip_addr = get_ip_addr(ioc_id)
            log.debug("RETUNING 1, %s, %s", ioc_id, ip_addr)

            return ioc_id, ip_addr
        except:
            pass

        try:
            ip_addr = get_ip_addr(
                key
            )  # This will return the hostname if inout is hostname!!!
            ioc_id = get_ip_key(ip_addr)
            if not ioc_id:
                raise ValueError
            log.debug("RETUNING 2, %s, %s", ioc_id, ip_addr)
            return ioc_id, ip_addr
        except:
            pass

        try:
            # See if perhaps the key is the IOC name
            ip_addr = socket.gethostbyname(key)
            ioc_id = get_ip_key(ip_addr)
            log.debug("RETUNING 3, %s, %s", ioc_id, ip_addr)

            return ioc_id, ip_addr
        except:
            pass

        raise ValueError("Failed to get ioc_id for %r" % key)

    def page_ioc_desc_submit(self, key):
        log.debug("called; key: %s", key)
        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)

        try:
            update_flag = False
            description = None

            for k, v in bottle.request.forms.items():
                if k == "cancel":
                    break
                if k == "submit":
                    update_flag = True
                if k == "text":
                    description = v

            if update_flag:

                # There are no quotes around the text but there definitely are
                # when it is put into the database
                self._dataman.set_ioc_description(ioc_id, description)

        except Exception as err:
            page = (
                self.make_page_header(css=CSS.IOC)
                + self.add_traceback(err)
                + "</body></html>"
            )
            return page

        return bottle.redirect(self.make_link_ioc(ioc_id, None, url_only=True))

    def page_ioc_forget(self, key):
        log.debug("called; key: %s", key)

        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)
        page = self.make_page_header(css=CSS.IOC)

        try:
            host_name = get_host_name(ioc_id)
            page += "<h2>Forget IOC: %s ? (%s) </h2>\n" % (host_name, ip_addr)
            description = self._dataman.get_ioc_description(ioc_id)

            if not description:
                page += "No Description available"
            else:
                page += "%s" % description

            page += (
                "<p>This action will delete all stored group memberships"
                " and expected applications.<br>"
            )
            page += (
                "The IOC will automatically be re-added to the database if"
                " an EPICS beacon is detected.<br><br>"
            )

            try:
                url_change = "%s/ioc_forget_submit/%d" % (
                    self.make_link_base(),
                    ioc_id,
                )
                page += (
                    '<form action="%s" method="POST">\n'
                    '<button type="submit" name="submit" value="Submit">Forget IOC</button>\n'
                    '<button type="submit" name="cancel" value="Cancel">Cancel</button>\n'
                    % url_change
                )
            finally:
                page += "</form>\n"

        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"

        return page

    def page_ioc_forget_submit(self, key):
        log.debug("called; key: %s", key)

        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)
        delete = False

        try:

            for k, v in bottle.request.forms.items():
                if k == "cancel":
                    break
                if k == "submit":
                    delete = True

            if delete:
                self._dataman.forget_ioc(ioc_id)

        except Exception as err:
            page = (
                self.make_page_header(css=CSS.IOC)
                + self.add_traceback(err)
                + "</body></html>"
            )
            return page

        if delete:
            # Show ACTIVE IOCs if IOC was deleted
            url = self.make_link_group_grid(GROUP.ACTIVE, None, url_only=True)
        else:
            # Go back to IOC page if IOC was not deleted
            url = self.make_link_ioc(ioc_id, None, url_only=True)

        return bottle.redirect(url)

    def page_ioc_log_add(self, key):
        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)
        page = self.make_page_header(css=CSS.IOC)

        try:
            host_name = get_host_name(ioc_id)
            page += (
                '<h2>Add Log: %s (%s) <span class="my_monospace">%d</span></h2>\n'
                % (host_name, ip_addr, ioc_id)
            )

            try:
                url_change = "%s/ioc_log_submit/%d" % (
                    self.make_link_base(),
                    ioc_id,
                )
                page += (
                    '<form action="%s" method="POST">\n'
                    '<input type="text" id="text" name="text" maxlength=200 size="100"><br><br>\n'
                    '<input type="checkbox" id="need_ack" name="need_ack" value=""need_ack>\n'
                    '<label for="check_ack">Require ACK</label><br><br>\n'
                    '<button type="submit" name="submit" value="Submit">Add</button>\n'
                    '<button type="submit" name="cancel" value="Cancel">Cancel</button>\n'
                    % url_change
                )
            finally:
                page += "</form>\n"

        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"
        return page

    def page_ioc_desc_edit(self, key):
        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)
        page = self.make_page_header(css=CSS.IOC)

        try:
            host_name = get_host_name(ioc_id)
            page += (
                '<h2>Edit Description: %s (%s) <span class="my_monospace">%d</span></h2>\n'
                % (host_name, ip_addr, ioc_id)
            )
            description = self._dataman.get_ioc_description(ioc_id)

            try:
                url_change = "%s/ioc_desc_submit/%d" % (
                    self.make_link_base(),
                    ioc_id,
                )
                page += (
                    '<form action="%s" method="POST">\n'
                    '<textarea id="text" name="text" rows="20" cols="120">%s</textarea><br><br>\n'
                    '<button type="submit" name="submit" value="Submit">Update IOC Description</button>\n'
                    '<button type="submit" name="cancel" value="Cancel">Cancel</button>\n'
                    % (url_change, description)
                )
            finally:
                page += "</form>\n"

        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"

        return page

    def database_to_display(self, input):
        """
        This converts a string from database formant to display format.
        This could be very tricky.  Different browsers might put in different
        newlines etc.
        """
        input = input.replace("\n", "")
        input = input.strip(" ")
        input = input.strip("\r")
        input = input.replace("\r", "<br>")
        input = input.strip(" ")

        return input

    def page_ioc_settings_submit(self, key):
        log.debug("called; key: %r", key)
        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)

        actions = [item[0] for item in IOC_OPTION_BUTTON_LIST]
        action = None
        option_list = []

        try:
            for k, v in bottle.request.forms.items():
                code = int(k)
                if code in actions:
                    action = code
                else:
                    option_list.append(code)

            url = self.make_link_ioc(ioc_id, None, url_only=True)

            if action == IOC_OPTION_BUTTON.APPLY:
                self._dataman.set_ioc_options(ioc_id, option_list)
            elif action == IOC_OPTION_BUTTON.DECOMISSION:
                raise ValueError("Decomission not implemented")
            elif action == IOC_OPTION_BUTTON.FORGET:
                url = self.make_link_forget_ioc(ioc_id, url_only=True)

        except Exception as err:
            page = (
                self.make_page_header(css=CSS.IOC)
                + self.add_traceback(err)
                + "</body></html>"
            )
            return page

        # Must be done outside or except
        return bottle.redirect(url)

    def page_ioc(self, key):
        """
        Display IOC parameters
        """
        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)
        page = self.make_page_header(css=CSS.IOC)
        cur_time = int(time.time())

        try:
            host_name = get_host_name(ioc_id)

            page += (
                '<div><span class="my_page_title">%s</span>\n' % (host_name)
                + '<span class="my_monospace">(%s - %s)</span>\n'
                % (
                    ip_addr,
                    ioc_id,
                )
                + "</div>"
            )

            description = self._dataman.get_ioc_description(ioc_id)
            description = self.database_to_display(description)
            if not description:
                description = "Add Host Description"

            page += "%s <small>%s</small>\n" % (
                description,
                self.make_link("/ioc_desc_edit/%d" % ioc_id, "edit"),
            )

            # ------------------------------------------------------------------
            # IOC Settings
            # ------------------------------------------------------------------
            url_change = "%s/ioc_settings_submit/%d" % (
                self.make_link_base(),
                ioc_id,
            )
            url_help = self.make_link("/ioc_settings_help/%d" % ioc_id, "help")

            have_options = self._dataman.get_ioc_options(ioc_id)

            page += "<h3>Host Settings&nbsp<small>%s</small></h3>\n" % url_help

            page += '<form action="%s" method="POST">\n' % url_change

            if have_options is None:
                have_options = {}

            for option in IOC_OPTION_LIST:
                id = option[0]
                display = option[1]
                if have_options:
                    value = have_options.get(id, 0)
                    checked = "checked" if value else ""
                else:
                    checked = ""
                check_box = (
                    '<input type="checkbox" id=%d name=%d value=%d %s>\n'
                    % (
                        id,
                        id,
                        id,
                        checked,
                    )
                )
                label = "<label for=%d>%s</label>\n" % (id, display)
                page += check_box
                page += label

            page += "<p>\n"
            for button in IOC_OPTION_BUTTON_LIST:
                id = button[0]
                display = button[1]
                page += (
                    '<button type="submit" name=%d value=%d>%s</button>\n'
                    % (
                        id,
                        id,
                        display,
                    )
                )

            page += "</form>\n"

            log_modes = [
                (IOC_OPTION.HIDE_FROM_ACTIVE, COLOR.RED, "This Host is Hidden"),
                (
                    IOC_OPTION.MONITOR_APP_COUNT,
                    COLOR.YELLOW,
                    "This Host is in Monitor App Count Mode",
                ),
                (
                    IOC_OPTION.MAINTENANCE,
                    COLOR.ORANGE,
                    "This Host is in Maintenance Mode",
                ),
                (
                    IOC_OPTION.DEVELOPMENT,
                    COLOR.BLUE,
                    "This Host is in Development Mode",
                ),
                (
                    IOC_OPTION.DECOMISSIONED,
                    COLOR.GRAY,
                    "This Host is in Decomissioned",
                ),
            ]

            for item in log_modes:
                option_id = item[0]
                color = item[1]
                msg = item[2]

                if have_options.get(option_id):
                    page += (
                        '<span  style="background-color:%s">%s</span><br>\n'
                        % (
                            color,
                            msg,
                        )
                    )

            # Latest requests from this IOC
            latest_requests_link = self.make_link_pv_list(
                ioc_id=ioc_id, kind=PV_LIST.LATEST_IOC, name="Latest Requests"
            )

            if self._dataman.has_linuxmon(host_name):
                linuxmon_pvs_link = self.make_link_linuxmon_ioc(
                    ioc_id, host_name
                )
                page += "<h3>%s&nbsp&nbsp%s</h3>" % (
                    latest_requests_link,
                    linuxmon_pvs_link,
                )
            else:
                page += (
                    "<h3>%s&nbsp&nbspLinuxMonitor not detected</h3>"
                    % latest_requests_link
                )

            # ------------------------------------------------------------------
            # PV Ports.  Get this early on so that we can make links to ports
            # in list lost of app/beacons/heartbeats
            # ------------------------------------------------------------------

            ports_pvs = self._dataman.get_ioc_mapped_ports(ioc_id)
            ports_pvs = sorted(ports_pvs)

            total_pvs = 0
            for item in ports_pvs:
                total_pvs += item[1]

            # ------------------------------------------------------------------
            # Monitored applications
            #
            # Note: There are applications that do not have beacons.  E.g.,
            # the pure python heartbeats
            # ------------------------------------------------------------------

            url_api_beacons = (
                self.make_link("/api/v1.0/beacons?host=%d", "api") % ioc_id
            )
            page += (
                "<h3>Monitored Applications&nbsp<small>%s</small></h3>"
                % url_api_beacons
            )
            beacons = self._dataman.get_ioc_beacons_display(ioc_id)

            for item in beacons:
                log.info("THIS IS A BEACON: %r", item)

            # Get the actual list of applications
            app_dict = self._dataman.get_ioc_app_dict(ioc_id)

            submit_keys = []

            url_change = "%s/ioc_apps_submit/%d" % (
                self.make_link_base(),
                ioc_id,
            )
            page += '<form action="%s" method="POST">' % url_change

            page += "<p><table>"
            page += (
                "<tr>\n"
                "<th>Critical</th>\n"
                "<th>Running</th>\n"
                "<th>Forget</th>\n"
                "<th>TCP Port</th>\n"
                "<th>Last Seen</th>\n"
                "<th>Up Time</th>\n"
                "<th>App ID</th>\n"
                "<th>Command</th>\n"
                "<th>Directory</th>\n"
                "</tr>"
            )

            checkbox_id = 10000
            have_id = 20000
            forget_id = 30000

            # The actual items that are displayed are beacons
            for item in beacons:
                port = item[0]
                app_id = item[1]
                critical = item[2]
                running = item[3]
                color = item[4]
                ##                uptime_sec = item[5]
                ##                last_seen_sec = item[6]
                ##                measure_time = item[7]

                result = self._dataman.get_app_times(
                    ioc_id, port, app_id
                )
                if result == None:
                    uptime_sec = None
                    last_seen_sec = None
                else:
                    uptime_sec, last_seen_sec = result

                if port is None:
                    port_str = ""
                else:
                    port_str = "%d" % port

                if app_id is None:
                    app_id_str = ""
                    cmd = "Unknown"
                    cwd = "Unknown"
                else:
                    app_id_str = "%d" % app_id
                    app_data = app_dict.get(app_id, {})
                    cmd = app_data.get(KEY.CMD, "Unknown")
                    cwd = app_data.get(KEY.CWD, "Unknown")

                sk = "%s-%s" % (port_str, app_id_str)
                submit_keys.append(sk)

                if not running and not critical:
                    forget_check = (
                        '<input type="checkbox" id=%s name=%s value=%s>'
                        % (
                            forget_id,
                            forget_id,
                            sk,
                        )
                    )
                    forget_id += 1
                else:
                    forget_check = ""

                if self._dataman.is_ready():
                    if critical:
                        critical_item = (
                            '<input type="checkbox" id=%s name=%s value=%s checked>'
                            % (checkbox_id, checkbox_id, sk)
                        )
                        checkbox_id += 1
                    else:
                        critical_item = (
                            '<input type="checkbox" id=%s name=%s value=%s>'
                            % (checkbox_id, checkbox_id, sk)
                        )
                        checkbox_id += 1
                else:
                    if critical:
                        critical_item = "Yes"
                    else:
                        critical_item = "No"

                if running:
                    if color:
                        running_item = (
                            '<span style="background-color:%s">YES</span>'
                            % color
                        )
                    else:
                        running_item = "YES"
                else:
                    if color:
                        running_item = (
                            '<span style="background-color:%s">NO</span>'
                            % color
                        )
                    else:
                        running_item = "NO"

                try:
                    last_seen_sec = cur_time - int(last_seen_sec)
                    last_seen_str = make_last_seen_str(last_seen_sec)
                except:
                    last_seen_str = ""

                try:
                    if running:
                        uptime_sec += last_seen_sec
                    uptime_str = my_uptime_str(int(uptime_sec))

                except:
                    uptime_str = ""

                page += (
                    "<tr>"
                    + "<td>%s</td>\n" % critical_item
                    + "<td>%s</td>\n" % running_item
                    + "<td>%s</td>\n" % forget_check
                    + "<td>%s</td>\n" % port_str
                    + "<td>%s</td>\n" % last_seen_str
                    + "<td>%s</td>\n" % uptime_str
                    + "<td>%s</td>\n" % app_id_str
                    + "<td>%s</td>\n" % cmd
                    + "<td>%s</td>\n" % cwd
                    + "</tr>\n"
                )

            page += "</table><p>\n"

            if self._dataman.is_ready():
                for sk in submit_keys:
                    page += '<input type="hidden" id=%s name=%s value=%s>\n' % (
                        have_id,
                        have_id,
                        sk,
                    )
                    have_id += 1

                page += '<button type="submit">Apply Application Settings</button>\n'

                # This refresh button does the same thing as the Apply Application
                # Settings button, which is cause the IOC to get processed
                page += '<button type="submit">Refresh</button>\n'

            page += "</form>\n"

            # ------------------------------------------------------------------
            # Show procserv data
            # ------------------------------------------------------------------
            procserv_data = self._dataman.get_procserv_data(ioc_id)
            if procserv_data is not None:
                if len(procserv_data) == 1:
                    page += "<h3>1 procServ Process Detected</h3>\n"
                else:
                    page += "<h3>%d procServ Processes Detected</h3>\n" % len(
                        procserv_data
                    )

                page += "<p><table>\n"
                page += (
                    "<tr>\n"
                    + "<th>Telnet Port</th>\n"
                    + "<th>Command</th>\n"
                    + "</tr>\n"
                )

                for item in procserv_data:

                    port = item[0]
                    cmd = item[1]

                    page += (
                        "<tr>"
                        + "<td>%s</td>" % repr(port).strip("'")
                        + "<td>%s</td>" % cmd
                        + "</tr>\n"
                    )
                page += "</table><p>\n"

            # ------------------------------------------------------------------
            # Heartbeat data
            # ------------------------------------------------------------------

            heartbeats = self._dataman.get_heartbeat_list_for_ioc(ioc_id)

            url_api_heartbeats = (
                self.make_link("/api/v1.0/heartbeats?host=%d", "api") % ioc_id
            )
            grammer = "Heartbeat" if len(heartbeats) == 1 else "Heartbeats"
            page += "<h3>%d %s Detected&nbsp<small>%s</small></h3>\n" % (
                len(heartbeats),
                grammer,
                url_api_heartbeats,
            )

            if len(heartbeats) > 0:
                page += "<table>\n"
                page += (
                    "<tr>\n"
                    "<th>Index</th>\n"
                    "<th>TCP Port</th>\n"
                    "<th>PVs</th>\n"
                    "<th>Up Time</th>\n"
                    "<th>Running</th>\n"
                    "<th>App ID</th>\n"
                    "<th>Forget</th>\n"
                    "</tr>\n"
                )

                for i, item in enumerate(heartbeats):
                    ioc_id = item[0]
                    port = item[1]
                    heartbeat = item[2]
                    app_id = item[3]

                    forget_link = self.make_link_forget_heartbeat(ioc_id, port)

                    data = heartbeat.get(HEARTBEAT_KEY.DATA)
                    lost = heartbeat.get(HEARTBEAT_KEY.LOST, False)
                    pvs = int(heartbeat.get(HEARTBEAT_KEY.PV_COUNT, 0))
                    last_time = heartbeat.get(HEARTBEAT_KEY.TIME)

                    # Compute the extra uptime
                    if lost:
                        extra_time = 0
                        lost_str = "<span>NO</span>"
                    else:
                        extra_time = cur_time - last_time
                        lost_str = "<span>YES</span>"

                    uptime_sec = int(data.get(HEARTBEAT_KEY.UP, 0)) + extra_time
                    uptime_str = my_uptime_str(uptime_sec)

                    index_link = self.make_link_heartbeat(
                        ioc_id, port, display="%d" % i
                    )

                    page += (
                        "<tr>\n"
                        + "<td>%s</td>\n" % index_link
                        + "<td>%d</td>\n" % port
                        + "<td>%d</td>\n" % pvs
                        + "<td>%s</td>\n" % uptime_str
                        + "<td>%s</td>\n" % lost_str
                        + "<td>%d</td>\n" % app_id
                        + "<td>%s</td>\n" % forget_link
                        + "</tr>\n"
                    )

                page += "</table>\n"

            # ------------------------------------------------------------------
            # Table of ports with PVs
            # ------------------------------------------------------------------
            if total_pvs == 0:
                page += (
                    "<p><h3>No PVs detected on this IOC. Check back later</h3>"
                )
            else:
                temp1 = "PV" if total_pvs == 1 else "PVs"
                temp2 = "port" if len(ports_pvs) == 1 else "ports"
                page += "<p><h3>%d %s detected on %d %s</h3>" % (
                    total_pvs,
                    temp1,
                    len(ports_pvs),
                    temp2,
                )

                page += "<p><table>"

                max_cols = 10
                col_count = 0
                row_count = 0

                for item in ports_pvs:
                    if col_count == 0:
                        if row_count > 0:
                            page += "</tr>"
                        page += "<tr>"
                        row_count += 1

                    col_count += 1
                    if col_count == max_cols:
                        col_count = 0

                    port = item[0]
                    count = item[1]
                    name = "%d (%d)" % (port, count)
                    link = self.make_link_pv_list(
                        kind="port", key=port, ioc_id=ioc_id, name=name
                    )
                    page += "<td>%s</td>" % link

                page += "</table>"

            # ------------------------------------------------------------------
            # groups
            # ------------------------------------------------------------------
            group_list = self._dataman.get_memberships_by_ioc(ioc_id)
            group_list_absent = self._dataman.get_memberships_by_ioc(
                ioc_id, not_member=True
            )

            if not group_list:
                page += "<p><h3>This IOC does not belong to any groups</h3>"
            else:
                page += (
                    "<p><h3>This IOC belongs to the following groups:</h3>"
                    "<TABLE>"
                    "<tr>"
                    "<th>Name</th>"
                    "<th>Remove<th>"
                    "</tr>"
                )

                for item in group_list:
                    name = item[0]
                    link = self.make_link_group_grid(item[1], name)
                    remove_link = self.make_link_group_ioc_remove(
                        item[1], ip_addr, "redirect_ioc", "X"
                    )
                    page += (
                        "<tr>"
                        + "<td>%s</td>" % link
                        + "<td>%s</td>" % remove_link
                        + "</tr>"
                    )

                page += "</table>"

            if len(group_list_absent):

                url_edit_ioc = "%s/group_edit" % self.make_link_base()

                page += (
                    '<p><FORM action="%s" method="GET">' % url_edit_ioc
                    + '<input type="hidden" name="ioc_id" value="%s">'
                    % get_ip_key(ip_addr)
                    + '<input type="hidden" name="redirect" value="redirect_ioc">'
                    + 'Add IOC to group: <select name="group_id">'
                )

                for item in group_list_absent:
                    display = item[0]
                    page += '<option value="%s">%s</option>' % (
                        item[1],
                        display,
                    )

                page += (
                    "</select>"
                    '&nbsp <button type="submit" name="add">Add</button>'
                    "</form>"
                )

            page += "<p>%s" % self.make_link_ping_ioc(ip_addr)

            # ------------------------------------------------------------------
            # Logs - For now show only the latest logs
            # ------------------------------------------------------------------

            total_logs = self._dataman.get_ioc_log_count(ioc_id)
            logs = self._dataman.get_ioc_logs(
                ioc_id, offset=0, count=SETTINGS.MAX_LOGS_IOC_PAGE
            )

            add_url = self.make_link("/ioc_log_add/%s" % ioc_id, "new")

            url_change = "%s/ioc_logs_ack/%d" % (self.make_link_base(), ioc_id)

            page += (
                "<h3>Latest %d of %d Total Logs&nbsp<small>%s</small></h3>\n"
                % (
                    len(logs),
                    total_logs,
                    add_url,
                )
                + '<FORM action="%s" method="POST">\n' % url_change
                + '<button type="submit" name="ack">Forget/Acknowledge Logs</button>\n'
                "<p><table>\n"
                "<tr>\n"
                "<th>Forget</th>\n"
                "<th>ACKed</th>\n"
                "<th>Date</th>\n"
                "<th>Time</th>\n"
                "<th>Log Message</th>\n"
                "</tr>"
            )

            for l in logs:
                log_id = l[0]
                ioc_id = l[1]
                time_str = l[2]
                date_str = l[3]
                ack_str = l[4]
                color = l[5]
                msg = l[6]

                if color is not None:
                    ack_str = '<span style="background-color:%s">%s</span>' % (
                        color,
                        ack_str,
                    )

                forget_check = (
                    '<input type="checkbox" id=%d name=%d value=%d>'
                    % (
                        log_id,
                        log_id,
                        log_id,
                    )
                )

                page += (
                    "<tr>"
                    + "<td>%s</td>\n" % forget_check
                    + "<td>%s</td>\n" % ack_str
                    + "<td>%s</td>\n" % date_str
                    + "<td>%s</td>\n" % time_str
                    + "<td>%s</td>\n" % msg
                    + "</tr>\n"
                )
            page += (
                "</table><p>\n"
                '<button type="submit" name="ack">Forget/Acknowledge Logs</button>\n'
                "</FORM>\n"
            )
        except Exception as err:
            page += self.add_traceback(err)

        page += "</body></html>"

        return page

    def page_ioc_logs_ack(self, key):
        log.debug("called; key: %r", key)
        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)

        try:
            log_id_list = []
            for k, v in bottle.request.forms.items():
                if k == "ack":
                    continue
                log_id_list.append(int(k))

            self._dataman.delete_logs(ioc_id, log_id_list)
            self._dataman.ack_ioc_logs(ioc_id)

        except Exception as err:
            page = (
                self.make_page_header(css=CSS.IOC)
                + self.add_traceback(err)
                + "</body></html>"
            )
            return page

        return bottle.redirect(self.make_link_ioc(ioc_id, None, url_only=True))

    def page_ioc_log_submit(self, key):
        log.debug("called; key: %r", key)
        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)

        try:
            cancel = False
            need_ack = False
            text = None
            for k, v in bottle.request.forms.items():
                if k == "cancel":
                    cancel = True
                    break
                elif k == "need_ack":
                    need_ack = True
                elif k == "text":
                    text = v

            # This can take a bit of time... lots to do....
            if log and not cancel:
                self._dataman.add_ioc_log(ioc_id, text, need_ack)

        except Exception as err:
            page = (
                self.make_page_header(css=CSS.IOC)
                + self.add_traceback(err)
                + "</body></html>"
            )
            return page

        # Go back to IOC page
        return bottle.redirect(self.make_link_ioc(ioc_id, None, url_only=True))

    def page_ioc_redirect(self, key):
        log.debug("called; key: %r", key)
        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)
        return bottle.redirect(self.make_link_ioc(ioc_id, None, url_only=True))

    def page_ioc_settings_help(self, key):

        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)
        host_name = get_host_name(ioc_id)
        url = self.make_link_ioc(ioc_id, host_name)

        page = self.make_page_header(css=CSS.IOC)
        page += (
            "<h2>IOC Settings Help&nbsp&nbsp%s</h2>\n" % url
            + "<h3>Development Mode</h3>\n"
            "<p>When selected, non-running <b>Critical</b> applications do\n"
            "not cause an error condition on the <b>Active IOCs</b> page,\n"
            "and the IOC is added to the <b>Development</b> group.\n"
            "The IOC still appears on the <b>Active IOCs</b> page\n"
            "provided it meets the required conditions (detected beacons <em>or</em>\n"
            "<b>Critical</b> applications).\n"
            "<h3>Maintenance Mode</h3>\n"
            "<p>When selected, the IOC is placed into maintenance mode\n"
            "and is added to the <b>Maintenance</b> group.\n"
            "However, the IOC still appears on the <b>Active Hosts</b> page\n"
            "provided it meets the required conditions (detected beacons <em>or</em>\n"
            "<b>Critical</b> applications).  Maintenance mode is indicated\n"
            "on the <b>Active IOCs</b> page.\n"
            "<h3>New Apps Critical</h3>\n"
            "<p>When selected, newly detected applications\n"
            "default to <b>Critical</b>. When deselected, newly detected applications\n"
            "default to <b>Not Critical</b>.  It is still possible to toggle\n"
            "applications between <b>Critical</b> and <b>Not Critical</b>.\n"
            "This option should not be selected for IOCs on\n"
            "which it is not possible to identify applications (e.g., Windows, EROCs, etc.)\n"
            "<h3>Monitor App Count</h3>\n"
            "<p>When selected, only the total number of applications on the IOC is monitored, not\n"
            "individual applications.  This option should only be selected for IOCs on\n"
            "which it is not possible to identify applications (e.g., Windows, EROCs, etc.)\n"
            "<h3>Hide from Active IOCs</h3>\n"
            "<p>When selected, the IOC is never shown on the <b>Active IOCs</b> page,\n"
            "and is added to the <b>Hidden</b> group.  Normally, an IOC\n"
            "is shown on the <b>Active IOCs</b> page if there are applications\n"
            "detected on it, <em>or</em> it has <b>Critical</b> applications.\n"
            "This option must be used sparingly to avoid accidentally\n"
            "hiding an IOC with <b>Critical</b> applications.\n"
            "<h3>Apply IOC Settings Button</h3>\n"
            "<p>Clicking this button applies the selected IOC settings.\n"
            "<h3>Decomission IOC Button</h3>\n"
            "<p>Clicking this button adds the IOC to the \n"
            "<b>Decomissioned</b> group and hides it from the <b>Active IOCs</b> page.\n"
            "<h3>Forget IOC Button</h3>\n"
            "<p>Clicking this button purges the IOC from the database and\n"
            "removes it from all groups. However, the IOC will be automatically\n"
            "re-added to the database (with default options) if a beacon is detected\n"
            "on it.\n"
        )
        url = "%s/ioc_redirect/%d" % (self.make_link_base(), ioc_id)
        page += (
            '<form action="%s">\n' % url
            + '<button type="submit" name="name" value="%s">OK</button>\n'
            "</form>\n"
            "</body></html>\n"
        )
        return page

    def page_ioc_apps_submit(self, key):
        log.debug("called; key: %r", key)
        ioc_id, ip_addr = self.get_ioc_id_and_addr(key)

        try:
            data = {}
            for k, v in bottle.request.forms.items():
                data[int(k)] = v

            # This can take a bit of time... lots to do....
            self._dataman.submit_ioc_apps(ioc_id, data)

        except Exception as err:
            page = (
                self.make_page_header(css=CSS.IOC)
                + self.add_traceback(err)
                + "</body></html>"
            )
            return page

        # Must be done outside or except
        return bottle.redirect(self.make_link_ioc(ioc_id, None, url_only=True))

    def page_beacons(self):
        return self.page_beacons_filtered()

    def page_beacons_ioc(self, key):
        return self.page_beacons_filtered(key=key)

    def page_beacons_filtered(self, key=None):

        page = self.make_page_header(css=CSS.IOC)

        try:
            if key:
                ip_key, ip_addr = self.get_ioc_id_and_addr(key)
                host_name = get_host_name(ip_key)
                page += "<h2>Beacons Detected from: %s (%s)</h2>" % (
                    host_name,
                    ip_addr,
                )

                beacons = self._dataman.get_beacons(ioc_id=ip_key)

            else:
                beacons = self._dataman.get_beacons()
                page += "<h2>%d Beacons Detected</h2>" % len(beacons)

            # Make list of entries that match the filtered ip_address.
            for beacon in beacons:
                ip_key = beacon[0]
                port = beacon[1]
                ### data = beacon[2]

            page += "<table>"
            page += (
                "<tr>"
                + '<th class="center">Index</th>'
                + '<th class="center">%s</th>'
                % self.make_host_sort_link(key=key)
                + '<th class="center">%s</th>'
                % self.make_addr_sort_link(key=key)
                + '<th class="center">TCP Port</th>'
                + '<th class="center">%s</th>'
                % self.make_uptime_sort_link(length=len(beacons))
                + '<th class="center">%s</th>'
                % self.make_restarts_sort_link(length=len(beacons))
                + '<th class="center">Forget</th>'
                + "</tr>"
            )

            for i, item in enumerate(beacons):

                ip_key = item[0]
                port = item[1]
                beacon = item[2]

                age_str = beacon_estimated_age_str(beacon)

                restarts_str = "fixme"

                host_name = get_host_name(ip_key)
                ip_addr = get_ip_addr(ip_key)

                host_link = self.make_link_ioc(ip_key, host_name)

                page += (
                    "<tr>"
                    + '<td class="center">%d</td>' % i
                    + "<td>%s</td>" % host_link
                    + "<td>%s</td>" % ip_addr
                    + '<td class="center">%d</td>' % port
                    + '<td class="right">%s</td>' % age_str
                    + '<td class="center">%s</td>' % restarts_str
                    + '<td class="center">%s</td>'
                    % self.make_link_forget_beacon(ip_key, port)
                    + "</tr>"
                )

            page += "</TABLE>"

        except Exception as err:
            page += self.add_traceback(err)

        page += "</BODY></HTML>"

        return page


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Monitor EPICS Beacons and Heartbeats"
    )
    parser.add_argument(
        "-m", "--mode", help="Mode: 'test' or 'prod'", type=str, required=True
    )
    parser.add_argument(
        "-g",
        "--gateway",
        help="Host acting as gateway device",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--config",
        help="Configration yml file",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-l",
        "--loglevel",
        default="CRITICAL",
        help="Set the logging level (e.g. NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    parser.add_argument(
        "-s",
        "--stackinfo",
        action="store_true",
        help="Include stack information in logging output",
    )

    # Must not start any threads before parse_args() otherwise program hangs
    # if parse_args() wants to terminate
    args = parser.parse_args()
    mode = args.mode.lower()
    gateway = args.gateway.lower()
    config_file = args.config
    level = args.loglevel
    stack_info = args.stackinfo
    # load configuration file from command line arg
    config.load_config(config_file)

    config_log(level=level.upper(), stack_info=stack_info)

    myApp = MyApp("PV Monitor", gateway, mode)
    myApp.start()

    while True:
        log.debug("Made it to here")
        time.sleep(10)
