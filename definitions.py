# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
# This needs to be importable by:
# gateway (python 2.7)
# client (python 3.7)

import logging
import os

from configmanager import Config

from utilities.pvm_logger import PVMonLogger


class Constant(object):

    @classmethod
    def validate(cls, value):

        for k, v in list(cls.__dict__.items()):
            if value == v:
                if not k.startswith("_"):
                    return True

    @classmethod
    def name(cls, value):

        for k, v in list(cls.__dict__.items()):
            if value == v:
                if not k.startswith("_"):
                    return "%s.%s" % (cls.__name__, k)

        return "%s.UNKNOWN" % cls.__name__

    @classmethod
    def items(cls):
        result = {}
        for k, v in list(cls.__dict__.items()):
            # print k, v
            if not k.startswith("_"):
                result[k] = v
        return result


# These were from iocmon I think


class KEY(object):

    # Use small keys so to minimize JSON size
    IOC_ID = "ioc_id"
    IP_KEY = "ip_k"
    IP_ADDR = "addr"
    ADDRESS_ORIG = "addr_orig"
    TIME = "t"
    DATA = "d"
    SEQ = "seq"
    COUNTER = "counter"
    UP = "up"
    LOST = "L"
    PV_COUNT = "pvc"
    PV_LIST = "pvs"
    LAST_TIME = "last"
    SERVER_PORT = "sp"
    VERSION = "ver"
    IMAGE = "img"
    DIRECTORY = "cwd"
    PID = "pid"
    SERVER_TIME = "cur"
    START_CMD = "stcmd"
    HANDLE = "handle"
    TOTAL = "total"
    PORT = "p"
    FIRST_TIME = "ft"
    CMD = "cmd"
    CWD = "cwd"
    ACTIVE_TCP_PORTS = "actp"
    WARNINGS = "warn"
    APP_COUNT = "ac"
    APP_ID = "ai"
    ERRORS = "err"
    MESSAGES = "msgs"
    MESSAGE = "msg"
    ID = "ID"
    STATE = "state"
    STATUS = "status"
    NAME = "n"
    IOC_COUNT = "ic"
    IOC_LIST = "IL"
    CAN_DELETE = "CD"
    REMAP = "remap"
    EXPECT = "expect"
    SORT_NAME = "sort_name"
    SUMMARY_NAME = "summary_name"
    COLOR = "color"
    COLOR2 = "color2"
    BEACONS = "beacons"
    KIND = "k"
    DESCRIPTION = "desc"
    TOOLTIP = "ttip"
    FLASH_FLAG = "flash"
    OPTIONS = "options"
    QTIME = "qt"
    UPTIME = "uptime"
    MEM = "mem"
    QSIZE = "qsize"
    COUNT_1 = "count1"
    COUNT_2 = "count2"
    TIME_START = "t_start"
    PV_INVALID = "inv"
    INTERVAL_MS = "ival"


# These were from pvmon I think - Unfortunately these keys are
# in the json data files and cannot be changed (although they
# could be renamed so as not to conflict with the iocmon key names


class PV_KEY(object):
    FIRST_TIME = "F"
    LAST_TIME = "L"
    LAST_TIME_GW = "g"
    HOST = "h"
    DUPLICATES = "D"
    PORT = "p"
    NAME = "N"
    HOST_KEY = "K"
    TIME = "T"
    GATEWAY = "G"
    IP = "IP"
    INFO = "i"
    REQUESTS = "r"
    REQ_COUNT = "c"
    CLIENT_REQ = "CR"
    VALUE = "v"
    SERVER_KEY = "sk"


class HISTORY(object):
    NEW = "N"
    LOST = "L"
    DETECTED = "D"
    RESTARTED = "R"
    RESUMED = "S"


HISTORY_STRING = {
    HISTORY.NEW: "NEW",
    HISTORY.LOST: "LOST",
    HISTORY.DETECTED: "DETECTED",
    HISTORY.RESTARTED: "RESTARTED",
    HISTORY.RESUMED: "RESUMED",
}


class APP_STATE(object):
    NOT_CRITICAL = 0  # Display, but not error if not running
    CRITICAL = 1  # Display, error if not running
    IGNORE = 2  # Do not display (if not running)


class KIND(object):
    NEW = "NEW"
    LOST = "LOST"
    DETECTED = "DETECTED"
    RESTARTED = "RESTARTED"


class IOC_STATE(object):
    ACTIVE = 1
    MAINTENANCE = 2
    TEST = 3
    DECOMISSIONED = 4


IOC_STATE_DESC = {
    IOC_STATE.ACTIVE: "ACTIVE",
    IOC_STATE.MAINTENANCE: "MAINTENANCE",
    IOC_STATE.TEST: "TEST/DEVELOPMENT",
    IOC_STATE.DECOMISSIONED: "DECOMISSIONED",
}


class PLOT_TYPE(object):
    REQ_USER = "req_user"
    REQ_GATEWAY = "req_gw"
    REQ_IOC = "req_ioc"
    PKT_USER = "pkt_user"
    PKT_GW = "pkt_gw"
    PVS_TOTAL = "pvs_total"
    PVS_LOST = "pvs_lost"
    PVS_DUPLICATE = "pvs_duplicate"
    PVS_INVALID = "pvs_invalid"
    MEM_RSS_USAGE = "mem_rss_usage"
    MEM_VMS_USAGE = "mem_vms_usage"
    CPU_USAGE = "cpu_usage"
    MOST_ACTIVE_PVS = "most_active_pvs"
    MOST_ACTIVE_IOCS = "most_active_iocs"
    SRV_UNIX_FREE_HOME = "srv_unix_free_hone"
    SRV_UNIX_FREE_IOCAPPS = "srv_unix_free_iocapps"


class PLOT_INTERVAL(object):
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    MONTH2 = "months2"
    MONTH6 = "months6"
    YEAR = "year"
    YEAR2 = "year2"


class LOG_KIND(object):
    IOC = 1


# Unfortunately the PriorityQueue assigns higher priority to lower numbers


class PRIORITY(object):
    HIGH = 10
    NORMAL = 20
    LOW = 30


CMD_NONE = "No Cmd"
CWD_NONE = "No Cwd"

# These things are 4-byte commands sent across the UDP Link


class CMD_UDP(object):
    LIST_PVS = 0x4DA3B921
    NETSTAT = 0x7F201C3B
    HEARTBEAT = 0xAB0F4397
    BEACON = 0x63FB072A
    CA_PROTO_SEARCH = 0x594D3EA1
    MSG_ERR = 0xE7509DE2
    MSG_INFO = 0x34B37A85
    CA_PROTO_SEARCH_RESP = 0xD368F21B
    STATS = 0xF4E20638
    DO_CA_PROTO_SEARCH = 0x8F63B20A
    DO_CA_PROTO_SEARCH_NO_RESP = 0x9B6F3CC2


class Configuration:
    def __init__(self) -> None:
        return

    # Shall we name this function set_config(), like a setter? I named it
    # load_config because it loads things from a file, not like a general
    # 'setter'. However, set_config helps with consistency.
    def load_config(self, config_file) -> None:
        self.config = Config()
        current_dir = os.path.dirname(__file__)
        config_path = os.path.join(current_dir, config_file)
        self.config.yaml.load(config_path, as_defaults=True)

        self.PVM_HOST = self.config.HOST_NAME.PVMonitorHost.value
        self.GW_HOST = self.config.HOST_NAME.GatewaySearcherHost.value
        self.CAINFO_HOST = self.config.HOST_NAME.CAINFOHost.value

        self.ADDRESS_GW = self.config.GatewaySearcher_ADDRESS.IP_Addr.value
        self.BCAST_ADDRESS_GW = (
            self.config.GatewaySearcher_ADDRESS.BCAST_Addr.value
        )

        self.DB_SERVER = self.config.DB.DB_Server.value
        self.DB_USER = self.config.DB.DB_User.value
        self.DB_NAME = self.config.DB.DB_Name.value
        self.DB_DEPLOY = self.config.DB.DB_Deploy.value

        self.HTTP_PORT = self.config.PORT.HTTP_PORT.value
        self.TO_CLIENT_PORT = self.config.PORT.TO_CLIENT_PORT.value
        self.TO_GATEWAY_PORT = self.config.PORT.TO_GATEWAY_PORT.value
        self.CA_PROTO_SEARCH_PORT = (
            5064  # Port on which CA_PROTO_SEARCH packets are sent
        )
        self.THREAD_TO_PVM = 5070
        self.BEACON_PORT = 5065  # Default port on which beacons are sent
        self.HEARTBEAT_PORT = 5066  # Default port on which heartbeats are sent
        self.CAINFO_SERVER_PORT = 5068  # cainfo server on epicsgateway

        self.HOSTNAME_IPS = self.config.HOSTNAME_IPS

        self.SAVE_INTERVAL = calculate_time(self.config.FILE_SAVE_INTERVAL)
        self.MAX_LINUXMON_AGE = calculate_time(self.config.MAX_LINUXMON_AGE)
        self.PV_PURGE_AGE = calculate_time(self.config.PV_PURGE_AGE)
        self.MAX_HOST_AGE = calculate_time(self.config.MAX_HOST_AGE)

    def get_hostname_for_ip(self, ip_addr: str):
        # Look up IP addresses in ADDR_LIST in the config file. Get its hostname if
        # the IP address is found in the list.  - Yi Luan Jun 21st, 2024
        for path, item in self.HOSTNAME_IPS.iter_items(recursive=True):
            if ip_addr in item.value:
                host_name = path[0].replace("_", "-")
                return host_name
        return None


config = Configuration()


# Converts the times entered in the config file to seconds
def calculate_time(time):
    second, minute, hour, day = 1, 60, 3600, 86400

    return (
        (time.SECONDS.value * second)
        + (time.MINUTES.value * minute)
        + (time.HOURS.value * hour)
        + (time.DAYS.value * day)
    )


def config_log(level="CRITICAL", stack_info=False) -> None:
    logging.setLoggerClass(PVMonLogger)
    try:
        logging.basicConfig(
            level=level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
    except:
        print(
            "Level should be one of NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL"
            "\nYou entered:",
            level,
        )
        exit(1)
    logger = logging.getLogger("PVMLogger")
    assert isinstance(logger, PVMonLogger)
    logger.setStackInfo(stack_info)


# logging.basicConfig(level=logging.NOTSET,
#     format='%(asctime)s - %(levelname)s - %(message)s')

## HTTP_PORT_PVS_PROD      = 5066
## HTTP_PORT_IOCS_PROD     = 5067

## HTTP_PORT_CAINFO        = 5068
## HTTP_PORT_IOC_TORNADO   = 5069


## HTTP_PORT_IOCS_TEST     = 5073
## HTTP_PORT_PVS_TEST      = 5074


# Internal commands
class CMD(Constant):
    HEARTBEAT_PV = 1000
    PV_RESPONSE = 1100
    PV_REQUEST = 1200
    HEARTBEAT = 1300
    PV_FORGET = 1400


class EVENT(object):

    BEACON_NEW = 1001
    BEACON_DETECTED = 1002
    BEACON_LOST = 1003
    BEACON_RESUMED = 1004
    BEACON_RESTARTED = 1005


EVENT_DISPLAY = {
    EVENT.BEACON_NEW: "NEW BEACON",
    EVENT.BEACON_DETECTED: "BEACON DETECTED",
    EVENT.BEACON_RESUMED: "BEACON RESUMED",
    EVENT.BEACON_LOST: "BEACON LOST",
    EVENT.BEACON_RESTARTED: "BEACON RESTARTED",
}


## class SORT_MODE(Constant):
##     UPTIME_ASC    = 0
##     UPTIME_DESC   = 1
##     HOST_ASC      = 2
##     HOST_DESC     = 3
##     ADDR_ASC      = 4
##     ADDR_DESC     = 5
##     RESTARTS_ASC  = 6
##     RESTARTS_DESC = 7
##
##     NAME           = 10
##     REQUESTS       = 20
##     LAST_TIME      = 30


class COLOR(object):
    RED = "#FF0000"
    GREEN = "#00FF00"
    BLUE = "#82CAFF"
    YELLOW = "#FFFF00"
    ORANGE = "#FFA500"
    GRAY = "#BABABA"
    DARK_GREEN = "#00CC00"
    WHITE = "#FFFFFF"


class GROUP(Constant):
    # These cannot be changed as the are stored in the database
    ACTIVE = 0
    ALL = 1
    UNNASSIGNED = 2
    MAINTENTANCE = 3
    ##    TEST                = 4
    DECOMISSIONED = 5
    DEVELOPMENT = 6


class IOC_OPTION(Constant):
    DEVELOPMENT = 100
    MAINTENANCE = 200
    NEW_APPS_CRITICAL = 300
    MONITOR_APP_COUNT = 400
    HIDE_FROM_ACTIVE = 500
    DECOMISSIONED = 600


IOC_OPTION_LIST = [
    (IOC_OPTION.DEVELOPMENT, "Development Mode"),
    (IOC_OPTION.MAINTENANCE, "Maintenance Mode"),
    (IOC_OPTION.NEW_APPS_CRITICAL, "New Apps Critical"),
    (IOC_OPTION.MONITOR_APP_COUNT, "Monitor App Count"),
    (IOC_OPTION.HIDE_FROM_ACTIVE, "Hide from Active IOCs"),
]


class IOC_OPTION_BUTTON(Constant):
    APPLY = 1000
    DECOMISSION = 2000
    FORGET = 3000


IOC_OPTION_BUTTON_LIST = [
    (IOC_OPTION_BUTTON.APPLY, "Apply IOC Settings"),
    (IOC_OPTION_BUTTON.DECOMISSION, "Decomission IOC"),
    (IOC_OPTION_BUTTON.FORGET, "Forget IOC"),
]


class SORT_MODE(Constant):
    NAME = "name"
    ADDRESS = "address"
    COUNT = "count"
    LOAD = "load"
    MEM = "mem"
    MEM_FREE = "memfree"
    SWAP = "swap"
    SWAP_FREE = "swapfree"
    UPTIME = "uptime"
    MACHINE = "machine"
    CPU_USER = "cpu_user"
    CPU_SYS = "cpu_system"
    CPU_IDLE = "cpu_idle"
    TIME = "time"
    LOST = "lost"
    LAST_SEEN = "last_seen"
    REQ_TIME = "req_time"
    REQ_COUNT = "req_count"
    IOC = "ioc_name"
    NONE = "none"


class EPICS(object):
    CA_PROTO_VERSION = 0
    CA_PROTO_SEARCH = 6
    DONT_REPLY = 5
    CA_CLIENT_MINOR_PROT_VER = 11
    HDR_LEN = 16
    MAX_HEARTBEAT_AGE = 65  # This allows us 1 missed heartbeat
    MAX_SEQ = 0xFFFF


class SETTINGS(object):
    DEBUG_LEVEL = 1

    LOG_HEARTBEATS = False
    LOG_BEACONS = False

    PV_SEARCH_ON_HEARTBEAT_FETCH = False
    PV_SEARCH_ON_FIRST_REQUEST = False

    UPDATE_HEARTBEAT_APP = False

    MAX_LOGS_IOC_PAGE = 200
    MAX_SEARCH_RESULTS = 10000

    LATEST_REQUEST_PVS = 10000
    LATEST_REQUEST_IOCS = 1000

    SCRIPT_PLOTLY_JS = (
        '\n<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>\n'
    )


LINUXMON_PV_SIGNATURE = "BOOTTIME"
# These are the PVs that are fetched regularly for the LinuxMonitor summary page
LINUXMON_PV_SUFFIX_POLL = [
    LINUXMON_PV_SIGNATURE,
    "LOAD15MIN",
    "MEMAVPCT",
    "MEMFREEPCT",
    ## 'SWAPACTIVEPCT',
    ## 'SWAPFREEPCT',
    "UPTIME",
    "MACHINE",
    "CPUUSER",
    "CPUSYSTEM",
    "CPUNICE",
    "CPUIDLE",
    "TIME",
]

# These are PVs that are fetched when we look at a specific machine
LINUXMON_PV_SUFFIX_POLL_ALL = [
    "LOAD",
    "LOAD15MIN",
    "UPTIME",
    "VERSION",
    "MACHINE",
    "SWAPAV",
    "LOAD5MIN",
    "MEMAVPCT",
    "IPADDR",
    "CPUSYSTEM",
    "SWAPACTIVEPCT",
    "SWAPFREEPCT",
    "CPUUSER",
    "MEMAV",
    "PIDAV",
    "HBT",
    "RELEASE",
    "MEMFREE",
    "WD",
    "SWAPCACH",
    "CPUNICE",
    "MEMBUFF",
    "SYSNAME",
    "LOAD1MIN",
    "SWAPUSED",
    "MEMSHRD",
    "PIDFREEPCT",
    "TIME",
    "BOOTTIME",
    "SWAPFREE",
    "MEMFREEPCT",
    "CPUIDLE",
    "PIDUSED",
    "PIDFREE",
    "MEMUSED",
]
