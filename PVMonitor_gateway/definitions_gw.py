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

from utilities_gw.pvm_logger import PVMonLogger


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

class PRIORITY(object):
    HIGH = 10
    NORMAL = 20
    LOW = 30

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

        self.GW_HOST = self.config.HOST_NAME.GatewaySearcherHost.value

        self.ADDRESS_GW = self.config.GatewaySearcher_ADDRESS.IP_Addr.value
        self.BCAST_ADDRESS_GW = (
            self.config.GatewaySearcher_ADDRESS.BCAST_Addr.value
        )

        self.TO_CLIENT_PORT = self.config.PORT.TO_CLIENT_PORT.value
        self.TO_GATEWAY_PORT = self.config.PORT.TO_GATEWAY_PORT.value
        self.CA_PROTO_SEARCH_PORT = (
            5064  # Port on which CA_PROTO_SEARCH packets are sent
        )
        self.THREAD_TO_PVM = 5070
        self.BEACON_PORT = 5065  # Default port on which beacons are sent
        self.HEARTBEAT_PORT = 5066  # Default port on which heartbeats are sent
        self.CAINFO_SERVER_PORT = 5068  # cainfo server on epicsgateway

    def get_hostname_for_ip(self, ip_addr: str):
        # Look up IP addresses in ADDR_LIST in the config file. Get its hostname if
        # the IP address is found in the list.  - Yi Luan Jun 21st, 2024
        for path, item in self.HOSTNAME_IPS.iter_items(recursive=True):
            if ip_addr in item.value:
                host_name = path[0].replace("_", "-")
                return host_name
        return None


config = Configuration()

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

class EPICS(object):
    CA_PROTO_VERSION = 0
    CA_PROTO_SEARCH = 6
    DONT_REPLY = 5
    CA_CLIENT_MINOR_PROT_VER = 11
    HDR_LEN = 16
    MAX_HEARTBEAT_AGE = 65  # This allows us 1 missed heartbeat
    MAX_SEQ = 0xFFFF
