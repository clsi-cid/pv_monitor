# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import datetime
import hashlib
import logging
import socket
import struct
import time
import threading

from PVMonitor_gateway.definitions_gw import config

log = logging.getLogger("GatewayLogger")


def get_ip_addr(key):

    try:
        key = int(key)
    except:
        pass

    if isinstance(key, int):
        return get_ip_from_key(key)

    if isinstance(key, str):
        if key.find(":") > 0:
            parts = key.split(":")
            key = parts[0]

        # Sanity check
        return key

    raise ValueError("bad host key: %s" % repr(key))


def get_ip_from_key(ip_key):
    return socket.inet_ntoa(struct.pack("!I", ip_key))


def get_ip_key(ip_addr):
    if isinstance(ip_addr, int):
        return ip_addr

    ip_addr = get_ip_addr(ip_addr)

    try:
        ip_key = struct.unpack("!I", socket.inet_aton(ip_addr))[0]
        log.debug(
            "RESULT: %r ->%r ",
            ip_addr,
            ip_key,
        )
        return int(ip_key)

    except Exception as err:
        log.error(
            "EXCEPTION: %r ip_addr: %r",
            err,
            ip_addr,
        )

def get_host_name_for_db(key, trim_domain=True):

    ip_addr = get_ip_addr(key)

    is_ip = False
    try:
        host_name = socket.gethostbyaddr(ip_addr)[0]
        if host_name is None:
            raise ValueError("gethostbtaddr() returned None")

    except Exception as err:
        log.error("Exception getting host_name for %r: %r", ip_addr, err, )
        host_name = config.get_hostname_for_ip(ip_addr=ip_addr)

        if host_name is None:
            host_name = str(ip_addr)
            is_ip = True

    if trim_domain:
        if not is_ip:
            host_name = host_name.split(".")[0]

    return host_name
