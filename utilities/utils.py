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

from definitions import KEY, PV_KEY, config

log = logging.getLogger("PVMLogger")

EPOCH = datetime.datetime.strptime("Jan 1 1970", "%b %d %Y")

SIZE_UNITS = [" ", "K", "M", "G", "T", "P", "E", "Z"]

INVALID_PV_NAME_CHARS = [
    "{",
    "}",
    "(",
    ")",
    "$",
    "&",
    " ",
    "!",
    "'",
    "~",
    "@",
    "^",
    "/",
    "\\",
    "=",
    '"',
    "[",
    "]",
]

BEACON_INTERVAL = [
    (0.0, 0),  # 0
    (0.02, 0.02),
    (0.04, 0.06),
    (0.08, 0.14),
    (0.16, 0.3),
    (0.32, 0.62),  # 5
    (0.64, 1.26),
    (1.28, 2.54),
    (2.56, 5.1),
    (5.12, 10.22),
    (10.24, 20.46),  # 10
    (15, 35.46),
]

MAX_BEACON_INTERVAL_INDEX = len(BEACON_INTERVAL)
BEACON_RAMP_TIME = int(BEACON_INTERVAL[MAX_BEACON_INTERVAL_INDEX - 1][1])


class ExceptionReturn(Exception):
    pass


def validate_pv_name_new(pv_name_raw):

    valid = True

    pv_name = ""
    if isinstance(pv_name_raw, bytes):
        pv_name_raw = pv_name_raw.decode("utf-8")
    for c in pv_name_raw:

        if c == ".":
            break
        d = ord(c)

        if d == 0:
            break

        if d <= 20 or d > 127:
            valid = False

        if c in INVALID_PV_NAME_CHARS:
            valid = False

        pv_name += c

    if len(pv_name) < 4:
        valid = False

    return str(pv_name), valid


def make_size_str(bytes):
    if bytes is None:
        return ""

    try:
        bytes = int(bytes)

        suffix = "B"
        for unit in SIZE_UNITS:
            if abs(bytes) < 1024.0:
                return "%.2f %s%s" % (bytes, unit, suffix)
            bytes /= 1024.0
        return "%.2f %s%s" % (bytes, "Yi", suffix)

    except:
        return "Error"


def make_server_key(data):
    items = None

    if isinstance(data, dict):
        duplicates = data.get(PV_KEY.DUPLICATES)
        if duplicates:
            items = duplicates

        if not items:
            info = data.get(PV_KEY.INFO, [])
            host = None
            port = None
            last_time = 0

            for item in info:
                t = item.get(PV_KEY.LAST_TIME)
                if t > last_time:
                    last_time = t
                    host = item.get(PV_KEY.HOST)
                    port = item.get(PV_KEY.PORT)

            if not host:
                return 0

            key = "%d:%d" % (host, port)

            return hash(str(key)) & 0xFFFFFFFFFFFFFFFF
    else:
        items = data

    if not items:
        return 0

    if len(items) == 1:
        item = items[0]
        key = "%d:%d" % (item[0], item[1])
        return hash(str(key)) & 0xFFFFFFFFFFFFFFFF

    temp = []
    for item in items:
        h = item[0]
        p = item[1]
        key = "%d:%d" % (h, p)
        temp.append((h * 100000 + p, key, [h, p]))

    temp.sort()

    keys = [item[1] for item in temp]
    key = "-".join(keys)
    return hash(str(key)) & 0xFFFFFFFFFFFFFFFF


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

host_name_lock = threading.Lock()
host_name_cache = {}

def get_host_name(key, purge=False):

    max_host_age = config.MAX_HOST_AGE
    try:
        host_name_lock.acquire()
        if purge is False:
            item = host_name_cache.get(key)
            if item is not None:
                age = time.time() - item[1]
                if age > max_host_age:
                    item = None
            if item is not None:
                return item[0]
        host_name = get_host_name_for_db(key)
        host_name_cache[key] = (host_name, time.time())
        return host_name
    finally:
        host_name_lock.release()

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


def make_host_sort_name(host_name):
    """
    There is an issue in which a host name IOC0000-1500 comes before IOC0000-500,
    so look for host names that have a second part that is an int.
    """
    sort_name = host_name.upper()

    parts = sort_name.split("-")
    if len(parts) == 2:
        second = parts[1]
        try:
            value = int(second)
            # The second portion is an int:
            return "%s-%06d" % (parts[0], value)

        except:
            pass

    return sort_name


def make_app_id(cmd, cwd, ip_key):

    cmd = cmd.replace('"', "")
    cmd = cmd.replace("'", "")
    key = "%s-%s-%d" % (str(cmd), str(cwd), int(ip_key))

    hash_object = hashlib.md5()
    hash_object.update(key.encode("utf-8"))
    hash_digest = hash_object.digest()
    result = int.from_bytes(hash_digest, "big") & 0xFFFFFFFFFFFFFFFF

    return result


def beacon_est_uptime_sec(seq):
    if seq is None:
        return None

    if seq < MAX_BEACON_INTERVAL_INDEX:
        interval = BEACON_INTERVAL[seq]
        return int(interval[1])

    periods = seq - MAX_BEACON_INTERVAL_INDEX + 1
    return 15 * periods + BEACON_RAMP_TIME


def beacon_estimated_age_str(b, seq=None):
    age_str = "Unknown"

    if seq is None:
        if b is None:
            return age_str

        seq = int(b.get(KEY.SEQ, 0))
    else:
        seq = int(seq)

    if seq >= 0:
        if seq > 6:
            seq -= 6

        est_age = datetime.timedelta(seconds=15.0 * seq)
        age_str = "%s" % est_age

    return age_str


def make_last_seen_str(seconds):

    try:
        seconds = int(seconds)
    except:
        return ""

    if seconds == 0:
        return "0 sec"

    days = int(seconds / 86400)
    seconds = seconds - days * 86400
    hours = int(seconds / 3600)
    seconds = seconds - hours * 3600
    minutes = int(seconds / 60)
    seconds = seconds - minutes * 60

    if days:
        if days == 1:
            result = "1 day"
        else:
            result = "%d days" % days

        if hours == 1:
            result += " 1 hour"
        else:
            result += " %d hours" % hours
        return result

    if hours:
        if hours == 1:
            result = "1 hour"
        else:
            result = "%d hours" % hours

        if minutes == 1:
            result += " 1 min"
        else:
            result += " %d mins" % minutes

        return result

    if minutes:

        if minutes == 1:
            result = " 1 min"
        else:
            result = " %d mins" % minutes

        if seconds == 1:
            result += " 1 sec"
        else:
            result += " %d sec" % seconds

        return result

    if seconds == 1:
        return "1 sec"

    return "%d sec" % seconds


def my_uptime_str(uptime_sec, zero=False):

    if uptime_sec == 0:
        if zero:
            return "0 sec"
        return ""

    days = int(uptime_sec / 86400)
    uptime_sec = uptime_sec - days * 86400
    hours = int(uptime_sec / 3600)
    uptime_sec = uptime_sec - hours * 3600
    min = int(uptime_sec / 60)
    uptime_sec = uptime_sec - min * 60

    if days == 0:
        d = ""
    elif days == 1:
        d = "1 day "
    else:
        d = "%d days " % days

    hms = "%02d:%02d:%02d" % (hours, min, uptime_sec)

    return d + hms


def make_duration_string(duration, no_seconds=False, verbose=True):
    if duration is None:
        return

    try:
        duration = int(duration)
    except:
        return ""

    if duration < 60:
        return "%d sec" % duration

    if duration < 3600:

        if duration < 120:
            no_seconds = False

        if no_seconds:
            minutes = int(float(duration) / 60.0)
            minutes = int(minutes)
            return "%d min" % minutes

        else:
            minutes = int(float(duration) / 60.0)
            minutes = int(minutes)
            seconds = duration - (minutes * 60)
            if verbose:
                return "%d min %d sec" % (minutes, seconds)
            else:
                return "0:%02d:%02d" % (minutes, seconds)

    hours = int(float(duration) / 3600)
    hours = int(hours)
    minutes = duration - (hours * 3600)
    minutes = int(float(minutes) / 60)

    days = int(float(hours) / float(24))

    if days > 0:
        hours -= days * 24

        if days == 1:
            if verbose:
                return "1 day %d hours %d min" % (hours, minutes)
            else:
                return "1 day %dh %dm" % (hours, minutes)
        else:
            if verbose:
                return "%d days %d hours %d min" % (days, hours, minutes)
            else:
                return "%d days %dh %dm" % (days, hours, minutes)

    else:
        if verbose:
            return "%d hr %d min" % (hours, minutes)
        else:
            return "%d:%02d:00" % (hours, minutes)


def linuxmon_date_to_int(date_str):

    try:
        n = datetime.datetime.now()

        # Unfortunately LinuxMon does not report year in its response;
        # Assume the current year, so add it to the time reported by LinuxMon
        year_str = " %04d" % n.year
        d = datetime.datetime.strptime(date_str + year_str, "%a %b %d %H:%M %Y")
        diff = d - EPOCH

        # Must add the timezone to the computed integer time so that it
        # can be directly compared to time.time elsewhere in the code
        total_seconds = int(diff.total_seconds()) + time.timezone

    except Exception as err:
        log.error(
            "Exception: %s (date_str: %r)",
            str(err),
            date_str,
        )
        total_seconds = None

    return total_seconds


def linuxmon_uptime_to_int(uptime_str):
    """
    Using custom code to convert uptime to int because it seems that uptime
    just reports days into the 1000's instead of years.   It was a major LinuxMon
    mistake to return formatted strings for data like this.
    """
    try:
        if not str:
            return ""

        if not isinstance(uptime_str, str):
            return "Not a string: %s" % type(uptime_str)

        parts = uptime_str.split("day")

        if len(parts) == 2:
            days = int(parts[0].strip())
            hmin = parts[1].strip("s ")

        elif len(parts) == 1:
            days = 0
            hmin = parts[0].strip(" ")

        else:
            raise ValueError("cant process '%s'" % uptime_str)

        parts = hmin.split(":")

        if len(parts) != 2:
            raise ValueError("cant process '%s'" % uptime_str)

        hours = int(parts[0].strip())
        mins = int(parts[1].strip())

        uptime = days * 24 * 60 * 60 + hours * 60 * 60 + mins * 60

        return uptime

    except Exception as err:
        return "Exception: %s %s" % (str(err), repr(uptime_str))


def make_tooltip(input):

    max_line_len = 70
    max_lines = 10

    word_list = []
    line_list = []
    line = ""
    append = False
    try:
        for _ in range(3):
            input = input.strip()
            input = input.strip('"')
            input = input.strip("'")

        input.replace("\n", " ")
        input.replace("<br>", " ")
        input.replace("\r", " ")

        if len(input) < max_line_len:
            return input

        word_list = input.split(" ")

        for word in word_list:
            word = word.strip()
            if len(word) > 50:
                continue

            if len(line) + len(word) > max_line_len:
                line_list.append(line.strip())
                line = word
                if len(line_list) >= max_lines:
                    append = True
                    break
            else:
                line = line + " " + word

        result = "<br>".join(line_list)
        if append:
            result += "..."
        return result

    except Exception as err:
        result = "Exception making tooltip: %s" % repr(err)

    return result

def remove_leading_space(s):
    if s is not None:
        s = s.lstrip()
    return s
