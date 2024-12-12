# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
# System imports
import logging
import re
import sys
import threading
import time

ANSI_ESC = re.compile(r"\x1b[^m]*m")

log = logging.getLogger("PVMLogger")


class ProcException(Exception):
    pass


class KEY(object):
    CMD = "Command Line"
    CWD = "Directory (cwd)"
    EXE = "Executable"
    PID = "PID"
    RAW = "raw"
    PORT = "port"


class RemoteCommand(object):

    def __init__(self):

        if "pexpect" not in sys.path:
            sys.path.append("pexpect")
        import pexpect

        self.pexpect = pexpect
        self.host = None
        self.user = "root"
        self.password = None
        self.child = None
        self.prompt = None

    def set_host(self, host):
        self.host = host

    def set_password(self, password):
        self.password = password

    def run_command(self, command):

        try:
            log.debug("Run command: '%s'", command)

            send = "%s;echo '__END__'\n" % command

            self.child.sendline(send)

            # Look for the issued echo command - it has a quote at the end
            self.child.expect_exact("__END__'")

            # Look for the echo command output
            self.child.expect_exact("__END__")
            result = self.child.before.strip()

            ## print 'Result after cleaning escape sequences'

            ## print 'Result after cleaning escape sequences'
            result = ANSI_ESC.sub("", result)
            result = result.strip()
        except Exception as e:
            log.exception("Exception running command: %s", str(e))
            result = None

        return result

    def run_netstat(self):

        self.run_command("echo here")
        result = self.run_command("netstat -eapn")

        temp = result.split("\n")

        result = []

        for line in temp:
            line = line.strip()
            if not line:
                continue
            if line.find("assword") > 0:
                continue
            if line.find(self.password) >= 0:
                continue

            result.append(line)

        return result

    def disconnect(self):
        if self.child:
            self.child.sendeof()

    def connect(self):
        result = self.connect2()
        if result:
            return True
        return self.connect2()

    def connect2(self):

        if not self.password:
            return False

        target = "ssh %s@%s" % (self.user, self.host)

        log.debug("connect() called --> %s", target)
        self.child = self.pexpect.spawn(target)

        response = [
            "(?i)password",
            "(yes/no)",
            self.pexpect.EOF,
            self.pexpect.TIMEOUT,
            "route",
        ]

        self.prompt = None

        while True:
            index = self.child.expect(response)
            log.debug(
                "connect got index %s, %s",
                index,
                self.child.before,
            )

            if index == 0:
                log.debug("sending the password")
                self.child.sendline("%s" % self.password)
                self.child.expect("\r\n")
                break

            elif index == 1:
                self.child.sendline("yes")

            elif index in [2, 3, 4]:
                self.child.sendeof()
                self.child = None
                break

        if not self.child:
            return False

        # Send one CR
        self.child.sendline("\n")

        last_before = ""

        for i in range(100):

            before = self.child.before
            after = self.child.after
            buffer = self.child.buffer

            log.debug("---- before ---- %r", before)
            log.debug("---- after ---- %r", after)
            log.debug("---- buffer ---- %r", self.child.buffer)

            if before and before.find("denied") >= 0:
                log.debug("found denied in before so break")
                break

            if after and after.find("denied") >= 0:
                log.debug("found denied in after so break")
                break

            before = before.strip()
            if before:
                if before == last_before:
                    log.debug(
                        "This might be the prompt: '%s'",
                        before,
                    )
                    self.prompt = before
                    break
                else:
                    last_before = before

            if buffer and buffer.find("\n") == -1:
                log.debug("found what looks like a prompt")
                self.prompt = buffer.strip()
                log.debug("PROMPT: '%s'", self.prompt)
                break

            log.debug("waiting for a CR")
            try:
                self.child.expect("\r\n")
            except:
                log.exception(
                    "Must have gotten a timeout connecting",
                )
                self.child = None
                return False

        if not self.prompt:
            self.child.sendeof()
            self.child = None
            return False

        return True

    def run_proc_scan(self, pid):

        result = None
        data = {}
        self.run_command("echo here")

        result = self.run_command("cat /proc/%d/cmdline" % pid)

        raw = result

        if result.strip():
            result = result.strip()
            # Replace any '00' with 'space'
            result = result.replace("\00", " ")
            data[KEY.CMD] = result

        result = self.run_command("ls -l /proc/%d/cwd" % pid)

        raw += result
        if result:
            data[KEY.CWD] = self._clean_symlink(result)

        result = self.run_command("ls -l /proc/%d/exe" % pid)
        raw += result

        if result:
            data[KEY.EXE] = self._clean_symlink(result)

        if data:
            data[KEY.PID] = pid

        return data

    def _clean_symlink(self, symlink):
        parts = symlink.split(" ")
        return parts[-1].strip()


class RemoteProc(object):

    def __init__(self):

        self.password = None
        self._cache = {}
        self._cache_lock = threading.Lock()

    def set_password(self, password):
        self.password = password

    def find_telnet_ports(self, data):
        """
        Extracts telnet ports from the commands, then sorts items by
        telnet port ascending.  Port numbers then converted to strings
        """
        no_port = 999999999
        result = []
        for item in data:
            command = item.get(KEY.CMD)
            if not command:
                log.warning("Ignoring data %s", item)
                continue

            parts = command.split(" ")
            parts = [part.strip() for part in parts if len(part.strip())]

            parts.reverse()

            port = None
            for part in parts:
                try:
                    port = int(part)
                    break
                except:
                    continue

            if not port:
                port = no_port
            result.append((port, item))

        result = sorted(result)
        result2 = []
        for item in result:
            if item[0] == no_port:
                port = ""
            else:
                port = str(item[0])
            result2.append((port, item[1]))

        return result2

    def get_cached(self, key):

        try:
            self._cache_lock.acquire()
            data = self._cache.get(key)
            if not data:
                log.debug("get_cached(%s): cache miss", key)
                return None

            log.debug("get_cached(%s): cache hit", key)
            age = data.get("age")
            now = int(time.time())
            if (now - age) > data.get("timeout"):
                log.debug("get_cached(%s): cache expired", key)

                try:
                    del self._cache[key]
                except:
                    pass
                return None

            return data.get("data")

        finally:
            self._cache_lock.release()

    def set_cached(self, key, data, timeout):

        if data is None:
            return

        try:
            self._cache_lock.acquire()
            self._cache[key] = {
                "age": int(time.time()),
                "data": data,
                "timeout": int(timeout),
            }

        finally:
            self._cache_lock.release()

    def connect(self, ip_addr):

        if not self.password:
            raise ProcException("Not Authorized")

        rc = RemoteCommand()
        rc.set_host(ip_addr)
        rc.set_password(self.password)

        connected = rc.connect()

        if not connected:
            raise ProcException("Connect Failed")

        return rc

    def get_pid_from_port(self, netstat, port):

        pid = None

        for line in netstat:
            parts = line.split(" ")
            parts = [p.strip() for p in parts if len(p.strip()) > 0]

            if len(parts) < 5:
                continue

            if parts[0] not in ["udp", "tcp"]:
                continue

            src_addr = parts[3]

            addr_parts = src_addr.split(":")
            if len(addr_parts) < 2:
                log.warning(
                    "Did not get expected number of address parts from %s",
                    src_addr,
                )
                continue

            try:
                src_port = int(addr_parts[-1])
            except Exception:
                log.exception(
                    "Exception getting port from %r",
                    addr_parts,
                )
                src_port = None

            if not src_port:
                continue

            if port != src_port:
                continue

            log.debug("FOUND PORT in line '%s'", line.strip())
            program = parts[-1]
            program_parts = program.split("/")
            log.debug("Program parts: %s ", program_parts)
            pid = int(program_parts[0])
            break

        return pid

    def get_procserv_pids(self, netstat):

        result = []
        for line in netstat:
            parts = line.split(" ")
            parts = [p.strip() for p in parts if len(p.strip()) > 0]

            if not parts:
                continue

            if parts[0] != "tcp":
                continue

            last_part = parts[-1]

            process_parts = last_part.split("/")
            if len(process_parts) != 2:
                continue

            if process_parts[1] != "procServ":
                continue

            try:
                pid = int(process_parts[0])
            except Exception:
                log.exception("Exception getting procServ PID %s", line)
                pid = None

            if pid:
                result.append(pid)

        return list(set(result))

    def get_netstat(self, ip_addr):

        netstat_key = "%s-netstat" % ip_addr
        return self.get_cached(netstat_key)

    def get_procs(self, ip_addr, port=None):

        log.debug("get_procs(%s, %r) called", ip_addr, port)

        try:
            result = []
            rc = None

            netstat_key = "%s-netstat" % ip_addr
            netstat = self.get_cached(netstat_key)
            if not netstat:
                rc = self.connect(ip_addr)
                netstat = rc.run_netstat()
                if not netstat:
                    raise ProcException("Failed to get nestat")

                self.set_cached(netstat_key, netstat, 600)

            if port is None:
                pids = self.get_procserv_pids(netstat)
            else:
                pid = self.get_pid_from_port(netstat, port)
                if pid:
                    pids = [pid]
                else:
                    pids = None

            if not pids:
                if not port:
                    raise ProcException("No procServ processes detected")
                else:
                    raise ProcException("Process not found (port: %r)" % port)

            for pid in pids:
                if port:
                    key = "%s-%d-%d" % (ip_addr, pid, port)
                else:
                    key = "%s-procServ-%d" % (ip_addr, pid)

                data = self.get_cached(key)

                if not data:
                    if not rc:
                        rc = self.connect(ip_addr)

                    data = rc.run_proc_scan(pid)
                    self.set_cached(key, data, 60 * 60 * 24 * 365)

                if data:
                    result.append(data)

        except ProcException as e:
            log.exception("ProcException(): %s", str(e))
            result = str(e)

        finally:
            log.debug("get_procs(%s, %r) called", ip_addr, port)
            if rc:
                rc.disconnect()

        return result


def run(host, port, filename=None):

    r = RemoteProc()
    r.set_password("password")

    data = r.get_procs("10.51.20.6", port=None)

    log.debug("GOT DATA --> %s", data)

    data = r.find_telnet_ports(data)

    for item in data:
        log.debug("---- ITEM---- %s", item)


if __name__ == "__main__":

    try:
        host = sys.argv[1]
    except:
        host = None

    try:
        port = int(sys.argv[2])
    except:
        port = 0

    try:
        filename = sys.argv[3]
    except:
        filename = None

    log.debug("running with host '%s' port '%d'", host, port)
    run(host, port, filename=filename)
