# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
import logging
import multiprocessing as mp
import pickle
import queue
import select
import socket
import struct
import threading
import time
import json
import traceback

from definitions import CMD_UDP, EPICS, config
from utilities.statistics import StatisticsManager
from utilities.thread_monitor import ThreadMonitor
from utilities.pvmonitor_lock import PVMonitorLock
from utilities.utils import get_ip_from_key, get_host_name

log = logging.getLogger("PVMLogger")

TM = ThreadMonitor()
STATS = StatisticsManager()

USE_MP = True


class Receiver(object):

    def __init__(self, queue_in, queue_out, listen_port):

        self._listen_port = listen_port
        self._queue_in = queue_in
        self._queue_out = queue_out
        self._terminate = False
        self._count = 0
        self._worker_rx = threading.Timer(1, self.worker_rx)

    def run(self):

        self._worker_rx.start()

        s = None

        while not self._terminate:

            if not s:
                # This thing receives UDP packets from the server (gateway)
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(("", self._listen_port))
                log.debug(
                    "Listening on port %d",
                    self._listen_port,
                )

            try:
                readable, _, exceptional = select.select([s], [], [s], 5.0)
                if s not in readable:
                    raise ValueError("Socket not readable")

                if s in exceptional:
                    raise ValueError("Socket select error")

                data = s.recvfrom(10000)
                self._queue_out.put_nowait((data, time.time()))
                self._count += 1

            except Exception as err:
                log.exception("Exception: %r", err)
                s.close()
                s = None
                time.sleep(1)

    def worker_rx(self):
        # This worker just listens for commands
        # If it does not get one it terminates this listener
        last_cmd_time = time.time()

        while True:
            try:
                item = self._queue_in.get(block=True, timeout=30)
                last_cmd_time = time.time()

            except queue.Empty:
                interval = time.time() - last_cmd_time
                log.debug("messages sent: %d", self._count)
                if interval > 70.0:
                    log.error("timeout... terminating!")
                    self._terminate = True
                    break

                continue

            last_cmd_time = time.time()

        log.debug("Done")


def rx_worker(queue_to, queue_from, listen_port):
    """
    Multiprocess Worker.  Runs in its own dedicated python process so that
    it can acquire the GIL
    """
    receiver = Receiver(queue_to, queue_from, listen_port)
    receiver.run()


class ProxyClient(object):

    def __init__(self, dataman):

        self._callback_beacon_func = None
        self._callback_pv_request_func = []
        self._callback_pv_response_func = None
        self._callback_alive = []

        self._queue_rx_priority = queue.PriorityQueue()
        self._listen_port = None

        self._count_missed = 0
        self._count_total = 0
        
        self._thread_data = None

        if USE_MP:
            self._q_tx = mp.Queue()
            self._q_rx = mp.Queue()
            self._process_rx = None

        else:
            self._q_rx = queue.Queue()
            self._timer_rx = threading.Timer(1, self.worker_rx)

        self._timer_rx_prioritize = threading.Timer(
            1, self.worker_rx_prioritize
        )
        self._timer_rx_process = threading.Timer(1, self.worker_rx_process)
        self._timer_rx_thread = threading.Timer(1, self.rx_thread_data)
        self._thread_data_lock = PVMonitorLock()

    def get_thread_data(self):
        self._thread_data_lock.acquire()
        try:
            return self._thread_data
        finally:
            self._thread_data_lock.release()

    def add_callback_alive(self, func):
        self._callback_alive.append(func)

    def set_listen_port(self, value):
        self._listen_port = value
        self._process_rx = mp.Process(
            target=rx_worker, args=(self._q_tx, self._q_rx, self._listen_port)
        )

    def set_callback_beacon(self, func):
        self._callback_beacon_func = func

    def set_callback_heartbeat(self, func):
        self._callback_heartbeat_func = func

    def set_callback_pv_request(self, func):
        self._callback_pv_request_func.append(func)

    def set_callback_stats(self, func):
        self._callback_stats_func = func

    def set_callback_pv_response(self, func):
        self._callback_pv_response_func = func

    def start(self):
        if USE_MP:
            self._process_rx.start()
        else:
            self._timer_rx.start()

        self._timer_rx_prioritize.start()
        self._timer_rx_process.start()
        self._timer_rx_thread.start()

    def call_callback_alive(self, alive):
        # Tell other managers that the gateway appears to be alive
        for func in self._callback_alive:
            func(alive)

    def worker_rx(self):

        # NOTE: this has not been tested and is probably busted after adding
        # USE_MP
        s = None

        # Move received messages from the "gateway" into a queue as fast as
        # possible
        while True:

            if not s:
                # This thing receives UDP packets from the server (gateway)
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(("", self._listen_port))
                log.debug(
                    "Listening on port %d",
                    self._listen_port,
                )

            try:
                readable, _, exceptional = select.select([s], [], [s], 5.0)
                if s not in readable:
                    STATS.increment("RX socket select not readable (client)")
                    raise ValueError("Socket not readable")
                if s in exceptional:
                    STATS.increment("RX socket select error (client)")
                    raise ValueError("Socket select error")

                self._q_rx.put_nowait(s.recvfrom(10000))

                if self._q_rx._qsize() > 500:
                    log.warning(
                        "RX DGRAM queue size: %d",
                        self._queue._qsize(),
                    )

            except Exception as err:
                log.exception(
                    "Exception: %r",
                    err,
                )
                s.close()
                s = None
                
    def rx_thread_data(self):
        # Receive gateway thread from the gateway
        s = None
        
        while True:
            if s == None:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(("", config.THREAD_TO_PVM))
        
            try:
                readable, _, exceptional = select.select([s], [], [s], 5.0)
                if s not in readable:
                    STATS.increment("RX socket select not readable (client)")
                    raise ValueError("Socket not readable")
                if s in exceptional:
                    STATS.increment("RX socket select error (client)")
                    raise ValueError("Socket select error")
                
                self._thread_data_lock.acquire()
                self._thread_data = json.loads(s.recvfrom(10000)[0].decode('utf-8'))
                self._thread_data_lock.release()

            except Exception as err:
                s.close()
                s = None
            

    def worker_rx_prioritize(self):

        prev_seq = None
        alive = False

        while True:
            try:
                item = self._q_rx.get(block=True, timeout=10)

            except queue.Empty:
                STATS.increment("Client RX timeouts")
                if alive:
                    alive = False
                    self.call_callback_alive(alive)
                continue

            try:

                if not alive:
                    alive = True
                    self.call_callback_alive(alive)

                msg = item[0]
                msg_time = item[1]
                data = msg[0]
                # Look at the header that the proxy added
                # HDR: CMD (32) IP_KEY (32) PORT (16) SEQ (16) PRIORITY (16)
                # TOTAL_LEN (16)
                c = struct.unpack_from("!LLHHHH", data)
                cmd = c[0]
                ip_key = int(socket.ntohl(c[1]))
                port = int(socket.ntohs(c[2]))
                seq = int(socket.ntohs(c[3]))
                priority = int(socket.ntohs(c[4]))
                total_len = int(socket.ntohs(c[5]))
                msg_time = time.time()

                self._count_total += 1

                if total_len != len(data):
                    raise ValueError(
                        "ERROR: data size mismatch! %d != %s"
                        % (len(data), total_len)
                    )

                data = data[EPICS.HDR_LEN :]

                if prev_seq is not None:

                    if seq == 0:
                        if prev_seq != EPICS.MAX_SEQ:
                            missing = EPICS.MAX_SEQ - prev_seq
                            log.debug(
                                "1 ===== seq: %d prev: %d missing: %d",
                                seq,
                                prev_seq,
                                missing,
                            )
                            self._count_missed += missing

                    elif seq < prev_seq:
                        missing = EPICS.MAX_SEQ - prev_seq + seq
                        log.debug(
                            "2 ===== seq: %d prev: %d missing: %d",
                            seq,
                            prev_seq,
                            missing,
                        )
                        self._count_missed += missing

                    elif prev_seq != (seq - 1):
                        missing = seq - prev_seq - 1
                        log.debug(
                            "3 ===== seq: %d prev: %d missing: %d",
                            seq,
                            prev_seq,
                            missing,
                        )
                        self._count_missed += missing

                prev_seq = seq

                serialized = pickle.dumps(
                    (priority, (cmd, ip_key, port, data, msg_time))
                )
                self._queue_rx_priority.put_nowait(serialized)

            except Exception as err:
                log.exception(
                    "Exception decoding received data hdr: %r",
                    err,
                )

    def worker_rx_process(self):
        """
        Process incoming messages from the gateway
        """

        cmd_map = {
            socket.htonl(CMD_UDP.BEACON): self.handle_cmd_beacon,  # queued
            socket.htonl(
                CMD_UDP.HEARTBEAT
            ): self.handle_cmd_heartbeat,  # queued
            socket.htonl(
                CMD_UDP.CA_PROTO_SEARCH
            ): self.handle_cmd_pv_request,  # queued
            socket.htonl(
                CMD_UDP.CA_PROTO_SEARCH_RESP
            ): self.handle_cmd_pv_response,  # queued
            socket.htonl(CMD_UDP.MSG_ERR): self.handle_cmd_msg_err,
            socket.htonl(CMD_UDP.MSG_INFO): self.handle_cmd_msg_info,
            socket.htonl(CMD_UDP.STATS): self.handle_cmd_stats,
        }

        thread_id = TM.add("ProxyClient: RX gateway msgs", 60)
        last_ping_time = time.time()
        last_print_time = time.time()

        while True:

            cur_time = time.time()
            if cur_time - last_ping_time > 30:
                TM.ping(thread_id)
                last_ping_time = cur_time
                if USE_MP:
                    self._q_tx.put_nowait("ping")

            if cur_time - last_print_time > 60:
                log.debug(
                    "RX: %d (size: %d missed: %d)",
                    self._count_total,
                    self._queue_rx_priority._qsize(),
                    self._count_missed,
                )
                last_print_time = cur_time

                STATS.set("Client Rx", self._count_total)
                STATS.set("Client Rx (missing)", self._count_missed)

            try:
                serialized = self._queue_rx_priority.get(block=True, timeout=10)
                thing = pickle.loads(serialized)
            except queue.Empty:
                continue

            try:
                item = thing[1]
                cmd = item[0]
                ip_key = item[1]
                port = item[2]
                data = item[3]
                qtime = item[4]

                # Do some sanity checking on the received data
                handler = cmd_map.get(cmd)
                if not handler:
                    raise ValueError("Unknown command: %X" % cmd)

                handler(ip_key, port, data, qtime)

            except Exception as err:
                t = traceback.format_exc(10)
                log.exception(
                    "Exception decoding data: %r\n Traceback %s",
                    err,
                    t,
                )

    def handle_cmd_stats(self, ip_key, port, data, qtime):
        log.debug("GOT STATS %d", len(data))

        if self._callback_stats_func:
            self._callback_stats_func(ip_key, port, data, self._process_rx.pid)

    def handle_cmd_pv_response(self, ip_key, port, data, qtime):
        log.debug("GOT CA_PROTO_SEARCH Response: %d", len(data))
        STATS.increment("PV Responses")

        if self._callback_pv_response_func:
            self._callback_pv_response_func(ip_key, port, data)

    def handle_cmd_pv_request(self, ip_key, port, data, qtime):
        STATS.increment("PV Requests")

        for func in self._callback_pv_request_func:
            func(ip_key, port, data)

    def handle_cmd_msg_err(self, ip_key, port, data, qtime):
        log.debug("GATEWAY ERROR: %r", data)

    def handle_cmd_msg_info(self, ip_key, port, data, qtime):
        log.debug("GATEWAY INFO: %r", data)

    def handle_cmd_beacon(self, ip_key, port, data, msg_time):

        if self._callback_beacon_func:
            self._callback_beacon_func(ip_key, port, data, msg_time)

    def handle_cmd_heartbeat(self, ip_key, port, data, msg_time):

        if self._callback_heartbeat_func:
            self._callback_heartbeat_func(ip_key, port, data, msg_time)


class NewSearcher(object):
    """A searcher that search PV info from network and send to the
    _gateway_addr
    """

    def __init__(self, gateway, client_to_gateway_port):

        self._socket_tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self._gateway_ip_addr = socket.gethostbyname(gateway)
        self._gateway_addr = (self._gateway_ip_addr, client_to_gateway_port)

        self._seq = 0
        self._cmd_search = socket.htonl(CMD_UDP.DO_CA_PROTO_SEARCH)
        self._cmd_search_no_resp = socket.htonl(
            CMD_UDP.DO_CA_PROTO_SEARCH_NO_RESP
        )

        self._queue = queue.PriorityQueue()

        self._search_count = 0
        self._timer = threading.Timer(1, self.worker)

        self._pending = {}
        self._pending_lock = threading.Lock()
        self._interval = {}

    def start(self):
        self._timer.start()

    def search(self, pv_name, priority, send_response=True):

        self._search_count += 1
        log.debug(
            "called; PV: %s (%d)",
            pv_name,
            self._search_count,
        )

        if not isinstance(pv_name, str):
            raise ValueError("PV not unicode: %r" % pv_name)

        if not send_response:
            raise ValueError("called with send_response == False")

        # Allow pv to be pending for 5 minutes.  We should get a response
        # before that unless somehow it is lost.  This relies on the queue
        # on the server not getting too large
        pending = False

        if not pending:
            serialized = pickle.dumps((priority, (pv_name, send_response)))
            self._queue.put_nowait(serialized)

        if pending:
            return False
        return True
    
    def tx_thread(self):
        """
        Send request to Gateway asking for Gateway thread info
        """
        s_thread = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cmd_thread = socket.htonl(
            CMD_UDP.THREAD
        )
        fmt = "!L"
        
        m = struct.pack(
            fmt,
            cmd_thread,)
        
        s_thread.sendto(m, (self._gateway_ip_addr, config.THREAD_TO_GW))

    def worker(self):

        thread_id = TM.add("CA_PROTO_SEARCH: Worker", 60)

        last_ping_time = 0
        while True:
            try:
                serialized = self._queue.get(block=True, timeout=10)
                thing = pickle.loads(serialized)
            except queue.Empty:
                TM.ping(thread_id)
                continue

            priority = thing[0]
            item = thing[1]
            pv_name = item[0]
            send_response = item[1]

            cur_time = time.time()
            if cur_time - last_ping_time > 30:
                TM.ping(thread_id)
                last_ping_time = cur_time

            try:
                pv_name = str.encode(pv_name)
            except Exception as err:
                log.exception(
                    "Exception searching for PV %r: %r",
                    pv_name,
                    err,
                )
                continue

            # HDR: CMD (32) IP_KEY (32) PORT (16) SEQ (16) PRIORITY (16)
            # TOTAL_LEN (16)
            fmt = "!LLHHHH%ds" % len(pv_name)

            if send_response:
                cmd_n = self._cmd_search
            else:
                cmd_n = self._cmd_search_no_resp

            seq_n = socket.htons(self._seq)
            priority_n = socket.htons(priority)
            total_len_n = socket.htons(len(pv_name) + EPICS.HDR_LEN)

            ip_key_n = 0
            port_n = 0

            m = struct.pack(
                fmt,
                cmd_n,
                ip_key_n,
                port_n,
                seq_n,
                priority_n,
                total_len_n,
                pv_name,
            )

            try:
                _, writeable, exceptional = select.select(
                    [], [self._socket_tx], [self._socket_tx], 5.0
                )

                if self._socket_tx not in writeable:
                    log.error("Socket not writable")
                    STATS.increment("TX socket not writable")
                    continue

                if self._socket_tx in exceptional:
                    log.error("Socket in exceptional")
                    STATS.increment("TX socket in exceptional")
                    continue

                self._socket_tx.sendto(m, self._gateway_addr)

            except Exception as err:
                log.exception("TX socket exception: %r", err)
                STATS.increment("TX socket in exceptional")

            log.debug(
                "CA_PROTO_SEARCH ---> %s (qsize: %d)",
                pv_name,
                self._queue._qsize(),
            )

            self._seq += 1
            if self._seq > EPICS.MAX_SEQ:
                self._seq = 0


class Runner(object):

    def __init__(self):

        self._client = None

    def run_client(self):
        self._client = ProxyClient()
        self._client.start()

    def run(self):
        while True:
            time.sleep(1)


if __name__ == "__main__":

    runner = Runner()
    runner.run_client()
