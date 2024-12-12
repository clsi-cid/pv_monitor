#---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
#---------------------------------------------------------------------
import argparse
import json
import logging
import pickle
import queue
import socket
import struct
import threading
import time
import sys
import logging

sys.path.insert(1, '..')

from utilities_gw.pvm_logger import PVMonLogger
logging.setLoggerClass(PVMonLogger)

from PVMonitor_gateway.definitions_gw import (CMD_UDP, EPICS, PRIORITY,
                         config, config_log)
from PVMonitor_gateway.gateway_search_worker import SearchWorker
from utilities_gw.thread_monitor import ThreadMonitor
from utilities_gw.utils import get_host_name_for_db, get_ip_key

log = logging.getLogger("GatewayLogger")

TM = ThreadMonitor()

TEST_HEARTBEATS = True

WORKER_THREADS = 10


class Gateway:

    def __init__(self, client_list, send_ca_proto_search_response):
        self._timer_rx_client = []
        self._timer_rx_heartbeat = None
        self._timer_rx_beacon = None
        self._timer_rx_ca_proto_search_response = None
        self._timer_tx = None
        self._timer_search = None

        self._client_list = [(item[0], item[1]) for item in client_list]
        self._client_port_list = list({item[2] for item in client_list})
        
        self._send_ca_proto_search_response = send_ca_proto_search_response

        self._queue_tx = queue.PriorityQueue()
        self._queue_rx_priority = queue.PriorityQueue()
        self._queue_rx = queue.Queue()

        self._queue_test_heartbeats = queue.Queue()
        self._test_heartbeats = {}

        self._my_ip_addrs = []
        self._broadcast_addrs = []
        self._skip_gateway_keys = []
        self._skip_gateway_addrs = []
        self._skip_archiver_keys = []

        self._count_rx = 0
        self._count_tx = 0
        self._count_ca_proto_search = 0
        self._count_ca_proto_response = 0

        self._count_ca_proto_search_gw = 0
        self._count_pv_requests_gw = 0

        self._count_ca_proto_search_ar = 0
        self._count_pv_requests_ar = 0

        self._count_tx_error = 0

        # For PV searching
        self._debug = True
        self._cid = 1000
        self._search_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._search_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._search_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._search_sock.setblocking(0)

        self._cmd_info = socket.htonl(CMD_UDP.MSG_INFO)
        self._cmd_err = socket.htonl(CMD_UDP.MSG_ERR)
        self._cmd_proto_search_resp = socket.htonl(CMD_UDP.CA_PROTO_SEARCH_RESP)
        self._cmd_stats = socket.htonl(CMD_UDP.STATS)

        self._search_worker_list = []
        for i in range(WORKER_THREADS):
            search_worker = SearchWorker(self, self._queue_tx, i)
            self._search_worker_list.append(search_worker)

    def start(self):
        log.debug("called")

        self._timer_tx = threading.Timer(1, self.worker_tx)
        self._timer_tx.start()

        self._timer_rx_beacon = threading.Timer(1, self.worker_listen_beacon)
        self._timer_rx_beacon.start()

        self._timer_rx_heartbeat = threading.Timer(
            1, self.worker_listen_heartbeat
        )
        self._timer_rx_heartbeat.start()

        if TEST_HEARTBEATS:
            self._timer_test_heartbeats = threading.Timer(
                1, self.worker_test_heartbeats
            )
            self._timer_test_heartbeats.start()

        for port in self._client_port_list:
            log.debug("client rx port: %d", port)
            timer = threading.Timer(1, lambda port=port: self.worker_rx(port))
            timer.start()
            # Probably do not have to keep track of these
            self._timer_rx_client.append(timer)

        if self._send_ca_proto_search_response:
            self._timer_rx_ca_proto_search = threading.Timer(
                1, self.worker_listen_ca_proto_search
            )
            self._timer_rx_ca_proto_search.start()
            
        self._timer_tx_thread = threading.Timer(
            1, self.tx_thread_data
        )
        self._timer_tx_thread.start()

        # This worker just decodes the header then places the request into a
        # priority Queue
        self._timer_rx_prioritize = threading.Timer(
            1, self.worker_rx_prioritize
        )
        self._timer_rx_prioritize.start()

        self._timer_rx_process = threading.Timer(1, self.worker_rx_process)
        self._timer_rx_process.start()

        self._timer_stats = threading.Timer(1, self.worker_send_stats)
        self._timer_stats.start()
        
        for worker in self._search_worker_list:
            worker.start()

        TM.set_callback(self.callback_tm)

    def callback_tm(self, dummy1, dummy2):
        TM.print_list()

    def increment_response_count(self):
        self._count_ca_proto_response += 1

    def set_broadcast_addrs(self, addrs):
        self._broadcast_addrs = [(IP, config.CA_PROTO_SEARCH_PORT) for IP in addrs]
        for worker in self._search_worker_list:
            worker.set_broadcast_addrs(self._broadcast_addrs)

    def set_gateway_addrs(self, addrs):
        for addr in addrs:
            ip_key = get_ip_key(addr)
            log.debug(
                "Skip CA_PROTO_SEARCH from GATEWAY addr %s (%d)",
                addr,
                ip_key,
                )
            self._skip_gateway_keys.append(ip_key)
            self._skip_gateway_addrs.append(addr)

        for worker in self._search_worker_list:
            worker.set_gateway_addrs(addrs)

    def get_rx_queue_size(self):
        return self._queue_rx_priority._qsize()

    def worker_rx_prioritize(self):
        """
        Unpack the message headers, prioritize and re-queue
        """

        thread_id = TM.add("Gateway: RX Prioritize", 60)
        last_ping_time = time.time()

        while True:

            cur_time = time.time()
            if cur_time - last_ping_time > 30:
                TM.ping(thread_id)
                last_ping_time = cur_time

            try:
                item = self._queue_rx.get(block=True, timeout=10)
            except queue.Empty:
                continue

            self._count_rx += 1

            data = item[0]

            try:
                # Unpack the message header
                # HDR: CMD (32) IP_KEY (32) PORT (16) SEQ (16) PRIORITY (16)
                # TOTAL_LEN (16)

                c = struct.unpack_from("!LLHHHH", data)

                cmd = c[0]
                ip_key = socket.ntohl(c[1])
                port = socket.ntohs(c[2])
                ## seq = socket.ntohs(c[3])
                priority = socket.ntohs(c[4])
                total_len = socket.ntohs(c[5])

                if total_len != len(data):
                    raise ValueError(
                        "ERROR: data size mismatch! %d != %s"
                        % (len(data), total_len)
                    )

                data = data[EPICS.HDR_LEN :]

                # Re-queue for further prioritized processing
                serialized = pickle.dumps((priority, (cmd, ip_key, port, data)))
                self._queue_rx_priority.put_nowait(serialized)

            except Exception as err:
                log.exception(
                    "Exception processing Rx msg: %r", 
                    err, 
                )


    def worker_send_stats(self):

        thread_id = TM.add("Gateway: Stats worker", 60)
        last_ping_time = time.time()
        last_print_time = time.time()

        while True:
            time.sleep(5)

            cur_time = time.time()
            if cur_time - last_ping_time > 30:
                TM.ping(thread_id)
                last_ping_time = cur_time

            qsize = self._queue_rx_priority._qsize()

            if cur_time - last_print_time > 30:
                log.debug(
                    "TX: %d (TX err: %d) RX: %d Search: %s Resp: %d rx_q: %d",
                    self._count_tx,
                    self._count_tx_error,
                    self._count_rx,
                    self._count_ca_proto_search,
                    self._count_ca_proto_response,
                    qsize,
                        )

                last_print_time = cur_time

            stats = {
                "gw": self._count_ca_proto_search_gw,
                "gw_pv": self._count_pv_requests_gw,
                "ar": self._count_ca_proto_search_ar,
                "ar_pv": self._count_pv_requests_ar,
                "rx_q": qsize,
                "rx_count": self._count_rx,
                "tx_count": self._count_tx,
                "search": self._count_ca_proto_search,
                "resp": self._count_ca_proto_response,
                "tx_err": self._count_tx_error,
            }
            serialized = pickle.dumps(
                (PRIORITY.HIGH, (self._cmd_stats, json.dumps(stats), 0, 0))
            )
            self._queue_tx.put_nowait(serialized)

            self._count_ca_proto_search_ar = 0
            self._count_pv_requests_ar = 0

            self._count_ca_proto_search_gw = 0
            self._count_pv_requests_gw = 0

            self._count_rx = 0
            self._count_tx = 0
            self._count_ca_proto_search = 0
            self._count_ca_proto_response = 0
            self._count_tx_error = 0    

    def worker_rx_process(self):
        """
        This worker performs CA_PROTO_SEARCHES.  Rate limit how fast this
        runs because we could flood IOCs with search requests
        """

        thread_id = TM.add("Gateway: RX Process", 60)
        next_ping_time = time.time()
        last_ping_time = None

        item_count = 0

        while True:

            cur_time = time.time()

            if cur_time > next_ping_time:
                TM.ping(thread_id)
                if last_ping_time:
                    elapsed = cur_time - last_ping_time
                    rate = float(item_count) / elapsed

                    log.debug(
                        "%d items in %.2f seconds (%.2f items/sec)",
                        item_count,
                        elapsed,
                        rate,
                        )

                next_ping_time += 30
                last_ping_time = cur_time
                item_count = 0

            try:
                serialized = self._queue_rx_priority.get(block=True, timeout=10)
                item = pickle.loads(serialized)
                item_count += 1

            except queue.Empty:
                continue

            while True:
                dispatched = False
                for worker in self._search_worker_list:
                    if worker.get_queue_size() < 2:
                        worker.search(item)
                        dispatched = True
                        break

                if dispatched:
                    break
                else:
                    TM.ping(thread_id)

                time.sleep(0.2)
                continue

    def worker_rx(self, port):
        """
        This worker listens for search requests from the client.
        It does nothing but put received packets into a queue
        """
        # This thing receives UDP packets from the server (gateway)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", port))

        while True:
            self._queue_rx.put_nowait(s.recvfrom(10000))

    
    def tx_thread_data(self):
        # transmit gateway thread data to the PVM server every 1 second
        s = None
        
        while True:
            time.sleep(5)
            
            if s == None:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                
            thread_data = TM.get_list()
            thread_data = json.dumps(thread_data, indent=2).encode('utf-8')
            fmt = "!%ds" % len(thread_data)
            
            m = struct.pack(
                fmt, 
                thread_data
            )
            
            try:
                for client in self._client_list:
                    s.sendto(m, (client[0], config.THREAD_TO_PVM))
            except Exception as err:
                    log.error("Exception: %r", err)
                    s.close()
                    s = None
        
            time.sleep(1)
        

    def worker_listen_ca_proto_search(self):

        max_size = 0

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", config.CA_PROTO_SEARCH_PORT))

        cmd_n = socket.htonl(CMD_UDP.CA_PROTO_SEARCH)

        while True:
            data, address = s.recvfrom(10000)

            # Debugging/sanity checking
            if len(data) > max_size:
                max_size = len(data)
                log.debug(
                    "Got a new max_size: %d", max_size
                )

            # Debugging/sanity checking
            if len(data) >= 10000:
                log.error(
                    "max data size exceeded: %d",
                    len(data),
                        )
                continue

            self._count_ca_proto_search += 1

            ip_key = struct.unpack("!I", socket.inet_aton(address[0]))[0]
            port = int(address[1])

            if ip_key in self._skip_gateway_keys:
                # There can be many PVs in any one request
                pv_request_count = self.count_pv_requests(data)
                self._count_ca_proto_search_gw += 1
                self._count_pv_requests_gw += pv_request_count
                continue

            if ip_key in self._skip_archiver_keys:
                pv_request_count = self.count_pv_requests(data)
                self._count_ca_proto_search_ar += 1
                self._count_pv_requests_ar += pv_request_count
                continue

            # Seeing data of sizes 1024 but no larger
            serialized = pickle.dumps(
                (PRIORITY.NORMAL, (cmd_n, data, ip_key, port))
            )
            self._queue_tx.put_nowait(serialized)

    def count_pv_requests(self, data):
        """
        Count the number pf PV requests in this packet.  This is only required
        if we want to count the number of requests from the gateway.  Dont want
        to transmit the entire packet to the client just to get this count
        """
        return int(float(len(data) / 45.7))

    def worker_listen_beacon(self):

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", config.BEACON_PORT))

        cmd_n = socket.htonl(CMD_UDP.BEACON)

        while True:
            data, address = s.recvfrom(10000)
            ip_key = struct.unpack("!I", socket.inet_aton(address[0]))[0]
            port = int(address[1])

            data_len = len(data)

            if data_len == 0:
                continue

            elif data_len != EPICS.HDR_LEN:
                host_name = get_host_name_for_db(ip_key)
                log.warning(
                    "Unexpected data len: %d from %s", 
                    data_len, 
                    host_name, 
                )

            serialized = pickle.dumps(
                (PRIORITY.NORMAL, (cmd_n, data, ip_key, port))
            )
            self._queue_tx.put_nowait(serialized)


    def worker_listen_heartbeat(self):

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", config.HEARTBEAT_PORT))

        cmd_n = socket.htonl(CMD_UDP.HEARTBEAT)

        while True:
            data, address = s.recvfrom(10000)
            ip_key = struct.unpack("!I", socket.inet_aton(address[0]))[0]
            port = int(address[1])
            serialized = pickle.dumps(
                (PRIORITY.NORMAL, (cmd_n, data, ip_key, port))
            )
            self._queue_tx.put_nowait(serialized)

            if TEST_HEARTBEATS:
                self._queue_test_heartbeats.put_nowait((data, ip_key, port))

    def worker_test_heartbeats(self):
        while True:

            try:
                item = self._queue_test_heartbeats.get(block=True, timeout=10)
            except queue.Empty:
                continue

            data_str = item[0]
            ioc_id = item[1]
            port = item[2]

            data = json.loads(data_str)

            seq = int(data.get("seq"))

            ioc_data = self._test_heartbeats.get(ioc_id, {})

            host_name = ioc_data.get("name")
            if host_name is None:
                host_name = get_host_name_for_db(ioc_id)
                ioc_data["name"] = host_name

            have_seq = ioc_data.get(port)

            if have_seq is not None:

                if seq > have_seq:
                    if seq != (have_seq + 1):
                        log.error(
                            "HEARTBEAT ERROR: %s port: %d: Got: %d expect %d",
                            host_name, port, seq, have_seq + 1,
                        )

            ioc_data[port] = seq
            self._test_heartbeats[ioc_id] = ioc_data

    def worker_tx(self):

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        seq = 0

        thread_id = TM.add("Gateway: Worker TX", 60)
        last_ping_time = time.time()

        while True:

            cur_time = time.time()
            if cur_time - last_ping_time > 30:
                TM.ping(thread_id)
                last_ping_time = cur_time

            try:
                serialized = self._queue_tx.get(block=True, timeout=10)
                thing = pickle.loads(serialized)
            except queue.Empty:
                continue

            try:
                priority = thing[0]
                item = thing[1]

                cmd_n = item[0]
                data = item[1]
                ip_key = item[2]
                port = item[3]

                # HDR: CMD (32) IP_KEY (32) PORT (16) SEQ (16) PRIORITY (16)
                # TOTAL_LEN (16)

                fmt = "!LLHHHH%ds" % len(data)

                ip_key_n = socket.htonl(ip_key)
                port_n = socket.htons(port)
                seq_n = socket.htons(seq)
                total_len = len(data) + 16
                total_len_n = socket.htons(total_len)
                priority_n = socket.htons(priority)

                if isinstance(data, str):
                    data = str.encode(data)
                m = struct.pack(
                    fmt,
                    cmd_n,
                    ip_key_n,
                    port_n,
                    seq_n,
                    priority_n,
                    total_len_n,
                    data,
                )

                seq += 1
                if seq > EPICS.MAX_SEQ:
                    seq = 0

            except Exception as err:
                log.exception(
                    "Exception preparing msg for Tx: %r",
                    err,
                        )
                self._count_tx_error += 1
                continue

            try:
                for client in self._client_list:
                    s.sendto(m, client)

            except Exception as err:
                self._count_tx_error += 1
                log.error("Exception: %r", err)

            qsize = self._queue_tx._qsize()
            if qsize > 500:
                log.warning("TX queue size: %d", qsize)

            self._count_tx += 1


class Runner:

    def __init__(self, args):
        """
        Sanity check the command line args
        """
        self._gateway = None

        client_list = str(args.client)
        mode_list = str(args.mode)

        clients = client_list.split(",")
        modes = mode_list.split(",")
        config_file = args.config
        
        # load configuration file from command line arg
        config.load_config(config_file)
    
        level = args.loglevel
        stack_info = args.stackinfo
        config_log(level=level.upper(), stack_info=stack_info)

        # TODO! Get rid multiple client support.  ITs not required and just
        # TODO: complicates things

        count = len(clients)
        if count == 0:
            raise ValueError("Must specify at least one client")
        if count != len(modes):
            raise ValueError(
                "Number of clients (%d) and modes (%d) must match"
                % (count, len(modes))
            )

        self._send_ca_proto_search_response = True

        log.debug(
            "Send CA_PROTO_SEARCH Responses: %r",
            self._send_ca_proto_search_response,
        )

        host_name = socket.gethostname().lower()
        self._my_host_name = get_host_name_for_db(host_name).lower()
        log.debug(
            "My hostname: %r", self._my_host_name
        )

        if self._my_host_name.startswith(config.GW_HOST):  # -050
            self._broadcast_addrs = config.BCAST_ADDRESS_GW
            self._my_ip_addrs = config.ADDRESS_GW
        else:
            raise ValueError(
                "Unable to determine broadcast addresses for: %r"
                % self._my_host_name
            )

        for addr in self._broadcast_addrs:
            log.debug(
                "My broadcast address: %r", addr
            )

        for addr in self._my_ip_addrs:
            log.debug("My address: %r", addr)

        self._clients = []

        for index, client in enumerate(clients):

            client = client.strip()
            try:
                ip_addr = socket.gethostbyname(client)
                log.debug(
                    "Got ip_addr %s for client %r",
                    ip_addr,
                    client,
                        )

            except Exception as err:
                raise ValueError(
                    "Client: %r Error: %r" % (client, err)
                )

            mode = modes[index]
            log.debug(
                "Mode for client %s: %r",
                ip_addr,
                mode,
                )

            mode = mode.lower()
            mode = mode.strip()

            if mode.startswith("t") or mode.startswith("p"):
                self._clients.append(
                    (ip_addr, config.TO_CLIENT_PORT, config.TO_GATEWAY_PORT)
                )
            else:
                raise ValueError(
                    "Invalid mode for client %s: %r" % (client, mode)
                )

        for item in self._clients:
            log.debug("Sending to: %r", item)

        log.debug("Done")

    def run_server(self):
        log.debug("called")

        self._gateway = Gateway(
            self._clients, self._send_ca_proto_search_response
        )
        self._gateway.set_broadcast_addrs(self._broadcast_addrs)
        self._gateway.set_gateway_addrs(self._my_ip_addrs)
        self._gateway.start()

    def run(self):
        while True:
            time.sleep(1)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Monitor EPICS Beacons and Heartbeats"
    )
    parser.add_argument(
        "-c",
        "--client",
        help="Comma separated host names (or IP addrs) to send beacon data",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-m",
        "--mode",
        help="Comma separated modes (1 per host) 'test' or 'prod'",
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
        '-l',
        '--loglevel',
        default='CRITICAL',
        help='Set the logging level (e.g. NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL)'
    )
    parser.add_argument(
        '-s',
        '--stackinfo',
        action='store_true',
        help='Include stack information in logging output'
    )

    ## parser.add_argument('-p', '--pvs', help='Send CA_PROTO_SEARCH_RESPONSE messages', required=False, action='store_true')

    args = parser.parse_args()
    runner = Runner(args)
    runner.run_server()
    runner.run()
    
