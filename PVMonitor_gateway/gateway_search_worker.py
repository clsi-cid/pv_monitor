#---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
#---------------------------------------------------------------------
import json
import logging
import pickle
import queue
import select
import socket
import struct
import threading
import time
import sys

sys.path.insert(1, '..')

from PVMonitor_gateway.ca_proto_search_bytes import ca_proto_search_bytes
from PVMonitor_gateway.definitions_gw import CMD_UDP, PRIORITY
from utilities_gw.print_binary import print_binary
from utilities_gw.utils import get_ip_key

log = logging.getLogger("GatewayLogger")


class SearchWorker(object):
    """
    Search worker
    """

    def __init__(self, parent, queue_tx, index):

        self._debug = False
        self._parent = parent
        self._index = index
        self._cid = 1000
        self._search_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._search_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._search_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._search_sock.setblocking(0)

        if self._index is None:
            self._index = 0

        self._queue_tx = queue_tx
        self._queue_rx = queue.PriorityQueue()

        self._broadcast_addrs = []
        self._skip_gateway_keys = []
        self._skip_gateway_addrs = []
        self._skip_archiver_keys = []

        self._cmd_info = socket.htonl(CMD_UDP.MSG_INFO)
        self._cmd_err = socket.htonl(CMD_UDP.MSG_ERR)
        self._cmd_proto_search_resp = socket.htonl(CMD_UDP.CA_PROTO_SEARCH_RESP)
        self._cmd_stats = socket.htonl(CMD_UDP.STATS)

        self._timer_worker = threading.Timer(1, self.worker)

    def set_debug(self, value):
        self._debug = value

    def get_queue_size(self):
        return self._queue_rx._qsize()

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

    def set_archiver_addrs(self, addrs):
        for addr in addrs:
            ip_key = get_ip_key(addr)
            log.debug(
                "Skip CA_PROTO_SEARCH from ARCHIVER addr %s (%d)",
                addr,
                ip_key,
                
            )
            self._skip_gateway_keys.append(ip_key)

    def set_broadcast_addrs(self, addrs):
        self._broadcast_addrs = addrs

    def start(self):
        self._timer_worker.start()

    def search(self, item):
        serialized = pickle.dumps(item)
        self._queue_rx.put_nowait(serialized)

    def worker(self):

        handler_map = {
            # 179463055
            socket.htonl(
                CMD_UDP.DO_CA_PROTO_SEARCH
            ): self.handle_cmd_ca_proto_search,
            # 3258740635
            socket.htonl(
                CMD_UDP.DO_CA_PROTO_SEARCH_NO_RESP
            ): self.handle_cmd_ca_proto_search_no_resp,
        }

        while True:

            try:
                serialized = self._queue_rx.get(block=True, timeout=10)
                item = pickle.loads(serialized)

            except queue.Empty:
                continue

            priority = item[0]
            msg = item[1]
            cmd = msg[0]
            data = msg[3]

            handler = handler_map.get(cmd)
            if not handler:
                raise ValueError("Unknown command: %X" % cmd)

            handler(priority, data)

    def handle_cmd_ca_proto_search_no_resp(self, priority, pv_name):
        return

    def handle_cmd_ca_proto_search(self, priority, pv_name):

        result = []

        # Rate limit number of CA_PROTO_SEARCH packets this app generates
        # There is a sleep in the timeout check below....

        try:
            if not isinstance(pv_name, bytes):
                pv_name = pv_name.encode("utf-8")
        except Exception as err:
            log.exception(
                "Exception searching for PV: %r",
                err,
                
            )
            return

        self._cid += 1

        if self._cid > 100000000:
            self._cid = 1000

        m = ca_proto_search_bytes(self._cid, pv_name)[0]
        if self._debug:
            print_binary(m)

        # Check for any old packets first
        while True:
            ready = select.select([self._search_sock], [], [], 0)
            if self._search_sock not in ready[0]:
                break
            _, _ = self._search_sock.recvfrom(100000)

        # Send the CA_PROTO_SEARCH packets out on all interfaces
        for addr in self._broadcast_addrs:
            log.debug("Sending to addr: %r", addr)
            sendto_result = self._search_sock.sendto(m, addr)

            if sendto_result < 0:
                msg = "Error sending CA_PROTO_SEARCH for '%s'" % pv_name
                log.error(msg)
                self.send_msg_err(msg)
                return

        # Allow time for the responses to come in
        time.sleep(1.0)

        while True:
            ready = select.select([self._search_sock], [], [], 0)

            if self._search_sock in ready[0]:
                data, rx_addr = self._search_sock.recvfrom(100000)

                self._parent.increment_response_count()
                addr = self.process_ca_proto_search_response(
                    data, rx_addr, self._cid
                ) 
                    
                log.debug("GOT ADDR: %r, %r", rx_addr, addr)

                if addr:
                    result.append(addr)

            else:
                log.debug(
                    "WORKER %d: Got %d responses (%s)",
                    self._index, len(result), pv_name,
                    
                )
                
                break


        log.debug("RESULT %r", result)

            # Returns an empty result list if there are no responses

        # Bytes object is not JSON serializable
        pv_name = pv_name.decode("utf-8")
        data = json.dumps({"p": pv_name, "s": result})

        # Use the "ip_addr" field to return the size of the rx_queue so that
        # the client knows how many CA_PROTO_SEARCH requests it can send

        qsize = self._parent.get_rx_queue_size()
        serialized = pickle.dumps(
            (priority, (self._cmd_proto_search_resp, data, qsize, 0))
        )
        self._queue_tx.put_nowait(serialized)

    def process_response_error(self, data, addr, expect_cid):

        try:
            c = struct.unpack_from("HHHHII", data)
            cmd = socket.ntohs(c[0])
            payload_len = socket.ntohs(c[1])

            ## reserved1 = socket.ntohs(c[2])
            ## reserved2 = socket.ntohs(c[3])
            cid = socket.ntohl(c[4])
            error_code = socket.ntohl(c[5])

            if cmd != 11:
                result = "Unxpected error command: %r != 11" % cmd
                log.error(result)
                self.send_msg_err(result)

            result = (
                "CA_PROTO_ERROR: cmd: %r payload_len: %r cid: %r error_code: %r"
                % (cmd, payload_len, cid, error_code)
            )

            log.error(result)
            self.send_msg_err(result)

        except Exception as err:
            result = "Exception decoding error msg: %r" % err
            log.exception(result)
            self.send_msg_err(result)

    def process_ca_proto_search_response(self, data, addr, expect_cid):

        ip_addr = addr[0]
        if ip_addr in self._skip_gateway_addrs:
            return

        data_len = len(data)

        # Why are we seeing a bunch of responses with length = 40 and not 24?
        # breem: 2021_08_10... could we be getting CA_PROTO_VERSION messages
        # in the response like we needed to add to the CA_PROTO_SEARCH
        # datagrams for the EPICS V7 apps?
        if data_len == 40:
            data = data[16:]
            
        c = struct.unpack_from("HHHHII", data)
        cmd = socket.ntohs(c[0])

        if cmd == 11:
            self.process_response_error(data, addr, expect_cid)
            return

        if cmd != 6:
            msg = "process_response: command %d is not 6-> returning" % cmd
            self.send_msg_info(msg)
            log.error(msg)
            return

        ## payload_len = socket.ntohs(c[1])
        port = socket.ntohs(c[2])
        ## count = socket.ntohs(c[3])
        ## sid = socket.ntohl(c[4])
        cid = socket.ntohl(c[5])

        if cid != expect_cid:
            msg = "CID mismatch: %d != %d" % (cid, expect_cid)
            self.send_msg_info(msg)
            log.error(msg)
            return

        return (addr[0], port)

        ## print "CA_PROTO_SEARCH", command, payload_len, port, count, sid, cid

    def process_response_error(self, data, addr, expect_cid):

        try:
            c = struct.unpack_from("HHHHII", data)
            cmd = socket.ntohs(c[0])
            payload_len = socket.ntohs(c[1])

            ## reserved1 = socket.ntohs(c[2])
            ## reserved2 = socket.ntohs(c[3])
            cid = socket.ntohl(c[4])
            error_code = socket.ntohl(c[5])

            if cmd != 11:
                result = "Unxpected error command: %r != 11" % cmd
                log.error(result)
                self.send_msg_err(result)

            result = (
                "CA_PROTO_ERROR: cmd: %r payload_len: %r cid: %r error_code: %r"
                % (cmd, payload_len, cid, error_code)
            )

            log.error(result)
            self.send_msg_err(result)

        except Exception as err:
            result = "Exception decoding error msg: %r" % err
            log.exception(result)
            self.send_msg_err(result)

    def send_msg_err(self, msg):
        if self._debug:
            log.debug(msg)
        else:
            serialized = pickle.dumps(
                (PRIORITY.HIGH, (self._cmd_err, msg, 0, 0))
            )
            self._queue_tx.put_nowait(serialized)

    def send_msg_info(self, msg):
        if self._debug:
            log.debug(msg)
        else:
            serialized = pickle.dumps(
                (PRIORITY.HIGH, (self._cmd_info, msg, 0, 0))
            )
            self._queue_tx.put_nowait(serialized)


class Parent(object):
    def increment_response_count(self):
        pass

    def get_rx_queue_size(self):
        return 0


def test1():
    # from local_epics import BCAST_ADDRESSES_EPICSGW_51_2
    from PVMonitor_gateway.definitions_gw import BCAST_ADDRESS_GW, PORT

    broadcast_addrs = [(IP, PORT.CA_PROTO_SEARCH) for IP in BCAST_ADDRESS_GW]
    queue_tx = queue.PriorityQueue()

    parent = Parent()
    worker = SearchWorker(parent, queue_tx, None)
    worker.set_debug(True)
    worker.set_broadcast_addrs(broadcast_addrs)

    worker.handle_cmd_ca_proto_search(PRIORITY.HIGH, "TRG2400:cycles")
    worker.handle_cmd_ca_proto_search(
        PRIORITY.HIGH, "QEM1408-01:QEM:Current1:Acquire"
    )
    worker.handle_cmd_ca_proto_search(
        PRIORITY.HIGH, "PDTR1607-8-I10-01:det1:NumImages_RBV"
    )


if __name__ == "__main__":
    test1()
