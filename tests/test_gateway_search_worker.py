#---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
#---------------------------------------------------------------------
import unittest
from unittest.mock import patch, MagicMock, PropertyMock

from queue import PriorityQueue
import struct
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], '..'))

from ca_proto_search_bytes import ca_proto_search_bytes
from gateway_search_worker import SearchWorker

# To get rid of ResourceWarning warning(This is not important and a known bug in unittest)
# unittest.main(warnings='ignore')

class TestGatewaySearchWorker_UDP_package(unittest.TestCase):
    """Test gateway_search_worker"""
    def setUp(self):
        """execute before each test runs"""
        self.q = PriorityQueue()
        self.i = 0
        self.search_worker = SearchWorker(self, self.q, self.i)
        
    def tearDown(self):
        """execute after each test terminates"""
        self.search_worker._search_sock.close()
    
    def increment_response_count(self):
        pass

    def get_rx_queue_size(self):
        pass
    
    def test_ca_proto_search_bytes_package_bytes(self):
        """Check if ca_proto_search_bytes generates correct bytes data"""
        cid = 1
        pv_name = b"test"
        bytes = b'\x00\x00\x00\x00\x00\x01\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x05\x00\x0b\x00\x00\x00\x01\x00\x00\x00\x01test\x00\x00\x00\x00'
        self.assertEqual(ca_proto_search_bytes(cid, pv_name)[0], bytes)
        
    def test_ca_proto_search_bytes_empty_pv(self):
        """Check if ca_proto_search_bytes generates correct bytes data with empty PV name"""
        cid = 11
        pv_name = b""
        bytes = b'\x00\x00\x00\x00\x00\x01\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x05\x00\x0b\x00\x00\x00\x0b\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00'
        self.assertEqual(ca_proto_search_bytes(cid, pv_name)[0], bytes)
    
    def test_ca_proto_search_bytes_cid_0(self):
        """Test with cid = 0"""
        cid = 0
        pv_name = b"test:cid0"
        data = ca_proto_search_bytes(cid, pv_name)
        bytes = b"\x00\x00\x00\x00\x00\x01\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x10\x00\x05\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00test:cid0\x00\x00\x00\x00\x00\x00\x00"
        self.assertEqual(data[0], bytes)
    
    def test_ca_proto_search_bytes_cid_1000(self):
        """Test with cid = 0"""
        cid = 1000
        pv_name = b"test:cid0"
        data = ca_proto_search_bytes(cid, pv_name)
        bytes = b"\x00\x00\x00\x00\x00\x01\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x10\x00\x05\x00\x0b\x00\x00\x03\xe8\x00\x00\x03\xe8test:cid0\x00\x00\x00\x00\x00\x00\x00"
        self.assertEqual(data[0], bytes)
    
    def test_ca_proto_search_bytes_cid_10000(self):
        """Test with cid = 0"""
        cid = 10000
        pv_name = b"test:cid0"
        data = ca_proto_search_bytes(cid, pv_name)
        bytes = b"\x00\x00\x00\x00\x00\x01\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x10\x00\x05\x00\x0b\x00\x00'\x10\x00\x00'\x10test:cid0\x00\x00\x00\x00\x00\x00\x00"
        self.assertEqual(data[0], bytes)
    
    def test_ca_proto_search_bytes_len(self):
        """Test pv_len, pad_len, payload_len
        """
        cid = 2
        pv_name = b"test:1"
        data = ca_proto_search_bytes(cid, pv_name)
        # pv_len
        self.assertEqual(data[1], 6)
        # pad_len
        self.assertEqual(data[2], 2)
        # payload_len
        self.assertEqual(data[3], 8)
    
    def test_ca_proto_search_bytes_len_empty_pv(self):
        """Test pv_len, pad_len, payload_len with empty PV name
        """
        cid = 22
        pv_name = b""
        data = ca_proto_search_bytes(cid, pv_name)
        # pv_len
        self.assertEqual(data[1], 0)
        # pad_len
        self.assertEqual(data[2], 8)
        # payload_len
        self.assertEqual(data[3], 8)
    
    def test_ca_proto_search_bytes_len_large_pv(self):
        """Test pv_len, pad_len, payload_len with PV name length larger than 8
        """
        cid = 22
        pv_name = b"123456789:1234567890" # len = 20
        data = ca_proto_search_bytes(cid, pv_name)
        # pv_len
        self.assertEqual(data[1], 20)
        # pad_len
        self.assertEqual(data[2], 4)
        # payload_len
        self.assertEqual(data[3], 24)
        
    def test_ca_proto_search_bytes_len_8_pv(self):
        """Test pv_len, pad_len, payload_len with PV name length = 8
        """
        cid = 22
        pv_name = b"12345678" # len = 8
        data = ca_proto_search_bytes(cid, pv_name)
        # pv_len
        self.assertEqual(data[1], 8)
        # pad_len
        self.assertEqual(data[2], 8)
        # payload_len
        self.assertEqual(data[3], 16)
    
    def test_ca_proto_search_bytes_non_byte_pv(self):
        """Test with a non byte pv name
        """
        cid = 3
        pv_name = "test:2"
        with self.assertRaises(struct.error) as err:
            ca_proto_search_bytes(cid, pv_name)
    
    def test_ca_proto_search_bytes_is_bytes(self):
        """Check whether the returned data is a byte
        """
        cid = 4
        pv_name = b"test:3"
        byte = ca_proto_search_bytes(cid, pv_name)[0]
        self.assertIsInstance(byte, bytes)
        
    def test_ca_proto_search_bytes_fmt(self):
        """Wrong fmt struct.pack will also cause the malformed package issue
        Test if fmt is correct
        """
        cid = 5
        pv_name = b"test:4" # pv len = 6, pan_len = 2
        # DO NOT change the following expect_fmt
        expect_fmt = "!HHHHLLHHHHLL6s2x"
        fmt = ca_proto_search_bytes(cid, pv_name)[-1]
        self.assertEqual(fmt, expect_fmt)
    
    def test_ca_proto_search_bytes_fmt_empty_pv(self):
        """Wrong fmt struct.pack will also cause the malformed package issue
        Test if fmt is correct with empty PV name
        """
        cid = 5
        pv_name = b""
        # DO NOT change the following expect_fmt
        expect_fmt = "!HHHHLLHHHHLL0s8x"
        fmt = ca_proto_search_bytes(cid, pv_name)[-1]
        self.assertEqual(fmt, expect_fmt)
    
    def test_ca_proto_search_bytes_fmt_len_8_pv(self):
        """Wrong fmt struct.pack will also cause the malformed package issue
        Test if fmt is correct with PV name length = 8
        """
        cid = 5
        pv_name = b"12345678"
        # DO NOT change the following expect_fmt
        expect_fmt = "!HHHHLLHHHHLL8s8x"
        fmt = ca_proto_search_bytes(cid, pv_name)[-1]
        self.assertEqual(fmt, expect_fmt)
    
    # def test_process_ca_proto_search_response_valid_cid(self):
    #     """With valid cid value"""
    #     # (1536, 5120, 0, 256, 16777216, 16777216)
    #     data = b'\x00\x06\x00\x14\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01'
    #     addr = ('123.1234', 0)
    #     # socket.ntohl(16777216) == 1
    #     expect_cid = 1

    #     result = self.search_worker.process_ca_proto_search_response(data, addr, expect_cid)
    #     self.assertEqual(result, ('123.1234', 0))
    
    # def test_process_ca_proto_search_response_invalid_cid(self):
    #     """With invalid cid value"""
    #     # (1536, 5120, 0, 256, 16777216, 16777216)
    #     data = b'\x00\x06\x00\x14\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01'
    #     addr = ('123.1234', 0)
    #     # socket.ntohl(16777216) == 1
    #     expect_cid = 123

    #     result = self.search_worker.process_ca_proto_search_response(data, addr, expect_cid)
    #     self.assertNotEqual(result, ('123.1234', 0))
    
    # def test_process_ca_proto_search_response_invalid_cmd_11(self):
    #     """With invalid cmd value"""
    #     # (2816, 5120, 0, 256, 16777216, 16777216)
    #     data = b'\x00\x0b\x00\x14\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01'
    #     addr = ('123.1234', 0)
    #     # socket.ntohl(16777216) == 1
    #     expect_cid = 1

    #     result = self.search_worker.process_ca_proto_search_response(data, addr, expect_cid)
    #     self.assertIsNone(result)
    
    # def test_process_ca_proto_search_response_invalid_cmd_not_6(self):
    #     """With invalid cmd value"""
    #     # (1792, 5120, 0, 256, 16777216, 16777216)
    #     data = b'\x00\x07\x00\x14\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01'
    #     addr = ('123.1234', 0)
    #     # socket.ntohl(16777216) == 1
    #     expect_cid = 1

    #     result = self.search_worker.process_ca_proto_search_response(data, addr, expect_cid)
    #     self.assertIsNone(result)
        
    
    def test_handle_cmd_ca_proto_search(self):
        """Check if all method calls works as expected"""
        # Create a SearchWorker instance
        test_pv = b"test5678"
        with patch.object(self.search_worker, '_search_sock') as mock_search_sock,\
            patch('select.select') as mock_select, patch.object(self.search_worker, 'process_ca_proto_search_response') as mock_search_response,\
            patch.object(self.search_worker, '_broadcast_addrs', new=["test_addr1"]):
                # Mock the sento return value to pass "sendto_result < 0"
                mock_search_sock.sendto.return_value = 1
                
                # Mock the select return value to pass the "if self._search_sock in ready[0]"
                mock_select.return_value = ([], [], [], 0)
                self.search_worker.handle_cmd_ca_proto_search(0, test_pv)
        
        # make assertion that sendto has been called once with correct arguments
        mock_search_sock.sendto.assert_called_once()
        args, kwargs = mock_search_sock.sendto.call_args
        mock_m, mock_addr = args
        
        expected_m = b'\x00\x00\x00\x00\x00\x01\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x10\x00\x05\x00\x0b\x00\x00\x03\xe9\x00\x00\x03\xe9test5678\x00\x00\x00\x00\x00\x00\x00\x00'      
        
        self.assertEqual(mock_m, expected_m, "Test failed: The data(m) is not correct")
        self.assertEqual(mock_addr, "test_addr1", "Test failed: The address(addr) is not correct")
        self.assertEqual(self.search_worker._queue_tx._qsize(), 1, "Test failed: the self._queue_tx should contain exactly one item")

if __name__ == '__main__':
    unittest.main()