#---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
#---------------------------------------------------------------------
import unittest
from unittest.mock import patch

import struct
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gateway import Gateway

# To get rid of ResourceWarning warning(This is not important and a known bug in unittest)
# unittest.main(warnings='ignore')

class TestGateway(unittest.TestCase):
    def setUp(self) -> None:
        self.gateway = Gateway([("test.ip.addr", 100, 101)], True)
    
    def tearDown(self) -> None:
        self.gateway = None
        
    def test_ca_proto_search_btyes_package_bytes(self):
        """Check if bytes that is going to broadcast is in correct form"""
        cid = 1
        pv_name = b"test:gateway"
        btyes = b'\x00\x06\x00\x10\x00\x05\x00\x0b\x00\x00\x00\x01\x00\x00\x00\x01test:gateway\x00\x00\x00\x00'
        self.assertEqual(self.gateway.ca_proto_search_bytes(cid, pv_name)[0], btyes)
    
    def test_ca_proto_search_btyes_len(self):
        """Test pv_len, pad_len, payload_len
        """
        cid = 2
        pv_name = b"test:gateway"
        data = self.gateway.ca_proto_search_bytes(cid, pv_name)
        # pv_len
        self.assertEqual(data[1], 12)
        # pad_len
        self.assertEqual(data[2], 4)
        # payload_len
        self.assertEqual(data[3], 16)
        
    def test_ca_proto_search_bytes_non_byte_pv(self):
        """Test with a non byte pv name
        """
        cid = 3
        pv_name = "test:gateway"
        with self.assertRaises(struct.error) as err:
            self.gateway.ca_proto_search_bytes(cid, pv_name)
            
    def test_ca_proto_search_bytes_is_bytes(self):
        """Check whether the returned byte is a byte
        """
        cid = 4
        pv_name = b"test:gateway"
        byte = self.gateway.ca_proto_search_bytes(cid, pv_name)[0]
        self.assertIsInstance(byte, bytes)
        
    def test_ca_proto_search_bytes_fmt(self):
        """Test if fmt is correct
        """
        cid = 5
        pv_name = b"test:4" # pv len = 6, pan_len = 2
        expect_fmt = "!HHHHLL6s2x"
        fmt = self.gateway.ca_proto_search_bytes(cid, pv_name)[-1]
        self.assertEqual(fmt, expect_fmt)
        
    def test_process_ca_proto_search_response_valid_cid(self):
        """With valid cid value"""
        # (1536, 5120, 0, 256, 16777216, 16777216)
        data = b'\x00\x06\x00\x14\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01'
        addr = ('123.1234', 0)
        # socket.ntohl(16777216) == 1
        expect_cid = 1

        result = self.gateway.process_ca_proto_search_response(data, addr, expect_cid)
        self.assertEqual(result, ('123.1234', 0))
        
    def test_process_ca_proto_search_response_invalid_cid(self):
        """With invalid cid value"""
        # (1536, 5120, 0, 256, 16777216, 16777216)
        data = b'\x00\x06\x00\x14\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01'
        addr = ('123.1234', 0)
        # socket.ntohl(16777216) == 1
        expect_cid = 123

        result = self.gateway.process_ca_proto_search_response(data, addr, expect_cid)
        self.assertNotEqual(result, ('123.1234', 0))
    
    def test_process_ca_proto_search_response_invalid_cmd_11(self):
        """With invalid cmd value"""
        # (2816, 5120, 0, 256, 16777216, 16777216)
        data = b'\x00\x0b\x00\x14\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01'
        addr = ('123.1234', 0)
        # socket.ntohl(16777216) == 1
        expect_cid = 1

        result = self.gateway.process_ca_proto_search_response(data, addr, expect_cid)
        self.assertIsNone(result)
    
    def test_process_ca_proto_search_response_invalid_cmd_not_6(self):
        """With invalid cmd value"""
        # (1792, 5120, 0, 256, 16777216, 16777216)
        data = b'\x00\x07\x00\x14\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01'
        addr = ('123.1234', 0)
        # socket.ntohl(16777216) == 1
        expect_cid = 1

        result = self.gateway.process_ca_proto_search_response(data, addr, expect_cid)
        self.assertIsNone(result)
        
    def test_handle_cmd_ca_proto_search_not_in_ready(self):
        """Check if all method calls works as expected"""
        # Create a SearchWorker instance
        test_pv = b"test5678"
        with patch.object(self.gateway, '_search_sock') as mock_search_sock,\
            patch('select.select') as mock_select, patch.object(self.gateway, 'process_ca_proto_search_response') as mock_search_response,\
            patch.object(self.gateway, '_broadcast_addrs', new=["test_addr1"]):
                # Mock the sento return value to pass "sendto_result < 0"
                mock_search_sock.sendto.return_value = 1
                
                # Mock the select return value to pass the "if self._search_sock in ready[0]"
                mock_select.return_value = ([], [], [], 0)
                self.gateway.handle_cmd_ca_proto_search(test_pv, 0)
        
        # make assertion that sendto has been called once with correct arguments
        mock_search_sock.sendto.assert_called_once()
        args, kwargs = mock_search_sock.sendto.call_args
        mock_m, mock_addr = args
        
        expected_m = b'\x00\x06\x00\x10\x00\x05\x00\x0b\x00\x00\x03\xe9\x00\x00\x03\xe9test5678\x00\x00\x00\x00\x00\x00\x00\x00'      
        
        self.assertEqual(mock_m, expected_m, "Test failed: The data(m) is not correct")
        self.assertEqual(mock_addr, "test_addr1", "Test failed: The address(addr) is not correct")
        self.assertEqual(self.gateway._queue_tx._qsize(), 1, "Test failed: the self._queue_tx should contain exactly one item")
        # process_ca_proto_search_response(mock_search_response) should never been called here
        mock_search_response.assert_not_called()

if __name__ == '__main__':
    unittest.main()