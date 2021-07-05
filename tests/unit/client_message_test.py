# coding: utf-8
import unittest
import uuid

from hazelcast import six
from hazelcast.connection import _Reader
from hazelcast.errors import _ErrorsCodec
from hazelcast.protocol import ErrorHolder
from hazelcast.protocol.builtin import (
    CodecUtil,
    FixSizedTypesCodec,
    ByteArrayCodec,
    DataCodec,
    EntryListCodec,
    StringCodec,
    EntryListUUIDListIntegerCodec,
    EntryListUUIDLongCodec,
    ListMultiFrameCodec,
    ListIntegerCodec,
    ListLongCodec,
    ListUUIDCodec,
    MapCodec,
)
from hazelcast.protocol.client_message import *
from hazelcast.protocol.codec import client_authentication_codec
from hazelcast.protocol.codec.custom.error_holder_codec import ErrorHolderCodec
from hazelcast.serialization.data import Data


class OutboundMessageTest(unittest.TestCase):
    def test_header_fields(self):
        # 6 bytes for the length + flags + 4 bytes message type + 8 bytes correlation id + 4 bytes partition id
        buf = bytearray(22)
        message = OutboundMessage(buf, False)
        self.assertFalse(message.retryable)
        message.set_correlation_id(42)
        message.set_partition_id(23)

        correlation_id = LE_LONG.unpack_from(message.buf, 6 + 4)[0]
        partition_id = LE_INT.unpack_from(message.buf, 6 + 4 + 8)[0]
        self.assertEqual(42, correlation_id)
        self.assertEqual(42, message.get_correlation_id())
        self.assertEqual(23, partition_id)

    def test_copy(self):
        buf = bytearray(range(20))
        message = OutboundMessage(buf, True)

        copy = message.copy()
        self.assertTrue(copy.retryable)
        buf[0] = 99
        self.assertEqual(99, message.buf[0])
        self.assertEqual(0, copy.buf[0])  # should be a deep copy


BEGIN_FRAME = Frame(bytearray(0), 1 << 12)
END_FRAME = Frame(bytearray(), 1 << 11)


class InboundMessageTest(unittest.TestCase):
    def test_fast_forward(self):
        message = InboundMessage(BEGIN_FRAME.copy())

        # New custom-typed parameter with its own begin and end frames
        message.add_frame(BEGIN_FRAME.copy())
        message.add_frame(Frame(bytearray(0), 0))
        message.add_frame(END_FRAME.copy())

        message.add_frame(END_FRAME.copy())

        # begin frame
        message.next_frame()
        CodecUtil.fast_forward_to_end_frame(message)
        self.assertFalse(message.has_next_frame())


class EncodeDecodeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reader = _Reader(None)
