# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Unit tests for Pickle message parser.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core.format import PickleMessageParser

class PickleParserTestCase(unittest.TestCase):
    """
    Unit tests for Pickle message parser.
    """

    def test_parse_messages(self):
        """
        Tests Pickle message parser.
        """
        parser = PickleMessageParser()

        msg1 = b"I35\n.(dp0\nS'msg'\np1\nS'hello world'\np2\ns."
        msg2 = b"I19\n.(dp0\nS'x'\np1\nI42\ns."

        parser.push(msg1)
        self.assertEquals([{'msg': 'hello world'}], list(parser.messages()))

        parser.push(msg2)
        self.assertEquals([{'x': 42}], list(parser.messages()))

    def test_parse_partial_msgs(self):
        """
        Tests that Pickle message parser accepts partial messages.
        """
        parser = PickleMessageParser()

        msg = b''.join([
            b"I35\n.(dp0\nS'msg'\np1\nS'hello world'\np2\ns.",
            b"I19\n.(dp0\nS'x'\np1\nI42\ns.",
        ])

        chunk_size = 8

        actual_messages = []
        for pos in range(0, len(msg), chunk_size):
            parser.push(msg[pos:pos+chunk_size])
            for parsed_message in parser.messages():
                actual_messages.append(parsed_message)

        self.assertEquals([{'msg': 'hello world'}, {'x': 42}], actual_messages)

    def test_buffer_exceeded(self):
        """
        Tests that Pickle message parser raises an exception when its buffer
        limit is exceeded.
        """
        parser = PickleMessageParser(8)

        msg = b"I35\n.(dp0\nS'msg'\np1\nS'hello world'\np2\ns."

        self.assertRaises(RuntimeError, parser.push, msg)
