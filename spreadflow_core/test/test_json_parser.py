# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Unit tests for JSON lines message parser.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core.format import JsonMessageParser

class JsonParserTestCase(unittest.TestCase):
    """
    Unit tests for Json lines message parser.
    """

    def test_parse_messages(self):
        """
        Tests Json lines message parser.
        """
        parser = JsonMessageParser()

        msg1 = b'{"msg": "hello world"}\n'
        msg2 = b'{"x": 42}\n'

        parser.push(msg1)
        self.assertEquals([{'msg': 'hello world'}], list(parser.messages()))

        parser.push(msg2)
        self.assertEquals([{'x': 42}], list(parser.messages()))

    def test_parse_partial_msgs(self):
        """
        Tests that Json lines message parser accepts partial messages.
        """
        parser = JsonMessageParser()

        msg = b''.join([
            b'{"msg": "hello world"}\n',
            b'{"x": 42}\n',
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
        Tests that Json lines message parser raises an exception when its
        buffer limit is exceeded.
        """
        parser = JsonMessageParser(8)

        msg = b'{"msg": "hello world"}\n'

        self.assertRaises(RuntimeError, parser.push, msg)
