# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Unit tests for Pickle message builder.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core.format import PickleMessageBuilder

class PickleBuilderTestCase(unittest.TestCase):
    """
    Unit tests for Pickle message builder.
    """

    def test_build_message(self):
        """
        Tests Pickle message builder.
        """
        builder = PickleMessageBuilder(2)

        result = builder.message({'msg': 'hello world'})
        self.assertEquals(b'\x80\x02K#.\x80\x02}q\x00X\x03\x00\x00\x00msgq\x01X\x0b\x00\x00\x00hello worldq\x02s.', result)

        result = builder.message({'x': 42})
        self.assertEquals(b'\x80\x02K\x11.\x80\x02}q\x00X\x01\x00\x00\x00xq\x01K*s.', result)
