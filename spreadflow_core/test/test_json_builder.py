# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Unit tests for Json message builder.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core.format import JsonMessageBuilder

class JsonBuilderTestCase(unittest.TestCase):
    """
    Unit tests for JSON line message builder.
    """

    def test_build_message(self):
        """
        Tests JSON line message builder.
        """
        builder = JsonMessageBuilder()

        result = builder.message({'msg': 'hello world'})
        self.assertEquals(b'{"msg": "hello world"}\n', result)

        result = builder.message({'x': 42})
        self.assertEquals(b'{"x": 42}\n', result)
