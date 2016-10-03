from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core import remote


class StrportGeneratorTestCase(unittest.TestCase):

    def test_strport_generator(self):
        """
        Test StrportGeneratorMixin.
        """

        gen = remote.StrportGeneratorMixin()
        result = gen.strport_generate('tcp')
        self.assertEqual(result, 'tcp')

        gen = remote.StrportGeneratorMixin()
        result = gen.strport_generate('tcp', 80)
        self.assertEqual(result, 'tcp:80')

        gen = remote.StrportGeneratorMixin()
        result = gen.strport_generate('tcp', 80, interface='127.0.0.1')
        self.assertEqual(result, 'tcp:80:interface=127.0.0.1')

        gen = remote.StrportGeneratorMixin()
        result = gen.strport_generate('custom', url='http://example.com/?some=param')
        self.assertEqual(result, r'custom:url=http\://example.com/?some\=param')
