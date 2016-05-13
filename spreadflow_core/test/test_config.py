from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os

from tempfile import NamedTemporaryFile
from unittest import TestCase

from spreadflow_core.config import config_eval
from spreadflow_core.flow import Flowmap

class ConfigTestCase(TestCase):

    def test_config_eval(self):
        with NamedTemporaryFile(delete=False) as tmpfile:
            tmpfile.write(b'from spreadflow_core.script import *')

        flowmap = config_eval(tmpfile.name)
        os.unlink(tmpfile.name)

        self.assertIsInstance(flowmap, Flowmap)
