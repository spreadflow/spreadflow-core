# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Integration tests for spreadflow twistd application runner.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import fixtures
import os
import subprocess
import unittest

FIXTURE_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures')

class SpreadflowTwistdIntegrationTestCase(unittest.TestCase):
    """
    Integration tests for spreadflow twistd application runner.
    """

    def test_oneshot(self):
        """
        Process should exit with a zero result.
        """

        config = os.path.join(FIXTURE_DIRECTORY, 'spreadflow-simple.conf')
        with fixtures.TempDir() as fix:
            rundir = fix.path
            argv = ['-n', '-d', rundir, '-c', config, '-o']
            proc = subprocess.Popen(['spreadflow-twistd'] + argv,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            stdout_value, stderr_value = proc.communicate()
            self.assertEqual(proc.returncode, 0)

    def test_exit_on_failure(self):
        """
        Process should exit with a non-zero result as soon as a process fails.
        """

        config = os.path.join(FIXTURE_DIRECTORY, 'spreadflow-fail-proc.conf')
        with fixtures.TempDir() as fix:
            rundir = fix.path
            argv = ['-n', '-d', rundir, '-c', config]
            proc = subprocess.Popen(['spreadflow-twistd'] + argv,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            stdout_value, stderr_value = proc.communicate()
            self.assertEqual(proc.returncode, 1)
