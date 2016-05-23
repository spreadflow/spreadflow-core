# -*- coding: utf-8 -*-

"""
Site customize module for subprocess coverage.

.. seealso:: `Coverage.py -- Measuring sub-processes
    <http://coverage.readthedocs.io/en/latest/subprocess.html>`_
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

try:
    import coverage
    coverage.process_startup()
except ImportError:
    pass
