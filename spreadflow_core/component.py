# -*- coding: utf-8 -*-

"""
Base components.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

class Compound(object):
    """
    A process wrapping other processes.
    """

    def __init__(self, children):
        assert len(children) == len(set(children)), 'Members must be unique'
        self._children = children
