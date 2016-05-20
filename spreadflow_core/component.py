# -*- coding: utf-8 -*-

"""
Base components.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

class PortCollection(object):
    """
    Base class for components with separate/multiple input/output ports.
    """

    @property
    def ins(self):
        """
        Return a list of input ports. Default port must be first.
        """
        return []

    @property
    def outs(self):
        """
        Return a list of output ports. Default port must be last.
        """
        return []

class ComponentBase(PortCollection):
    """
    A process with separate/multiple input and output ports.
    """

    @property
    def ins(self):
        return [self]

    @property
    def outs(self):
        return [self]
