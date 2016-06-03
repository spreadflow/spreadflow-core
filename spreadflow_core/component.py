# -*- coding: utf-8 -*-

"""
Base components.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections

class PortCollection(collections.Container):
    """
    Base class for components with separate/multiple input/output ports.
    """

    @property
    def ins(self):
        """
        Return a list of input ports. Default port must be first.
        """
        raise NotImplementedError()

    @property
    def outs(self):
        """
        Return a list of output ports. Default port must be last.
        """
        raise NotImplementedError()

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

    def __contains__(self, port):
        return port in self.outs or port in self.ins

class Compound(PortCollection):
    """
    A process wrapping other processes.
    """

    def __init__(self, children):
        assert len(children) == len(set(children)), 'Members must be unique'
        self._children = children

    @property
    def ins(self):
        ports = []
        for member in self._children:
            if isinstance(member, PortCollection):
                ports.extend(member.ins)
            else:
                ports.append(member)
        return ports

    @property
    def outs(self):
        ports = []
        for member in self._children:
            if isinstance(member, PortCollection):
                ports.extend(member.outs)
            else:
                ports.append(member)
        return ports

    def __contains__(self, port):
        return port in self.outs or port in self.ins
