# -*- coding: utf-8 -*-

"""
Base components.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

COMPONENT_VISITORS = []

class RegisteredComponentFactory(object):
    """
    A decorator for factory functions/methods which calls all visitors for any
    created instance.
    """

    def __init__(self, factory, visitors=None):
        self.factory = factory
        self.visitors = visitors or COMPONENT_VISITORS

    def __call__(self, *args, **kwds):
        inst = self.factory(*args, **kwds)
        for visitor in self.visitors:
            visitor(inst)
        return inst

class RegisteredComponent(object):
    """
    A class decorator for components which are associated with ports but do not
    directly act as a port.
    """

    def __init__(self, visitors=None):
        self.visitors = visitors or COMPONENT_VISITORS

    def __call__(self, klass):
        klass.__new__ = RegisteredComponentFactory(klass.__new__, self.visitors)
        return klass

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

@RegisteredComponent()
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
