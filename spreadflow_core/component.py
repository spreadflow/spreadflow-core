# -*- coding: utf-8 -*-

"""
Base components.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections
from spreadflow_core.dsl.context import Context, NoContextError
from spreadflow_core.dsl.tokens import ComponentToken

class RegisteredComponentFactory(object):
    """
    A decorator for factory functions/methods which calls all visitors for any
    created instance.
    """

    def __init__(self, factory, context=None):
        self.factory = factory
        self.context = context

    def __call__(self, *args, **kwds):
        inst = self.factory(*args, **kwds)
        if self.context is None:
            try:
                context = Context.top()
            except NoContextError:
                pass
            else:
                context.add(ComponentToken(inst))
        return inst

class RegisteredComponent(object):
    """
    A class decorator for components which are associated with ports but do not
    directly act as a port.
    """

    def __init__(self, context=None):
        self.context = context

    def __call__(self, klass):
        bound_new = klass.__new__
        wrapped_new = lambda cls, *args, **kwds: bound_new(cls)
        klass.__new__ = RegisteredComponentFactory(wrapped_new, self.context)
        return klass

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

    def __contains__(self, port):
        return port in self.outs or port in self.ins

@RegisteredComponent()
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
