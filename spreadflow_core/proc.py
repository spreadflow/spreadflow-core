from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from collections import Mapping

from twisted.internet import defer, task
from twisted.logger import Logger, LogLevel

from spreadflow_core.flow import ComponentBase, PortCollection, ComponentCollection


class SyntheticSource(object):
    def __init__(self, items):
        self.items = items

    def attach(self, scheduler, reactor):
        for delay, item in self.items:
            reactor.callLater(delay, scheduler.send, item, self)

    def detach(self):
        pass

    def __call__(self, item, send):
        send(item, self)


class DebugLog(object):
    """
    A minimal processor which simply logs every item received.
    """

    log = Logger()

    def __init__(self, message='Item received: {item}', level='debug'):
        self.level = LogLevel.levelWithName(level)
        self.message = message

    def __call__(self, item, send):
        self.log.emit(self.level, self.message, item=item)
        send(item, self)


class Compound(PortCollection, ComponentCollection):
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

    @property
    def children(self):
        return list(self._children)


class Duplicator(ComponentBase):
    """
    A processor capable of sending messages to another flow.
    """

    def __init__(self):
        self.out_duplicate = object()

    def __call__(self, item, send):
        send(copy.deepcopy(item), self.out_duplicate)
        send(item, self)

    @property
    def outs(self):
        return [self.out_duplicate, self]


class Sleep(object):
    """
    A processor which delays every incomming message by the specified amount.

    When sleeping, no other incoming message is accepted. Use a throttle before
    this component in order to avoid queue overflow.
    """

    sleep = None

    def __init__(self, delay):
        self.delay = delay

    def attach(self, scheduler, reactor):
        self.sleep = lambda delay: task.deferLater(reactor, delay, lambda: self)

    def detach(self):
        self.sleep = None

    @defer.inlineCallbacks
    def __call__(self, item, send):
        assert self.sleep, 'Must call attach() before'
        yield self.sleep(self.delay)

        send(item, self)


class Throttle(object):
    """
    A processor which forwards only one incomming message per time period.
    """

    last = None
    now = None

    def __init__(self, delay, initial=0):
        self.delay = delay
        self.initial = initial

    def attach(self, scheduler, reactor):
        self.now = reactor.seconds
        self.last = self.now() - self.delay + self.initial

    def detach(self):
        self.now = None
        self.last = None

    def __call__(self, item, send):
        assert self.now, 'Must call attach() before'

        now = self.now()
        if now - self.last >= self.delay:
            self.last = now
            send(item, self)
