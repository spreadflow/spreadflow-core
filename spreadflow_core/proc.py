from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import copy

from collections import Mapping
from datetime import datetime, timedelta

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
    The delay is calculated relative to the timestamp on the incoming message.
    """

    sleep = None

    def __init__(self, delay=5, msgdate=None):
        self.delay = timedelta(seconds=delay)
        self.msgdate = msgdate if msgdate else self.msgdate_default

    def attach(self, dispatcher, reactor):
        self.sleep = lambda delay: task.deferLater(reactor, delay, lambda: self)

    def detach(self):
        self.sleep = None

    def msgdate_default(self, item, now):
        return item.get('date', now) if isinstance(item, Mapping) else now

    @defer.inlineCallbacks
    def __call__(self, item, send):
        assert self.sleep, 'Must call attach() before'
        now = datetime.now()
        delay = (item['date'] + self.delay - datetime.now()).total_seconds()
        if delay > 0:
            yield self.sleep(delay)

        send(item, self)
