# -*- coding: utf-8 -*-
"""Event Dispatcher

Implements an priority queue based event dispatcher.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections
import heapq
import itertools

from twisted.internet import defer
from twisted.logger import Logger

Key = collections.namedtuple('Key', ['priority', 'serial'])
Entry = collections.namedtuple('Entry', ['key', 'callback'])

class EventDispatcher(object):
    log = Logger()

    def __init__(self):
        self._active = set()
        self._counter = itertools.count()
        self._listeners = {}

    def add_listener(self, event_type, priority, callback):
        listeners = self._listeners.setdefault(event_type, [])
        key = Key(priority, next(self._counter))
        heapq.heappush(listeners, Entry(key, callback))
        self._active.add(key)
        return key

    def remove_listener(self, key):
        self._active.remove(key)

    @defer.inlineCallbacks
    def dispatch(self, event, logfails=False):
        event_type = type(event)
        if event_type in self._listeners:
            listeners = self._listeners[event_type]
            keyfunc = lambda entry: entry.key.priority

            for priority, group in itertools.groupby(listeners, keyfunc):
                batch = []

                self.log.debug('Calling handlers with priority {priority} for {event}', event=event, priority=priority)
                for key, callback in group:
                    if key in self._active:
                        d = defer.maybeDeferred(callback, event)
                        if logfails:
                            d.addErrback(self._logfail, 'Failure ignored while handling event {event}', event=event)
                        batch.append(d)

                if len(batch):
                    yield defer.DeferredList(batch, fireOnOneErrback=True)

                self.log.debug('Called {n} handlers with priority {priority} for {event}', n=len(batch), event=event, priority=priority)

    def prune(self):
        pruned_listeners = {}
        for event_type, listeners in self._listeners.items():
            listeners = [(key, callback) for key, callback in listeners if key in self._active]
            if len(listeners):
                pruned_listeners[event_type] = listeners

        self._listeners = pruned_listeners

    def _logfail(self, failure, fmt, *args, **kwds):
        """
        Errback: Logs and consumes a failure.
        """
        self.log.failure(fmt, failure, *args, **kwds)
