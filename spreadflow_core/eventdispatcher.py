# -*- coding: utf-8 -*-
"""Event Dispatcher

Implements an priority queue based event dispatcher.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections
import itertools
import bisect

from twisted.internet import defer
from twisted.logger import Logger

Key = collections.namedtuple('Key', ['priority', 'serial'])
Handler = collections.namedtuple('Handler', ['callback', 'args', 'kwds'])
Entry = collections.namedtuple('Entry', ['key', 'handler'])

class EventDispatcher(object):
    log = Logger()

    def __init__(self):
        self._active = set()
        self._counter = itertools.count()
        self._listeners = {}

    def add_listener(self, event_type, priority, callback, *args, **kwds):
        listeners = self._listeners.setdefault(event_type, [])
        key = Key(priority, next(self._counter))
        bisect.insort(listeners, Entry(key, Handler(callback, args, kwds)))
        self._active.add(key)
        return key

    def remove_listener(self, key):
        self._active.remove(key)

    def get_listeners(self, event_type):
        if event_type in self._listeners:
            keyfunc = lambda entry: entry.key.priority
            filterfunc = lambda entry: entry.key in self._active

            event_listeners = self._listeners[event_type]
            filtered_listeners = itertools.ifilter(filterfunc, event_listeners)
            return itertools.groupby(filtered_listeners, keyfunc)
        else:
            return iter([])

    @defer.inlineCallbacks
    def dispatch(self, event, logfails=False):
        for priority, group in self.get_listeners(type(event)):
            self.log.debug('Calling handlers with priority {priority} for {event}', event=event, priority=priority)

            batch = []
            for key, handler in group:
                d = defer.maybeDeferred(handler.callback, event, *handler.args, **handler.kwds)
                if logfails:
                    d.addErrback(self._logfail, 'Failure ignored while handling event {event} with {handler}', event=event, priority=priority, key=key, handler=handler)
                batch.append(d)

            if len(batch):
                yield defer.DeferredList(batch, fireOnOneErrback=True)

            self.log.debug('Called {n} handlers with priority {priority} for {event}', n=len(batch), event=event, priority=priority)

    def prune(self):
        pruned_listeners = {}
        for event_type, listeners in self._listeners.items():
            listeners = [(key, handler) for key, handler in listeners if key in self._active]
            if len(listeners):
                pruned_listeners[event_type] = listeners

        self._listeners = pruned_listeners

    def _logfail(self, failure, fmt, *args, **kwds):
        """
        Errback: Logs and consumes a failure.
        """
        self.log.failure(fmt, failure, *args, **kwds)
