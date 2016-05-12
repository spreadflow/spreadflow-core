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

try:
    from builtins import filter as ifilter
except ImportError:
    from itertools import ifilter

from twisted.internet import defer
from twisted.logger import Logger

Key = collections.namedtuple('Key', ['priority', 'serial'])
Handler = collections.namedtuple('Handler', ['callback', 'args', 'kwds'])
Entry = collections.namedtuple('Entry', ['key', 'handler'])

class FailMode(object):
    RAISE = 0
    RETURN = 1

class HandlerError(Exception):
    def __init__(self, handler, wrapped_failure):
        self.handler = handler
        self.wrapped_failure = wrapped_failure

    def __repr__(self):
        return 'HandlerError[{:s}, {:s}]'.format(self.handler, self.wrapped_failure.value)

    def __str__(self):
        return 'HandlerError[{:s}, {:s}]'.format(self.handler, self.wrapped_failure)

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
            filtered_listeners = ifilter(filterfunc, event_listeners)
            return itertools.groupby(filtered_listeners, keyfunc)
        else:
            return iter([])

    @defer.inlineCallbacks
    def dispatch(self, event, fail_mode=FailMode.RAISE):
        results = []

        fire_on_one_errback = (fail_mode == FailMode.RAISE)

        for priority, group in self.get_listeners(type(event)):
            self.log.debug('Calling handlers with priority {priority} for {event}', event=event, priority=priority)

            handlers = [handler for key, handler in group]
            batch = [defer.maybeDeferred(handler.callback, event, *handler.args, **handler.kwds).addErrback(self._wrap_handler_error, handler) for handler in handlers]

            if len(batch):
                dl = defer.DeferredList(batch, consumeErrors=True, fireOnOneErrback=fire_on_one_errback)
                if fire_on_one_errback:
                    dl.addErrback(self._trap_first_error, handlers)
                group_results = yield dl
                results.append((priority, group_results))

            self.log.debug('Called {n} handlers with priority {priority} for {event}', n=len(batch), event=event, priority=priority)

        defer.returnValue(results)

    def prune(self):
        pruned_listeners = {}
        for event_type, listeners in self._listeners.items():
            listeners = [entry for entry in listeners if entry.key in self._active]
            if len(listeners):
                pruned_listeners[event_type] = listeners

        self._listeners = pruned_listeners

    def log_failures(self, result, event):
        for priority, group_results in result:
            for success, result in group_results:
                if not success:
                    if result.check(HandlerError):
                        self.log.failure('Failure while handling event {event} with {handler}', result, event=event, priority=priority, handler=result.value.handler)
                    else:
                        self.log.failure('Failure while handling event {event}', result, event=event, priority=priority)
        return result

    def _wrap_handler_error(self, failure, handler):
        raise HandlerError(handler, failure)

    def _trap_first_error(self, failure, handlers):
        failure.trap(defer.FirstError)
        if isinstance(failure.value.subFailure, HandlerError):
            raise failure.value.subFailure
        else:
            raise HandlerError(handlers[failure.value.index], failure.value.subFailure)
