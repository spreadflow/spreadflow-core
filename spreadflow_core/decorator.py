from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from twisted.internet import defer, threads
from twisted.logger import Logger

from spreadflow_core import graph


def DecoratorGenerator(decorator, predicate):
    def _generate(scheduler, reactor):
        for v in graph.vertices(scheduler.flowmap.graph(), predicate):
            yield v, decorator

    return _generate


class JobDebugDecorator(object):
    log = Logger()

    def __call__(self, enqueue):
        def _decorated_enqueue(port, handler, item, send):
            return enqueue(port, handler, item, send).addErrback(self._job_debug_errback, port, handler, item)
        return _decorated_enqueue

    def _job_debug_errback(self, failure, port, handler, item):
        if not failure.check(defer.CancelledError):
            self.log.debug('Job failed {port} with {failure} while processing {item}', failure=failure, port=port, item=item)
        return failure


class OneshotDecorator(object):

    queue = None
    _done = defer.Deferred()

    def __call__(self, enqueue):
        def _decorated_enqueue(port, handler, item, send):
            return enqueue(port, handler, item, send).addCallback(self._check_queue)
        return _decorated_enqueue

    def _check_queue(self, result):
        if len(self.queue) == 0:
            self._done.callback(self)

    def watch(self, queue):
        self.queue = queue
        return self._done


class OneshotDecoratorGenerator(object):
    def __call__(self, scheduler, reactor):
        decorator = OneshotDecorator()
        decorator.watch(scheduler.pending).chainDeferred(scheduler.done)
        flowgraph = scheduler.flowmap.graph()
        for v in graph.vertices(flowgraph):
            yield v, decorator


class ThreadpoolDecorator(object):
    def __init__(self, reactor):
        threadpool = reactor.getThreadPool()
        self._dump_stats = threadpool.dumpStats
        self._defer_to_thread = lambda f, *args, **kw: threads.deferToThreadPool(reactor, threadpool, f, *args, **kw)
        self._call_from_thread = lambda f, *args, **kw: threads.blockingCallFromThread(reactor, f, *args, **kw)

    def __call__(self, enqueue):
        def _decorated_enqueue(port, handler, item, send):
            handle_in_thread = lambda *args, **kw: self._defer_to_thread(handler, *args, **kw)
            send_from_thread = lambda *args, **kw: self._call_from_thread(send, *args, **kw)
            return enqueue(port, handle_in_thread, item, send_from_thread)
        return _decorated_enqueue


class ThreadpoolDecoratorGenerator(object):
    def __call__(self, scheduler, reactor):
        is_blocking = lambda port: hasattr(port, 'blocking') and port.blocking
        flowgraph = scheduler.flowmap.graph()
        for port in graph.vertices(flowgraph, is_blocking):
            yield port, ThreadpoolDecorator(reactor)
