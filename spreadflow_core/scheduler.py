from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections

from toposort import toposort
from twisted.internet import defer, task
from twisted.logger import Logger

from spreadflow_core import graph
from spreadflow_core.jobqueue import JobQueue

class Scheduler(object):
    log = Logger()

    def __init__(self, flowmap):
        self.flowmap = flowmap
        self._done = defer.Deferred()
        self._enqueuer = None
        self._pending = {}
        self._queue = JobQueue()
        self._queue_done = None
        self._queue_task = None
        self._stopped = False

    def _job_callback(self, result, completed):
        self._pending.pop(completed)
        return result

    def _job_errback(self, reason, port, item):
        if not self._stopped:
            self.log.failure('Job failed on {port} while processing {item}', reason, port=port, item=item)
            return self.stop(reason)
        else:
            return reason

    def _enqueue(self, port, handler, item, send):
        completed = self._queue.put(port, handler, item, send)
        self._pending[completed] = (port, handler, item, send)
        completed.addBoth(self._job_callback, completed)
        return completed

    def send(self, item, port_out):
        assert self._enqueuer != None, 'Must call start() before send()'
        if not self._stopped and port_out in self.flowmap:
            port_in = self.flowmap[port_out]
            completed = self._enqueuer[port_in](port_in, port_in, item, self.send)
            completed.addErrback(self._job_errback, port_in, item)

    @property
    def pending(self):
        return self._pending.viewvalues()

    @defer.inlineCallbacks
    def run(self, reactor=None):
        assert self._enqueuer == None and not self._stopped, 'Must not call start() more than once'

        if reactor == None:
            from twisted.internet import reactor

        self._queue_task = task.cooperate(self._queue)
        self._queue_done = self._queue_task.whenDone()
        self._enqueuer = collections.defaultdict(lambda: self._enqueue)

        for decogen in self.flowmap.decorators:
            for proc, decorator in decogen(self, reactor):
                self._enqueuer[proc] = decorator(self._enqueuer[proc])

        flowgraph = self.flowmap.graph()

        is_attachable = lambda p: hasattr(p, 'attach') and callable(p.attach)
        procs = graph.vertices(graph.contract(flowgraph, is_attachable))
        yield defer.DeferredList([defer.maybeDeferred(p.attach, self, reactor) for p in procs], fireOnOneErrback=True)

        is_startable = lambda p: hasattr(p, 'start') and callable(p.start)
        plan = toposort(graph.contract(flowgraph, is_startable))
        for proc_set in plan:
            yield defer.DeferredList([defer.maybeDeferred(p.start) for p in proc_set], fireOnOneErrback=True)

        yield self._done

    def stop(self, reason):
        if not self._stopped:
            self._stopped = True
            return self._done.callback(reason)
        else:
            return reason

    @defer.inlineCallbacks
    def join(self):
        # Prevent that new items are enqueued.
        self._stopped = True

        # Cancel all pending jobs.
        _trapcancel = lambda f: f.trap(defer.CancelledError)
        _logfail = lambda f, port, handler, item, send: self.log.failure('Failed to cancel job', f, port=port, item=item, handler=handler, send=send)
        for deferred_job, (port, handler, item, send) in self._pending.items():
            deferred_job.addErrback(_trapcancel)
            deferred_job.addErrback(_logfail, port, handler, item, send)
        for deferred_job in self._pending.keys():
            deferred_job.cancel()

        # Clear the backlog and wait for queue termination.
        self._queue.clear()
        self._queue.stopempty = True
        yield self._queue_done
        self._pending.clear()

        flowgraph = self.flowmap.graph()

        # Join sources and services.
        is_joinable = lambda p: hasattr(p, 'join') and callable(p.join)
        plan = toposort(graph.reverse(graph.contract(flowgraph, is_joinable)))
        for proc_set in plan:
            job_list = []
            for proc in proc_set:
                _logfail = lambda f, proc=proc: self.log.failure('Failed to join proc {proc}', f, proc=proc)
                join_job = defer.maybeDeferred(proc.join).addErrback(_logfail)
                job_list.append(join_job)

            yield defer.DeferredList(job_list)

        is_detachable = lambda p: hasattr(p, 'detach') and callable(p.detach)
        procs = graph.vertices(graph.contract(flowgraph, is_detachable))
        job_list = []
        for proc in procs:
            _logfail = lambda f, proc=proc: self.log.failure('Failed to detach proc {proc}', f, proc=proc)
            detach_job = defer.maybeDeferred(proc.detach).addErrback(_logfail)
            job_list.append(detach_job)
        yield defer.DeferredList(job_list)

        self._enqueuer = None
        self._queue_done = None
        self._queue_task = None
