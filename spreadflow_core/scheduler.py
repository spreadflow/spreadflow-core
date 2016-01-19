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

        self.log.info('Starting scheduler')
        self._queue_task = task.cooperate(self._queue)
        self._queue_done = self._queue_task.whenDone()
        self._enqueuer = collections.defaultdict(lambda: self._enqueue)

        self.log.debug('Applying {decogen_len} port decorator generators', decogen=self.flowmap.decorators, decogen_len=len(self.flowmap.decorators))
        for decogen in self.flowmap.decorators:
            for proc, decorator in decogen(self, reactor):
                self._enqueuer[proc] = decorator(self._enqueuer[proc])
        self.log.debug('Applied {decogen_len} port decorator generators', decogen=self.flowmap.decorators, decogen_len=len(self.flowmap.decorators))

        flowgraph = self.flowmap.graph()

        is_attachable = lambda p: hasattr(p, 'attach') and callable(p.attach)
        procs = graph.vertices(graph.contract(flowgraph, is_attachable))
        self.log.debug('Attaching {procs_len} sources and services', procs=procs, procs_len=len(procs))
        yield defer.DeferredList([defer.maybeDeferred(p.attach, self, reactor) for p in procs], fireOnOneErrback=True)
        self.log.debug('Attached {procs_len} sources and services', procs=procs, procs_len=len(procs))

        is_startable = lambda p: hasattr(p, 'start') and callable(p.start)
        plan = list(toposort(graph.contract(flowgraph, is_startable)))
        for batch_num, proc_set in enumerate(plan, 1):
            logkwds = {
                'procs': proc_set,
                'procs_len': len(proc_set),
                'batch_num': batch_num,
                'batch_len': len(plan)
            }
            self.log.debug('Starting {procs_len} sources and services ({batch_num}/{batch_len})', **logkwds)
            yield defer.DeferredList([defer.maybeDeferred(p.start) for p in proc_set], fireOnOneErrback=True)
            self.log.debug('Started {procs_len} sources and services ({batch_num}/{batch_len})', **logkwds)

        self.log.info('Started scheduler')

        yield self._done

    def stop(self, reason):
        if not self._stopped:
            self.log.info('Stopping scheduler', reason=reason)
            self._stopped = True
            return self._done.callback(reason)
        else:
            return reason

    def _logfail(self, failure, fmt, *args, **kwds):
        """
        Errback: Logs and consumes a failure.
        """
        self.log.failure(fmt, failure, *args, **kwds)

    @defer.inlineCallbacks
    def join(self):
        # Prevent that new items are enqueued.
        self._stopped = True

        # Cancel all pending jobs.
        self.log.debug('Cancel {pending_len} pending jobs', pending=self._pending, pending_len=len(self._pending))
        _trapcancel = lambda f: f.trap(defer.CancelledError)
        for deferred_job, (port, handler, item, send) in self._pending.items():
            deferred_job.addErrback(_trapcancel)
            deferred_job.addErrback(self._logfail, 'Failed to cancel job', port=port, handler=handler, item=item, send=send)
        for deferred_job in self._pending.keys():
            deferred_job.cancel()

        # Clear the backlog and wait for queue termination.
        self._queue.clear()
        self._queue.stopempty = True
        self.log.debug('Stopping queue')
        yield self._queue_done
        self._pending.clear()
        self.log.debug('Stopped queue')

        flowgraph = self.flowmap.graph()

        # Join sources and services.
        is_joinable = lambda p: hasattr(p, 'join') and callable(p.join)
        plan = list(toposort(graph.reverse(graph.contract(flowgraph, is_joinable))))
        for batch_num, proc_set in enumerate(plan, 1):
            job_list = []
            for proc in proc_set:
                join_job = defer.maybeDeferred(proc.join).addErrback(self._logfail, 'Failed to join proc {proc}', proc=proc)
                job_list.append(join_job)

            logkwds = {
                'procs': proc_set,
                'procs_len': len(proc_set),
                'batch_num': batch_num,
                'batch_len': len(plan)
            }

            self.log.debug('Joining {procs_len} sources and services ({batch_num}/{batch_len})', **logkwds)
            yield defer.DeferredList(job_list)
            self.log.debug('Joined {procs_len} sources and services ({batch_num}/{batch_len})', **logkwds)

        # Detach sources and services.
        is_detachable = lambda p: hasattr(p, 'detach') and callable(p.detach)
        procs = graph.vertices(graph.contract(flowgraph, is_detachable))
        job_list = []
        for proc in procs:
            detach_job = defer.maybeDeferred(proc.detach).addErrback(self._logfail, 'Failed to detach proc {proc}', proc=proc)
            job_list.append(detach_job)

        self.log.debug('Detaching {procs_len} sources and services', procs=procs, procs_len=len(procs))
        yield defer.DeferredList(job_list)
        self.log.debug('Detached {procs_len} sources and services', procs=procs, procs_len=len(procs))

        self._enqueuer = None
        self._queue_done = None
        self._queue_task = None

        self.log.info('Stopped scheduler')
