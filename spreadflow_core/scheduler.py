from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections

from twisted.internet import defer, task
from twisted.logger import Logger

from spreadflow_core.jobqueue import JobQueue

class Scheduler(object):
    log = Logger()

    def __init__(self, flowmap, eventdispatcher):
        self.flowmap = flowmap
        self.eventdispatcher = eventdispatcher
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

        # FIXME: Reimplement decorators as events
        self.log.debug('Applying {decogen_len} port decorator generators', decogen=self.flowmap.decorators, decogen_len=len(self.flowmap.decorators))
        for decogen in self.flowmap.decorators:
            for proc, decorator in decogen(self, reactor):
                self._enqueuer[proc] = decorator(self._enqueuer[proc])
        self.log.debug('Applied {decogen_len} port decorator generators', decogen=self.flowmap.decorators, decogen_len=len(self.flowmap.decorators))

        yield self.eventdispatcher.dispatch('attach', {'scheduler': self, 'reactor': reactor})

        yield self.eventdispatcher.dispatch('start')

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

        yield self.eventdispatcher.dispatch('join', logfails=True)

        yield self.eventdispatcher.dispatch('deattach', logfails=True)

        self._enqueuer = None
        self._queue_done = None
        self._queue_task = None

        self.log.info('Stopped scheduler')
