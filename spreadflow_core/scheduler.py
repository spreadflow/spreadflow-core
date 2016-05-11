from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import namedtuple

from twisted.internet import defer, task
from twisted.logger import Logger

from spreadflow_core.jobqueue import JobQueue

class Job(object):
    def __init__(self, port, item, send, origin=None, handler=None):
        self.port = port
        self.item = item
        self.send = send
        self.origin = origin
        self.handler = handler or port

JobEvent = namedtuple('JobEvent', ['scheduler', 'job', 'completed'])
AttachEvent = namedtuple('AttachEvent', ['scheduler', 'reactor'])
StartEvent = namedtuple('StartEvent', ['scheduler'])
JoinEvent = namedtuple('JoinEvent', ['scheduler'])
DetachEvent = namedtuple('DetachEvent', ['scheduler'])

class Scheduler(object):
    log = Logger()

    def __init__(self, flowmap, eventdispatcher):
        self.flowmap = flowmap
        self.eventdispatcher = eventdispatcher
        self._done = defer.Deferred()
        self._pending = {}
        self._queue = JobQueue()
        self._queue_done = None
        self._queue_task = None
        self._stopped = False

    def _job_callback(self, result, completed):
        self._pending.pop(completed)
        return result

    def _job_errback(self, reason, job):
        if not self._stopped:
            self.log.failure('Job failed on {job.port} while processing {job.item}', reason, job=job)
            return self.stop(reason)
        else:
            return reason

    def _enqueue(self, job):
        completed = defer.Deferred()

        defered = self.eventdispatcher.dispatch(JobEvent(scheduler=self, job=job, completed=completed))
        defered.addCallback(lambda ignored, job: self._queue.put(job.port, job.handler, job.item, job.send), job)

        defered.pause()
        self._pending[completed] = job
        defered.addBoth(self._job_callback, completed)
        defered.chainDeferred(completed)
        defered.unpause()

        return completed

    def send(self, item, port_out):
        assert self._queue_task is not None, 'Must call start() before send()'
        if not self._stopped and port_out in self.flowmap:
            port_in = self.flowmap[port_out]

            job = Job(port_in, item, self.send, origin=port_out)
            completed = self._enqueue(job)

            completed.addErrback(self._job_errback, job)

    @property
    def pending(self):
        return self._pending.viewvalues()

    @defer.inlineCallbacks
    def run(self, reactor=None):
        assert self._queue_task is None and not self._stopped, 'Must not call start() more than once'

        if reactor == None:
            from twisted.internet import reactor

        self.log.info('Starting scheduler')
        self._queue_task = task.cooperate(self._queue)
        self._queue_done = self._queue_task.whenDone()

        self.log.debug('Attaching sources and services')
        yield self.eventdispatcher.dispatch(AttachEvent(scheduler=self, reactor=reactor))
        self.log.debug('Attached sources and services')

        self.log.debug('Starting sources and services')
        yield self.eventdispatcher.dispatch(StartEvent(scheduler=self))
        self.log.debug('Started sources and services')

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
        for deferred_job, job in self._pending.items():
            deferred_job.addErrback(_trapcancel)
            deferred_job.addErrback(self._logfail, 'Failed to cancel job', job=job)
        for deferred_job in self._pending.keys():
            deferred_job.cancel()

        # Clear the backlog and wait for queue termination.
        self._queue.clear()
        self._queue.stopempty = True
        self.log.debug('Stopping queue')
        yield self._queue_done
        self._pending.clear()
        self.log.debug('Stopped queue')

        self.log.debug('Joining sources and services')
        yield self.eventdispatcher.dispatch(JoinEvent(scheduler=self), logfails=True)
        self.log.debug('Joined sources and services')

        self.log.debug('Detaching sources and services')
        yield self.eventdispatcher.dispatch(DetachEvent(scheduler=self), logfails=True)
        self.log.debug('Detached sources and services')

        self._queue_done = None
        self._queue_task = None

        self.log.info('Stopped scheduler')
