from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import namedtuple

from twisted.internet import defer, task
from twisted.logger import Logger

from spreadflow_core.jobqueue import JobQueue
from spreadflow_core.eventdispatcher import FailMode

class Job(object):
    def __init__(self, port, item, send, origin=None, handler=None):
        self.port = port
        self.item = item
        self.send = send
        self.origin = origin
        self.handler = handler or port

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
                and self.__dict__ == other.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return 'Job(port={:s}, item={:s}, send={:s}, origin={:s}, handler={:s})'.format(
            self.port, self.item, self.send, self.origin, self.handler)

JobEvent = namedtuple('JobEvent', ['scheduler', 'job', 'completed'])
AttachEvent = namedtuple('AttachEvent', ['scheduler', 'reactor'])
StartEvent = namedtuple('StartEvent', ['scheduler'])
JoinEvent = namedtuple('JoinEvent', ['scheduler'])
DetachEvent = namedtuple('DetachEvent', ['scheduler'])

class Scheduler(object):
    log = Logger()

    def __init__(self, flowmap, eventdispatcher, cooperate=None):
        if cooperate is None:
            from twisted.internet.task import cooperate

        self.flowmap = flowmap
        self.eventdispatcher = eventdispatcher
        self.cooperate = cooperate
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
            self.stop(reason)
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
        return self._pending.items()

    @defer.inlineCallbacks
    def run(self, reactor=None):
        assert self._queue_task is None and not self._stopped, 'Must not call start() more than once'

        if reactor == None:
            from twisted.internet import reactor

        self.log.info('Starting scheduler')
        self._queue_task = self.cooperate(self._queue)
        self._queue_done = self._queue_task.whenDone()

        self.log.debug('Attaching sources and services')
        yield self.eventdispatcher.dispatch(AttachEvent(scheduler=self, reactor=reactor))
        self.log.debug('Attached sources and services')

        self.log.debug('Starting sources and services')
        yield self.eventdispatcher.dispatch(StartEvent(scheduler=self))
        self.log.debug('Started sources and services')

        self.log.info('Started scheduler')

        reason = yield self._done
        defer.returnValue(reason)

    def stop(self, reason):
        if not self._stopped:
            self.log.info('Stopping scheduler', reason=reason)
            self._stopped = True
            self._done.callback(reason)
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
        event = JoinEvent(scheduler=self)
        yield self.eventdispatcher.dispatch(event, fail_mode=FailMode.RETURN).addCallback(self.eventdispatcher.log_failures, event)
        self.log.debug('Joined sources and services')

        self.log.debug('Detaching sources and services')
        event = DetachEvent(scheduler=self)
        yield self.eventdispatcher.dispatch(event, fail_mode=FailMode.RETURN).addCallback(self.eventdispatcher.log_failures, event)
        self.log.debug('Detached sources and services')

        self._queue_done = None
        self._queue_task = None

        self.log.info('Stopped scheduler')
