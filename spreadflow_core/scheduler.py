from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import namedtuple

from twisted.internet import defer
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
            str(self.port), str(self.item), str(self.send),
            str(self.origin), str(self.handler))

Entry = namedtuple('Entry', ['deferred', 'job'])

JobEvent = namedtuple('JobEvent', ['scheduler', 'job', 'completed'])
AttachEvent = namedtuple('AttachEvent', ['scheduler', 'reactor'])
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
        self._detached = False

    def _job_callback(self, result, completed):
        self._pending.pop(completed)
        return result

    def _job_errback(self, reason, job):
        if not self._stopped:
            self.log.failure('Job failed on {job.port} while processing {job.item}', reason, job=job)
            self.stop(reason)
        else:
            return reason

    def _job_cancel(self, entry):
        subtask, _ = self._pending[entry.deferred]
        subtask.cancel()

    def _enqueue(self, job):
        completed = defer.Deferred(lambda dfr: self._job_cancel(Entry(dfr, job)))

        defered = self.eventdispatcher.dispatch(JobEvent(scheduler=self, job=job, completed=completed))
        defered.addCallback(lambda ignored, job: self._queue.put(job.port, job.handler, job.item, job.send), job)

        defered.pause()
        self._pending[completed] = Entry(defered, job)
        defered.addBoth(self._job_callback, completed)
        defered.chainDeferred(completed)
        defered.unpause()

        return completed

    def send(self, item, port_out):
        assert not self._detached, 'Must not send() any items after ports have been detached'
        if port_out in self.flowmap:
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

        self.log.debug('Attaching sources and services')
        yield self.eventdispatcher.dispatch(AttachEvent(scheduler=self, reactor=reactor))
        self.log.debug('Attached sources and services')

        self.log.debug('Starting queue')
        self._queue_task = self.cooperate(self._queue)
        self._queue_done = self._queue_task.whenDone()
        self.log.debug('Started queue')

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
        # Prevent that any queued items are run.
        self._stopped = True
        self._queue_task.pause()

        self.log.debug('Detaching sources and services')
        event = DetachEvent(scheduler=self)
        yield self.eventdispatcher.dispatch(event, fail_mode=FailMode.RETURN).addCallback(self.eventdispatcher.log_failures, event)
        self.log.debug('Detached sources and services')

        # Prevent that new items are enqueued.
        self._detached = True

        # Clear the backlog and wait for queue termination.
        self.log.debug('Cancel {pending_len} pending jobs', pending=self._pending, pending_len=len(self._pending))
        _trapcancel = lambda f: f.trap(defer.CancelledError)
        for deferred_job, (_, job) in self._pending.items():
            deferred_job.addErrback(_trapcancel)
            deferred_job.addErrback(self._logfail, 'Failed to cancel job', job=job)
        for deferred_job in list(self._pending.keys()):
            deferred_job.cancel()

        self.log.debug('Stopping queue')
        self._queue.stopempty = True
        self._queue_task.resume()
        yield self._queue_done
        self.log.debug('Stopped queue')

        self._queue_done = None
        self._queue_task = None

        self.log.info('Stopped scheduler')
