from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mock import Mock
from testtools import matchers, twistedsupport #, TestCase, run_test_with
from testtools.assertions import assert_that
from unittest import TestCase
#from twisted.trial.unittest import TestCase

from twisted.internet import defer, task

from spreadflow_core.eventdispatcher import EventDispatcher
from spreadflow_core.scheduler import Scheduler, Job, JobEvent, AttachEvent, StartEvent, JoinEvent, DetachEvent
from spreadflow_core.test.matchers import MatchesInvocation

defer.setDebugging(True)

def _no_termination_factory():
    return lambda: False

def _port_callback(item, send):
    pass

class MatchesEvent(matchers.MatchesAll):
    def __init__(self, event_type, *args, **kwds):
        super(MatchesEvent, self).__init__(
            matchers.IsInstance(event_type),
            matchers.MatchesListwise(*args) if args else matchers.Always(),
            matchers.MatchesStructure(**kwds) if kwds else matchers.Always(),
        )



class SchedulerTestCase(TestCase):

    epsilon = task._EPSILON

    def setUp(self):
        super(SchedulerTestCase, self).setUp()

        self.clock = task.Clock()

        self.cooperator = task.Cooperator(
            terminationPredicateFactory=_no_termination_factory,
            scheduler=lambda x: self.clock.callLater(self.epsilon, x)
        )
        self.cooperate = self.cooperator.cooperate

        self.dispatcher = EventDispatcher()
        self.flowmap = dict()

        self.scheduler = Scheduler(self.flowmap, self.dispatcher, self.cooperate)

    def test_run_scheduler(self):
        """
        Tests run(), stop() and join() and ensures that appropriate events are fired.
        """
        job_handler = Mock()
        attach_handler = Mock()
        start_handler = Mock()
        join_handler = Mock()
        detach_handler = Mock()

        self.dispatcher.add_listener(JobEvent, 0, job_handler)
        self.dispatcher.add_listener(AttachEvent, 0, attach_handler)
        self.dispatcher.add_listener(StartEvent, 0, start_handler)
        self.dispatcher.add_listener(JoinEvent, 0, join_handler)
        self.dispatcher.add_listener(DetachEvent, 0, detach_handler)

        # Start the scheduler.
        run_deferred = self.scheduler.run(self.clock)
        assert_that(run_deferred, twistedsupport.has_no_result())

        self.assertEquals(job_handler.call_count, 0)
        attach_handler.assert_called_once_with(AttachEvent(scheduler=self.scheduler, reactor=self.clock))
        start_handler.assert_called_once_with(StartEvent(scheduler=self.scheduler))
        self.assertEquals(join_handler.call_count, 0)
        self.assertEquals(detach_handler.call_count, 0)

        job_handler.reset_mock()
        attach_handler.reset_mock()
        start_handler.reset_mock()
        join_handler.reset_mock()
        detach_handler.reset_mock()

        # Turn a bit on the clock.
        self.clock.advance(self.epsilon)
        assert_that(run_deferred, twistedsupport.has_no_result())

        self.assertEquals(job_handler.call_count, 0)
        self.assertEquals(attach_handler.call_count, 0)
        self.assertEquals(start_handler.call_count, 0)
        self.assertEquals(join_handler.call_count, 0)
        self.assertEquals(detach_handler.call_count, 0)

        job_handler.reset_mock()
        attach_handler.reset_mock()
        start_handler.reset_mock()
        join_handler.reset_mock()
        detach_handler.reset_mock()

        # Stop the scheduler.
        self.scheduler.stop('some reason')
        assert_that(run_deferred, twistedsupport.succeeded(matchers.Equals('some reason')))

        self.assertEquals(job_handler.call_count, 0)
        self.assertEquals(attach_handler.call_count, 0)
        self.assertEquals(start_handler.call_count, 0)
        self.assertEquals(join_handler.call_count, 0)
        self.assertEquals(detach_handler.call_count, 0)

        job_handler.reset_mock()
        attach_handler.reset_mock()
        start_handler.reset_mock()
        join_handler.reset_mock()
        detach_handler.reset_mock()

        # Stop the queue and join processes.
        join_deferred = self.scheduler.join()
        self.clock.advance(self.epsilon)
        assert_that(join_deferred, twistedsupport.succeeded(matchers.Always()))

        self.assertEquals(job_handler.call_count, 0)
        self.assertEquals(attach_handler.call_count, 0)
        self.assertEquals(start_handler.call_count, 0)
        join_handler.assert_called_once_with(JoinEvent(scheduler=self.scheduler))
        detach_handler.assert_called_once_with(DetachEvent(scheduler=self.scheduler))

    def test_run_job(self):
        """
        Tests send() and ensure that the job-event is fired.
        """
        job_handler = Mock()
        self.dispatcher.add_listener(JobEvent, 0, job_handler)

        port_out = object()
        port_in = Mock(spec=_port_callback)
        self.flowmap[port_out] = port_in

        self.scheduler.run(self.clock)
        self.scheduler.send('some item', port_out)

        expected_job = Job(port_in, 'some item', self.scheduler.send, port_out)
        self.assertEquals(job_handler.call_count, 1)
        assert_that(job_handler.call_args, MatchesInvocation(
            MatchesEvent(JobEvent,
                         scheduler=matchers.Equals(self.scheduler),
                         job=matchers.Equals(expected_job),
                         completed=twistedsupport.has_no_result())
        ))
        self.assertEquals(port_in.call_count, 0)

        self.assertEquals(len(list(self.scheduler.pending)), 1)

        # Trigger queue run.
        self.clock.advance(self.epsilon)

        port_in.assert_called_once_with('some item', self.scheduler.send)

        self.assertEquals(len(list(self.scheduler.pending)), 0)

    def test_fail_job(self):
        """
        Tests that scheduler is stopped whenever a port is failing.
        """
        port_out = object()
        port_in = Mock(spec=_port_callback, side_effect=RuntimeError('failed!'))
        self.flowmap[port_out] = port_in

        run_deferred = self.scheduler.run(self.clock)
        self.scheduler.send('some item', port_out)

        expected_message = 'Job failed on {:s} while processing {:s}'.format(
            port_in, 'some item')

        from testtools.twistedsupport._runtest import _NoTwistedLogObservers
        with _NoTwistedLogObservers():
            with twistedsupport.CaptureTwistedLogs() as twisted_logs:
                # Trigger queue run.
                self.clock.advance(self.epsilon)
                assert_that(twisted_logs.getDetails(), matchers.MatchesDict({
                    'twisted-log': matchers.AfterPreprocessing(
                        lambda log: log.as_text(), matchers.Contains(expected_message))
                }))

        port_in.assert_called_once_with('some item', self.scheduler.send)

        matcher = matchers.AfterPreprocessing(lambda f: f.value, matchers.IsInstance(RuntimeError))
        assert_that(run_deferred, twistedsupport.failed(matcher))

    def test_cancel_job(self):
        """
        Tests that scheduler is stopped whenever a port is failing.
        """
        port_out = object()
        port_in = Mock(spec=_port_callback)
        self.flowmap[port_out] = port_in

        run_deferred = self.scheduler.run(self.clock)
        self.scheduler.send('some item', port_out)

        self.assertEquals(len(list(self.scheduler.pending)), 1)

        self.scheduler.stop('bye!')

        self.assertEquals(len(list(self.scheduler.pending)), 1)

        join_deferred = self.scheduler.join()
        self.clock.advance(self.epsilon)
        assert_that(join_deferred, twistedsupport.succeeded(matchers.Always()))
        assert_that(run_deferred, twistedsupport.succeeded(matchers.Equals('bye!')))

        self.assertEquals(len(list(self.scheduler.pending)), 0)

        self.assertEquals(port_in.call_count, 0)
