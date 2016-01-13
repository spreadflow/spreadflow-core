# -*- coding: utf-8 -*-
"""Tests for cooperative job queue.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from twisted.internet import defer
from twisted.trial import unittest

from spreadflow_core.jobqueue import JobQueue


class JobQueueTestCase(unittest.TestCase):

    def test_returns_deferred_when_empty(self):
        """
        A :class:`twisted.internet.defer.Deferred` is returned from
        :meth:`spreadflow_core.jobqueue.JobQueue.next` if queue is empty.
        """
        queue = JobQueue()

        queue_ready_1 = queue.next()
        self.assertIsInstance(queue_ready_1, defer.Deferred)

        queue_ready_2 = queue.next()
        self.assertIsInstance(queue_ready_2, defer.Deferred)

        self.assertEqual(queue_ready_1, queue_ready_2)
        self.assertFalse(queue_ready_1.called)

    def test_fires_deferred_when_ready(self):
        """
        A deferred returned from :meth:`spreadflow_core.jobqueue.JobQueue.next`
        is fired after a new job is added to the queue.
        """
        channel = object()
        queue = JobQueue()

        queue_ready_1 = queue.next()
        self.assertIsInstance(queue_ready_1, defer.Deferred)
        self.assertFalse(queue_ready_1.called)

        job_completed = queue.put(channel, lambda: 'Bazinga!')
        job_completed.addCallback(lambda result: self.assertEqual(result, 'Bazinga!'))

        # Scheduling a job must result in the deferred being called.
        self.assertTrue(queue_ready_1.called)

        # Scheduling a job must in direct execution.
        self.assertFalse(job_completed.called)

        # Iterating the queue should obviously execute the job.
        queue_ready_2 = queue.next()
        self.assertTrue(job_completed.called)

        # After executing one job, the queue must not return a deferred in
        # order to signal the cooperator task that it wants to run again
        # immediately.
        self.assertIsNone(queue_ready_2)

        # Should return a new deferred if the queue gets empty again.
        queue_ready_3 = queue.next()
        self.assertNotEqual(queue_ready_1, queue_ready_3)
        self.assertFalse(queue_ready_3.called)

    def test_iterates_in_fifo_order(self):
        """
        Jobs are executed in FIFO order.
        """
        results = []
        def job(*args, **kwds):
            results.append((args, kwds))

        channel = object()
        queue = JobQueue()

        self.assertEqual(len(results), 0)
        queue.put(channel, job, 'first', arg='one')
        queue.put(channel, job, 'second')
        queue.put(channel, job)
        queue.put(channel, job, 'fourth', 'additional', 'positional', plus='some', key='words')

        queue_ready = []
        for times in range(4):
            self.assertEqual(len(results), times)
            queue_ready = queue.next()
            self.assertIsNone(queue_ready)

        self.assertEqual(results, [
            (('first',), {'arg': 'one'}),
            (('second',), {}),
            ((), {}),
            (('fourth', 'additional', 'positional'), {'plus': 'some', 'key': 'words'})
        ])

        queue_ready = queue.next()
        self.assertIsInstance(queue_ready, defer.Deferred)
