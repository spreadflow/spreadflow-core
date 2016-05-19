# -*- coding: utf-8 -*-
"""Cooperative job queue.

Provides a job queue (and iterator) specifically designed for the twisted
cooperative multitasking facilities.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections

from twisted.internet import defer

class QueueNoneReady(Exception):
    pass

Job = collections.namedtuple("Job", ["channel", "func", "args", "kwds"])
Entry = collections.namedtuple("Entry", ["deferred", "job"])

class JobQueue(collections.Iterator):
    """Cooperative job queue.

    A job queue (and iterator) specifically designed for the twisted
    cooperative multitasking facilities.

    A job is any callable together with positional and keyword arguments.
    Results returned by a job are passed back to the caller by a deferred. If
    the job itself returns a deferred, any queued jobs on the same channel are
    blocked until a result becomes available.

    Example:

        The following example illustrates how the
        :class:`spreadflow_core.jobqueue.JobQueue` can be used together with
        :func:`twisted.internet.task.cooperate`. Note that this function
        returns an instance of :class:`twisted.internet.task.CooperativeTask`.
        It can be used to pause, resume and stop the queue::

            from __future__ import print_function
            from twisted.internet import defer, task, reactor
            from spreadflow_core.jobqueue import JobQueue

            def say(message):
                '''
                Prints a message and returns immediately.
                '''
                print(message)

            def pause(seconds):
                '''
                Returns a deferred which fires after the specified amount of
                time.
                '''
                d = defer.Defered()
                reactor.callLater(seconds, d.success, None)
                return d

            def stop(result):
                reactor.stop()

            queue = JobQueue()
            queue_task = task.cooperate(queue)

            queue.put('channel one', say, 'hello')
            queue.put('channel one', pause, 2)
            queue.put('channel one', say, 'world!').addCallback(done)

            queue.put('channel two', pause, 1)
            queue.put('channel two', say, 'what?')

            reactor.run()

        This example will generate the following output, pausing for one second
        between every line::

            hello
            what?
            world!

    """

    def __init__(self):
        self._backlog = []
        self._jobs = {}
        self._wakeup = None
        self._stopempty = False

    def put(self, channel, func, *args, **kwds):
        """
        Queue up a job for later execution on a specified channel.

        Args:
            channel: Any hashable value representing a channel. Jobs sent to
                the same channel are executed in FIFO sequence.
            func (callable): The function to call upon job execution.
            *args: Positional parameters passed to the function upon job
                execution.
            **kwds: Keyword parameters passed to the function upon job
                execution.

        Returns:
            :class:`twisted.internet.defer.Deferred`: A deferred firing when the
            job completed.
        """
        job = Job(channel, func, args, kwds)
        completed = defer.Deferred(lambda dfr: self._job_cancel(Entry(dfr, job)))

        self._backlog.append(Entry(completed, job))
        self._wake()

        return completed

    def get(self):
        """
        Removes and returns the first item in the queue where the channel is
        ready.

        Returns
            tuple: A 5-tuple containing the channel, func, args, kwds, deferred

        Raises:
            spreadflow_core.jobqueue.QueueNoneReady: Raised if there is either
                no item ready or no channel.
        """
        active_channels = set([job.channel for _, job in self._jobs.values()])
        for idx, (_, job) in enumerate(self._backlog):
            if job.channel not in active_channels:
                return self._backlog.pop(idx)

        raise QueueNoneReady()

    def clear(self):
        """
        Clears the backlog.
        """
        self._backlog = []

    @property
    def stopempty(self):
        """
        bool: True if iterator should stop if both, the backlog gets empty and
            there are no more pending jobs.
        """
        return self._stopempty

    @stopempty.setter
    def stopempty(self, value):
        self._stopempty = bool(value)
        self._wake()

    def __next__(self):
        """
        Implements :meth:`iterator.__next__` (Python >= 3)
        """
        if self.stopempty and len(self._backlog) == 0 and len(self._jobs) == 0:
            raise StopIteration()

        try:
            completed, job = self.get()
        except QueueNoneReady:
            if not self._wakeup:
                self._wakeup = defer.Deferred()
        else:
            defered = defer.maybeDeferred(job.func, *job.args, **job.kwds)

            defered.pause()
            self._jobs[completed] = Entry(defered, job)
            defered.addBoth(self._job_callback, completed)
            defered.chainDeferred(completed)
            defered.unpause()

        return self._wakeup

    def next(self):
        """
        Implements :meth:`iterator.__next__` (Python < 3)
        """
        return self.__next__()

    def _wake(self):
        """
        Run the wakeup callback in order to signal that there are jobs waiting
        in the backlog.
        """
        if self._wakeup:
            self._wakeup.callback(self)
            self._wakeup = None

    def _job_callback(self, result, completed):
        """
        Free up the channel after a job has completed.
        """
        self._jobs.pop(completed)
        self._wake()
        return result

    def _job_cancel(self, entry):
        try:
            subtask, _ = self._jobs[entry.deferred]
        except KeyError:
            pass
        else:
            subtask.cancel()
            return

        try:
            self._backlog.remove(entry)
        except ValueError:
            pass
        else:
            return

        assert 0, "Failed to cancel a job which is neither in backlog nor in jobs"
