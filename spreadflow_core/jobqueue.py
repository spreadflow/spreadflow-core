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
        self._wakeup = defer.succeed(self)

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
            :class:`twisted.internet.defer.Deferred` A deferred firing when the
            job completed.
        """
        completed = defer.Deferred()
        self._backlog.append((channel, func, args, kwds, completed))

        if not self._wakeup.called:
            self._wakeup.callback(self)

        return completed

    def next(self):
        """
        Implements :meth:`iterator.next`.
        """
        self._wakeup = defer.Deferred()

        readyidx = None
        for idx, item in enumerate(self._backlog):
            if item[0] not in self._jobs:
                readyidx = idx
                break

        if readyidx != None:
            channel, func, args, kwds, completed = self._backlog.pop(readyidx)

            defered = defer.maybeDeferred(func, *args, **kwds)

            defered.pause()
            self._jobs[channel] = completed
            defered.addBoth(self._job_callback, channel)
            defered.chainDeferred(completed)
            defered.unpause()

        elif not self._wakeup.called:
            return self._wakeup

    def _job_callback(self, result, channel):
        """
        Free up the channel after a job has completed.
        """
        self._jobs.pop(channel)

        if not self._wakeup.called:
            self._wakeup.callback(self)

        return result

