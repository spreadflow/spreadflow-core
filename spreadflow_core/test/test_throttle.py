from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mock import Mock, call
from testtools import TestCase
from twisted.internet import task

from spreadflow_core.scheduler import Scheduler

from spreadflow_core.proc import Throttle

class ThrottleTestCase(TestCase):

    def test_without_initial_delay(self):
        clock = task.Clock()
        scheduler = Mock(spec=Scheduler)

        sut = Throttle(4)
        sut.attach(scheduler, clock)

        msg = 'a message'
        send = Mock(spec=Scheduler.send)
        sut(msg, send)
        self.assertEquals(send.call_count, 1)
        self.assertEquals(send.call_args, call(msg, sut))

        clock.advance(1)

        msg = 'another message, should be dropped'
        send = Mock(spec=Scheduler.send)
        sut(msg, send)
        self.assertEquals(send.call_count, 0)

        clock.advance(3)

        msg = 'a third message'
        send = Mock(spec=Scheduler.send)
        sut(msg, send)
        self.assertEquals(send.call_count, 1)
        self.assertEquals(send.call_args, call(msg, sut))

    def test_with_initial_delay(self):
        clock = task.Clock()
        scheduler = Mock(spec=Scheduler)

        sut = Throttle(99, initial=2)
        sut.attach(scheduler, clock)

        msg = 'an early message, should be dropped'
        send = Mock(spec=Scheduler.send)
        sut(msg, send)
        self.assertEquals(send.call_count, 0)

        clock.advance(2)

        msg = 'another message'
        send = Mock(spec=Scheduler.send)
        sut(msg, send)
        self.assertEquals(send.call_count, 1)
        self.assertEquals(send.call_args, call(msg, sut))
