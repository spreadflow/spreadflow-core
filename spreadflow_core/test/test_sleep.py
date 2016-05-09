from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mock import Mock, call
from testtools import TestCase
from twisted.internet import task

from spreadflow_core.scheduler import Scheduler

from spreadflow_core.proc import Sleep

class SleepTestCase(TestCase):

    def test_sleep(self):
        clock = task.Clock()
        scheduler = Mock(spec=Scheduler)

        sut = Sleep(4)
        sut.attach(scheduler, clock)

        msg1 = 'a message'
        send1 = Mock(spec=Scheduler.send)
        d1 = sut(msg1, send1)
        self.assertEquals(send1.call_count, 0)
        self.assertFalse(d1.called)

        clock.advance(1)

        self.assertEquals(send1.call_count, 0)
        self.assertFalse(d1.called)

        msg2 = 'a second message'
        send2 = Mock(spec=Scheduler.send)
        d2 = sut(msg2, send2)
        self.assertEquals(send2.call_count, 0)
        self.assertFalse(d1.called)

        clock.advance(3)

        self.assertEquals(send1.call_count, 1)
        self.assertEquals(send1.call_args, call(msg1, sut))
        self.assertTrue(d1.called)

        self.assertEquals(send2.call_count, 0)

        clock.advance(4)

        self.assertEquals(send2.call_count, 1)
        self.assertEquals(send2.call_args, call(msg2, sut))
        self.assertTrue(d2.called)
