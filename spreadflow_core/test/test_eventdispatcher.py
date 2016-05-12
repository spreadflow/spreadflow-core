from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mock import Mock
from testtools import TestCase

from spreadflow_core.eventdispatcher import EventDispatcher

class TestEvent(object):
    pass

class OtherEvent(object):
    pass

class EventDispatcherTestCase(TestCase):

    def test_manage_listeners(self):
        dispatcher = EventDispatcher()

        listeners = list(dispatcher.get_listeners(TestEvent))
        self.assertEqual(len(listeners), 0)

        test_callback_prio_0_cb_0 = Mock()
        test_callback_prio_0_cb_1 = Mock()
        test_callback_prio_1_cb_0 = Mock()
        other_callback_prio_2_cb_0 = Mock()

        # Register callbacks, first priority 1 ...
        key_prio_1_cb_0 = dispatcher.add_listener(TestEvent, 1, test_callback_prio_1_cb_0)

        # ... aftwerwards priority 0 ...
        key_prio_0_cb_0 = dispatcher.add_listener(TestEvent, 0, test_callback_prio_0_cb_0)
        key_prio_0_cb_1 = dispatcher.add_listener(TestEvent, 0, test_callback_prio_0_cb_1)

        # ... and finally priority 2 for another event.
        key_prio_2_cb_0 = dispatcher.add_listener(OtherEvent, 2, other_callback_prio_2_cb_0)

        # Collect callbacks from listeners list.
        actual_handlers = []
        for priority, group in dispatcher.get_listeners(TestEvent):
            callbacks = [handler.callback for key, handler in group]
            actual_handlers.append((priority, callbacks))

        expected_handlers = [
            (0, [test_callback_prio_0_cb_0, test_callback_prio_0_cb_1]),
            (1, [test_callback_prio_1_cb_0]),
        ]

        self.assertEqual(expected_handlers, actual_handlers)

        # Remove one listener.
        dispatcher.remove_listener(key_prio_0_cb_0)

        # Collect callbacks from listeners list.
        actual_handlers = []
        for priority, group in dispatcher.get_listeners(TestEvent):
            callbacks = [handler.callback for key, handler in group]
            actual_handlers.append((priority, callbacks))

        expected_handlers = [
            (0, [test_callback_prio_0_cb_1]),
            (1, [test_callback_prio_1_cb_0]),
        ]

        self.assertEqual(expected_handlers, actual_handlers)
