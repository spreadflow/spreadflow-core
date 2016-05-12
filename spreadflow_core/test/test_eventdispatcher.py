from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from mock import Mock
from testtools import TestCase, matchers, twistedsupport
from twisted.internet import defer

from spreadflow_core.eventdispatcher import EventDispatcher, HandlerError, FailMode

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

        # Call prune after removal.
        dispatcher.prune()

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

    def test_dispatch_event(self):
        dispatcher = EventDispatcher()

        test_callback_prio_0_cb_0 = Mock()
        test_callback_prio_0_cb_1 = Mock()
        test_callback_prio_1_cb_0 = Mock()
        other_callback_prio_2_cb_0 = Mock()

        key_prio_0_cb_0 = dispatcher.add_listener(TestEvent, 0, test_callback_prio_0_cb_0)
        key_prio_0_cb_1 = dispatcher.add_listener(TestEvent, 0, test_callback_prio_0_cb_1)
        key_prio_1_cb_0 = dispatcher.add_listener(TestEvent, 1, test_callback_prio_1_cb_0)
        key_prio_2_cb_0 = dispatcher.add_listener(OtherEvent, 2, other_callback_prio_2_cb_0)

        event = TestEvent()
        d = dispatcher.dispatch(event)
        self.assertTrue(d.called)

        test_callback_prio_0_cb_0.assert_called_once_with(event)
        test_callback_prio_0_cb_1.assert_called_once_with(event)
        test_callback_prio_1_cb_0.assert_called_once_with(event)
        self.assertEqual(other_callback_prio_2_cb_0.call_count, 0)

        dispatcher.remove_listener(key_prio_0_cb_1)

        test_callback_prio_0_cb_0.reset_mock()
        test_callback_prio_0_cb_1.reset_mock()
        test_callback_prio_1_cb_0.reset_mock()
        other_callback_prio_2_cb_0.reset_mock()

        event = TestEvent()
        d = dispatcher.dispatch(event)
        self.assertTrue(d.called)

        test_callback_prio_0_cb_0.assert_called_once_with(event)
        self.assertEqual(test_callback_prio_0_cb_1.call_count, 0)
        test_callback_prio_1_cb_0.assert_called_once_with(event)
        self.assertEqual(other_callback_prio_2_cb_0.call_count, 0)

    def test_dispatch_with_fails(self):
        dispatcher = EventDispatcher()

        test_callback_prio_0_cb_0 = Mock()
        test_callback_prio_0_cb_1 = Mock(side_effect=RuntimeError('boom!'))
        test_callback_prio_1_cb_0 = Mock()

        dispatcher.add_listener(TestEvent, 0, test_callback_prio_0_cb_0)
        dispatcher.add_listener(TestEvent, 0, test_callback_prio_0_cb_1)
        dispatcher.add_listener(TestEvent, 1, test_callback_prio_1_cb_0)

        event = TestEvent()
        d = dispatcher.dispatch(event)

        matcher = matchers.AfterPreprocessing(lambda f: f.value, matchers.IsInstance(HandlerError))
        self.assertThat(d, twistedsupport.failed(matcher))

    def test_dispatch_with_return_fails(self):
        dispatcher = EventDispatcher()

        test_callback_prio_0_cb_0 = Mock(return_value='hello')
        test_callback_prio_0_cb_1 = Mock(side_effect=RuntimeError('boom!'))
        test_callback_prio_1_cb_0 = Mock(return_value='world')

        dispatcher.add_listener(TestEvent, 0, test_callback_prio_0_cb_0)
        dispatcher.add_listener(TestEvent, 0, test_callback_prio_0_cb_1)
        dispatcher.add_listener(TestEvent, 1, test_callback_prio_1_cb_0)

        event = TestEvent()
        d = dispatcher.dispatch(event, fail_mode=FailMode.RETURN)

        matcher = matchers.MatchesListwise([
            matchers.MatchesListwise([
                matchers.Equals(0), matchers.MatchesListwise([
                    matchers.Equals((True, 'hello')),
                    matchers.MatchesListwise([
                        matchers.Equals(False),
                        matchers.AfterPreprocessing(lambda f: f.value, matchers.IsInstance(HandlerError)),
                    ])
                ]),
            ]),
            matchers.MatchesListwise([
                matchers.Equals(1), matchers.MatchesListwise([
                    matchers.Equals((True, 'world')),
                ]),
            ]),
        ])

        self.assertThat(d, twistedsupport.succeeded(matcher))

    def test_dispatch_with_args_kwds(self):
        dispatcher = EventDispatcher()

        callback = Mock()
        dispatcher.add_listener(TestEvent, 0, callback, 'hello', 'world', some='kwds', also='here')

        event = TestEvent()
        d = dispatcher.dispatch(event)
        self.assertTrue(d.called)

        callback.assert_called_once_with(event, 'hello', 'world', some='kwds', also='here')
