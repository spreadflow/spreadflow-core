# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Tests for the flowmap
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core.dsl.context import Context, NoContextError
from spreadflow_core.dsl.stream import \
    AddTokenOp, \
    DuplicateTokenError, \
    NoSuchTokenError, \
    RemoveTokenOp, \
    SetDefaultTokenOp, \
    stream_divert, \
    stream_extract, \
    token_attr_map, \
    token_map

class ContextTestCase(unittest.TestCase):
    """
    Unit tests for compiler Context.
    """

    def test_push_pop(self):
        """
        Covers Context.push(), Context.pop(), Context.top()
        """
        ctx = Context(self)
        Context.push(ctx)
        self.assertIs(ctx, Context.top())
        Context.pop(ctx)

    def test_contextmanager(self):
        """
        Covers context manager interface.
        """
        with Context(self) as ctx:
            self.assertIs(ctx, Context.top())

            with Context(self) as ctx2:
                self.assertIsNot(ctx2, ctx)
                self.assertIs(ctx2, Context.top())

            self.assertIs(ctx, Context.top())

    def test_token_operations(self):
        """
        Covers ctx.setdefault(), ctx.add() and ctx.remove()
        """
        with Context(self) as ctx:
            ctx.setdefault('foo')
            ctx.add('bar')
            ctx.remove('baz')

        self.assertListEqual(ctx.tokens, [
            SetDefaultTokenOp('foo'),
            AddTokenOp('bar'),
            RemoveTokenOp('baz')
        ])

    def test_raises_no_context(self):
        """
        Raises an error if the context stack is empty.
        """
        self.assertRaises(NoContextError, Context.top)

    def test_raises_unbalanced_context(self):
        """
        Raises if context push and pop calls are not balanced.
        """
        ctx1 = Context(self)
        ctx2 = Context(self)

        Context.push(ctx1)
        Context.push(ctx2)
        self.assertRaises(AssertionError, Context.pop, ctx1)


    def test_raises_push_invalid(self):
        """
        Raises if non-context are pushed.
        """
        self.assertRaises(AssertionError, Context.push, self)

class StreamTestCase(unittest.TestCase):
    """
    Unit tests for stream manipulation functions.
    """

    def test_stream_extract(self):
        """
        Covers stream_extract()
        """
        class T1(object):
            pass

        class T2(object):
            pass

        tokens = [
            SetDefaultTokenOp(T1()),
            SetDefaultTokenOp(T2()),
            AddTokenOp(T1()),
            AddTokenOp(T2()),
            RemoveTokenOp(T1()),
            RemoveTokenOp(T2()),
        ]

        stream = iter(tokens)
        t1_stream, orig_stream = stream_extract(stream, T1)

        expected_t1_tokens = [
            tokens[0],
            tokens[2],
            tokens[4]
        ]

        self.assertListEqual(list(t1_stream), expected_t1_tokens)
        self.assertListEqual(list(orig_stream), tokens)

    def test_stream_divert(self):
        """
        Covers stream_extract()
        """
        class T1(object):
            pass

        class T2(object):
            pass

        tokens = [
            SetDefaultTokenOp(T1()),
            SetDefaultTokenOp(T2()),
            AddTokenOp(T1()),
            AddTokenOp(T2()),
            RemoveTokenOp(T1()),
            RemoveTokenOp(T2()),
        ]

        stream = iter(tokens)
        t1_stream, remaining_stream = stream_divert(stream, T1)

        expected_t1_tokens = [
            tokens[0],
            tokens[2],
            tokens[4]
        ]

        expected_remaining_tokens = [
            tokens[1],
            tokens[3],
            tokens[5]
        ]

        self.assertListEqual(list(t1_stream), expected_t1_tokens)
        self.assertListEqual(list(remaining_stream), expected_remaining_tokens)

class TokenMapTestCase(unittest.TestCase):
    """
    Unit tests for token mapping.
    """

    def test_token_map_default(self):
        """
        Add a token to the stream using the default-token operation.
        """
        class T1(object):
            def __init__(self, key, value):
                self.key = key
                self.value = value

        t = T1('foo', 'bar')
        tokens = [
            SetDefaultTokenOp(t)
        ]

        self.assertDictEqual(token_map(tokens), {t: t})

    def test_token_map_add(self):
        """
        Add a token to the stream using the add-token operation.
        """
        class T1(object):
            def __init__(self, key, value):
                self.key = key
                self.value = value

        t = T1('foo', 'bar')
        tokens = [
            AddTokenOp(t)
        ]

        self.assertDictEqual(token_map(tokens), {t: t})

    def test_duplicate_token(self):
        """
        Token map rejects duplicate add-token operations.
        """
        class T1(object):
            def __init__(self, key, value):
                self.key = key
                self.value = value

        t = T1('foo', 'bar')
        tokens = [
            AddTokenOp(t),
            AddTokenOp(t)
        ]

        self.assertRaises(DuplicateTokenError, token_map, tokens)

    def test_token_map_default_add(self):
        """
        Token map accepts default token and add token operations.
        """
        class T1(object):
            def __init__(self, key, value):
                self.key = key
                self.value = value

        t = T1('foo', 'baz')
        tokens = [
            AddTokenOp(t),
            SetDefaultTokenOp(t)
        ]

        self.assertDictEqual(token_map(tokens), {t: t})

        tokens = [
            SetDefaultTokenOp(t),
            AddTokenOp(t)
        ]

        self.assertDictEqual(token_map(tokens), {t: t})

        tokens = [
            SetDefaultTokenOp(t),
            AddTokenOp(t),
            SetDefaultTokenOp(t)
        ]

        self.assertDictEqual(token_map(tokens), {t: t})

    def test_token_map_add_remove(self):
        """
        A token can be removed from the result using the remove operation.
        """

        class T1(object):
            def __init__(self, key, value):
                self.key = key
                self.value = value

        t = T1('foo', 'baz')
        tokens = [
            AddTokenOp(t),
        ]

        self.assertDictEqual(token_map(tokens), {t: t})

        tokens.append(RemoveTokenOp(t))
        self.assertDictEqual(token_map(tokens), {})

    def test_token_map_default_remove(self):
        """
        A default token can be removed from the result.
        """

        class T1(object):
            def __init__(self, key, value):
                self.key = key
                self.value = value

        t = T1('foo', 'baz')
        tokens = [
            SetDefaultTokenOp(t),
        ]

        self.assertDictEqual(token_map(tokens), {t: t})

        tokens.append(RemoveTokenOp(t))
        self.assertDictEqual(token_map(tokens), {})

    def token_map_remove_missing(self):
        """
        Raise an exception on token-remove for non-existing tokens.
        """

        class T1(object):
            def __init__(self, key, value):
                self.key = key
                self.value = value

        t = T1('foo', 'baz')
        tokens = [
            RemoveTokenOp(t),
        ]

        self.assertRaises(NoSuchTokenError, token_map, tokens)

    def token_map_custom_key_callback(self):
        class T1(object):
            def __init__(self, key, value):
                self.key = key
                self.value = value

        t1 = T1('foo', 'bar')
        t2 = T1('foo', 'baz')
        tokens = [
            AddTokenOp(t1),
            SetDefaultTokenOp(t2)
        ]

        keyfunc = lambda op: 'some {:s}'.format(op.token.key)
        valuefunc = lambda op: 'my {:s}'.format(op.token.value)

        self.assertDictEqual(token_map(tokens), {t1: t1, t2: t2})
        self.assertDictEqual(token_map(tokens, keyfunc), {'some foo': t1})
        self.assertDictEqual(token_map(tokens, keyfunc, valuefunc), {
            'some foo': 'my bar'
        })

    def test_token_attr_map(self):
        class T1(object):
            def __init__(self, key, value):
                self.key = key
                self.value = value

        t1 = T1('foo', 'bar')
        t2 = T1('foo', 'baz')
        tokens = [
            AddTokenOp(t1),
            SetDefaultTokenOp(t2)
        ]

        self.assertDictEqual(token_attr_map(tokens, 'key'), {
            'foo': 'foo'
        })

        self.assertDictEqual(token_attr_map(tokens, 'key', 'value'), {
            'foo': 'bar'
        })
