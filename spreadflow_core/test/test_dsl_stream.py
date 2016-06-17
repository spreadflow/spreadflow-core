# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Test generic functions to manipulate and inspect token operation streams.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core.dsl.stream import \
    AddTokenOp, \
    DuplicateTokenError, \
    NoSuchTokenError, \
    RemoveTokenOp, \
    SetDefaultTokenOp, \
    StreamBranch, \
    token_attr_map, \
    token_map

class StreamBranchTestCase(unittest.TestCase):
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

        class Parser(StreamBranch):
            def predicate(self, operation):
                return isinstance(operation.token, T1)

        tokens = [
            SetDefaultTokenOp(T1()),
            SetDefaultTokenOp(T2()),
            AddTokenOp(T1()),
            AddTokenOp(T2()),
            RemoveTokenOp(T1()),
            RemoveTokenOp(T2()),
        ]

        parser = Parser()

        stream = iter(tokens)
        result_stream = parser.extract(stream)

        expected_t1_tokens = [
            tokens[0],
            tokens[2],
            tokens[4]
        ]

        self.assertListEqual(list(parser.selected), expected_t1_tokens)
        self.assertListEqual(list(result_stream), tokens)

    def test_stream_divert(self):
        """
        Covers stream_extract()
        """
        class T1(object):
            pass

        class T2(object):
            pass

        class Parser(StreamBranch):
            def predicate(self, operation):
                return isinstance(operation.token, T1)

        tokens = [
            SetDefaultTokenOp(T1()),
            SetDefaultTokenOp(T2()),
            AddTokenOp(T1()),
            AddTokenOp(T2()),
            RemoveTokenOp(T1()),
            RemoveTokenOp(T2()),
        ]

        parser = Parser()

        stream = iter(tokens)
        result_stream = parser.divert(stream)

        expected_t1_tokens = [
            tokens[0],
            tokens[2],
            tokens[4]
        ]

        expected_rejected_tokens = [
            tokens[1],
            tokens[3],
            tokens[5]
        ]

        self.assertListEqual(list(parser.selected), expected_t1_tokens)
        self.assertListEqual(list(result_stream), expected_rejected_tokens)

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
