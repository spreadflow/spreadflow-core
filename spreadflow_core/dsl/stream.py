# -*- coding: utf-8 -*-

"""
Generic functions to manipulate and inspect token operation streams.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import namedtuple, OrderedDict

class StreamError(Exception):
    """
    Token operation stream base error.
    """

class DuplicateTokenError(StreamError):
    """
    Raised when applying a token operation would result in duplicate tokens.
    """

class NoSuchTokenError(StreamError):
    """
    Raised when an operation is attempted on a token which does not exist.
    """

AddTokenOp = namedtuple('AddTokenOp', ['token'])
SetDefaultTokenOp = namedtuple('SetDefaultTokenOp', ['token'])
RemoveTokenOp = namedtuple('RemoveTokenOp', ['token'])

def stream_extract(stream, token_class):
    """
    Copy all operations addressing tokens of the given class.
    """
    ops = list(stream)
    extracted_stream = (op for op in ops if isinstance(op.token, token_class))
    return extracted_stream, ops

def stream_divert(stream, token_class):
    """
    Remove all operations addressing tokens of the given class from the stream.
    """
    ops = list(stream)
    extracted_stream = (op for op in ops if isinstance(op.token, token_class))
    remaining_stream = (op for op in ops if not isinstance(op.token, token_class))
    return extracted_stream, remaining_stream

def token_map(stream, keyfunc=lambda op: op.token, valuefunc=lambda op: op.token):
    """
    Apply all token operations in order and construct a mapping.
    """
    present = {}
    tokens = OrderedDict()

    for operation in stream:
        key = keyfunc(operation)

        if isinstance(operation, AddTokenOp):
            if present.get(key, False):
                raise DuplicateTokenError(key, operation.token)
            else:
                present[key] = True

            tokens[key] = valuefunc(operation)
        elif isinstance(operation, SetDefaultTokenOp):
            tokens.setdefault(key, valuefunc(operation))
        elif isinstance(operation, RemoveTokenOp):
            try:
                del tokens[key]
            except KeyError:
                raise NoSuchTokenError(key, operation.token)

            present[key] = False

    return tokens

def token_attr_map(stream, keyattr, valueattr=None):
    """
    Apply all token operations in order and construct a mapping.
    """
    if valueattr is None:
        valueattr = keyattr
    extract_key = lambda op: getattr(op.token, keyattr)
    extract_value = lambda op: getattr(op.token, valueattr)
    return token_map(stream, extract_key, extract_value)

class StreamBranch(object):
    stream = None
    selected = None
    rejected = None

    def push(self, stream):
        """
        Extracts the matching operations from the given stream.

        Arguments:
            - stream: A stream consisting of token operations.

        Returns:
            self
        """

        # Copy incoming stream.
        ops = list(stream)
        self.stream = iter(ops)
        self.selected = (op for op in ops if self.predicate(op))
        self.rejected = (op for op in ops if not self.predicate(op))
        return self

    def extract(self, stream):
        self.push(stream)
        return self.stream

    def divert(self, stream):
        self.push(stream)
        return self.rejected

    def predicate(self, operation):
        """
        Abstract filter method. Must be implemented in subclass.
        """
        raise NotImplementedError()

class TokenClassPredicateMixin(object):
    token_class = None

    def predicate(self, operation):
        return isinstance(operation.token, self.token_class)
