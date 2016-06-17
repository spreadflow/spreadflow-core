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
    """
    Abstract base class for parsers operating on selected tokens.
    """

    original = None
    selected = None
    rejected = None

    def consume(self, stream):
        """
        Divides a stream into `selected` and `rejected` substreams.

        Arguments:
            - stream: A stream consisting of token operations.
        """

        # Copy incoming stream.
        ops = list(stream)
        self.original = iter(ops)
        self.selected = (op for op in ops if self.predicate(op))
        self.rejected = (op for op in ops if not self.predicate(op))

    def extract(self, stream):
        """
        Processes a stream and returns an iterator over all operations.
        """
        self.consume(stream)
        return self.original

    def divert(self, stream):
        """
        Processes a stream and returns an iterator over rejected operations.
        """
        self.consume(stream)
        return self.rejected

    def predicate(self, operation):
        """
        Abstract filter method. Must be implemented in subclass.

        Returns:
            bool: True if the operation should be added to the selected stream,
            False if it should be added to the rejected stream.
        """
        raise NotImplementedError()
