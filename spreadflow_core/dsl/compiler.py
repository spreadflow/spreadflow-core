# -*- coding: utf-8 -*-

"""
Domain-specific language for building up flowmaps.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import inspect
from collections import namedtuple, OrderedDict

class CompilerError(Exception):
    pass

class NoContextError(CompilerError):
    pass

class DuplicateTokenError(CompilerError):
    pass

class NoSuchTokenError(CompilerError):
    pass

AddTokenOp = namedtuple('AddTokenOp', ['token'])
SetDefaultTokenOp = namedtuple('SetDefaultTokenOp', ['token'])
RemoveTokenOp = namedtuple('RemoveTokenOp', ['token'])

CONTEXT_STACK = []

class Context(object):
    """
    DSL context.
    """

    _ctx_stack = CONTEXT_STACK

    def __init__(self, origin, stack=None):
        self.origin = origin
        self.tokens = []
        self.stack = stack if stack is not None else inspect.stack()[1:]

    def __enter__(self):
        self.push(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pop(self)
        return False

    def setdefault(self, token):
        self.tokens.append(SetDefaultTokenOp(token))

    def add(self, token):
        self.tokens.append(AddTokenOp(token))

    def remove(self, token):
        self.tokens.append(RemoveTokenOp(token))

    @classmethod
    def push(cls, ctx):
        """
        Push the ctx onto the shared stack.
        """
        cls._ctx_stack.append(ctx)

    @classmethod
    def pop(cls, ctx):
        """
        Remove the ctx from the shared stack.
        """
        top = cls._ctx_stack.pop()
        assert top is ctx, 'Unbalanced DSL ctx stack'

    @classmethod
    def top(cls):
        """
        Returns the topmost ctx from the stack.
        """
        try:
            return cls._ctx_stack[-1]
        except IndexError:
            raise NoContextError()

def stream_extract(stream, token_class):
    tokens = list(stream)
    extracted_stream = (op for op in tokens if isinstance(op.token, token_class))
    return extracted_stream, tokens

def stream_divert(stream, token_class):
    tokens = list(stream)
    extracted_stream = (op for op in tokens if isinstance(op.token, token_class))
    remaining_stream = (op for op in tokens if not isinstance(op.token, token_class))
    return extracted_stream, remaining_stream

def minimize_strict(stream, keyfunc=lambda op: op.token):
    present = {}
    tokens = OrderedDict()

    for op in stream:
        key = keyfunc(op)

        if isinstance(op, AddTokenOp):
            if present.get(key, False):
                raise DuplicateTokenError(op.token)
            else:
                present[key] = True

            tokens[key] = op.token
        elif isinstance(op, SetDefaultTokenOp):
            tokens.setdefault(key, op.token)
        elif isinstance(op, RemoveTokenOp):
            try:
                del tokens[key]
            except KeyError:
                raise NoSuchTokenError(op.token)

            present[key] = False

    return tokens.values()

