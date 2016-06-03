# -*- coding: utf-8 -*-

"""
Utility class which simplifies construction of token streams.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import inspect

from spreadflow_core.dsl.stream import \
    AddTokenOp, SetDefaultTokenOp, RemoveTokenOp

class NoContextError(Exception):
    """
    Raised a global context is expected but none exists.
    """

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
        """
        Add a set-default operation to the stream.
        """
        self.tokens.append(SetDefaultTokenOp(token))

    def add(self, token):
        """
        Add an add-token operation to the stream.
        """
        self.tokens.append(AddTokenOp(token))

    def remove(self, token):
        """
        Add a remove-token operation to the stream.
        """
        self.tokens.append(RemoveTokenOp(token))

    @classmethod
    def push(cls, ctx):
        """
        Push the ctx onto the shared stack.
        """
        assert isinstance(ctx, cls), 'Argument must be a Context'
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
