# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Test utility class which simplifies construction of token streams.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core.script import Context, NoContextError

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
