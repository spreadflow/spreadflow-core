# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Tests for the flowmap
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core.component import Compound
from spreadflow_core.dsl.parser import ParentParser
from spreadflow_core.dsl.stream import SetDefaultTokenOp, AddTokenOp
from spreadflow_core.dsl.tokens import \
    AliasToken, \
    ComponentToken, \
    ConnectionToken, \
    DescriptionToken, \
    LabelToken, \
    ParentElementToken, \
    PartitionToken
from spreadflow_core.script import Process, ComponentTemplate

class ProcessDecoratorTestCase(unittest.TestCase):
    """
    Unit tests for the process decorator.
    """

    def test_process_class(self):
        """
        Process decorator replaces class definition with instantiated process.
        """
        process = object()

        @Process()
        class TrivialProcess(ComponentTemplate):
            """
            Docs for the trivial process.
            """
            def apply(self):
                yield AddTokenOp(ComponentToken(process))

        tokens = list(TrivialProcess())

        self.assertIn(SetDefaultTokenOp(LabelToken(process, 'TrivialProcess')), tokens)
        self.assertIn(SetDefaultTokenOp(DescriptionToken(process, 'Docs for the trivial process.')), tokens)
        self.assertIn(AddTokenOp(AliasToken(process, 'TrivialProcess')), tokens)

    def test_process_def(self):
        """
        Process decorator replaces function definition with compound.
        """

        port = object()

        @Process()
        def trivial_proc():
            """
            Docs for another trivial process.
            """
            yield port

        parent_parser = ParentParser()
        tokens = list(parent_parser.extract(trivial_proc()))

        parentmap = parent_parser.get_parentmap()
        self.assertEqual(len(parentmap), 1)
        _, process = parentmap.popitem()

        self.assertIsInstance(process, Compound)

        self.assertIn(SetDefaultTokenOp(AliasToken(process, 'trivial_proc')), tokens)
        self.assertIn(SetDefaultTokenOp(LabelToken(process, 'trivial_proc')), tokens)
        self.assertIn(SetDefaultTokenOp(DescriptionToken(process, 'Docs for another trivial process.')), tokens)
        self.assertIn(AddTokenOp(ParentElementToken(port, process)), tokens)

    def test_process_port_chain(self):
        """
        Process decorator replaces function definition with compound.
        """

        port1 = lambda item, send: send(item)
        port2 = lambda item, send: send(item)
        port3 = lambda item, send: send(item)

        @Process()
        def proc_chain():
            yield port1
            yield port2
            yield port3

        parent_parser = ParentParser()
        tokens = list(parent_parser.extract(proc_chain()))

        parentmap = parent_parser.get_parentmap()
        self.assertEqual(len(parentmap), 3)
        _, process = parentmap.popitem()

        self.assertIsInstance(process, Compound)

        self.assertIn(AddTokenOp(ConnectionToken(port1, port2)), tokens)
        self.assertIn(AddTokenOp(ConnectionToken(port2, port3)), tokens)
        self.assertIn(AddTokenOp(ParentElementToken(port1, process)), tokens)
        self.assertIn(AddTokenOp(ParentElementToken(port2, process)), tokens)
        self.assertIn(AddTokenOp(ParentElementToken(port3, process)), tokens)

    def test_process_params(self):
        """
        Process decorator parameters for alias, label, description and partition.
        """
        process = object()

        @Process(alias='trivproc', label='trivial process',
                 description='...', partition='trivia')
        class TrivialProcess(ComponentTemplate):
            """
            Docs for the trivial process.
            """
            def apply(self):
                yield AddTokenOp(ComponentToken(process))

        tokens = list(TrivialProcess())
        self.assertIn(AddTokenOp(AliasToken(process, 'trivproc')), tokens)
        self.assertIn(AddTokenOp(LabelToken(process, 'trivial process')), tokens)
        self.assertIn(AddTokenOp(DescriptionToken(process, '...')), tokens)
        self.assertIn(AddTokenOp(PartitionToken(process, 'trivia')), tokens)

    def test_process_tokens_from_template(self):
        """
        Template can provide additional tokens.
        """
        class MyToken(object):
            pass

        token = MyToken()

        process = object()

        @Process()
        class TrivialProcess(ComponentTemplate):
            """
            Docs for the trivial process.
            """
            def apply(self):
                yield AddTokenOp(token)
                yield AddTokenOp(ComponentToken(process))

        tokens = list(TrivialProcess())

        self.assertIn(AddTokenOp(token), tokens)
