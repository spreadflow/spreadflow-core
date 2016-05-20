# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Tests for the flowmap
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core.flow import Flowmap, PortCollection

class _StaticPortCollection(PortCollection):
    """
    Static test-only port collection.
    """
    def __init__(self, ins=None, outs=None):
        self._ins = ins or []
        self._outs = outs or []

    @property
    def ins(self):
        return self._ins

    @property
    def outs(self):
        # As per the port-collection contract, output ports must be
        # returned in reverse order (default port last).
        return list(reversed(self._outs))

class FlowmapTestCase(unittest.TestCase):
    """
    Tests for the Flowmap class.
    """

    def test_empty(self):
        """
        Empty flowmap compiles to empty list.
        """
        flowmap = Flowmap()
        links = flowmap.compile()
        self.assertEqual(list(links), [])

    def test_one_input_one_output(self):
        """
        One input port can be connected to one output port.
        """
        port_in = lambda item: None
        port_out = object()

        flowmap = Flowmap()
        flowmap.connections.append((port_out, port_in))

        expected_links = set([(port_out, port_in)])

        links = flowmap.compile()
        self.assertEqual(set(links), expected_links)

    def test_one_input_many_output(self):
        """
        One input port can be connected to multiple output ports.
        """
        port_in = lambda item: None
        port_out_1 = object()
        port_out_2 = object()

        flowmap = Flowmap()
        flowmap.connections.append((port_out_1, port_in))
        flowmap.connections.append((port_out_2, port_in))

        expected_links = set([(port_out_1, port_in), (port_out_2, port_in)])

        links = flowmap.compile()
        self.assertEqual(set(links), expected_links)

    def test_port_alias(self):
        """
        Ports may have aliases.
        """
        port_in = lambda item: None
        port_out = object()

        flowmap = Flowmap()
        flowmap.connections.append(("port_out_alias", "port_in_alias"))
        flowmap.aliasmap["port_in_alias"] = port_in
        flowmap.aliasmap["port_out_alias"] = port_out

        expected_links = set([(port_out, port_in)])

        links = flowmap.compile()
        self.assertEqual(set(links), expected_links)

    def test_port_collection(self):
        """
        Default ports are used when adding port collections.
        """

        upstream_out_1 = object()
        upstream_out_2 = object()
        upstream = _StaticPortCollection(outs=[upstream_out_1, upstream_out_2])

        downstream_in_1 = lambda item: None
        downstream_in_2 = lambda item: None
        downstream = _StaticPortCollection(ins=[downstream_in_1, downstream_in_2])

        flowmap = Flowmap()
        flowmap.connections.append((upstream, downstream))

        expected_links = set([(upstream_out_1, downstream_in_1)])

        links = flowmap.compile()
        self.assertEqual(set(links), expected_links)

    def test_input_not_callable(self):
        """
        Input ports must be callable.
        """

        port_in = object()
        port_out = object()

        flowmap = Flowmap()
        flowmap.connections.append((port_out, port_in))

        self.assertRaises(RuntimeError, flowmap.compile)

    def test_output_duplicate(self):
        """
        Input ports must be callable.
        """

        port_in_1 = lambda item: None
        port_in_2 = lambda item: None
        port_out = object()

        flowmap = Flowmap()
        flowmap.connections.append((port_out, port_in_1))
        flowmap.connections.append((port_out, port_in_2))

        self.assertRaises(RuntimeError, flowmap.compile)
