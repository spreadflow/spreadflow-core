# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

"""
Tests for the flowmap
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core.component import ComponentBase
from spreadflow_core.flow import Flowmap, FlowmapEmptyError, FlowmapPortError
from spreadflow_core.subprocess import SubprocessController, SubprocessWorker

class _StaticComponent(ComponentBase):
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
        upstream = _StaticComponent(outs=[upstream_out_1, upstream_out_2])

        downstream_in_1 = lambda item: None
        downstream_in_2 = lambda item: None
        downstream = _StaticComponent(ins=[downstream_in_1, downstream_in_2])

        flowmap = Flowmap()
        flowmap.connections.append((upstream, downstream))

        expected_links = set([(upstream_out_1, downstream_in_1)])

        links = flowmap.compile()
        self.assertEqual(set(links), expected_links)

    def test_partition_substitution(self):
        """
        Partitions are substituted by subprocess controllers in main graph.
        """
        part_1_upstream = object()
        part_1_downstream = lambda item: None
        part_1_out_3 = object()
        nopart_upstream = lambda item: None
        nopart_downstream = lambda item: None
        part_2_upstream = lambda item: None
        part_2_downstream = lambda item: None
        part_3_in_1 = lambda item: None

        flowmap = Flowmap()
        components = []
        annotations = {}

        flowmap.connections.append((part_1_upstream, part_1_downstream))
        flowmap.connections.append((part_1_downstream, nopart_upstream))
        flowmap.connections.append((part_1_out_3, part_3_in_1))
        flowmap.connections.append((nopart_upstream, nopart_downstream))
        flowmap.connections.append((nopart_downstream, part_2_upstream))
        flowmap.connections.append((part_2_upstream, part_2_downstream))

        annotations[part_1_upstream] = {'partition': 'part_1'}
        annotations[part_1_downstream] = {'partition': 'part_1'}
        annotations[part_1_out_3] = {'partition': 'part_1'}
        annotations[part_2_upstream] = {'partition': 'part_2'}
        annotations[part_2_downstream] = {'partition': 'part_2'}
        annotations[part_3_in_1] = {'partition': 'part_3'}

        connections = flowmap.compile()

        partitions = flowmap.generate_partitions(connections, components, annotations)
        connections, components = flowmap.replace_partitions_with_controllers(
                    partitions, connections, components)

        self.assertEqual(len(components), 3)
        self.assertIsInstance(components[0], SubprocessController)
        self.assertIsInstance(components[1], SubprocessController)
        self.assertIsInstance(components[2], SubprocessController)

        controllers = {comp.strport: comp for comp in components}

        expected_connections = set([
            (nopart_upstream, nopart_downstream),
            (controllers['spreadflow-worker:part_1'].outs[0], nopart_upstream),
            (controllers['spreadflow-worker:part_1'].outs[1], controllers['spreadflow-worker:part_3'].ins[0]),
            (nopart_downstream, controllers['spreadflow-worker:part_2'].ins[0]),
        ])
        self.assertEqual(expected_connections, set(connections))

    def test_partition_isolation(self):
        """
        A partition is isolated and connected to one worker in subprocess.
        """
        part_1_upstream = object()
        part_1_downstream = lambda item: None
        part_1_out_3 = object()
        nopart_upstream = lambda item: None
        nopart_downstream = lambda item: None
        part_2_upstream = lambda item: None
        part_2_downstream = lambda item: None
        part_3_in_1 = lambda item: None

        flowmap = Flowmap()
        components = []
        annotations = {}

        flowmap.connections.append((part_1_upstream, part_1_downstream))
        flowmap.connections.append((part_1_downstream, nopart_upstream))
        flowmap.connections.append((part_1_out_3, part_3_in_1))
        flowmap.connections.append((nopart_upstream, nopart_downstream))
        flowmap.connections.append((nopart_downstream, part_2_upstream))
        flowmap.connections.append((part_2_upstream, part_2_downstream))

        annotations[part_1_upstream] = {'partition': 'part_1'}
        annotations[part_1_downstream] = {'partition': 'part_1'}
        annotations[part_1_out_3] = {'partition': 'part_1'}
        annotations[part_2_upstream] = {'partition': 'part_2'}
        annotations[part_2_downstream] = {'partition': 'part_2'}
        annotations[part_3_in_1] = {'partition': 'part_3'}

        connections = flowmap.compile()

        partitions = flowmap.generate_partitions(connections, components, annotations)
        connections, components = flowmap.replace_partition_with_worker(
            partitions['part_1'], connections, components)

        self.assertEqual(len(components), 1)
        self.assertIsInstance(components[0], SubprocessWorker)

        expected_connections = set([
            (part_1_upstream, part_1_downstream),
            (part_1_downstream, components[0].ins[0]),
            (part_1_out_3, components[0].ins[1]),
        ])
        self.assertEqual(expected_connections, set(connections))

    def test_empty(self):
        """
        Empty flowmap raises exception.
        """
        flowmap = Flowmap()
        self.assertRaises(FlowmapEmptyError, flowmap.compile)

    def test_input_not_callable(self):
        """
        Input ports must be callable.
        """

        port_in = object()
        port_out = object()

        flowmap = Flowmap()
        flowmap.connections.append((port_out, port_in))

        self.assertRaises(FlowmapPortError, flowmap.compile)

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

        self.assertRaises(FlowmapPortError, flowmap.compile)
