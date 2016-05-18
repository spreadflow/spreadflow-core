from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import unittest

from spreadflow_core import graph


class GraphTestCase(unittest.TestCase):

    def test_digraph(self):
        """
        Test digraph construction
        """

        edgelist = []
        expected_graph = {}
        result = graph.digraph(edgelist)
        self.assertEqual(expected_graph, result)

        edgelist = [
            ('A', 'B'),
        ]
        expected_graph = {
            'A': {'B'},
        }
        result = graph.digraph(edgelist)
        self.assertEqual(expected_graph, result)

        edgelist = [
            ('A', 'B'),
            ('A', 'C'),
            ('B', 'C'),
            ('D', None),
        ]
        expected_graph = {
            'A': {'B', 'C'},
            'B': {'C'},
            'D': set(),
        }
        result = graph.digraph(edgelist)
        self.assertEqual(expected_graph, result)

    def test_contract_identity(self):
        """
        Test that returned graph is identical to input graph if the callback
        always returns True.
        """
        f = lambda v: True

        g = {}
        result = graph.contract(g, f)
        self.assertEqual(result, g)

        g = {
            'A': {'B'},
        }
        result = graph.contract(g, f)
        self.assertEqual(result, g)

        g = {
            'A': {'B', 'C'},
            'B': {'C'},
        }
        result = graph.contract(g, f)
        self.assertEqual(result, g)


    def test_contract_empty(self):
        """
        Test that returned graph is empty if the callback function always
        returns False.
        """
        f = lambda v: False

        g = {}
        result = graph.contract(g, f)
        self.assertEqual(result, {})

        g = {
            'A': {'B'},
        }
        result = graph.contract(g, f)
        self.assertEqual(result, {})

        g = {
            'A': {'B', 'C'},
            'B': {'C'},
        }
        result = graph.contract(g, f)
        self.assertEqual(result, {})


    def test_contract_odd(self):
        """
        Test that returned graph is of the expected structure if the callback
        only accepts even labels.
        """
        f = lambda v: bool(v % 2)

        g = {}
        result = graph.contract(g, f)
        self.assertEqual(result, {})

        g = {
            1: {2},
        }
        expect_g = {
            1: set(),
        }
        result = graph.contract(g, f)
        self.assertEqual(result, expect_g)

        g = {
            2: {1},
        }
        expect_g = {
        }
        result = graph.contract(g, f)
        self.assertEqual(result, expect_g)

        g = {
            1: {5},
            2: {3},
            3: {7},
            4: {8},
            5: {6},
            6: {10},
            7: {11},
            8: {7},
            9: set(),
            10: {11},
            11: {12}
        }
        expect_g = {
            1: {5},
            3: {7},
            5: {11},
            7: {11},
            9: set(),
            11: set(),
        }
        result = graph.contract(g, f)
        self.assertEqual(result, expect_g)

    def test_contract_cycle(self):
        """
        Test that returned graph is of the expected structure if it contains
        cycles.
        """
        f = lambda v: True

        g = {
            1: {1},
        }
        expect_g = {
            1: {1},
        }
        result = graph.contract(g, f)
        self.assertEqual(result, expect_g)

        g = {
            2: {1},
            1: {2},
        }
        expect_g = {
            1: {2},
            2: {1},
        }
        result = graph.contract(g, f)
        self.assertEqual(result, expect_g)

        f = lambda v: v == 2
        g = {
            2: {1},
            1: {2},
        }
        expect_g = {
            2: {2},
        }
        result = graph.contract(g, f)
        self.assertEqual(result, expect_g)

        f = lambda v: v.startswith('A')
        g = {
            'A': {'B'},
            'B': {'B'},
        }
        expect_g = {
            'A': set(),
        }
        result = graph.contract(g, f)
        self.assertEqual(result, expect_g)


    def test_vertices_accept_all(self):
        """
        Test that all vertices are returned if no filter was specified.
        """
        f = lambda v: True

        g = {}
        result = graph.vertices(g, f)
        self.assertEqual(result, set())

        g = {
            'A': {'B'},
        }
        result = graph.vertices(g, f)
        self.assertEqual(result, {'A', 'B'})

        g = {
            'A': {'B', 'C'},
            'B': {'C'},
        }
        result = graph.vertices(g, f)
        self.assertEqual(result, {'A','B','C'})


    def test_vertices_reject_all(self):
        """
        Test that no vertices are returned if filter rejects all.
        """
        f = lambda v: False

        g = {}
        result = graph.vertices(g, f)
        self.assertEqual(result, set())

        g = {
            'A': {'B'},
        }
        result = graph.vertices(g, f)
        self.assertEqual(result, set())

        g = {
            'A': {'B', 'C'},
            'B': {'C'},
        }
        result = graph.vertices(g, f)
        self.assertEqual(result, set())


    def test_vertices_select(self):
        """
        Test that only vertices are reported which are accepted by the filter.
        """
        f = lambda v: v in {'A', 'C'}

        g = {}
        result = graph.vertices(g, f)
        self.assertEqual(result, set())

        g = {
            'A': {'B'},
        }
        result = graph.vertices(g, f)
        self.assertEqual(result, {'A'})

        g = {
            'A': {'B', 'C'},
            'B': {'C'},
        }
        result = graph.vertices(g, f)
        self.assertEqual(result, {'A','C'})


    def test_reverse(self):
        """
        Test graph reversal.
        """
        g = {}
        result = graph.reverse(g)
        self.assertEqual(result, g)

        g = {
            'A': {'B'},
        }
        result = graph.reverse(g)
        self.assertEqual(result, {'B': {'A'}})

        g = {
            'A': {'B', 'C'},
            'B': {'C'},
        }
        result = graph.reverse(g)
        self.assertEqual(result, {'B': {'A'}, 'C': {'A', 'B'}})
