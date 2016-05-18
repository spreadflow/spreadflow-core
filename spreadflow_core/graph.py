# -*- test-case-name: spreadflow_core.test.test_graph -*-
"""
Graph utilities.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

def digraph(iterator):
    """
    Returns a directed graph built from the specified edge list.
    """
    result = {}

    for v, w in iterator:
        result.setdefault(v, set())
        if w is not None:
            result[v].add(w)

    return result

def contract(g, f):
    """
    Returns a directed graph with all vertices from the input graph for which
    f() returns True while maintaining reachability.
    """
    vertices = [v for v in g.keys() if f(v)]
    result = {v: set() for v in vertices}
    visited = set()

    stack = list(zip(vertices, vertices))
    while len(stack):
        pair = stack.pop()
        if pair not in visited:
            visited.add(pair)
            v, base = pair
            for w in g.get(v, set()):
                if f(w):
                    result.setdefault(base, set()).add(w)
                else:
                    stack.append((w, base))

    return result

def vertices(g, f=lambda v: True):
    result = set([v for v in g.keys() if f(v)])
    result.update([w for arcs in g.values() for w in arcs if f(w)])
    return result

def reverse(g):
    result = {}
    for v, arcs in g.items():
        for w in arcs:
            result.setdefault(w, set()).add(v)

    return result
