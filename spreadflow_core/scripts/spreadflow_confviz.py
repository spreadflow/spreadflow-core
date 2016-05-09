from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from spreadflow_core import graph
from spreadflow_core.config import config_eval
from spreadflow_core.flow import PortCollection, ComponentCollection
from collections import defaultdict
from graphviz import Digraph
from pprint import pformat
from toposort import toposort, toposort_flatten

import sys
import argparse
import os

class ConfvizCommand(object):

    compactjoin = False
    compactstart = False
    path = None
    toposort = False
    verbose = False

    def __init__(self, out=sys.stdout):
        self._out = out

    def _strip_angle_brackets(self, text):
        while text.startswith('<') and text.endswith('>'):
            text = text[1:-1]
        return text

    def run(self, args):
        parser = argparse.ArgumentParser(prog=args[0])
        parser.add_argument('path', metavar='FILE',
                            help='Path to config file')
        parser.add_argument('-t', '--toposort', action='store_true',
                            help='Add cluster subgraphs generated by applying topological sorting to the graph.')
        parser.add_argument('-s', '--compactstart', action='store_true',
                            help='Only show startable items')
        parser.add_argument('-j', '--compactjoin', action='store_true',
                            help='Only show joinable items')
        parser.add_argument('-v', '--verbose', action='store_true',
                            help='Show all resources, not only documented ones')

        parser.parse_args(args[1:], namespace=self)

        flowmap = config_eval(self.path)
        g = flowmap.graph()

        is_controller = lambda n: hasattr(n, 'start') or hasattr(n, 'join')
        is_component_collection = lambda c: isinstance(c, ComponentCollection)
        is_port_collection = lambda c: isinstance(c, PortCollection)
        component_collections = set(c for c in flowmap.annotations if is_component_collection(c))
        port_collections = set(c for c in flowmap.annotations if is_port_collection(c))
        if self.compactstart:
            g = graph.contract(g, lambda n: hasattr(n, 'start'))
        if self.compactjoin:
            g = graph.reverse(graph.contract(g, lambda n: hasattr(n, 'join')))
        if not self.verbose:
            first_in_port_collection = set(c.ins[0] for c in port_collections)
            g = graph.contract(g, lambda n: n in first_in_port_collection)

        dg = Digraph(os.path.basename(self.path), engine='dot')

        if self.toposort:
            # Build clusters according to a topological sort of the dependencies.
            for i, s in enumerate(toposort(g)):
                sg = Digraph('cluster_{:d}'.format(i))
                sg.attr('graph', label="Group {:d}".format(i), color="blue")
                for n in s:
                    sg.node(str(hash(n)))
                dg.subgraph(sg)
        else:
            # Build clusters wrapping port collections and component collections.
            subgraphs = {}
            visible_ports = graph.vertices(g)
            component_tree = {c: set(c.children) for c in component_collections}
            for c in toposort_flatten(component_tree):
                if is_component_collection(c) or (self.verbose and is_port_collection(c)):
                    sg = Digraph('cluster_{:s}'.format(str(hash(c))))
                    label = flowmap.annotations.get(c, {}).get('label', self._strip_angle_brackets(str(c)))
                    tooltip = flowmap.annotations.get(c, {}).get('description', repr(c) + "\n" + pformat(vars(c)))
                    sg.attr('graph', label=label, tooltip=tooltip, color="blue")

                    if is_port_collection(c):
                        for port in c.ins + c.outs:
                            if port in visible_ports:
                                sg.node(str(hash(port)))

                    if is_component_collection(c):
                        for child in c.children:
                            if child in subgraphs:
                                sg.subgraph(subgraphs.pop(child))

                    subgraphs[c] = sg

            for sg in subgraphs.values():
                dg.subgraph(sg)

        # Edges
        for src, sinks in g.iteritems():
            for sink in sinks:
                dg.edge(str(hash(src)), str(hash(sink)))

        # Tooltips
        for n in graph.vertices(g):
            try:
                tooltip = flowmap.annotations.get(n, {}).get('description', repr(n) + "\n" + pformat(vars(n)))
            except TypeError:
                tooltip = ''
            label = flowmap.annotations.get(n, {}).get('label', self._strip_angle_brackets(str(n)))
            node = dg.node(str(hash(n)), label=label, tooltip=tooltip, fontcolor='blue' if is_controller(n) else 'black')

        print(dg.pipe(format='svg'), file=self._out)
        return 0

def main():
    cmd = ConfvizCommand()
    sys.exit(cmd.run(sys.argv))
