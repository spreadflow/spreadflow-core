from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import defaultdict


class Flowmap(dict):
    def __init__(self):
        super(Flowmap, self).__init__()
        self.decorators = []
        self.annotations = {}

    def graph(self):
        result = defaultdict(set)
        backlog = set()
        processed = set()

        for port_out, port_in in self.iteritems():
            result[port_out].add(port_in)
            backlog.add(port_in)

        while len(backlog):
            node = backlog.pop()
            if node in processed:
                continue
            else:
                processed.add(node)

            try:
                arcs = tuple(node.dependencies)
            except AttributeError:
                continue

            for port_out, port_in in arcs:
                result[port_out].add(port_in)
                backlog.add(port_out)
                backlog.add(port_in)

        return result
