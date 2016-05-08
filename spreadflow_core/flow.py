from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import defaultdict, MutableMapping


class Flowmap(MutableMapping):
    def __init__(self):
        super(Flowmap, self).__init__()
        self.annotations = {}
        self.connections = {}
        self.decorators = []

    def __getitem__(self, key):
        return self.connections[key]

    def __setitem__(self, key, value):
        self.connections[key] = value

    def __delitem__(self, key):
        del self.connections[key]

    def __iter__(self):
        return iter(self.connections)

    def __len__(self):
        return len(self.connections)

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
