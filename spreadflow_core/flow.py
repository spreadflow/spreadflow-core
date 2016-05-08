from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import defaultdict, MutableMapping

try:
  StringType = basestring
except NameError:
  StringType = str

class Flowmap(MutableMapping):
    def __init__(self):
        super(Flowmap, self).__init__()
        self.annotations = {}
        self.connections = {}
        self.decorators = []
        self.aliasmap = {}

    def __getitem__(self, key):
        port_out = self.resolve(key)
        port_in_key = self.connections[port_out]
        return self.resolve(port_in_key)

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

    def resolve(self, key):
        while isinstance(key, StringType):
            key = self.aliasmap[key]
        return key
