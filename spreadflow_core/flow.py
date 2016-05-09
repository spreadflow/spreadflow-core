from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import defaultdict, MutableMapping

try:
  StringType = basestring
except NameError:
  StringType = str

class PortCollection(object):
    """
    Base class for components with separate/multiple input/output ports.
    """

    @property
    def ins(self):
        """
        Return a list of input ports. Default port must be first.
        """
        return []

    @property
    def outs(self):
        """
        Return a list of output ports. Default port must be last.
        """
        return []

    @property
    def dependencies(self):
        """
        Return a list of dependencies representing the internal wiring of the
        ports.
        """
        return []

class ComponentCollection(object):
    """
    Base class for components wrapping other components.
    """

    @property
    def children(self):
        """
        Return a list of subcomponents.
        """
        return []

class ComponentBase(PortCollection):
    """
    A process with separate/multiple input and output ports.
    """

    @property
    def ins(self):
        return [self]

    @property
    def outs(self):
        return [self]

    @property
    def dependencies(self):
        for port_out in self.outs:
            for port_in in self.ins:
                if port_in is not port_out:
                    yield (port_in, port_out)

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
