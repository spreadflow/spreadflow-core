from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import defaultdict, MutableMapping

from toposort import toposort

from spreadflow_core import graph

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

class Flowmap(object):
    def __init__(self):
        super(Flowmap, self).__init__()
        self.annotations = {}
        self.connections = {}
        self.aliasmap = {}

        self._compiled_connections = None
        self._eventhandlers = []

    def compile(self):
        if self._compiled_connections is None:
            self._compiled_connections = {}

            for port_out_key, port_in_key in self.connections.items():
                port_out = self.resolve(port_out_key)
                port_in = self.resolve(port_in_key)

                if port_out in self._compiled_connections:
                    RuntimeError('Attempting to connect more than one input port to an output port')

                self._compiled_connections[port_out] = port_in

        return self._compiled_connections.items()

    def graph(self):
        result = defaultdict(set)
        backlog = set()
        processed = set()

        for port_out, port_in in self.compile():
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

    @property
    def attachable_components(self):
        # FIXME: move to flowmap builder
        is_attachable = lambda p: hasattr(p, 'attach') and callable(p.attach)
        nodes = graph.vertices(graph.contract(self.graph(), is_attachable))
        for node in nodes:
            yield 0, node

    @property
    def startable_components(self):
        # FIXME: move to flowmap builder
        is_startable = lambda p: hasattr(p, 'start') and callable(p.start)
        plan = list(toposort(graph.contract(self.graph(), is_startable)))
        for priority, node_set in enumerate(plan):
            for node in node_set:
                yield priority, node

    @property
    def joinable_components(self):
        # FIXME: move to flowmap builder
        is_joinable = lambda p: hasattr(p, 'join') and callable(p.join)
        plan = list(toposort(graph.reverse(graph.contract(self.graph(), is_joinable))))
        for priority, node_set in enumerate(plan):
            for node in node_set:
                yield priority, node

    @property
    def detachable_components(self):
        # FIXME: move to flowmap builder
        is_detachable = lambda p: hasattr(p, 'detach') and callable(p.detach)
        nodes = graph.vertices(graph.contract(self.graph(), is_detachable))
        for node in nodes:
            yield 0, node

    def register_event_handlers(self, eventdispatcher):
        # FIXME: move to flowmap builder
        for priority, component in self.attachable_components:
            key = eventdispatcher.add_listener('attach', priority, lambda event, data, component=component: component.attach(data['scheduler'], data['reactor']))
            self._eventhandlers.append(key)

        for priority, component in self.startable_components:
            key = eventdispatcher.add_listener('start', priority, lambda event, data, component=component: component.start())
            self._eventhandlers.append(key)

        for priority, component in self.joinable_components:
            key = eventdispatcher.add_listener('join', priority, lambda event, data, component=component: component.join())
            self._eventhandlers.append(key)

        for priority, component in self.detachable_components:
            key = eventdispatcher.add_listener('detach', priority, lambda event, data, component=component: component.detach())
            self._eventhandlers.append(key)

    def unregister_event_handlers(self, eventdispatcher):
        # FIXME: move to flowmap builder
        for key in self._eventhandlers:
            eventdispatcher.remove_listener(key)
        eventdispatcher.prune()
