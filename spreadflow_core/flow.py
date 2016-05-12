from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import defaultdict, MutableMapping

from toposort import toposort

from spreadflow_core import graph, scheduler

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
        self.connections = []
        self.aliasmap = {}

        self._compiled_connections = None
        self._eventhandlers = []

    def compile(self):
        if self._compiled_connections is None:
            self._compiled_connections = {}

            for port_out, port_in in self.connections:
                while True:
                    if isinstance(port_out, StringType):
                        port_out = self.aliasmap[port_out]
                    elif isinstance(port_out, PortCollection):
                        self.annotations.setdefault(port_out, {})
                        if port_out is not port_out.outs[-1]:
                            port_out = port_out.outs[-1]
                        else:
                            break
                    else:
                        break

                while True:
                    if isinstance(port_in, StringType):
                        port_in = self.aliasmap[port_in]
                    elif isinstance(port_in, PortCollection):
                        self.annotations.setdefault(port_in, {})
                        if port_in is not port_in.ins[0]:
                            port_in = port_in.ins[0]
                        else:
                            break
                    else:
                        break

                if not callable(port_in):
                    RuntimeError('Attempting to use an port as input which is not callable')

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
            key = eventdispatcher.add_listener(scheduler.AttachEvent, priority, lambda event, component=component: component.attach(event.scheduler, event.reactor))
            self._eventhandlers.append((scheduler.AttachEvent, key))

        for priority, component in self.startable_components:
            key = eventdispatcher.add_listener(scheduler.StartEvent, priority, lambda event, component=component: component.start())
            self._eventhandlers.append((scheduler.StartEvent, key))

        for priority, component in self.joinable_components:
            key = eventdispatcher.add_listener(scheduler.JoinEvent, priority, lambda event, component=component: component.join())
            self._eventhandlers.append((scheduler.JoinEvent, key))

        for priority, component in self.detachable_components:
            key = eventdispatcher.add_listener(scheduler.DetachEvent, priority, lambda event, component=component: component.detach())
            self._eventhandlers.append((scheduler.DetachEvent, key))

    def unregister_event_handlers(self, eventdispatcher):
        # FIXME: move to flowmap builder
        for event_type, key in self._eventhandlers:
            eventdispatcher.remove_listener(event_type, key)
