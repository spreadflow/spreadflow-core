from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import defaultdict

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
        self.annotations = defaultdict(dict)
        self.connections = []
        self.aliasmap = {}

        self._compiled_connections = None
        self._compiled_dependencies = None
        self._eventhandlers = None
        self._eventhandlerkeys = []

    def compile(self):
        # Build port connections.
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

        # Build dependency graph.
        if self._compiled_dependencies is None:
            self._compiled_dependencies = defaultdict(set)
            backlog = set()
            processed = set()

            for port_out, port_in in self._compiled_connections.items():
                self._compiled_dependencies[port_out].add(port_in)
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
                    self._compiled_dependencies[port_out].add(port_in)
                    backlog.add(port_out)
                    backlog.add(port_in)

        if self._eventhandlers is None:
            self._eventhandlers = []

            # Build attach event handlers.
            is_attachable = lambda comp: \
                    hasattr(comp, 'attach') and callable(comp.attach)
            attachable_components = graph.vertices(
                graph.contract(self._compiled_dependencies, is_attachable))
            for comp in attachable_components:
                callback = lambda event, comp=comp: \
                        comp.attach(event.scheduler, event.reactor)
                entry = (scheduler.AttachEvent, 0, callback)
                self.annotations[comp].setdefault('events', []).append(entry)
                self._eventhandlers.append(entry)

            # Build start event handlers.
            is_startable = lambda comp: \
                    hasattr(comp, 'start') and callable(comp.start)
            plan = list(toposort(
                graph.contract(self._compiled_dependencies, is_startable)))
            for priority, comp_set in enumerate(plan):
                for comp in comp_set:
                    callback = lambda event, comp=comp: comp.start()
                    entry = (scheduler.StartEvent, priority, callback)
                    self.annotations[comp].setdefault('events', []).append(entry)
                    self._eventhandlers.append(entry)

            # Build join event handlers.
            is_joinable = lambda comp: \
                    hasattr(comp, 'join') and callable(comp.join)
            plan = list(toposort(graph.reverse(
                graph.contract(self._compiled_dependencies, is_joinable))))
            for priority, comp_set in enumerate(plan):
                for comp in comp_set:
                    callback = lambda event, comp=comp: comp.join()
                    entry = (scheduler.JoinEvent, priority, callback)
                    self.annotations[comp].setdefault('events', []).append(entry)
                    self._eventhandlers.append(entry)

            # Build detach event handlers.
            is_detachable = lambda comp: \
                    hasattr(comp, 'detach') and callable(comp.detach)
            detachable_components = graph.vertices(
                graph.contract(self._compiled_dependencies, is_detachable))
            for comp in detachable_components:
                callback = lambda event, comp=comp: comp.detach()
                entry = (scheduler.DetachEvent, 0, callback)
                self.annotations[comp].setdefault('events', []).append(entry)
                self._eventhandlers.append(entry)

        return self._compiled_connections.items()

    def graph(self):
        self.compile()
        return dict(self._compiled_dependencies)

    def register_event_handlers(self, eventdispatcher):
        self.compile()
        for event_type, priority, callback in self._eventhandlers:
            key = eventdispatcher.add_listener(event_type, priority, callback)
            self._eventhandlerkeys.append((event_type, key))

    def unregister_event_handlers(self, eventdispatcher):
        for event_type, key in self._eventhandlerkeys:
            eventdispatcher.remove_listener(event_type, key)
