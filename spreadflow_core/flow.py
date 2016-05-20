from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import defaultdict

from spreadflow_core import scheduler

try:
  StringType = basestring # pylint: disable=undefined-variable
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

class Flowmap(object):
    def __init__(self):
        super(Flowmap, self).__init__()
        self.annotations = defaultdict(dict)
        self.connections = []
        self.aliasmap = {}

        self._compiled_connections = None
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

        if self._eventhandlers is None:
            self._eventhandlers = []

            ports = set(self._compiled_connections.keys()
                        + self._compiled_connections.values())

            # Build attach event handlers.
            is_attachable = lambda port: \
                    hasattr(port, 'attach') and callable(port.attach)
            attachable_ports = (port for port in ports if is_attachable(port))
            for port in attachable_ports:
                callback = lambda event, port=port: \
                        port.attach(event.scheduler, event.reactor)
                entry = (scheduler.AttachEvent, 0, callback)
                self.annotations[port].setdefault('events', []).append(entry)
                self._eventhandlers.append(entry)

            # Build detach event handlers.
            is_detachable = lambda port: \
                    hasattr(port, 'detach') and callable(port.detach)
            detachable_ports = (port for port in ports if is_detachable(port))
            for port in detachable_ports:
                callback = lambda event, port=port: port.detach()
                entry = (scheduler.DetachEvent, 0, callback)
                self.annotations[port].setdefault('events', []).append(entry)
                self._eventhandlers.append(entry)

        return self._compiled_connections.items()

    def register_event_handlers(self, eventdispatcher):
        self.compile()
        for event_type, priority, callback in self._eventhandlers:
            key = eventdispatcher.add_listener(event_type, priority, callback)
            self._eventhandlerkeys.append((event_type, key))

    def unregister_event_handlers(self, eventdispatcher):
        for event_type, key in self._eventhandlerkeys:
            eventdispatcher.remove_listener(event_type, key)
