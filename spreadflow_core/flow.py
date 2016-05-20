from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import defaultdict, Counter

from spreadflow_core import scheduler
from spreadflow_core.component import PortCollection

try:
  StringType = basestring # pylint: disable=undefined-variable
except NameError:
  StringType = str

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
            connections = list(self._resolve_port_aliases())

            if len(connections):
                outs, ins = zip(*connections)

                non_callable_ins = [port for port in ins if not callable(port)]
                if len(non_callable_ins):
                    raise RuntimeError('Attempting to use a port as input which is not callable')

                out_counts = Counter(outs).items()
                multi_outs = [port for port, count in out_counts if count > 1]
                if len(multi_outs):
                    raise RuntimeError('Attempting to connect more than one input port to a single output port')

            self._compiled_connections = dict(connections)

        if self._eventhandlers is None:
            self._eventhandlers = []

            ports = set(list(self._compiled_connections.keys())
                        + list(self._compiled_connections.values()))

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

    def _resolve_port_aliases(self):
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

            yield port_out, port_in

    def register_event_handlers(self, eventdispatcher):
        self.compile()
        for event_type, priority, callback in self._eventhandlers:
            key = eventdispatcher.add_listener(event_type, priority, callback)
            self._eventhandlerkeys.append((event_type, key))

    def unregister_event_handlers(self, eventdispatcher):
        for event_type, key in self._eventhandlerkeys:
            eventdispatcher.remove_listener(event_type, key)
