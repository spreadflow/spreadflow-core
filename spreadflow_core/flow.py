# -*- coding: utf-8 -*-

"""
Flowmap builder.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import itertools
from collections import  Counter, defaultdict, namedtuple

from spreadflow_core import scheduler
from spreadflow_core.component import Compound, PortCollection
from spreadflow_core.subprocess import SubprocessWorker, SubprocessController

try:
    StringType = basestring # pylint: disable=undefined-variable
except NameError:
    StringType = str

class FlowmapError(Exception):
    """
    Base exception for all flowmap errors.
    """

class FlowmapEmptyError(FlowmapError):
    """
    There are no connections inside a flowmap or a partition.
    """
    def __init__(self, partition=None, message=None):
        if partition:
            default_message = 'There are no connections inside the partition ' \
                '"{:s}"'.format(partition)
        else:
            default_message = 'There are no connections in the flowmap'

        super(FlowmapEmptyError, self).__init__(message or default_message)
        self.partition = partition

class FlowmapPortError(FlowmapError):
    """
    One or more ports do not meet requirements.
    """
    def __init__(self, ports, message=None):
        super(FlowmapPortError, self).__init__(message)
        self.ports = ports

PartitionBounds = namedtuple('PartitionBounds', ['outs', 'ins'])

class Partition(Compound):
    def __init__(self, children, bounds):
        super(Partition, self).__init__(children)
        self.bounds = bounds

class Flowmap(object):
    def __init__(self):
        super(Flowmap, self).__init__()
        self.aliasmap = {}
        self.connections = []

    def compile(self):
        # Build port connections.
        connections = list(self._resolve_port_aliases(self.connections,
                                                      self.aliasmap))
        self._validate_links(connections)
        return connections

    @staticmethod
    def _resolve_port_aliases(links, aliasmap):
        for port_out, port_in in links:
            while True:
                if isinstance(port_out, StringType):
                    port_out = aliasmap[port_out]
                elif isinstance(port_out, PortCollection):
                    if port_out is not port_out.outs[-1]:
                        port_out = port_out.outs[-1]
                    else:
                        break
                else:
                    break

            while True:
                if isinstance(port_in, StringType):
                    port_in = aliasmap[port_in]
                elif isinstance(port_in, PortCollection):
                    if port_in is not port_in.ins[0]:
                        port_in = port_in.ins[0]
                    else:
                        break
                else:
                    break

            yield port_out, port_in

    @staticmethod
    def _validate_links(connections):
        if len(connections) == 0:
            raise FlowmapEmptyError()

        outs, ins = zip(*connections)

        non_callable_ins = [port for port in ins if not callable(port)]
        if len(non_callable_ins):
            raise FlowmapPortError(non_callable_ins, 'Input ports must be '
                                   'callable')

        out_counts = Counter(outs).items()
        multi_outs = [port for port, count in out_counts if count > 1]
        if len(multi_outs):
            raise FlowmapPortError(multi_outs, 'Output ports must not have '
                                   'more than one connection')



    @staticmethod
    def generate_partitions(connections, components, annotations):
        """
        Generate a map port -> partition name.
        """

        # Generate a map port -> partition name
        # and also a map partition name -> set of components
        port_partition = set()
        for port in itertools.chain(*zip(*connections)):
            try:
                port_partition.add((port, annotations[port]['partition']))
            except KeyError:
                continue

        partition_children = defaultdict(set)
        for comp in components:
            try:
                partition_name = annotations[comp]['partition']
            except KeyError:
                continue

            partition_children[partition_name].add(comp)

            for port in itertools.chain(comp.ins, comp.outs):
                port_partition.add((port, partition_name))

        if len(port_partition) == 0:
            raise FlowmapEmptyError(message='No single port is inside a '
                                    'partition')

        ports, part_names = zip(*port_partition)
        port_counts = Counter(ports).items()
        multi_parts = [port for port, count in port_counts if count > 1]
        if len(multi_parts):
            raise FlowmapPortError(multi_parts, 'Ports cannot be part of more '
                                   'than one partition')


        port_partition_map = dict(port_partition)

        # Collect all ports connected accross partition boundaries.
        partition_bounds = {name: PartitionBounds([], []) for name in set(part_names)}
        for port_out, port_in in connections:
            partition_out = port_partition_map.get(port_out, None)
            partition_in = port_partition_map.get(port_in, None)
            if partition_out != partition_in:
                if partition_out:
                    partition_bounds[partition_out].outs.append(port_out)
                if partition_in:
                    bounds_ins = partition_bounds[partition_in].ins
                    if port_in not in bounds_ins:
                        bounds_ins.append(port_in)

        # Collect all direct children of every partition.
        partition_component_ports = set()
        for partition_name, comps in partition_children.items():
            for comp in comps:
                partition_component_ports.update(comp.outs)
                partition_component_ports.update(comp.ins)

        for port in itertools.chain(*zip(*connections)):
            if port not in partition_component_ports and port in port_partition_map:
                partition_children[port_partition_map[port]].add(port)

        # Finally generate a map partition name -> partition
        partitions = {}
        for name in set(part_names):
            partitions[name] = Partition(partition_children[name],
                                         partition_bounds[name])

        return partitions


    @staticmethod
    def replace_partition_with_worker(partition, connections, components):
        innames = list(range(len(partition.bounds.outs)))
        outnames = list(range(len(partition.bounds.ins)))
        worker = SubprocessWorker(innames=innames, outnames=outnames)

        # Purge/rewire connections.
        mapped_connections = []
        outmap = dict(zip(partition.bounds.outs, worker.ins))
        inmap = dict(zip(partition.bounds.ins, worker.outs))

        for port_out, port_in in connections:
            if port_out in partition and port_in in partition:
                mapped_connections.append((port_out, port_in))
            elif port_out in partition:
                mapped_connections.append((port_out, outmap[port_out]))
            elif port_in in partition:
                mapped_connections.append((inmap[port_in], port_in))

        # Purge/replace components.
        mapped_components = [worker]
        for comp in components:
            any_port = set(list(comp.outs)[:1] + list(comp.ins)[:1]).pop()
            if any_port in partition:
                mapped_components.append(comp)

        return mapped_connections, mapped_components


    @staticmethod
    def replace_partitions_with_controllers(partitions, connections, components):
        outmap = dict()
        inmap = dict()
        inner_ports = set()

        mapped_components = []

        for name, partition in partitions.items():
            innames = list(range(len(partition.bounds.ins)))
            outnames = list(range(len(partition.bounds.outs)))
            controller = SubprocessController(name, innames=innames, outnames=outnames)

            mapped_components.append(controller)

            outmap.update(zip(partition.bounds.outs, controller.outs))
            inmap.update(zip(partition.bounds.ins, controller.ins))
            inner_ports.update(set(partition.outs + partition.ins))

        # Purge/rewire connections.
        mapped_connections = []
        for port_out, port_in in connections:
            port_out = outmap.get(port_out, port_out)
            port_in = inmap.get(port_in, port_in)
            if port_out not in inner_ports or port_in not in inner_ports:
                mapped_connections.append((port_out, port_in))

        # Purge/replace components.
        for comp in components:
            any_port = set(list(comp.outs)[:1] + list(comp.ins)[:1]).pop()
            if any_port not in inner_ports:
                mapped_components.append(comp)

        return mapped_connections, mapped_components


    @staticmethod
    def register_event_handlers(eventdispatcher, connections, components):
        result = []
        entries = []

        outs, ins = zip(*connections)
        comps = set(list(outs) + list(ins) + list(components))

        # Build attach event handlers.
        is_attachable = lambda comp: \
                hasattr(comp, 'attach') and callable(comp.attach)
        attachable_comps = (comp for comp in comps if is_attachable(comp))
        for comp in attachable_comps:
            callback = lambda event, comp=comp: \
                    comp.attach(event.scheduler, event.reactor)
            entries.append((scheduler.AttachEvent, 0, callback))

        # Build detach event handlers.
        is_detachable = lambda comp: \
                hasattr(comp, 'detach') and callable(comp.detach)
        detachable_comps = (comp for comp in comps if is_detachable(comp))
        for comp in detachable_comps:
            callback = lambda event, comp=comp: comp.detach()
            entries.append((scheduler.DetachEvent, 0, callback))

        for event_type, priority, callback in entries:
            key = eventdispatcher.add_listener(event_type, priority, callback)
            result.append((event_type, key))

        return result

    def unregister_event_handlers(self, eventdispatcher, eventhandlerkeys):
        for event_type, key in eventhandlerkeys:
            eventdispatcher.remove_listener(event_type, key)
