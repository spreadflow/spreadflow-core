# -*- coding: utf-8 -*-

"""
Parser steps used in the domain-specific language for building up flowmaps.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import itertools
from collections import Counter, namedtuple

from spreadflow_core import scheduler
from spreadflow_core.component import PortCollection
from spreadflow_core.dsl.stream import \
    AddTokenOp, \
    stream_divert, \
    stream_extract, \
    token_attr_map
from spreadflow_core.dsl.tokens import \
    AliasToken, \
    ComponentToken, \
    ConnectionToken, \
    EventHandlerToken, \
    PartitionBoundsToken, \
    PartitionSelectToken, \
    PartitionToken
from spreadflow_core.subprocess import SubprocessWorker, SubprocessController

try:
    StringType = basestring # pylint: disable=undefined-variable
except NameError:
    StringType = str

def portmap(stream):
    return token_attr_map(stream, 'port_out', 'port_in')

def ports(stream):
    all_connections = portmap(stream).items()
    return itertools.chain(*zip(*all_connections))

class ParserError(object):
    pass

class AliasResolverPass(object):
    def __call__(self, stream):
        # Capture aliases and connections, yield all the rest
        alias_ops, stream = stream_divert(stream, AliasToken)
        connection_ops, stream = stream_divert(stream, ConnectionToken)
        for op in stream: yield op

        # Generate alias map.
        aliases = token_attr_map(alias_ops, 'alias', 'element')

        # Generate connection operations.
        for port_out, port_in in portmap(connection_ops).items():
            while True:
                if isinstance(port_out, StringType):
                    port_out = aliases[port_out]
                elif isinstance(port_out, PortCollection):
                    if port_out is not port_out.outs[-1]:
                        port_out = port_out.outs[-1]
                    else:
                        break
                else:
                    break

            while True:
                if isinstance(port_in, StringType):
                    port_in = aliases[port_in]
                elif isinstance(port_in, PortCollection):
                    if port_in is not port_in.ins[0]:
                        port_in = port_in.ins[0]
                    else:
                        break
                else:
                    break

            yield AddTokenOp(ConnectionToken(port_out, port_in))

class PortsValidatorPass(object):
    """
    Verifies used input/output ports.
    """

    def __call__(self, stream):
        connection_ops, stream = stream_extract(stream, ConnectionToken)

        connection_list = list(portmap(connection_ops).items())

        if len(connection_list) > 0:
            outs, ins = zip(*connection_list)

            non_callable_ins = [port for port in ins if not callable(port)]
            if len(non_callable_ins):
                raise ParserError(non_callable_ins, 'Input ports must be '
                                       'callable')

            out_counts = Counter(outs).items()
            multi_outs = [port for port, count in out_counts if count > 1]
            if len(multi_outs):
                raise ParserError(multi_outs, 'Output ports must not have '
                                       'more than one connection')

        return stream

class PartitionExpanderPass(object):
    """
    Propagate the partition of assigned to components to its ports.
    """
    def __call__(self, stream):
        # Capture partitions, read components, yield all the rest
        partition_ops, stream = stream_divert(stream, PartitionToken)
        component_ops, stream = stream_extract(stream, ComponentToken)
        for op in stream: yield op

        # Generate partition map.
        partition_map = token_attr_map(partition_ops, 'element', 'partition')

        # Process components.
        for component in token_attr_map(component_ops, 'element'):
            try:
                partition_name = partition_map[component]
            except KeyError:
                continue
            else:
                for port in set(component.ins + component.outs):
                    partition_map.setdefault(port, partition_name)

        # Produce updated partition map.
        for element, partition in partition_map.items():
            yield AddTokenOp(PartitionToken(element, partition))

PartitionBounds = namedtuple('PartitionBounds', ['outs', 'ins'])

class PartitionBoundsPass(object):
    def __call__(self, stream):
        # Read partitions and connections re-yield evereything
        connection_ops, stream = stream_extract(stream, ConnectionToken)
        partition_ops, stream = stream_extract(stream, PartitionToken)
        for op in stream: yield op

        # Generate partition map.
        partition_map = token_attr_map(partition_ops, 'element', 'partition')
        partitions = set(partition_map.values())

        partition_bounds = {name: PartitionBounds([], []) for name in partitions}
        for port_out, port_in in portmap(connection_ops).items():
            partition_out = partition_map.get(port_out, None)
            partition_in = partition_map.get(port_in, None)
            if partition_out != partition_in:
                if partition_out:
                    partition_bounds[partition_out].outs.append(port_out)
                if partition_in:
                    bounds_ins = partition_bounds[partition_in].ins
                    if port_in not in bounds_ins:
                        bounds_ins.append(port_in)

        for partition, bounds in partition_bounds.items():
            yield AddTokenOp(PartitionBoundsToken(partition, bounds))

class PartitionWorkerPass(object):
    def __call__(self, stream):
        # Capture connections, partition and partition bounds, yield rest.
        connection_ops, stream = stream_divert(stream, ConnectionToken)
        partition_bounds_ops, stream = stream_divert(stream, PartitionBoundsToken)
        partition_ops, stream = stream_divert(stream, PartitionToken)
        partition_select_ops, stream = stream_divert(stream, PartitionSelectToken)
        for op in stream: yield op

        # Find the selected partition.
        partition_select_tokens = list(token_attr_map(partition_select_ops, 'partition'))
        if len(partition_select_tokens) != 1:
            raise ParserError('Exactly one partition must be selected')

        selected_partition = partition_select_tokens[0]

        # Generate partition map.
        partition_map = token_attr_map(partition_ops, 'element', 'partition')
        partitions_elements = {}
        for element, partition in partition_map.items():
            partitions_elements.setdefault(partition, set()).add(element)

        # Generate partition bounds map.
        partition_bounds_map = token_attr_map(partition_bounds_ops,
                                              'partition', 'bounds')

        inner_ports = partitions_elements[selected_partition]
        bounds = partition_bounds_map[selected_partition]

        innames = list(range(len(bounds.outs)))
        outnames = list(range(len(bounds.ins)))

        worker = SubprocessWorker(innames=innames, outnames=outnames)
        yield AddTokenOp(ComponentToken(worker))

        # Purge/rewire connections.
        outmap = dict(zip(bounds.outs, worker.ins))
        inmap = dict(zip(bounds.ins, worker.outs))

        emitted_tokens = set()
        for port_out, port_in in portmap(connection_ops).items():
            if port_out in inner_ports and port_in in inner_ports:
                yield AddTokenOp(ConnectionToken(port_out, port_in))
            elif port_out in inner_ports:
                yield AddTokenOp(ConnectionToken(port_out, outmap[port_out]))
            elif port_in in inner_ports:
                # A workers output port potentially replaces multiple outputs
                # outside the partition. Hence it is necessary to guard against
                # adding duplicate connections here.
                token = ConnectionToken(inmap[port_in], port_in)
                if token not in emitted_tokens:
                    emitted_tokens.add(token)
                    yield AddTokenOp(token)

class PartitionControllersPass(object):
    def __call__(self, stream):
        # Capture connections, partition and partition bounds, yield rest.
        connection_ops, stream = stream_divert(stream, ConnectionToken)
        partition_ops, stream = stream_divert(stream, PartitionToken)
        partition_bounds_ops, stream = stream_divert(stream, PartitionBoundsToken)
        for op in stream: yield op

        outmap = dict()
        inmap = dict()
        inner_ports = set()

        # Generate partition map.
        partition_map = token_attr_map(partition_ops, 'element', 'partition')
        partitions_elements = {}
        for element, partition in partition_map.items():
            partitions_elements.setdefault(partition, set()).add(element)

        # Generate partition bounds map.
        partition_bounds_map = token_attr_map(partition_bounds_ops,
                                              'partition', 'bounds')

        for partition_name, partition_elements in partitions_elements.items():
            bounds = partition_bounds_map[partition_name]

            innames = list(range(len(bounds.ins)))
            outnames = list(range(len(bounds.outs)))
            controller = SubprocessController(partition_name, innames=innames, outnames=outnames)
            yield AddTokenOp(ComponentToken(controller))

            outmap.update(zip(bounds.outs, controller.outs))
            inmap.update(zip(bounds.ins, controller.ins))
            inner_ports.update(partition_elements)

        # Purge/rewire connections.
        for port_out, port_in in portmap(connection_ops).items():
            port_out = outmap.get(port_out, port_out)
            port_in = inmap.get(port_in, port_in)
            if port_out not in inner_ports or port_in not in inner_ports:
                yield AddTokenOp(ConnectionToken(port_out, port_in))

class ComponentsPurgePass(object):
    def __call__(self, stream):
        # Capture components, read connections, yield the rest.
        component_ops, stream = stream_divert(stream, ComponentToken)
        connection_ops, stream = stream_extract(stream, ConnectionToken)
        for op in stream: yield op

        all_ports = list(ports(connection_ops))
        for component in token_attr_map(component_ops, 'element'):
            some_ports = set(list(component.outs)[:1] + list(component.ins)[:1])
            if len(some_ports) and some_ports.pop() in all_ports:
                yield AddTokenOp(ComponentToken(component))

class EventHandlersPass(object):
    def __call__(self, stream):
        # Read components and connections, yield everything.
        component_ops, stream = stream_extract(stream, ComponentToken)
        connection_ops, stream = stream_extract(stream, ConnectionToken)
        for op in stream: yield op

        comps = list(ports(connection_ops))
        comps += list(token_attr_map(component_ops, 'element'))

        # Build attach event handlers.
        is_attachable = lambda comp: \
                hasattr(comp, 'attach') and callable(comp.attach)
        attachable_comps = (comp for comp in comps if is_attachable(comp))
        for comp in attachable_comps:
            callback = lambda event, comp=comp: \
                    comp.attach(event.scheduler, event.reactor)
            yield AddTokenOp(EventHandlerToken(scheduler.AttachEvent, 0, callback))

        # Build detach event handlers.
        is_detachable = lambda comp: \
                hasattr(comp, 'detach') and callable(comp.detach)
        detachable_comps = (comp for comp in comps if is_detachable(comp))
        for comp in detachable_comps:
            callback = lambda event, comp=comp: comp.detach()
            yield AddTokenOp(EventHandlerToken(scheduler.DetachEvent, 0, callback))
