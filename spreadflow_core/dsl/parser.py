# -*- coding: utf-8 -*-

"""
Parser steps used in the domain-specific language for building up flowmaps.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import itertools
from collections import Counter, namedtuple
from toposort import toposort_flatten

from spreadflow_core import scheduler, graph
from spreadflow_core.dsl.stream import \
    AddTokenOp, \
    SetDefaultTokenOp, \
    StreamBranch, \
    token_attr_map, \
    token_map
from spreadflow_core.dsl.tokens import \
    AliasToken, \
    ComponentToken, \
    ConnectionToken, \
    DefaultInputToken, \
    DefaultOutputToken, \
    DescriptionToken, \
    EventHandlerToken, \
    LabelToken, \
    ParentElementToken, \
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

def parentmap(stream):
    return token_attr_map(stream, 'element', 'parent')

def treenodes(stream):
    all_nodes = parentmap(stream).items()
    return itertools.chain(*zip(*all_nodes))

class ParserError(Exception):
    pass

class AliasParser(StreamBranch):
    """
    Builds an alias map from stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, AliasToken)

    def get_aliasmap(self):
        """
        Returns a map alias -> element
        """
        return token_attr_map(self.selected, 'alias', 'element')

class ComponentParser(StreamBranch):
    """
    Extracts exactly one component from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, ComponentToken)

    def get_component(self):
        """
        Returns the component.
        """
        components = list(token_attr_map(self.selected, 'component'))

        if len(components) != 1:
            raise ParserError('Process template must generate exactly one component token')

        return components[0]

class ConnectionParser(StreamBranch):
    """
    Extracts information about connected ports from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, ConnectionToken)

    def get_portmap(self):
        """
        Returns a map output -> input
        """
        return token_attr_map(self.selected, 'port_out', 'port_in')

    def get_links(self):
        """
        Returns an iterator over output, input pairs
        """
        return self.get_portmap().items()

    def get_portset(self):
        """
        Returns a set containing all ports.
        """
        all_connections = self.get_links()
        return set(itertools.chain(*zip(*all_connections)))

class DefaultInputParser(StreamBranch):
    """
    Builds map of default inputs from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, DefaultInputToken)

    def get_portmap(self):
        """
        Returns a map element -> port
        """
        return token_attr_map(self.selected, 'element', 'port')

class DefaultOutputParser(StreamBranch):
    """
    Builds map of default outputs from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, DefaultOutputToken)

    def get_portmap(self):
        """
        Returns a map element -> port
        """
        return token_attr_map(self.selected, 'element', 'port')

class DescriptionParser(StreamBranch):
    """
    Builds map of descriptions from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, DescriptionToken)

    def get_descriptionmap(self):
        """
        Returns a map element -> description
        """
        return token_attr_map(self.selected, 'element', 'description')

class EventHandlerParser(StreamBranch):
    """
    Extracts a list of event handlers from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, EventHandlerToken)

    def get_handlers(self):
        """
        Returns an iterator over event handler tokens.
        """
        return token_map(self.selected).values()

class LabelParser(StreamBranch):
    """
    Builds map of labels from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, LabelToken)

    def get_labelmap(self):
        """
        Returns a map element -> label
        """
        return token_attr_map(self.selected, 'element', 'label')

class ParentParser(StreamBranch):
    """
    Extracts information about component hierarchy from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, ParentElementToken)

    def get_parentmap(self):
        """
        Returns a map element -> parent.
        """
        return token_attr_map(self.selected, 'element', 'parent')

    def get_parentmap_toposort(self, reverse=False):
        """
        Returns an iterator in topological order over element, parent pairs.
        """
        parent_map = self.get_parentmap()

        digraph = graph.digraph(parent_map.items())
        if reverse:
            digraph = graph.reverse(digraph)

        for element in toposort_flatten(digraph, sort=False):
            yield element, parent_map.get(element, None)

    def get_nodeset(self):
        """
        Returns a set containing all elements.
        """
        all_nodes = self.get_parentmap().items()
        return set(itertools.chain(*zip(*all_nodes)))

class PartitionBoundsParser(StreamBranch):
    """
    Builds map of partition bounds from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, PartitionBoundsToken)

    def get_partition_bounds(self):
        """
        Returns a map partition name -> bounds
        """
        return token_attr_map(self.selected, 'partition', 'bounds')

class PartitionParser(StreamBranch):
    """
    Builds map of partitions from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, PartitionToken)

    def get_partitionmap(self):
        """
        Returns a map element -> partition name
        """
        return token_attr_map(self.selected, 'element', 'partition')

class PartitionSelectParser(StreamBranch):
    """
    Extracts the selected partition from a stream of operations.
    """

    def predicate(self, operation):
        return isinstance(operation.token, PartitionSelectToken)

    def get_selected_partition(self):
        """
        Returns the name of the selected partition.
        """
        partition_select_tokens = list(token_attr_map(self.selected, 'partition'))

        if len(partition_select_tokens) != 1:
            raise ParserError('Exactly one partition must be selected')

        return partition_select_tokens[0]

class AliasResolverPass(object):
    alias_parser = AliasParser()
    connection_parser = ConnectionParser()
    default_ins_parser = DefaultInputParser()
    default_outs_parser = DefaultOutputParser()

    def __call__(self, stream):
        # Capture aliases, connections, default ins/outs and yield all the rest
        stream = self.alias_parser.divert(stream)
        stream = self.connection_parser.divert(stream)
        stream = self.default_ins_parser.divert(stream)
        stream = self.default_outs_parser.divert(stream)
        for op in stream: yield op

        # Generate alias map.
        aliases = self.alias_parser.get_aliasmap()
        default_inputs = self.default_ins_parser.get_portmap()
        default_outputs = self.default_outs_parser.get_portmap()

        # Generate connection operations.
        for port_out, port_in in self.connection_parser.get_links():
            while True:
                if isinstance(port_out, StringType):
                    port_out = aliases[port_out]
                elif port_out in default_outputs:
                    port_out = default_outputs[port_out]
                else:
                    break

            while True:
                if isinstance(port_in, StringType):
                    port_in = aliases[port_in]
                elif port_in in default_inputs:
                    port_in = default_inputs[port_in]
                else:
                    break

            yield AddTokenOp(ConnectionToken(port_out, port_in))

class PortsValidatorPass(object):
    """
    Verifies used input/output ports.
    """

    connection_parser = ConnectionParser()

    def __call__(self, stream):
        stream = self.connection_parser.extract(stream)
        connection_list = list(self.connection_parser.get_links())

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

    parent_parser = ParentParser()
    partition_parser = PartitionParser()

    def __call__(self, stream):
        # Capture partitions, read components, yield all the rest
        stream = self.parent_parser.extract(stream)
        stream = self.partition_parser.divert(stream)
        for op in stream: yield op

        # Generate parent map and partition map.
        partition_map = self.partition_parser.get_partitionmap()

        # Inherit partition settings by walking down the component tree in
        # topological order.
        for element, parent in self.parent_parser.get_parentmap_toposort():
            try:
                parent_partition = partition_map[parent]
            except KeyError:
                continue

            partition_map.setdefault(element, parent_partition)

        # Produce updated partition map.
        for element, partition in partition_map.items():
            yield AddTokenOp(PartitionToken(element, partition))

PartitionBounds = namedtuple('PartitionBounds', ['outs', 'ins'])

class PartitionBoundsPass(object):
    connection_parser = ConnectionParser()
    partition_parser = PartitionParser()

    def __call__(self, stream):
        # Read partitions and connections re-yield evereything
        stream = self.connection_parser.extract(stream)
        stream = self.partition_parser.extract(stream)
        for op in stream: yield op

        # Generate partition map.
        partition_map = self.partition_parser.get_partitionmap()
        partitions = set(partition_map.values())

        partition_bounds = {name: PartitionBounds([], []) for name in partitions}
        for port_out, port_in in self.connection_parser.get_links():
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
    connection_parser = ConnectionParser()
    partition_bounds_parser = PartitionBoundsParser()
    partition_parser = PartitionParser()
    partition_select_parser = PartitionSelectParser()

    def __call__(self, stream):
        # Capture connections, partition and partition bounds, yield rest.
        stream = self.connection_parser.divert(stream)
        stream = self.partition_bounds_parser.divert(stream)
        stream = self.partition_parser.divert(stream)
        stream = self.partition_select_parser.divert(stream)
        for op in stream: yield op

        selected_partition = self.partition_select_parser.get_selected_partition()

        partition_map = self.partition_parser.get_partitionmap()
        partition_elements = set(elm for elm, part in partition_map.items() if part == selected_partition)

        partition_bounds_map = self.partition_bounds_parser.get_partition_bounds()
        bounds = partition_bounds_map[selected_partition]

        innames = list(range(len(bounds.outs)))
        outnames = list(range(len(bounds.ins)))

        worker = SubprocessWorker(innames=innames, outnames=outnames)
        for port in worker.ins + worker.outs:
            yield AddTokenOp(ParentElementToken(port, worker))

        # Purge/rewire connections.
        outmap = dict(zip(bounds.outs, worker.ins))
        inmap = dict(zip(bounds.ins, worker.outs))

        emitted_tokens = set()
        for port_out, port_in in self.connection_parser.get_links():
            if port_out in partition_elements and port_in in partition_elements:
                yield AddTokenOp(ConnectionToken(port_out, port_in))
            elif port_out in partition_elements:
                yield AddTokenOp(ConnectionToken(port_out, outmap[port_out]))
            elif port_in in partition_elements:
                # A workers output port potentially replaces multiple outputs
                # outside the partition. Hence it is necessary to guard against
                # adding duplicate connections here.
                token = ConnectionToken(inmap[port_in], port_in)
                if token not in emitted_tokens:
                    emitted_tokens.add(token)
                    yield AddTokenOp(token)

class PartitionControllersPass(object):
    connection_parser = ConnectionParser()
    partition_bounds_parser = PartitionBoundsParser()
    partition_parser = PartitionParser()

    def __call__(self, stream):
        # Capture connections, partition and partition bounds, yield rest.
        stream = self.connection_parser.divert(stream)
        stream = self.partition_bounds_parser.divert(stream)
        stream = self.partition_parser.divert(stream)
        for op in stream: yield op

        outmap = dict()
        inmap = dict()

        partition_map = self.partition_parser.get_partitionmap()
        partition_bounds_map = self.partition_bounds_parser.get_partition_bounds()

        for partition_name, bounds in partition_bounds_map.items():
            innames = list(range(len(bounds.ins)))
            outnames = list(range(len(bounds.outs)))
            controller = SubprocessController(partition_name, innames=innames, outnames=outnames)
            yield SetDefaultTokenOp(LabelToken(controller, "Subprocess {:s}".format(partition_name)))

            for port in controller.ins + controller.outs:
                yield AddTokenOp(ParentElementToken(port, controller))

            outmap.update(zip(bounds.outs, controller.outs))
            inmap.update(zip(bounds.ins, controller.ins))

        # Purge/rewire connections.
        for port_out, port_in in self.connection_parser.get_links():
            partition_out = partition_map.get(port_out, None)
            partition_in = partition_map.get(port_in, None)
            if partition_out is None and partition_in is None:
                yield AddTokenOp(ConnectionToken(port_out, port_in))
            elif partition_out != partition_in:
                port_out = outmap.get(port_out, port_out)
                port_in = inmap.get(port_in, port_in)
                yield AddTokenOp(ConnectionToken(port_out, port_in))

class ComponentsPurgePass(object):
    connection_parser = ConnectionParser()
    parent_parser = ParentParser()

    def __call__(self, stream):
        # Capture parents, read connections, yield the rest.
        stream = self.connection_parser.extract(stream)
        stream = self.parent_parser.divert(stream)
        for op in stream: yield op

        # Initialize connected elements set with all connected ports.
        connected_elements = self.connection_parser.get_portset()

        # Walk the component tree from leaves to roots and collect connected
        # elements on the way down.
        for element, parent in self.parent_parser.get_parentmap_toposort(reverse=True):
            if parent is not None and element in connected_elements:
                connected_elements.add(parent)
                yield AddTokenOp(ParentElementToken(element, parent))

class EventHandlersPass(object):
    connection_parser = ConnectionParser()
    parent_parser = ParentParser()

    def __call__(self, stream):
        # Read components and connections, yield everything.
        stream = self.connection_parser.extract(stream)
        stream = self.parent_parser.extract(stream)
        for op in stream: yield op

        portset = self.connection_parser.get_portset()
        nodeset = self.parent_parser.get_nodeset()
        comps = portset.union(nodeset)

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
