# -*- coding: utf-8 -*-

"""
Domain-specific language for building up flowmaps.
"""

import inspect
import itertools
from collections import namedtuple, OrderedDict, Counter

from spreadflow_core import scheduler
from spreadflow_core.component import Compound, PortCollection, COMPONENT_VISITORS
from spreadflow_core.proc import Duplicator
from spreadflow_core.subprocess import SubprocessWorker, SubprocessController

try:
    StringType = basestring # pylint: disable=undefined-variable
except NameError:
    StringType = str

class DSLError(Exception):
    pass

class NoContextError(DSLError):
    pass

class ProcessDecoratorError(DSLError):
    pass

class DuplicateTokenError(DSLError):
    pass

class NoSuchTokenError(DSLError):
    pass

class CompilerError(DSLError):
    pass

AliasToken = namedtuple('AliasToken', ['element', 'alias'])
ComponentToken = namedtuple('ComponentToken', ['element'])
ConnectionToken = namedtuple('ConnectionToken', ['port_out', 'port_in'])
DescriptionToken = namedtuple('DescriptionToken', ['element', 'description'])
EventHandlerToken = namedtuple('EventHandlerToken', ['event_type', 'priority', 'callback'])
LabelToken = namedtuple('LabelToken', ['element', 'label'])
PartitionSelectToken = namedtuple('PartitionSelectToken', ['partition'])
PartitionToken = namedtuple('PartitionToken', ['element', 'partition'])
class PartitionBoundsToken(namedtuple('PartitionBoundsToken', ['partition', 'outs', 'ins'])):
    __slots__ = ()
    def __hash__(self):
        return hash(self.partition)

AddTokenOp = namedtuple('AddTokenOp', ['token'])
SetDefaultTokenOp = namedtuple('SetDefaultTokenOp', ['token'])
RemoveTokenOp = namedtuple('RemoveTokenOp', ['token'])

CONTEXT_STACK = []

class Context(object):
    """
    DSL context.
    """

    _ctx_stack = CONTEXT_STACK
    _saved_visitors = None

    def __init__(self, origin, stack=None):
        self.origin = origin
        self.tokens = []
        self.stack = stack if stack is not None else inspect.stack()[1:]

    def __enter__(self):
        self.push(self)
        self._saved_visitors = list(COMPONENT_VISITORS)
        COMPONENT_VISITORS.append(self._add_component)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pop(self)
        if COMPONENT_VISITORS.pop() != self._add_component or COMPONENT_VISITORS != self._saved_visitors:
            raise RuntimeError('COMPONENT_VISITORS changed during script evaluation')
        return False

    def setdefault(self, token):
        self.tokens.append(SetDefaultTokenOp(token))

    def add(self, token):
        self.tokens.append(AddTokenOp(token))

    def remove(self, token):
        self.tokens.append(RemoveTokenOp(token))

    def _add_component(self, comp):
        self.add(ComponentToken(comp))

    @classmethod
    def push(cls, ctx):
        """
        Push the ctx onto the shared stack.
        """
        cls._ctx_stack.append(ctx)

    @classmethod
    def pop(cls, ctx):
        """
        Remove the ctx from the shared stack.
        """
        top = cls._ctx_stack.pop()
        assert top is ctx, 'Unbalanced DSL ctx stack'

    @classmethod
    def top(cls):
        """
        Returns the topmost ctx from the stack.
        """
        try:
            return cls._ctx_stack[-1]
        except IndexError:
            raise NoContextError()

class ProcessTemplate(object):
    def apply(self, ctx):
        raise NotImplementedError()

class ChainTemplate(ProcessTemplate):
    chain = None

    def __init__(self, chain=None):
        if chain is not None:
            self.chain = chain

    def apply(self, ctx):
        chain = list(self.chain)
        for idx, element in enumerate(chain):
            if isinstance(element, ProcessTemplate):
                chain[idx] = element.apply(ctx)
        process = Compound(chain)

        upstream = chain[0]
        for downstream in chain[1:]:
            ctx.add(ConnectionToken(upstream, downstream))
            upstream = downstream

        return process

class DuplicatorTemplate(ProcessTemplate):
    destination = None

    def __init__(self, destination=None):
        if destination is not None:
            self.destination = destination

    def apply(self, ctx):
        process = Duplicator()

        destination = self.destination
        if isinstance(destination, ProcessTemplate):
            destination = destination.apply(ctx)

        ctx.add(ConnectionToken(process.out_duplicate, destination))
        ctx.setdefault(LabelToken(process, 'Copy to "{:s}"'.format(destination)))

        return process

def duplicate(ctx, *destinations):
    for dest in destinations:
        yield DuplicatorTemplate(destination=dest).apply(ctx)

class Process(object):
    """
    Produces a flow process from a template class or chain generator function.
    """

    def __init__(self, alias=None, label=None, description=None, partition=None):
        self.alias = alias
        self.label = label
        self.description = description
        self.partition = partition

    def __call__(self, template_factory):
        ctx = Context.top()

        if isinstance(template_factory, type) and issubclass(template_factory, ProcessTemplate):
            template = template_factory()
        elif isinstance(template_factory, object) and callable(template_factory):
            template = ChainTemplate(chain=template_factory(ctx))
        else:
            raise ProcessDecoratorError('Process decorator only works on '
                                        'subclasses of ProcessTemplate or '
                                        'functions')

        process = template.apply(ctx)

        ctx.setdefault(AliasToken(process, template_factory.__name__))
        ctx.setdefault(DescriptionToken(process, template_factory.__doc__))
        ctx.setdefault(LabelToken(process, template_factory.__name__))

        if self.alias is not None:
            ctx.add(AliasToken(process, self.alias))
        if self.label is not None:
            ctx.add(LabelToken(process, self.label))
        if self.description is not None:
            ctx.add(DescriptionToken(process, self.description))
        if self.partition is not None:
            ctx.add(PartitionToken(process, self.partition))

        return process

def stream_extract(stream, token_class):
    tokens = list(stream)
    extracted_stream = (op for op in tokens if isinstance(op.token, token_class))
    return extracted_stream, tokens

def stream_divert(stream, token_class):
    tokens = list(stream)
    extracted_stream = (op for op in tokens if isinstance(op.token, token_class))
    remaining_stream = (op for op in tokens if not isinstance(op.token, token_class))
    return extracted_stream, remaining_stream

def minimize_strict(stream, keyfunc=lambda op: op.token):
    present = {}
    tokens = OrderedDict()

    for op in stream:
        key = keyfunc(op)

        if isinstance(op, AddTokenOp):
            if present.get(key, False):
                raise DuplicateTokenError(op.token)
            else:
                present[key] = True

            tokens[key] = op.token
        elif isinstance(op, SetDefaultTokenOp):
            tokens.setdefault(key, op.token)
        elif isinstance(op, RemoveTokenOp):
            try:
                del tokens[key]
            except KeyError:
                raise NoSuchTokenError(op.token)

            present[key] = False

    return tokens.values()

class AliasResolverPass(object):
    def __call__(self, stream):
        # Capture aliases and connections, yield all the rest
        alias_ops, stream = stream_divert(stream, AliasToken)
        connection_ops, stream = stream_divert(stream, ConnectionToken)
        for op in stream: yield op

        # Generate alias map.
        alias_tokens = minimize_strict(alias_ops)
        elements, aliases = zip(*alias_tokens)
        alias_counts = Counter(aliases).items()
        multi_aliases = [alias for alias, count in alias_counts if count > 1]
        if len(multi_aliases):
            raise CompilerError('Alias must be unique', multi_aliases)

        aliasmap = dict(zip(aliases, elements))

        # Generate connection operations.
        for port_out, port_in in minimize_strict(connection_ops):
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

            yield AddTokenOp(ConnectionToken(port_out, port_in))

class PortsValidatorPass(object):
    """
    Verifies used input/output ports.
    """

    def __call__(self, stream):
        connection_ops, stream = stream_extract(stream, ConnectionToken)

        connection_list = list(minimize_strict(connection_ops))

        if len(connection_list) == 0:
            raise CompilerError('Connection list is empty')

        outs, ins = zip(*connection_list)

        non_callable_ins = [port for port in ins if not callable(port)]
        if len(non_callable_ins):
            raise CompilerError(non_callable_ins, 'Input ports must be '
                                   'callable')

        out_counts = Counter(outs).items()
        multi_outs = [port for port, count in out_counts if count > 1]
        if len(multi_outs):
            raise CompilerError(multi_outs, 'Output ports must not have '
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
        partition_tokens = set(minimize_strict(partition_ops))
        elements, partitions = zip(*partition_tokens)
        element_counts = Counter(elements).items()
        multi_elements = [element for element, count in element_counts if count > 1]
        if len(multi_elements):
            raise CompilerError('Elements cannot be in more than one partition', multi_elements)

        partition_map = dict(zip(elements, partitions))

        # Process components.
        for component, in minimize_strict(component_ops):
            try:
                partition_name = partition_map[component]
            except KeyError:
                continue
            else:
                for port in set(component.ins + component.outs):
                    partition_tokens.add(PartitionToken(port, partition_name))

        # Generate updated partition map.
        elements, partitions = zip(*partition_tokens)
        element_counts = Counter(elements).items()
        multi_elements = [element for element, count in element_counts if count > 1]
        if len(multi_elements):
            raise CompilerError('Elements cannot be in more than one partition', multi_elements)

        for token in partition_tokens:
            yield AddTokenOp(token)

class PartitionBoundsPass(object):
    def __call__(self, stream):
        # Read partitions and connections re-yield evereything
        connection_ops, stream = stream_extract(stream, ConnectionToken)
        partition_ops, stream = stream_extract(stream, PartitionToken)
        for op in stream: yield op

        # Generate partition map.
        partition_tokens = set(minimize_strict(partition_ops))
        elements, partitions = zip(*partition_tokens)
        element_counts = Counter(elements).items()
        multi_elements = [element for element, count in element_counts if count > 1]
        if len(multi_elements):
            raise CompilerError('Elements cannot be in more than one partition', multi_elements)

        partition_map = dict(zip(elements, partitions))

        partition_bounds = {name: PartitionBoundsToken(name, [], []) for name in set(partitions)}
        for port_out, port_in in minimize_strict(connection_ops):
            partition_out = partition_map.get(port_out, None)
            partition_in = partition_map.get(port_in, None)
            if partition_out != partition_in:
                if partition_out:
                    partition_bounds[partition_out].outs.append(port_out)
                if partition_in:
                    bounds_ins = partition_bounds[partition_in].ins
                    if port_in not in bounds_ins:
                        bounds_ins.append(port_in)

        for token in partition_bounds.values():
            yield AddTokenOp(token)

class PartitionWorkerPass(object):
    def __call__(self, stream):
        # Capture connections, partition and partition bounds, yield rest.
        connection_ops, stream = stream_divert(stream, ConnectionToken)
        partition_bounds_ops, stream = stream_divert(stream, PartitionBoundsToken)
        partition_ops, stream = stream_divert(stream, PartitionToken)
        partition_select_ops, stream = stream_divert(stream, PartitionSelectToken)
        for op in stream: yield op

        # Find the selected partition.
        partition_select_tokens = list(minimize_strict(partition_select_ops))
        if len(partition_select_tokens) != 1:
            raise CompilerError('Exactly one partition must be selected')

        partition = partition_select_tokens[0].partition

        # Generate partition map.
        partition_tokens = set(minimize_strict(partition_ops))
        elements, partitions = zip(*partition_tokens)
        element_counts = Counter(elements).items()
        multi_elements = [element for element, count in element_counts if count > 1]
        if len(multi_elements):
            raise CompilerError('Elements cannot be in more than one partition', multi_elements)

        partitions_elements = {name: set() for name in set(partitions)}
        for element, partition in partition_tokens:
            partitions_elements[partition].add(element)

        # Generate partition_bounds bounds map.
        partition_bounds_tokens = set(minimize_strict(partition_bounds_ops))
        partitions, partition_outs_list, partition_ins_list = zip(*partition_bounds_tokens)
        partition_counts = Counter(partitions).items()
        multi_partitions = [partition for partition, count in partition_counts if count > 1]
        if len(multi_partitions):
            raise CompilerError('Multiple conflicting definitions of partition bounds', multi_partitions)

        partition_bounds_outs = dict(zip(partitions, partition_outs_list))
        partition_bounds_ins = dict(zip(partitions, partition_ins_list))

        partition_elements = partitions_elements[partition]
        bounds_outs = partition_bounds_outs[partition]
        bounds_ins = partition_bounds_ins[partition]

        innames = list(range(len(bounds_outs)))
        outnames = list(range(len(bounds_ins)))

        worker = SubprocessWorker(innames=innames, outnames=outnames)
        yield AddTokenOp(ComponentToken(worker))

        # Purge/rewire connections.
        outmap = dict(zip(bounds_outs, worker.ins))
        inmap = dict(zip(bounds_ins, worker.outs))

        for port_out, port_in in minimize_strict(connection_ops):
            if port_out in partition_elements and port_in in partition_elements:
                yield AddTokenOp(ConnectionToken(port_out, port_in))
            elif port_out in partition_elements:
                yield AddTokenOp(ConnectionToken(port_out, outmap[port_out]))
            elif port_in in partition_elements:
                yield AddTokenOp(ConnectionToken(inmap[port_in], port_in))

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
        partition_tokens = set(minimize_strict(partition_ops))
        elements, partitions = zip(*partition_tokens)
        element_counts = Counter(elements).items()
        multi_elements = [element for element, count in element_counts if count > 1]
        if len(multi_elements):
            raise CompilerError('Elements cannot be in more than one partition', multi_elements)

        partitions_elements = {name: set() for name in set(partitions)}
        for element, partition in partition_tokens:
            partitions_elements[partition].add(element)

        # Generate partition_bounds bounds map.
        partition_bounds_tokens = set(minimize_strict(partition_bounds_ops))
        partitions, partition_outs_list, partition_ins_list = zip(*partition_bounds_tokens)
        partition_counts = Counter(partitions).items()
        multi_partitions = [partition for partition, count in partition_counts if count > 1]
        if len(multi_partitions):
            raise CompilerError('Multiple conflicting definitions of partition bounds', multi_partitions)

        partition_bounds_outs = dict(zip(partitions, partition_outs_list))
        partition_bounds_ins = dict(zip(partitions, partition_ins_list))

        for partition_name, partition_elements in partitions_elements.items():
            bounds_outs = partition_bounds_outs[partition_name]
            bounds_ins = partition_bounds_ins[partition_name]

            innames = list(range(len(bounds_ins)))
            outnames = list(range(len(bounds_outs)))
            controller = SubprocessController(partition_name, innames=innames, outnames=outnames)
            yield AddTokenOp(ComponentToken(controller))

            outmap.update(zip(bounds_outs, controller.outs))
            inmap.update(zip(bounds_ins, controller.ins))
            inner_ports.update(partition_elements)

        # Purge/rewire connections.
        for port_out, port_in in minimize_strict(connection_ops):
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

        all_ports = list(itertools.chain(*zip(*minimize_strict(connection_ops))))
        for component, in minimize_strict(component_ops):
            any_port = set(list(component.outs)[:1] + list(component.ins)[:1]).pop()
            if any_port in all_ports:
                yield AddTokenOp(ComponentToken(component))

class EventHandlersPass(object):
    def __call__(self, stream):
        # Read components and connections, yield everything.
        component_ops, stream = stream_extract(stream, ComponentToken)
        connection_ops, stream = stream_extract(stream, ConnectionToken)
        for op in stream: yield op

        all_ports = list(itertools.chain(*zip(*minimize_strict(connection_ops))))
        all_components = [component for component, in minimize_strict(component_ops)]
        comps = all_ports + all_components

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
