"""
Provides utility functions for spreadflow config script.
"""

import inspect

from spreadflow_core.component import Compound
from spreadflow_core.dsl.context import Context
from spreadflow_core.dsl.stream import AddTokenOp, SetDefaultTokenOp
from spreadflow_core.dsl.parser import ComponentParser
from spreadflow_core.dsl.tokens import \
    AliasToken, \
    ComponentToken, \
    ConnectionToken, \
    DefaultInputToken, \
    DefaultOutputToken, \
    DescriptionToken, \
    LabelToken, \
    ParentElementToken, \
    PartitionToken
from spreadflow_core.proc import Duplicator

class ProcessTemplate(object):
    def apply(self):
        raise NotImplementedError()

class ChainTemplate(ProcessTemplate):
    chain = None

    def __init__(self, chain=None):
        if chain is not None:
            self.chain = chain

    def apply(self):
        elements = []

        # Apply (sub)templates if necessary.
        for element in self.chain:
            if isinstance(element, ProcessTemplate):
                parser = ComponentParser().push(element.apply())
                elements.append(parser.get_component())
                for op in parser.rejected: yield op
            else:
                elements.append(element)

        component = Compound(elements)
        yield AddTokenOp(ComponentToken(component))

        for element in elements:
            yield AddTokenOp(ParentElementToken(element, component))

        # Connect all ports in the chain.
        if len(elements) > 1:
            upstream = elements[0]
            for downstream in elements[1:]:
                yield AddTokenOp(ConnectionToken(upstream, downstream))
                upstream = downstream

        # Set default input to first and default output to the last port.
        yield AddTokenOp(DefaultInputToken(component, elements[0]))
        yield AddTokenOp(DefaultOutputToken(component, elements[-1]))

class DuplicatorTemplate(ProcessTemplate):
    destination = None

    def __init__(self, destination=None):
        if destination is not None:
            self.destination = destination

    def apply(self):
        # Apply (sub)template if necessary.
        destination = self.destination
        if isinstance(destination, ProcessTemplate):
            parser = ComponentParser().push(destination.apply())
            destination = parser.get_component()
            for op in parser.rejected: yield op

        process = Duplicator()
        yield AddTokenOp(ComponentToken(process))

        # Set the parent for the secondary output.
        yield AddTokenOp(ParentElementToken(process.out_duplicate, process))

        # Connect the secondary output to the given downstream port.
        yield AddTokenOp(ConnectionToken(process.out_duplicate, destination))
        yield SetDefaultTokenOp(LabelToken(process, 'Copy to "{:s}"'.format(destination)))

def duplicate(*destinations):
    for dest in destinations:
        for template in DuplicatorTemplate(destination=dest):
            yield template

class ProcessDecoratorError(Exception):
    pass

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
            template = ChainTemplate(chain=template_factory())
        else:
            raise ProcessDecoratorError('Process decorator only works on '
                                        'subclasses of ProcessTemplate or '
                                        'functions')

        parser = ComponentParser().push(template.apply())
        process = parser.get_component()
        operations = list(parser.rejected)

        operations.append(SetDefaultTokenOp(AliasToken(process, template_factory.__name__)))
        operations.append(SetDefaultTokenOp(DescriptionToken(process, inspect.cleandoc(template_factory.__doc__ or ''))))
        operations.append(SetDefaultTokenOp(LabelToken(process, template_factory.__name__)))

        if self.alias is not None:
            operations.append(AddTokenOp(AliasToken(process, self.alias)))
        if self.label is not None:
            operations.append(AddTokenOp(LabelToken(process, self.label)))
        if self.description is not None:
            operations.append(AddTokenOp(DescriptionToken(process, self.description)))
        if self.partition is not None:
            operations.append(AddTokenOp(PartitionToken(process, self.partition)))

        ctx.tokens.extend(operations)

        return process

def Chain(name, *procs, **kw): # pylint: disable=C0103
    """
    Forms a chain of the given components by connecting the default input port
    to the default output port of its predecessor.
    """

    ctx = Context.top()
    template = ChainTemplate(chain=procs)

    parser = ComponentParser().push(template.apply())
    process = parser.get_component()
    operations = list(parser.rejected)

    operations.append(AddTokenOp(LabelToken(process, name)))
    operations.append(AddTokenOp(AliasToken(process, name)))

    if 'description' in kw:
        operations.append(AddTokenOp(DescriptionToken(process, kw['description'])))
    if 'partition' in kw:
        operations.append(AddTokenOp(PartitionToken(process, kw['partition'])))

    ctx.tokens.extend(operations)

    return process

def Duplicate(port_in, **kw): # pylint: disable=C0103
    """
    Creates a message duplicator and connects its secondary output port to the
    given input port.
    """

    ctx = Context.top()
    template = DuplicatorTemplate(destination=port_in)

    parser = ComponentParser().push(template.apply())
    process = parser.get_component()
    operations = list(parser.rejected)

    if 'alias' in kw:
        operations.append(AddTokenOp(AliasToken(process, kw['alias'])))
    if 'label' in kw:
        operations.append(AddTokenOp(LabelToken(process, kw['label'])))
    if 'description' in kw:
        operations.append(AddTokenOp(DescriptionToken(process, kw['description'])))
    if 'partition' in kw:
        operations.append(AddTokenOp(PartitionToken(process, kw['partition'])))

    ctx.tokens.extend(operations)

    return process
