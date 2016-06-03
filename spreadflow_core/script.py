"""
Provides utility functions for spreadflow config script.
"""

import inspect

from spreadflow_core.component import Compound
from spreadflow_core.dsl.context import Context
from spreadflow_core.dsl.tokens import \
    AliasToken, \
    ComponentToken, \
    ConnectionToken, \
    DefaultInputToken, \
    DefaultOutputToken, \
    DescriptionToken, \
    LabelToken, \
    PartitionToken
from spreadflow_core.proc import Duplicator

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

        if len(chain) > 1:
            upstream = chain[0]
            for downstream in chain[1:]:
                ctx.add(ConnectionToken(upstream, downstream))
                upstream = downstream

        ctx.add(DefaultInputToken(process, chain[0]))
        ctx.add(DefaultOutputToken(process, chain[-1]))
        ctx.add(ComponentToken(process))

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

        ctx.add(ComponentToken(process))
        ctx.add(ConnectionToken(process.out_duplicate, destination))
        ctx.setdefault(LabelToken(process, 'Copy to "{:s}"'.format(destination)))

        return process

def duplicate(ctx, *destinations):
    for dest in destinations:
        yield DuplicatorTemplate(destination=dest).apply(ctx)

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
            template = ChainTemplate(chain=template_factory(ctx))
        else:
            raise ProcessDecoratorError('Process decorator only works on '
                                        'subclasses of ProcessTemplate or '
                                        'functions')

        process = template.apply(ctx)

        ctx.setdefault(AliasToken(process, template_factory.__name__))
        ctx.setdefault(DescriptionToken(process, inspect.cleandoc(template_factory.__doc__ or '')))
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

def Chain(name, *procs, **kw): # pylint: disable=C0103
    """
    Forms a chain of the given components by connecting the default input port
    to the default output port of its predecessor.
    """

    ctx = Context.top()
    process = ChainTemplate(chain=procs).apply(ctx)

    ctx.add(LabelToken(process, name))
    ctx.add(AliasToken(process, name))

    if 'description' in kw:
        ctx.add(DescriptionToken(process, kw['description']))
    if 'partition' in kw:
        ctx.add(PartitionToken(process, kw['partition']))

    return process

def Duplicate(port_in, **kw): # pylint: disable=C0103
    """
    Creates a message duplicator and connects its secondary output port to the
    given input port.
    """

    ctx = Context.top()
    process = DuplicatorTemplate(destination=port_in).apply(ctx)

    if 'alias' in kw:
        ctx.add(AliasToken(process, kw['alias']))
    if 'label' in kw:
        ctx.add(LabelToken(process, kw['label']))
    if 'description' in kw:
        ctx.add(DescriptionToken(process, kw['description']))
    if 'partition' in kw:
        ctx.add(PartitionToken(process, kw['partition']))

    return process
