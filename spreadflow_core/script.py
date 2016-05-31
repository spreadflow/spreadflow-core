"""
Provides utility functions for spreadflow config script.
"""

from spreadflow_core.dsl import \
    AliasToken, \
    ChainTemplate, \
    Context, \
    DescriptionToken, \
    DuplicatorTemplate, \
    LabelToken, \
    PartitionToken

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
