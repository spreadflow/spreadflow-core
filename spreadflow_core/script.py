"""
Provides utility functions for spreadflow config script.
"""

from spreadflow_core.decorator import DecoratorGenerator
from spreadflow_core.flow import PortCollection, Flowmap
from spreadflow_core.proc import Duplicator, Compound

import collections

flowmap = Flowmap()

def Subscribe(port_in, port_out):
    """
    Connect an input port with an output port.
    """
    if isinstance(port_out, PortCollection):
        flowmap.annotations.setdefault(port_out, {})
        port_out = port_out.outs[-1]
    if isinstance(port_in, PortCollection):
        flowmap.annotations.setdefault(port_in, {})
        port_in = port_in.ins[0]

    if port_out in flowmap:
        RuntimeError('Attempting to connect more than one input port to an output port')
    flowmap[port_out] = port_in

def Chain(*procs):
    compound = Compound(procs)
    flowmap.annotations.setdefault(compound, {})

    upstream = procs[0]
    for downstream in procs[1:]:
        Subscribe(downstream, upstream)
        upstream = downstream

    return compound

def Duplicate(port_in):
    """
    Creates a message duplicator and connects its secondary output port to the
    given input port.
    """

    duplicator = Duplicator()
    Subscribe(port_in, duplicator.out_duplicate)
    return duplicator


def Decorate(decorator, predicate=lambda p: True):
    flowmap.decorators.append(DecoratorGenerator(decorator, predicate))

def Annotate(target, **kw):
    items = flowmap.annotations.get(target, {}).items() + kw.items()
    flowmap.annotations[target] = dict(items)

def DecorateSinks(decorator, *port_in):
    Decorate(decorator, lambda p: p in port_in)
