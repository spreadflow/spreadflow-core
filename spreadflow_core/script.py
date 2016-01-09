"""
Provides utility functions for spreadflow config script.
"""

import collections

from spreadflow_core.flow import Flowmap
from spreadflow_core.decorator import DecoratorGenerator

flowmap = Flowmap()

def Subscribe(port_in, port_out):
    """
    Connect an input port with an output port.
    """
    if isinstance(port_out, collections.Sequence):
        port_out = port_out[-1]
    if isinstance(port_in, collections.Sequence):
        port_in = port_in[0]

    if port_out in flowmap:
        RuntimeError('Attempting to connect more than one input port to an output port')
    flowmap[port_out] = port_in

def Chain(*procs):
    proc = procs[0]
    for downstream in procs[1:]:
        Subscribe(downstream, proc)
        proc = downstream

    return procs

def Decorate(decorator, predicate=lambda p: True):
    flowmap.decorators.append(DecoratorGenerator(decorator, predicate))

def Annotate(target, **kw):
    if isinstance(target, collections.Sequence):
        target = target[0]
    items = flowmap.annotations.get(target, {}).items() + kw.items()
    flowmap.annotations[target] = dict(items)

def DecorateSinks(decorator, *port_in):
    Decorate(decorator, lambda p: p in port_in)
