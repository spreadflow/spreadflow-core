"""
Provides utility functions for spreadflow config script.
"""

from spreadflow_core.flow import PortCollection, Flowmap
from spreadflow_core.proc import Duplicator, Compound

import collections

flowmap = Flowmap()

def Chain(name, *procs, **kw):
    compound = Compound(procs)
    flowmap.aliasmap[name] = compound

    flowmap.annotations[compound] = kw
    flowmap.annotations[compound].setdefault('label', name)

    upstream = procs[0]
    for downstream in procs[1:]:
        flowmap.connections.append((upstream, downstream))
        upstream = downstream

    return compound

def Duplicate(port_in, **kw):
    """
    Creates a message duplicator and connects its secondary output port to the
    given input port.
    """

    duplicator = Duplicator()

    flowmap.annotations[duplicator] = kw
    flowmap.annotations[duplicator].setdefault('label', 'copy to ' + port_in)

    flowmap.connections.append((duplicator.out_duplicate, port_in))

    return duplicator

def Annotate(target, **kw):
    items = flowmap.annotations.get(target, {}).items() + kw.items()
    flowmap.annotations[target] = dict(items)
