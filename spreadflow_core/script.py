"""
Provides utility functions for spreadflow config script.
"""

from spreadflow_core.flow import PortCollection, Flowmap
from spreadflow_core.proc import Duplicator, Compound

import collections

flowmap = Flowmap()

def Chain(*procs, **kw):
    compound = Compound(procs)
    flowmap.annotations[compound] = kw

    upstream = procs[0]
    for downstream in procs[1:]:
        flowmap.connections.append((upstream, downstream))
        upstream = downstream

    return compound

def Duplicate(port_in):
    """
    Creates a message duplicator and connects its secondary output port to the
    given input port.
    """

    duplicator = Duplicator()
    flowmap.connections.append((duplicator.out_duplicate, port_in))
    return duplicator

def Annotate(target, **kw):
    items = flowmap.annotations.get(target, {}).items() + kw.items()
    flowmap.annotations[target] = dict(items)
