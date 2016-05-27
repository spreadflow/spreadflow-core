"""
Provides utility functions for spreadflow config script.
"""

from spreadflow_core.component import Compound
from spreadflow_core.flow import Flowmap
from spreadflow_core.proc import Duplicator

annotations = {} # pylint: disable=C0103
flowmap = Flowmap() # pylint: disable=C0103

def Chain(name, *procs, **kw): # pylint: disable=C0103
    """
    Forms a chain of the given components by connecting the default input port
    to the default output port of its predecessor.
    """

    compound = Compound(procs)
    flowmap.aliasmap[name] = compound

    annotations[compound] = kw
    annotations[compound].setdefault('label', name)

    upstream = procs[0]
    for downstream in procs[1:]:
        flowmap.connections.append((upstream, downstream))
        upstream = downstream

    return compound

def Duplicate(port_in, **kw): # pylint: disable=C0103
    """
    Creates a message duplicator and connects its secondary output port to the
    given input port.
    """

    duplicator = Duplicator()

    annotations[duplicator] = kw
    annotations[duplicator].setdefault('label', 'copy to ' + port_in)

    flowmap.connections.append((duplicator.out_duplicate, port_in))

    return duplicator

def Annotate(target, **kw): # pylint: disable=C0103
    """
    Adds key value pairs as annotations to the given port or component.
    """

    items = annotations.get(target, {}).items() + kw.items()
    annotations[target] = dict(items)
