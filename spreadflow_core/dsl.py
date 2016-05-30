# -*- coding: utf-8 -*-

"""
Domain-specific language for building up flowmaps.
"""

import inspect

from spreadflow_core.component import Compound
from spreadflow_core.proc import Duplicator

GLOBAL_CONTEXT_STACK = []

try:
    StringType = basestring # pylint: disable=undefined-variable
except NameError:
    StringType = str

class DSLError(Exception):
    pass

class NoContextError(DSLError):
    pass

class Context(object):
    """
    DSL context.
    """

    _context_stack = GLOBAL_CONTEXT_STACK
    flowmap = None
    annotations = None

    def __init__(self, origin, stack=None):
        self.origin = origin
        self.stack = stack if stack is not None else inspect.stack()[1:]

    def __enter__(self):
        self.push(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pop(self)
        return False

    @classmethod
    def push(cls, ctx):
        """
        Push the context onto the shared stack.
        """
        cls._context_stack.append(ctx)

    @classmethod
    def pop(cls, ctx):
        """
        Remove the context from the shared stack.
        """
        top = cls._context_stack.pop()
        assert top is ctx, 'Unbalanced DSL context stack'

    @classmethod
    def top(cls):
        """
        Returns the topmost context from the stack.
        """
        try:
            return cls._context_stack[-1]
        except IndexError:
            raise NoContextError()

class ProcessTemplate(object):
    def apply(self, context):
        raise NotImplementedError()

class ChainTemplate(ProcessTemplate):
    ports = None

    def __init__(self, ports=None):
        if ports is not None:
            self.ports = ports

    def apply(self, context):
        ports = list(self.ports)
        process = Compound(ports)

        upstream = ports[0]
        for downstream in ports[1:]:
            if isinstance(upstream, ProcessTemplate):
                upstream = upstream.apply(context)
            if isinstance(downstream, ProcessTemplate):
                downstream = downstream.apply(context)
            context.flowmap.connections.append((upstream, downstream))
            upstream = downstream

        return process

class DuplicatorTemplate(ProcessTemplate):
    destination = None

    def __init__(self, destination=None):
        if destination is not None:
            self.destination = destination

    def apply(self, context):
        process = Duplicator()

        destination = self.destination
        if isinstance(destination, ProcessTemplate):
            destination = destination.apply(context)
        context.flowmap.connections.append((process.out_duplicate, destination))
        annotations = context.annotations.setdefault(process, {})
        annotations.setdefault('label', 'copy to {:s}'.format(destination))

        return process

def Process(label_or_cls_or_func, *args, **kwds):
    """
    Instantiate a flow process and register it with the current context.
    """

    ctx = Context.top()
    default_annotations = {}

    if isinstance(label_or_cls_or_func, type) and issubclass(label_or_cls_or_func, ProcessTemplate):
        assert len(args) == 0, "Process decorator does not take any positional parameters"
        assert len(kwds) == 0, "Process decorator does not take any keyword parameters"

        template_cls = label_or_cls_or_func
        default_annotations = {
            'label': template_cls.__name__,
            'description': template_cls.__doc__
        }

        template = template_cls()

    elif callable(label_or_cls_or_func):
        assert len(args) == 0, "Process decorator does not take any positional parameters"
        assert len(kwds) == 0, "Process decorator does not take any keyword parameters"

        template_func = label_or_cls_or_func
        default_annotations = {
            'label': template_func.__name__,
            'description': template_func.__doc__
        }

        template = ChainTemplate(ports=template_func(ctx))

    elif isinstance(label_or_cls_or_func, StringType):
        assert len(args) > 0, "Process chain template takes a list of ports/port collections"

        default_annotations = kwds.copy()
        default_annotations.setdefault('label', label_or_cls_or_func)

        template = ChainTemplate(ports=args)

    process = template.apply(ctx)
    # FIXME: assert stuff about the process?

    # Merge default annotations.
    process_annotations = ctx.annotations.setdefault(process, {})
    for key, value in default_annotations.items():
        if value is not None:
            process_annotations.setdefault(key, value)

    # Add an alias.
    try:
        label = process_annotations['label']
    except KeyError:
        pass
    else:
        ctx.flowmap.aliasmap[label] = process

    return process

def Apply(template_cls, *args, **kwds):
    ctx = Context.top()
    template = template_cls(*args, **kwds)
    process = template.apply(ctx)
    return process
