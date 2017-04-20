"""
Provides utility functions for spreadflow config script.
"""

import collections
import inspect
import abc

from spreadflow_core.dsl.stream import AddTokenOp, SetDefaultTokenOp
from spreadflow_core.dsl.tokens import \
    AliasToken, \
    ConnectionToken, \
    DefaultInputToken, \
    DefaultOutputToken, \
    DescriptionToken, \
    LabelToken, \
    ParentElementToken, \
    PartitionToken
from spreadflow_core.proc import Duplicator


class TemplateFactory(object):
    """
    Marker interface for templates which will be instantiated automatically.
    """

class TemplateBase(collections.Iterable):
    def __iter__(self):
        return self.apply()

    @abc.abstractmethod
    def apply(self):
        """
        Generate collection stream tokens representing dataflow building
        blocks.
        """

class ComponentTemplate(TemplateBase):
    component = None

    def get_component(self):
        """
        Returns the component.
        """
        if self.component is None:
            self.component = self.create_component()
        return self.component

    @abc.abstractmethod
    def create_component(self):
        """
        Constructs the component.
        """

class ChainTemplate(ComponentTemplate):
    chain = None

    def __init__(self, chain=None):
        if chain is not None:
            self.chain = list(chain)

    class _ChainContainer(object):
        """
        The chain process wrapping other processes.
        """

        def __init__(self, children):
            assert len(children) == len(set(children)), 'Members must be unique'
            self.children = children

        def get_children(self):
            return self.children

    def create_component(self):
        # Apply (sub)templates if necessary.
        elements = []

        for element in self.chain:
            if isinstance(element, ComponentTemplate):
                elements.append(element.get_component())
            else:
                elements.append(element)

        return self._ChainContainer(elements)

    def apply(self):
        component = self.get_component()

        for element in self.chain:
            if isinstance(element, ComponentTemplate):
                for operation in element:
                    yield operation

        elements = component.get_children()
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

class DuplicatorTemplate(ComponentTemplate):
    destination = None

    def __init__(self, destination=None):
        if destination is not None:
            self.destination = destination

    def create_component(self):
        return Duplicator()

    def apply(self):
        # Apply (sub)template if necessary.
        destination = self.destination
        if isinstance(destination, ComponentTemplate):
            for operation in destination:
                yield operation
            destination = destination.get_component()

        process = self.get_component()

        # Set the parent for the secondary output.
        yield AddTokenOp(ParentElementToken(process.out_duplicate, process))

        # Connect the secondary output to the given downstream port.
        yield AddTokenOp(ConnectionToken(process.out_duplicate, destination))
        yield SetDefaultTokenOp(LabelToken(process, 'Copy to "{:s}"'.format(destination)))

class ProcessDecoratorError(Exception):
    pass

class TemplateFactoryMissingError(ProcessDecoratorError):
    pass

class ProcessTemplate(ComponentTemplate):
    _wrapped_factory = TemplateFactoryMissingError
    alias = None
    label = None
    description = None
    partition = None
    template = None

    def create_template(self):
        if isinstance(self._wrapped_factory, type) and issubclass(self._wrapped_factory, ComponentTemplate):
            template = self._wrapped_factory()
        elif isinstance(self._wrapped_factory, object) and callable(self._wrapped_factory):
            template = ChainTemplate(chain=self._wrapped_factory())
        else:
            raise ProcessDecoratorError('Process decorator only works on '
                                        'subclasses of ComponentTemplate or '
                                        'functions')
        return template

    def get_template(self):
        if self.template is None:
            self.template = self.create_template()
        return self.template

    def create_component(self):
        template = self.get_template()
        return template.get_component()

    def apply(self):
        template = self.get_template()
        for operation in template:
            yield operation

        process = self.get_component()

        yield SetDefaultTokenOp(AliasToken(process, self._wrapped_factory.__name__))
        yield SetDefaultTokenOp(DescriptionToken(process, inspect.cleandoc(self._wrapped_factory.__doc__ or '')))
        yield SetDefaultTokenOp(LabelToken(process, self._wrapped_factory.__name__))

        if self.alias is not None:
            yield AddTokenOp(AliasToken(process, self.alias))
        if self.label is not None:
            yield AddTokenOp(LabelToken(process, self.label))
        if self.description is not None:
            yield AddTokenOp(DescriptionToken(process, self.description))
        if self.partition is not None:
            yield AddTokenOp(PartitionToken(process, self.partition))

def Process(alias=None, label=None, description=None, partition=None):
    """
    Produces a flow process class from a template class or chain generator
    function.
    """

    def decorate_factory(wrapped_factory):
        return type(wrapped_factory.__name__, (ProcessTemplate, TemplateFactory), {
            '_wrapped_factory': staticmethod(wrapped_factory),
            'alias': alias,
            'label': label,
            'description': description,
            'partition': partition,
            '__doc__': wrapped_factory.__doc__,
            '__module__': wrapped_factory.__module__
        })
    return decorate_factory
