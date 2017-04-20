"""
Provides utility functions for spreadflow config script.
"""

import collections
import inspect
import abc

from spreadflow_core.component import Compound
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
    pass

class ChainTemplate(ComponentTemplate):
    chain = None
    component_parser = ComponentParser()

    def __init__(self, chain=None):
        if chain is not None:
            self.chain = chain

    def apply(self):
        elements = []

        # Apply (sub)templates if necessary.
        for element in self.chain:
            if isinstance(element, ComponentTemplate):
                for operation in self.component_parser.divert(element):
                    yield operation
                elements.append(self.component_parser.get_component())
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

class DuplicatorTemplate(ComponentTemplate):
    component_parser = ComponentParser()
    destination = None

    def __init__(self, destination=None):
        if destination is not None:
            self.destination = destination

    def apply(self):
        # Apply (sub)template if necessary.
        destination = self.destination
        if isinstance(destination, ComponentTemplate):
            for operation in self.component_parser.divert(destination):
                yield operation
            destination = self.component_parser.get_component()

        process = Duplicator()
        yield AddTokenOp(ComponentToken(process))

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

    component_parser = ComponentParser()

    def apply(self):
        if isinstance(self._wrapped_factory, type) and issubclass(self._wrapped_factory, ComponentTemplate):
            template = self._wrapped_factory()
        elif isinstance(self._wrapped_factory, object) and callable(self._wrapped_factory):
            template = ChainTemplate(chain=self._wrapped_factory())
        else:
            raise ProcessDecoratorError('Process decorator only works on '
                                        'subclasses of ComponentTemplate or '
                                        'functions')

        for operation in self.component_parser.divert(template):
            yield operation

        process = self.component_parser.get_component()

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
