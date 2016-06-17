# -*- coding: utf-8 -*-

"""
Tokens used in the domain-specific language for building up flowmaps.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import namedtuple

AliasToken = namedtuple('AliasToken', ['element', 'alias'])
ComponentToken = namedtuple('ComponentToken', ['component'])
ConnectionToken = namedtuple('ConnectionToken', ['port_out', 'port_in'])
DefaultInputToken = namedtuple('DefaultInputToken', ['element', 'port'])
DefaultOutputToken = namedtuple('DefaultOutputToken', ['element', 'port'])
DescriptionToken = namedtuple('DescriptionToken', ['element', 'description'])
EventHandlerToken = namedtuple('EventHandlerToken', ['event_type', 'priority', 'callback'])
LabelToken = namedtuple('LabelToken', ['element', 'label'])
ParentElementToken = namedtuple('ParentElementToken', ['element', 'parent'])
PartitionBoundsToken = namedtuple('PartitionBoundsToken', ['partition', 'bounds'])
PartitionSelectToken = namedtuple('PartitionSelectToken', ['partition'])
PartitionToken = namedtuple('PartitionToken', ['element', 'partition'])
