# -*- coding: utf-8 -*-

"""
Domain-specific language for building up flowmaps.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from collections import namedtuple

AliasToken = namedtuple('AliasToken', ['element', 'alias'])
ComponentToken = namedtuple('ComponentToken', ['element'])
ConnectionToken = namedtuple('ConnectionToken', ['port_out', 'port_in'])
DescriptionToken = namedtuple('DescriptionToken', ['element', 'description'])
EventHandlerToken = namedtuple('EventHandlerToken', ['event_type', 'priority', 'callback'])
LabelToken = namedtuple('LabelToken', ['element', 'label'])
PartitionBoundsToken = namedtuple('PartitionBoundsToken', ['partition', 'outs', 'ins'])
PartitionSelectToken = namedtuple('PartitionSelectToken', ['partition'])
PartitionToken = namedtuple('PartitionToken', ['element', 'partition'])
