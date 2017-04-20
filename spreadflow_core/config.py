from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import imp
import inspect
from spreadflow_core.script import TemplateFactory
from spreadflow_core.dsl.stream import AddTokenOp
from spreadflow_core.dsl.tokens import TemplateFactoryToken

def _is_template_factory(item):
    return inspect.isclass(item) and issubclass(item, TemplateFactory) and not inspect.isabstract(item)

def config_eval(path):
    module_name = 'spreadflow_core._conf{:X}'.format(hash(path))

    tokens = []

    with open(path, 'r') as infile:
        mod = imp.load_source(module_name, path, infile)

        for name, _ in inspect.getmembers(mod, _is_template_factory):
            tokens.append(AddTokenOp(TemplateFactoryToken(name, module_name, path)))

    return tokens
