from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_file_location
from spreadflow_core.script import Context

def config_eval(path):
    module_name = 'spreadflow_core._conf{:X}'.format(hash(path))
    config_loader=SourceFileLoader(module_name, path)
    module_spec=spec_from_file_location(module_name, location=path, loader=config_loader)
    config_module=module_from_spec(module_spec)

    with Context(path) as ctx:
        config_loader.exec_module(config_module)

    return ctx.tokens
