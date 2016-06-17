from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import imp
from spreadflow_core.script import Context

def config_eval(path):
    module_name = 'spreadflow_core._conf{:X}'.format(hash(path))

    with Context(path) as ctx:
        with open(path, 'r') as f:
            imp.load_source(module_name, path, f)

    return ctx.tokens
