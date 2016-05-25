from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import imp
from spreadflow_core.component import COMPONENT_VISITORS

def config_eval(path):
    module_name = 'spreadflow_core._conf{:X}'.format(hash(path))

    # Evaluate config script.
    components = []

    saved_visitors = list(COMPONENT_VISITORS)
    COMPONENT_VISITORS.append(components.append)

    with open(path, 'r') as f:
        result = imp.load_source(module_name, path, f)

    if COMPONENT_VISITORS.pop() != components.append or COMPONENT_VISITORS != saved_visitors:
        raise RuntimeError('COMPONENT_VISITORS changed during script evaluation')

    # Cleanup script environment.
    import sys
    sys.modules.pop('spreadflow_core.script')
    return result.flowmap, components, result.annotations
