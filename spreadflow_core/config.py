from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import imp

def config_eval(path):
    module_name = 'spreadflow_core._conf{:X}'.format(hash(path))

    # Evaluate config script.
    with open(path, 'r') as f:
        result = imp.load_source(module_name, path, f)

    # Cleanup script environment.
    import sys
    sys.modules.pop('spreadflow_core.script')
    return result.flowmap
