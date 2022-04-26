import os
if os.environ.get("ZEFDB_DEVELOPER_CIRCULAR_IMPORTS", "FALSE") == "TRUE":
    from .circular_imports import check_circular_imports
    check_circular_imports()

########################################################
# * Exposing common functions
#------------------------------------------------------

# This set of imports is to define the order. Later imports are the ones to
# actually provide useful exports.
from . import core
from . import pyzef
from . import ops

from .core import *

pyzef.internals.finished_loading_python_core()

import os
if os.environ.get("ZEFDB_DEVELOPER_CIRCULAR_IMPORTS", "FALSE") == "TRUE":
    from .circular_imports import disable_check_circular_imports
    disable_check_circular_imports()

############################################
# * Starting the butler
#------------------------------------------

# Putting this into a function to avoid polluting namespace
def _autostart_behaviour():
    auto_start = ops.config("butler.autoStart", KW.get)
    if auto_start == "true":
        core.internals.initialise_butler()

_autostart_behaviour()
from . import _version
__version__ = _version.get_versions()['version']
