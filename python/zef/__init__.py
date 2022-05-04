from . import _version
__version__ = _version.get_versions()['version']

import os
if os.environ.get("ZEFDB_DEVELOPER_CIRCULAR_IMPORTS", "FALSE") == "TRUE":
    from .circular_imports import check_circular_imports
    check_circular_imports()

# See comments in libzef CMakeLists.txt. This is to pass appropriate certificate
# information to libzef if it is bundled with pyzef.
try:
    # I don't think this will ever fail as python requires an SSL to build now I
    # think. What this block should really be doing is checking for OpenSSL not
    # LibreSSL... or maybe the libzef library should be using LibreSSL on macos.
    import ssl
except ImportError:
    raise ImportError("zef requires OpenSSL to be installed.")

_ssl_paths = ssl.get_default_verify_paths()
os.environ["LIBZEF_CA_BUNDLE"] = _ssl_paths.cafile or ""
os.environ["LIBZEF_CA_PATH"] = _ssl_paths.capath or ""

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
