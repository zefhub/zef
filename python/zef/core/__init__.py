# Copyright 2022 Synchronous Technologies Pte Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .. import report_import
report_import("zef.core")

####################################
# * Locating libzef
#----------------------------------

import ctypes, sys, sysconfig, os

# ** Circular import checks
import os
if os.environ.get("ZEFDB_DEVELOPER_CIRCULAR_IMPORTS", "FALSE") == "TRUE":
    from .circular_imports import check_circular_imports
    check_circular_imports()

# ** TLS certificates
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

# ** auth.html location for bundled installs
# This is only relevant for windows, but we can set it regardless
import pathlib
os.environ["LIBZEF_AUTH_HTML_PATH"] = str(pathlib.Path(__file__).resolve().parent.parent)

########################################################
# * Exposing common functions
#------------------------------------------------------
from .. import pyzef
from ._core import *

# Force patching to execute
from . import patching
# Also override merge
from .overrides import *

# This set of imports is to define the order. Later imports are the ones to
# actually provide useful exports.
from . import internals
from . import pure_utils
from . import VT
from . import _error
from . import generators
from .VT import extended_containers
from . import user_value_type
from . import _image
from . import _decimal
from . import _bytes
# Up to here, DEFINITELY no zefops can be called
from . import abstract_raes
from . import graph_slice
from . import delegates
from . import op_structs
from . import _ops
from . import zef_functions
from . import graph_delta
from . import flat_graph
from . import fx
from . import serialization
from . import graph_events
from . import streams
from . import z_expression

from .VT import *

from .fx import FX

from .units import unit

from .zef_functions import func

from .serialization import serialize, deserialize

from .symbolic_expression import SV, SVs, V
from .z_expression import ZZ    # TODO: rename this to "Z" and replace the ZefOp "Z"

from .graph_events import assigned, terminated, instantiated

# Implementations come last, so that they can make use of everything else
from . import op_implementations


# Because I can't figure out why vscode displaying rich console output removes
# colours, even after forcing them in the string output, we have to hook into
# the ipython excepthook to print directly to the console.
#
# This is not needed for an ipython shell, hence the _is_jupyter check.
def visual_exception_view(error_value):
    from zef.core._error import zef_ui_err
    try:
        # zef_ui_err(error_value.wrapped)
        print(error_value.wrapped)
    except Exception as e:
        try:
            e_s = str(e)
        except:
            e_s = "Can't take str of failure exception"
        print("Failed in displaying zef error: {e_s}")
        pass
try:
    from IPython import get_ipython
    ip = get_ipython()
    # Use the same check as what rich does
    import rich.console
    if rich.console._is_jupyter():
        def ip_exception_handler(self, etype, evalue, tb, tb_offset=None):
            from ._error import ExceptionWrapper
            if etype == ExceptionWrapper:
                # Replace the wrapper object so that we don't output twice
                self.showtraceback((etype, "see visual below", tb), tb_offset=tb_offset)  # standard IPython's printout
                # Show our fancy view
                visual_exception_view(evalue)
            else:
                return self.showtraceback((etype, evalue, tb), tb_offset=tb_offset)  # standard IPython's printout

        # Overloading ipython exception handler
        ip.set_custom_exc((Exception,), ip_exception_handler) 
except:
    pass

pyzef.internals.finished_loading_python_core()

# ############################################
# # * Starting the butler
# #------------------------------------------

# # Putting this into a function to avoid poluting namespace
# def _autostart_behaviour():
#     auto_start = ops.config("butler.autoStart", KW.get)
#     if auto_start == "true":
#         internals.initialise_butler()

# _autostart_behaviour()
        
