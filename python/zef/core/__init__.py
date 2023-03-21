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
from . import _image
from . import _decimal
from . import _bytes
from . import op_structs
from . import _ops
from . import zef_functions
from . import abstract_raes
from . import graph_delta
from . import graph_slice
from . import flat_graph
from . import fx
from . import serialization

from ._image import Image

from .fx.fx_types import Effect, FX

from .graph_slice import GraphSlice

from .flat_graph import FlatGraph, FlatRef, FlatRefs, Val

from .abstract_raes import Entity, AttributeEntity, Relation, TXNode, Root, make_custom_entity

from .zef_functions import func

from .op_structs import ZefOp, LazyValue

from .serialization import serialize, deserialize

from .user_value_type import UserValueType

from .VT import TX, Instantiated, Assigned, Terminated

# instantiating these here, since not all of the core has been
# initialized when Python imports the abstract_raes module
# and a circular import error occurs.
please_instantiate = make_custom_entity(name_to_display='please_instantiate', predetermined_uid='783320c1c3de2610')
please_terminate   = make_custom_entity(name_to_display='please_terminate', predetermined_uid='67cb88b71523f6d9')
please_assign      = make_custom_entity(name_to_display='please_assign',    predetermined_uid='4d4a93522f75ed21')

instantiated     = make_custom_entity(name_to_display='instantiated', predetermined_uid='60252a53a03086b7')
terminated       = make_custom_entity(name_to_display='terminated', predetermined_uid='4f676154ffeb9dc8')
assigned         = make_custom_entity(name_to_display='assigned', predetermined_uid='c31287dab677f38c')

infinity           = make_custom_entity(name_to_display='infinity',    predetermined_uid='4906648460291096')
nil                = make_custom_entity(name_to_display='nil',         predetermined_uid='1654670075329719') #| register_call_handler[f1] | run[execute] | get['entity'] | collect  # TODO


# Implementations come last, so that they can make use of everything else
from . import op_implementations

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
        
