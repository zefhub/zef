####################################
# * Locating libzef
#----------------------------------

import ctypes, sys, sysconfig, os
if sys.platform == "linux":
    libext = ".so"
elif sys.platform == "darwin":
    libext = ".dylib"
# path = os.path.abspath(os.path.join(sysconfig.get_path("data"), "lib", "libzef" + libext))
path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, "libzef" + libext))
try:
    ctypes.cdll.LoadLibrary(path)
except:
    print(f"FAILED TO FIND LIBZEF at {path} - hopefully pyzef has it hard-coded (true when running from source directory)")

if os.environ.get("ZEFDB_DEVELOPER_CIRCULAR_IMPORTS", "FALSE") == "TRUE":
    from .circular_imports import check_circular_imports
    check_circular_imports()

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
from . import error
from . import image
from . import op_structs
from . import _ops
from . import zef_functions
from . import abstract_raes
from . import graph_delta
from . import graph_slice
from . import flat_graph
from . import fx
from . import serialization

from .graph_delta import GraphDelta

from .error import Error

from .image import Image

from .fx.fx_types import Effect, FX

from .units import unit

from .graph_slice import GraphSlice


from .flat_graph import FlatGraph, FlatRef, FlatRefs, Val

from .VT import TX
from .abstract_raes import Entity, AtomicEntity, Relation

from .zef_functions import func

from .op_structs import ZefOp, LazyValue


from .serialization import serialize, deserialize

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
        