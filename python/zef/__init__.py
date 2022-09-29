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

from . import _version
__version__ = _version.get_versions()['version']

########################################################
# * Exposing common functions
#------------------------------------------------------

import_order = []
def report_import(x):
    if x in import_order:
        return
    import_order.append(x)
report_import("zef")

# This set of imports is to define the order. Later imports are the ones to
# actually provide useful exports.
from . import core
from . import pyzef
from . import ops
from . import ui

from .core import *

pyzef.internals.finished_loading_python_core()

############################################
# * Starting the butler
#------------------------------------------

# Putting this into a function to avoid polluting namespace
def _autostart_behaviour():
    auto_start = ops.config("butler.autoStart", KW.get)
    if auto_start:
        core.internals.initialise_butler()

_autostart_behaviour()

# We always run this, in case the user has started the butler manually instead of automatically
import atexit
@atexit.register
def _stop_butler():
    from . import pyzef
    pyzef.internals.stop_butler()
