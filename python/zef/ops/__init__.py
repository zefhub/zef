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

from ..core import _ops, func

from . import (
    privileges as _privileges,
    _config,
    _values,
    transactor_role
)

# This must happen last, as _ops has additions while we are importing submodules here.
from ..core._ops import *

import types
lazy_all = [x for x in dir(_ops) if not x.startswith("_") and not isinstance(getattr(_ops, x), types.ModuleType)]

# __all__ = [
#     # "_",
#     "func",
#     "value_or",
#     "maybe_value",
#     "config",
# ] + privileges.__all__ + lazy_all + transactor_role.__all__

from .privileges import grant, revoke, login, login_as_guest, logout

__all__ = _privileges.__all__ + lazy_all