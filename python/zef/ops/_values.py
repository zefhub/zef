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

from ..core._ops import value
from ..core import func

@func
def value_or(x, fallback):
    if x is None:
        return fallback
    else:
        return value(x)
maybe_value = value_or[None]
from ..core.op_structs import _overloaded_repr
_overloaded_repr[maybe_value.el_ops[0]] = "maybe_value"