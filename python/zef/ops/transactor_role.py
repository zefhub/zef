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

__all__ = [
    "take_transactor_role",
    "release_transactor_role",
    "have_transactor_role",
]

from ..core import *
from ..core._ops import *

from ..core.op_implementations.dispatch_dictionary import _op_to_functions

def take_transactor_role_implementation(g):
    return Effect({"type": FX.Graph.TakeTransactorRole,
                   "graph": g})
def take_transactor_role_tp(x):
    return VT.Any
def release_transactor_role_implementation(g):
    return Effect({"type": FX.Graph.ReleaseTransactorRole,
                   "graph": g})
def release_transactor_role_tp(x):
    return VT.Any

def have_transactor_role_implementation(g):
    return g.graph_data.is_primary_instance
def have_transactor_role_tp(x):
    return VT.Bool

_op_to_functions[RT.TakeTransactorRole] = (take_transactor_role_implementation, take_transactor_role_tp)
_op_to_functions[RT.ReleaseTransactorRole] = (release_transactor_role_implementation, release_transactor_role_tp)
_op_to_functions[RT.HaveTransactorRole] = (have_transactor_role_implementation, have_transactor_role_tp)

take_transactor_role = make_zefop(RT.TakeTransactorRole)
release_transactor_role = make_zefop(RT.ReleaseTransactorRole)
have_transactor_role = make_zefop(RT.HaveTransactorRole)