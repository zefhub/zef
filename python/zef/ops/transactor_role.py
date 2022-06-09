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

@zefop
def take_transactor_role(g):
    return Effect({"type": FX.Graph.TakeTransactorRole,
                   "graph": g})
@add_tp
def take_transactor_role(x):
    return VT.Any

@zefop
def release_transactor_role(g):
    return Effect({"type": FX.Graph.ReleaseTransactorRole,
                   "graph": g})
@add_tp
def release_transactor_role(x):
    return VT.Any

@zefop
def have_transactor_role(g):
    return g.graph_data.is_primary_instance
@add_tp
def have_transactor_role(x):
    return VT.Bool