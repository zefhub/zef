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
    "merge",
]

from ..pyzef.main import Graph

# Override merge so that we may pass in a dict into the C++ code for GraphDeltas
def merge(obj, g : Graph, fire_and_forget : bool = False):
    from ..pyzef.main import merge as orig_merge
    from .serialization import serialize, deserialize
    from .graph_slice import GraphSlice

    serialized = serialize(obj)
    gs_tx,rs = orig_merge(serialized, g)
    receipt = deserialize(rs)
    gs = GraphSlice(gs_tx)
    return gs,receipt

