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


def _graphdelta_merge(g, serialized_delta):
    """Don't call this explicitly. Only for the zefdb core."""

    from ..serialization import serialize, deserialize
    from .. import Graph, Effect, FX
    from .._ops import run, transact

    # Double check primary role here.
    # if g.
    # TODO The reason we are constructing an effect is that serialized 
    # data contains the commands and not the actions.
    # Can we somehow fix that?
    commands = deserialize(serialized_delta)
    # receipt = delta | transact[g] | run
    receipt = Effect({
            "type": FX.TX.Transact,
            "target_graph": g,
            "commands": commands
    }) | run

    return serialize(receipt)
