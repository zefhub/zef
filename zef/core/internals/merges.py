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
    from .. import Graph
    from .._ops import run, execute

    # Double check primary role here.
    # if g.
    delta = deserialize(serialized_delta)

    receipt = delta | g | run[execute]

    return serialize(receipt)
