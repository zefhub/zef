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
    "subgraph",
]

from ..core import *
from ..ops import *


def subgraph(gs, ets=ET, rts=RT):
    """Produce a simple directed graph with nodes and edges that represents a
    subgraph of a given GraphSlice `gs`.
    
    Given a set of EntityTypes `ets` to interpret as nodes, and a set of
    RelationTypes `rts` to interpret as edges, builds a list `nodes` which maps
    each index of the list to a ZefRef of the entity, and an adjacency matrix
    `A` that maps edges to the ZefRef of the relation.
    
    The adjacency matrix returned is a sparse matrix containing integers where
    an edge is present, and those integers are themselves indices into an
    additional list, `edges`, which maps edges to their original ZefRef.

    Note: due to limitations of the scipy.sparse package, index 0 cannot be used
    as an index. As a workaround, we set `edges[0] = None`, and the
    first true edge is at `edges[1]`.

    Return signature is `(A, nodes, edges)`
    
    If `ets` is a single EntityType, it will be automatically promoted to a
    list. Similarly with `rts`.

    """
    from scipy.sparse import csc_matrix, lil_matrix

    # if isinstance(ets, EntityType):
    #     ets = [ets]
    # if isinstance(rts, RelationType):
    #     rts = [rts]

    next_edge_ind = 1

    # nodes = gs | all[ET] | filter[rae_type | contained_in[ets]] | func[list] | collect
    nodes = gs | all[ets] | func[list] | collect
    edges = [None]

    N = len(nodes)
    A = lil_matrix((N,N), dtype=int)
    for i,i_node in enumerate(nodes):
        for edge in (i_node > L[RT]
                     | filter[is_a[rts]]
                     | filter[target | contained_in[nodes]]):
            j = nodes.index(target(edge))

            A[i,j] = next_edge_ind
            next_edge_ind += 1
            print("In here", i, j, A)
            edges += [edge]

    # A = A.tocsc()
    return A, nodes, edges