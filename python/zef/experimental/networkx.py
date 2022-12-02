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

import networkx
from .. import *
from ..ops import *

from enum import Enum
class Direction(Enum):
    BOTH = 1
    OUTGOING = 2
    INCOMING = 3

@func
def ZefRef_to_edge_ref(z, direction):
    src_ref = source(z)
    trg_ref = target(z)
    if direction == Direction.INCOMING:
        return (trg_ref, src_ref)
    elif direction == Direction.OUTGOING:
        return (src_ref, trg_ref)
    else:
        assert direction == Direction.BOTH
        # For consistency, make the smallest index the first node
        if index(source(z)) < index(target(z)):
            return (src_ref, trg_ref)
        else:
            return (trg_ref, src_ref)

@func
def all_edges(z, rt, direction):
    return all_edges_with_end(z, rt, direction) | map[first]

@func
def all_edges_with_end(z, rt, direction):
    if direction == Direction.BOTH:
        return concat(all_edges_with_end(z, rt, Direction.OUTGOING),
                      all_edges_with_end(z, rt, Direction.INCOMING))
    elif direction == Direction.OUTGOING:
        return z | out_rels[rt] | map[lambda z: (z, target(z))] | collect
    else:
        assert direction == Direction.INCOMING
        return z | in_rels[rt] | map[lambda z: (z, source(z))] | collect

class ProxyGraph:
    def __init__(self,
                 gs: GraphSlice,
                 node_filter=ET, # Can be a list (explicit list of ZefRefs) or something that is_a accepts.
                 rts=RT, # must be something that is_a accepts, returning relations
                 *args,
                 undirected=False,
                 multigraph=False,
                 reversed=False,
                 include_type_as_field=True,
                 **kwds
                 ):
        if isinstance(gs, Graph):
            raise Exception("Create a ProxyGraph with a GraphSlice instead of a Graph. Use `now(g)` if you want the latest slice.")
        if not isinstance(gs, GraphSlice) or args or kwds:
            raise Exception("Trying to create a graph with args/kwds we don't understand. If you are encountering this error as a result of running a networkX algorithm, it is likely that the algorithm is trying to construct a new graph of the same type. Construction of a graph is not possible with this class. You can instaed pass a native networkX class to the algorithm by writing, for example, `nx.minimum_spanning_tree(pg.to_native())`")

        self.gs = gs
        self.node_filter = node_filter
        self.rts = rts
        self.undirected = undirected
        self.multigraph = multigraph
        self.reversed = reversed
        self.include_type_as_field = include_type_as_field
        assert not multigraph, "Currently no multigraph support"

        if isinstance(self.node_filter, tuple):
            raise Exception("Don't pass a tuple to ProxyGraph as the node_filter")

        if isinstance(self.node_filter, list):
            # This is a special case that I think will later not be needed
            self.node_filter = set(self.node_filter)

        # On creation, we make a consistent mapping of indices, just so we can
        # be sure this won't change in the future.
        # self.node_mapping = self._all_node_refs()

        if self.undirected:
            self.direction = Direction.BOTH
        elif self.reversed:
            self.direction = Direction.INCOMING
        else:
            self.direction = Direction.OUTGOING
        
    def _lazy_node_refs(self):
        if type(self.node_filter) == set:
            # This is an optimisation that will hopefully not be needed later on
            return self.node_filter
        else:
            return self.gs | all[self.node_filter]

    def _in_node_filter(self, node):
        if type(self.node_filter) == set:
            # This is an optimisation that will hopefully not be needed later on
            return node in self.node_filter
        else:
            return node | is_a[self.node_filter] | collect

    @property
    def nodes(self):
        return ProxyNodeView(self)

    @property
    def edges(self):
        return ProxyEdgeView(self)

    @property
    def adj(self):
        return ProxyAdjacencyView(self)

    @property
    def succ(self):
        return ProxyAdjacencyView(self)

    @property
    def pred(self):
        return ProxyAdjacencyView(self.reverse())

    def successors(self, n):
        return iter(self.pred[n])
    neighbors = successors

    def predecessors(self, n):
        return iter(self.succ[n])

    @property
    def degree(self):
        return ProxyDegreeView(self, Direction.BOTH)

    @property
    def out_degree(self):
        if self.reversed:
            return ProxyDegreeView(self, Direction.INCOMING)
        else:
            return ProxyDegreeView(self, Direction.OUTGOING)

    @property
    def out_degree(self):
        if self.reversed:
            return ProxyDegreeView(self, Direction.OUTGOING)
        else:
            return ProxyDegreeView(self, Direction.INCOMING)

    def __repr__(self):
        info = []
        if self.reversed:
            info += ["reversed"]
        if isinstance(self.node_filter, list):
            info += [f"subgraph of {len(self.node_filter)} nodes"]
        elif self.node_filter != ET:
            info += [f"ET={self.node_filter}"]
        if self.rts != RT:
            info += [f"RT={self.rts}"]
        if self.multigraph:
            info += [f"multigraph"]
        if self.multigraph:
            info += [f"undirected"]
        info += [f"{repr(self.gs)}"]
        return "ProxyGraph(" + ", ".join(info) + ")"
    def _repr_pretty_(self, p, cycle):
        p.text(str(self))

    def number_of_nodes(self):
        return len(self.nodes)
    def number_of_edges(self, u=None, v=None):
        # raise NotImplementedError("Need this for multigraphs")
        assert u is None and v is None, "TODO for multigraphs"
        return len(self.edges)

    def __getitem__(self, item):
        # TODO: Some of this should be moved to e.g. g.nodes[1] logic


        # Returns a view from a node which is the first half of an edge query.
        resolved = None
        if False:
            pass
        # elif isinstance(item, NodeRef):
        #     resolved = item.z
        elif isinstance(item, (str,int)) or is_a(item, UID):
            resolved = self.gs[item]
        elif isinstance(item, ZefRef):
            resolved = in_frame(item, self.gs)

        if not resolved:
            raise Exception(f"Couldn't find node {item}")

        return ProxyAtlasView(resolved, self)

    def __iter__(self):
        return iter(self._lazy_node_refs())
    def __len__(self):
        return length(self._lazy_node_refs())
    def nbunch_iter(self, nbunch=None):
        if nbunch is None:
            return iter(self)
        return (z for z in set(nbunch) if self._in_node_filter(x))
        

    # Graph properties
    def is_directed(self):
        return not self.undirected
    def is_multigraph(self):
        return self.multigraph


    # TODO:
    # has_edge
    # adjacency
    # size

    def order(self):
        return self.number_of_nodes()

    def subgraph(self, nodes):
        if type(nodes) != set:
            raise Exception("Subgraph requires a set. In the future it will be possible to consider the union of query types.")

        return ProxyGraph(self.gs,
                           nodes,
                           self.rts,
                           undirected=self.undirected,
                           multigraph=self.multigraph,
                           reversed=self.reversed,
                           include_type_as_field=self.include_type_as_field,
                          )
                           
    def reverse(self):
        return ProxyGraph(self.gs,
                           self.node_filter,
                           self.rts,
                           undirected=self.undirected,
                           multigraph=self.multigraph,
                           reversed=not self.reversed,
                           include_type_as_field=self.include_type_as_field,
                          )

    def to_undirected(self):
        return ProxyGraph(self.gs,
                           self.node_filter,
                           self.rts,
                           undirected=True,
                           multigraph=self.multigraph,
                           reversed=self.reversed,
                           include_type_as_field=self.include_type_as_field,
                          )

    def to_directed(self):
        return ProxyGraph(self.gs,
                           self.node_filter,
                           self.rts,
                           undirected=False,
                           multigraph=self.multigraph,
                           reversed=self.reversed,
                           include_type_as_field=self.include_type_as_field,
                          )

    def to_native(self, include_fields=False):
        import networkx as nx
        assert not self.multigraph
        if self.undirected:
            nxg = nx.Graph()
        else:
            nxg = nx.DiGraph()

        if include_fields:
            nxg.add_nodes_from(self.nodes.items())
            nxg.add_edges_from(self.edges.data())
        else:
            nxg.add_nodes_from(self.nodes)
            nxg.add_edges_from(self.edges)

        return nxg
            

# Note: this does not represent the node itself (that is whatever ZefRef_to_ref
# returns) but this is rather the interface that allows modification of the
# node's properties smoothly.
class ProxyNodeView:
    def __init__(self, dg):
        self.dg = dg

    # def __repr__(self):
    #     return f"Node({repr(ET(self.z))}:{self._to_ref()})"
    # def _repr_pretty_(self, p, cycle):
    #     p.text(str(self))

    def __getitem__(self, node):
        z = self.dg[node].z
        return get_props_on(z, include_type=self.dg.include_type_as_field)
    
    # def __setitem__(self, item, val):
    #     # Set props
    #     raise NotImplementedError()

    def __iter__(self):
        return iter(self.dg)
    def __len__(self):
        return len(self.dg)

    def items(self):
        for n in self:
            yield n,self[n]

    def data(self):
        for n in self:
            yield n,self[n]

    def __call__(self, data=False, default=None):
        assert data is False
        return self
        

class ProxyEdgeView:
    def __init__(self, dg, dataview=False):
        self.dg = dg
        self.dataview = dataview

    # def __repr__(self):
    #     return f"Edge({repr(RT(self.z))}:{self._to_ref()})"
    # def _repr_pretty_(self, p, cycle):
    #     p.text(str(self))

    def __getitem__(self, e):
        if isinstance(e,tuple):
            src,trg = e
            z_src = self.dg[src].z
            z_trg = self.dg[trg].z
            rel = (#z_src > L[self.dg.rts]
                z_src | all_edges_with_end[self.dg.rts][self.dg.direction]
                | filter[second | equals[z_trg]]
                | map[first]
                | single
                | collect)
            return get_props_on(rel, include_type=self.dg.include_type_as_field)
        else:
            # This is a Zef-specific extension.

            # This is a single-arg call, it should be the ZefRef of the relation directly
            rel = e
            src = source(rel)
            trg = target(rel)
            assert is_a(rel, self.dg.rts)
            # Throw exceptions if the src/trg is not in the valid node list
            self.dg[src]
            self.dg[trg]
            return get_props_on(rel, include_type=self.dg.include_type_as_field)
    
    # def __setitem__(self, item, val):
    #     # Set props
    #     raise NotImplementedError()

    def __iter__(self):
        if self.dg.undirected:
            # When grabbing all edges, we don't want to double up.
            direction_override = Direction.OUTGOING
        else:
            direction_override = self.dg.direction

        out = (LazyValue(self.dg._lazy_node_refs())
                 | map_cat[all_edges_with_end[self.dg.rts][direction_override]]
                 | filter[second | func[self.dg._in_node_filter]]
                 | map[first]
                 | map[ZefRef_to_edge_ref[self.dg.direction]])
        if self.dataview:
            out = out | map[lambda x: (*x, self.__getitem__(*x))]
        return iter(out)
    def __len__(self):
        return length(iter(self))
    def __contains__(self, e):
        # The logic here is only unusual for undirected graphs, where the
        # ordering of the edges shouldn't matter. For consistency, we have made
        # the node with the smallest index on the graph be the first.
        # u,v = e
        # if self.dg.undirected:
        #     if index(u.z) > index(v.z):
        #         u,v = v,u
        # return (u,v) in iter(self)

        # I don't like this, but it's the fastest for now
        try:
            u,v = e
            self[u,v]
            return True
        except StopIteration:
            # TODO: This might change in the future with zef, so will have to update this
            return False

    def items(self):
        for e in self:
            yield n,self[n]

    def data(self):
        for e in self:
            yield e[0],e[1],self[e]

    def __call__(self, data=False, default=None):
        assert default is None
        return ProxyEdgeView(self.dg, data)
            

def get_props_on(z, include_type):
    props = {}
    for rel in z | out_rels[RT] | filter[target | is_a[AttributeEntity]]:
        props[token_name(RT(rel))] = rel|target|value|collect
    if include_type:
        props["type"] = rae_type(z)
    return props

# This is the view of a node in the context of a ProxyGraph, that allows for
# querying of its edges. In other words, it is the first half of an edge query,
# completed by a getitem or similar on this object.
class ProxyAtlasView:
    def __init__(self, z : ZefRef, dg : ProxyGraph):
        self.z = z
        self.dg = dg

    def __repr__(self):
        return f"AtlasView({self.z})"
    def _repr_pretty_(self, p, cycle):
        p.text(str(self))

    def __iter__(self):
        # This returns the neighbors, not the edges
        # all_edges_with_end returns a tuple with (rel,ent)
        lazy = self.z | all_edges_with_end[self.dg.rts][self.dg.direction] | map[second] | filter[self.dg._in_node_filter]
        return iter(lazy)
    def __len__(self):
        return length(iter(self))
    def keys(self):
        return iter(self)
    def items(self):
        return ((x, self[x]) for x in self.keys())

    def __getitem__(self, other):
        # The other is a node itself, so we need to look it up and then find the
        # relation between them.

        # We can work with another ProxyAtlastView ourselves
        other_view = self.dg[other]
        other_z = other_view.z

        # all_edges_with_end returns a tuple with (rel,ent)
        opts = self.z | all_edges_with_end[self.dg.rts][self.dg.direction] | filter[second | equals[other_z]] | map[first] | collect
        if len(opts) == 0:
            raise IndexError(f"There is no edge between objects {ZefRef_to_zef(self.z)} and {ZefRef_to_zef(other_z)}")

        rel = single(opts)

        return get_props_on(rel, include_type=self.dg.include_type_as_field)

# I am a little confused how repetitive this implementation seems...
class ProxyAdjacencyView:
    def __init__(self, dg):
        self.dg = dg

    def __iter__(self):
        return iter(self.dg.nodes)
    def __len__(self):
        return len(self.dg.nodes)

    def __getitem__(self, item):
        return self.dg[item]

    def items(self):
        return ((x, self[x]) for x in iter(self))

class ProxyDegreeView:
    def __init__(self, dg, direction, nbunch=None):
        self.dg = dg
        self.direction = direction
        self.nbunch = nbunch

    def __call__(self, nbunch=None):
        if nbunch is None:
            return self
        if isinstance(nbunch, list):
            return ProxyDegreeView(self.dg, self.direction, nbunch)

        return self[nbunch]

    def __getitem__(self, n):
        return self.dg[n].z | all_edges[self.dg.rts][self.direction] | length | collect

    def __iter__(self):
        lazy = LazyValue(iter(self.dg))
        if self.nbunch is not None:
            lazy = lazy | filter[contained_in[self.nbunch]]
        lazy = lazy | map[lambda x: (x,self[x])]
        return iter(lazy)

    def __len__(self):
        return len(self.dg)



##################################################################
# * Old ideas for writing to graph
#----------------------------------------------------------------

    # def add_node(self, hashable, *, typ=ET.Node, **kwds):
    #     h = hash(hashable)
    #     import ctypes
    #     h1 = ctypes.c_int32(h >> 32).value
    #     h2 = ctypes.c_int32(h & 0xFFFFFFFF).value
    #     if hash(hashable) in self.node_lookup:
    #         return self.node_lookup[h]

    #     z = instantiate(typ, self.g) | attach[[
    #         (RT._Hash1, h1),
    #         (RT._Hash2, h2),
    #         *( (RT(key), val) for key,val in kwds),
    #     ]]

    #     self.object_lookup[h] = hashable
    #     self.node_lookup[h] = z | to_ezefref
    #     return z | to_ezefref

    # def add_nodes_from(self, hashables, *, typ=ET.Node, **kwds):
    #     # TODO: Handle adding from another graph

    #     for hashable in hashables:
    #         self.add_node(self, hashable, typ=typ, **kwds)

    # def add_edge(self, na, nb, *, typ=RT.Edge, **kwds):
    #     z_a = self.add_node(na)
    #     z_b = self.add_node(nb)

    #     if has_relation(z_a|now, typ, z_b|now):
    #         return relation(z_a|now, typ, z_b|now)

    #     z = instantiate(z_a, typ, z_b, self.g)
    #     return z
