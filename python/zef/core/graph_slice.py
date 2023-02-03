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

from .. import report_import
report_import("zef.core.graph_slice")


from ._core import *
from .abstract_raes import *
from .internals import EternalUID
from .VT import *

class GraphSlice_:
    def __init__(self, *args):
        # TODO: Temp imports, going to move op definitions around
        from ._ops import to_ezefref
        """
        Construct a GraphSlice, aka reference frame.

        Signature:
            TX -> GraphSlice
            (Time, Graph) -> GraphSlice
            (Graph, Time) -> GraphSlice
        """

        # ZefRef[TX] -> GraphSlice and EZefRef[TX] -> GraphSlice
        if len(args) == 1:
            z = args[0]
            from .VT import TX
            if not isinstance(z, TX):
                raise TypeError(f'When calling the GraphSlice constructor with a single arguments, this has to be a TX. Called with: args={args}')
            if isinstance(z, AtomClass):
                from .atom import _get_ref_pointer
                z = _get_ref_pointer(z)
                if z is None:
                    raise TypeError(f"Atom did not have a BlobPtr contained in it: {args}.")
            self.tx = to_ezefref(z)
            # Hold a reference to the graph open
            self._g = Graph(self.tx)
            return

        if len(args) == 2:
            if {type(args[0]), type(args[1])} != {Graph, Time}:
                raise TypeError(f'When calling the GraphSlice constructor with two args, this has to be with types (Time, Graph). Called with args={args}')

            t, g = args if isinstance(args[0], Time) else args[::-1]
            if t > ops.now():
                raise RuntimeError('One cannot ask for a GraphSlice at a fixed time in the future.')
            txs = g | ops.all[zefdb.TX] | ops.collect
            if t < ops.collect(txs | ops.first | ops.time):
                raise RuntimeError(f'In trying to determine a GraphSlice given a time: provided time {t} is before the graph instantiation time.')
            
            last_tx = txs | last
            self.tx = (
                last_tx                                 # treat the case of the 
                if t >= time(last_tx) else
                txs | filter[lambda x: time(x) >= t] | ops.first | ops.collect
            )
            # Hold a reference to the graph open
            self._g = Graph(self.tx)
            return

        raise TypeError(f'The GraphSlice constructor can only be called with one or two args. Called with args={args}')
        
        
    def __repr__(self):
        from ._ops import uid, graph_slice_index
        return f"GraphSlice(graph_uid='{str(uid(Graph(self.tx)))}', index={graph_slice_index(self)})"

    def __eq__(self, other):        
        return (
            self.tx == other.tx 
            if isinstance(other, GraphSlice)
            else False
            )
            
    def __int__(self):
        """
        Returns the time slice as an integer
        """
        return self | graph_slice_index | collect

    def __hash__(self):
        return hash(self.tx)

    def __getitem__(self, thing):
        from ._ops import to_frame, uid, collect, to_delegate, exists_at
        from . import internals

        from .VT import Delegate
        if isinstance(thing, Delegate):
            # In case thing is a BlobPtr, convert it to DelegateRef first
            d = to_delegate(thing)
            maybe_z = to_delegate(d, self)
            if maybe_z is None:
                raise KeyError(f"Delegate {thing} not present in this timeslice")
            return maybe_z

        from .VT import Val
        if isinstance(thing, Val):
            val = internals.val_as_serialized_if_necessary(thing)
            maybe_z = Graph(self.tx).get_value_node(val)
            if maybe_z is None:
                raise KeyError(f"ValueNode {thing} doesn't exist on graph") 
            if not maybe_z | exists_at[self] | collect:
                raise KeyError(f"ValueNode {thing} isn't alive in this timeslice") 
            return maybe_z | to_frame[self] | collect
        
        g = Graph(self.tx)
        ezr = g[thing]
        # We magically transform any FOREIGN_ENTITY_NODE accesses to the real RAEs.
        # Accessing the low-level BTs can only be done through traversals
        if BT(ezr) in [BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE]:
            res = get_instance_rae(uid(thing), self)
            if res is None:
                raise KeyError("RAE doesn't have an alive instance in this timeslice")
            return res

        else:
            return ezr | to_frame[GraphSlice(self.tx)] | collect

    def __contains__(self, thing):
        from ._ops import exists_at, uid, collect, to_delegate
        from . import internals
        from .rae_type_definitions import RAE

        if isinstance(thing, RAE):
            return get_instance_rae(origin_uid(thing), self) is not None

        from .VT import Delegate
        if isinstance(thing, Delegate):
            # In case thing is a BlobPtr, convert it to DelegateRef first
            d = to_delegate(thing)
            maybe_z = to_delegate(d, self)
            return maybe_z is not None

        from .VT import Val
        if isinstance(thing, Val):
            val = internals.val_as_serialized_if_necessary(thing)
            maybe_z = Graph(self.tx).get_value_node(val)
            if maybe_z is None:
                return False
            return maybe_z | exists_at[self] | collect
            

        g = Graph(self.tx)
        if isinstance(thing, EternalUID):
            z = get_instance_rae(thing, self)
            return z is not None

        if thing not in g:
            return False
        z = g[thing]

        return z | exists_at[GraphSlice(self.tx)] | collect

from .VT import make_VT
GraphSlice = make_VT("GraphSlice", pytype=GraphSlice_)


def get_instance_rae(origin_uid: EternalUID, gs: GraphSlice, allow_tombstone=False)->ZefRef:
    """
    Returns the instance of a foreign rae in the given slice. It could be that
    the node asked for has its origin on this graph (the original rae may still
    be alive or it may be terminated)

    Args:
        origin_uid (EternalUID): the uid of the origin rae we are looking for
        g (Graph): on which graph are we looking?

    Returns:
        ZefRef: this graph knows about this: found instance
        None: this graph knows nothing about this RAE
    """
    # TODO: Temporary imports while ops are moving around
    from ._ops import map, target, exists_at, filter, collect, only, to_frame, Ins
    g = Graph(gs.tx)
    if origin_uid not in g:
        return None

    zz = g[origin_uid]
    if BT(zz) in {BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE}:
        z_candidates = zz | Ins[BT.ORIGIN_RAE_EDGE] | map[target] | collect
        if not allow_tombstone:
            z_candidates = z_candidates | filter[exists_at[gs]] | collect
        if len(z_candidates) > 1:
            raise RuntimeError(f"Error: More than one instance alive found for RAE with origin uid {origin_uid}")
        elif len(z_candidates) == 1:
            from . import _ops
            return z_candidates | only | to_frame[gs][_ops.allow_tombstone] | collect
        else:
            return None     # no instance alive at the moment
        
    elif BT(zz) in {BT.ENTITY_NODE, BT.ATTRIBUTE_ENTITY_NODE, BT.RELATION_EDGE, BT.TX_EVENT_NODE, BT.ROOT_NODE}:
        if allow_tombstone:
            from . import _ops
            return zz | to_frame[gs][_ops.allow_tombstone] | collect
        else:
            return zz | to_frame[gs] | collect
    else:
        raise RuntimeError(f"Unexpected option in get_instance_rae: {zz}")