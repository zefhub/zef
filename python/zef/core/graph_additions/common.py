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

from ... import report_import
report_import("zef.core.graph_additions.common")

from ..VT import *
from ..VT.rae_types import RAET_get_token, RAET_get_names, RAET_without_names

from .._ops import *

from ..zef_functions import func

# from ..atom import _get_atom_id, _get_fields, _get_atom_type, _get_ref_pointer, get_all_ids, get_most_authorative_id, find_zefref
from ..atom import get_all_ids, get_most_authorative_id, find_concrete_pointer

from .types import *

# ID generation uses a counting integer to distinguish ids in the same set and a prefix (e.g. coming from a time) to try and distinguish between different runs
GenIDState = Tuple[String,Int]

##############################
# * General Utils

def get_uvt_type(uvi: UserValueInstance):
    from ..user_value_type import _user_value_type_registry
    return _user_value_type_registry[uvi._user_type_id]

# TODO: This needs to be turned into a UVT.
from dataclasses import dataclass
@dataclass(frozen=True)
class OriginallyUserID:
    obj: None

    def __repr__(self):
        return f"was:{self.obj}"

@func
def maybe_unwrap_variable(x):
    return LazyValue(x) | match[
        (Variable & Is[get_field["name"]
            | is_a[OriginallyUserID]],
         get_field["name"]
               | get_field["obj"]),
        (Any, identity)
    ] | collect

def maybe_unwrap_variables_in_receipt(receipt):
    return (receipt
               | items
               | map[apply[first | maybe_unwrap_variable,
                           second]]
               | func[dict]
               | collect)

def wrap_user_id(thing):
    if isinstance(thing, ExtraUserAllowedIDs):
        return convert_extra_allowed_id(thing)
    return SymbolicExpression(OriginallyUserID(thing))

def convert_extra_allowed_id(thing: ExtraUserAllowedIDs):
    if isinstance(thing, NamedZ):
        id = thing.root_node.arg2
    elif isinstance(thing, NamedAny):
        id = single(absorbed(thing))
    else:
        raise Exception("Shouldn't get here")
    # Tag this with OriginallyUserID so that we can unwrap it later on
    return SymbolicExpression(OriginallyUserID(id))

# def names_of_raet(raet):
#     from .types import WishID
#     return raet._d["absorbed"] | filter[WishID] | collect
# def bare_raet(raet):
#     from .types import WishID
#     # Note: need tuple as collect produces lists by default which are not what
#     # is usually in absorbed.
#     return raet._replace(absorbed=tuple(raet._d["absorbed"] | filter[Not[WishID]] | collect))
def names_of_raet(raet):
    from .types import WishID
    names = (raet
             | match[
                 (PureET | PureRT | PureAET, RAET_get_names),
                 (AtomClass, get_all_ids),
             ]
             | map[match[
                 (AllIDs, identity),
                 (Any, wrap_user_id)
                 ]]
             | collect)
    return names
def bare_raet(raet):
    if isinstance(raet, PureET | PureRT | PureAET):
        return RAET_without_names(raet)
    elif isinstance(raet, AtomClass):
        return rae_type(raet)
    else:
        raise Exception(f"Unknown type in bare_raet: {raet}")


@func
def not_implemented_error(obj, text, *others):
    raise NotImplementedError(text + f" Obj: {obj}")

@func
def wrap_list(x):
    return [x]

def generate_initial_state(label: String) -> GenIDState:
    import time
    return (label + str(time.time()), 0)

def gen_internal_id(prev_state: GenIDState) -> Tuple[WishIDInternal, GenIDState]:
    prefix,prev_id = prev_state
    next_id = prev_id + 1
    id_str = WishIDInternal(f"{prefix}_{next_id}")
    next_state = (prefix, next_id)
    return id_str, next_state

# Temporary hack for simpler prototyping
def global_gen_internal_id():
    global_gen_internal_id.last_id += 1
    return WishIDInternal(f"global_{global_gen_internal_id.last_id}")
global_gen_internal_id.last_id = 1
    

def convert_scalar(scalar):
    return AETWithValue({"aet": map_scalar_to_aet(scalar),
                         "value": Val(scalar)})

def map_scalar_to_aet(x):
    return x | match[
        (PyBool, always[AET.Bool]),
        (Int, always[AET.Int]),
        (Float, always[AET.Float]),
        (String, always[AET.String]),
        (Time, always[AET.Time]),
        (Enum, make_enum_aet),
        (QuantityFloat, make_qf_aet), 
        (QuantityInt, make_qi_aet), 
        (ValueType, always[AET.Type]),
        (UserValueInstance, lambda x: x._get_type()),
        (Any, not_implemented_error["TODO scalar type"]),
    ] | collect

def make_enum_aet(x):
    return getattr(AET.Enum, x.enum_type)

def make_qf_aet(x):
    return getattr(AET.QuantityFloat, x.unit.enum_value)

def make_qi_aet(x):
    return getattr(AET.QuantityInt, x.unit.enum_value)

def can_assign_to(val, z: AtomClass):
    if not isinstance(z, AttributeEntity):
        return False
    # Special cases
    if isinstance(val, Int):
        if isinstance(z, AET.Float):
            return True

    if isinstance(val, PrimitiveValue):
        return map_scalar_to_aet(val) == rae_type(z)

    aet = rae_type(z)
    # TODO: Need to get the type as a ValueType from aet (for simple and/or
    # complex types) and then do the isinstance check on val for that.
    from ..VT.rae_types import RAET_get_token
    if RAET_get_token(aet).complex_value is not None:
        return isinstance(val, RAET_get_token(aet).complex_value)
    else:
        raise NotImplementedError("TODO: Need to map AET to value type to compare with value")
    
def most_recent_rae_on_graph(origin_uid: EternalUID, g: Graph) -> Nil|ZefRef:
    """
    Will definitely not return a BT.ForeignInstance, always an instance.
    It could be that the node asked for has its origin on this graph 
    (the original rae may still be alive or it may be terminated)

    This function should only be called DURING a transaction, not in preparation
    for a transaction.

    Returns:
        ZefRef: this graph knows about this: found instance
        None: this graph knows nothing about this RAE
    """
    if origin_uid not in g:
        return None     # this graph never knew about a RAE with this origin uid

    zz = g[origin_uid]
    if BT(zz) in {BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE}:
        from ..graph_slice import get_instance_rae
        return get_instance_rae(origin_uid, now(g))
        
    elif BT(zz) in {BT.ENTITY_NODE, BT.ATTRIBUTE_ENTITY_NODE, BT.RELATION_EDGE}:
        if zz | exists_at[now(g)] | collect:
            return zz | in_frame[now(g)] | collect
        else:
            return None
    elif BT(zz) in {BT.ROOT_NODE, BT.TX_EVENT_NODE}:
        return zz | in_frame[now(g)] | collect
    else:
        raise RuntimeError("Unexpected option in most_recent_rae_on_graph")


@func
def match_rules(input: Tuple, rules: List[Tuple[ValueType,Any]]):
    # This is like match_on but also splats the input arguments out (input must
    # be a tuple and first argument is used for the match condition).

    for typ, func in rules:
        # Note: we have to use isinstance here, as LazyValues could be passed in
        # and need to be processed without evaluation.
        if isinstance(input[0], typ):
            return func(*input)
    raise Error.MatchError("No case matched", input[0])


@func
def scanmany(itr_in, reducers, maybe_init=None):
    def scanmany_inner():
        itr = iter(itr_in)
        if maybe_init is None:
            try:
                cur_item = next(itr)
            except StopIteration:
                raise Exception("scanmany: no init item")
        else:
            cur_item = maybe_init

        yield cur_item
        for new_item in itr:
            comb_item = ()
            for cur,new,func in zip(cur_item, new_item, reducers):
                comb = func(cur, new)
                comb_item = comb_item + (comb,)
            cur_item = comb_item
            yield cur_item
            
    return ZefGenerator(scanmany_inner)

@func
def reducemany(itr, reducers, maybe_init=None):
    return itr | scanmany[reducers][maybe_init] | last | collect

@func
def merge_nodups(x, y):
    shared_keys = set(keys(x)) & set(keys(y))
    assert len(shared_keys) == 0, f"Duplicates found in keys of dictionaries passed into merge_nodups: {shared_keys}"
    return merge(x,y)

def maybe_parse_uid(s) -> Nil|EternalUID:
    if isinstance(s, str) and s.startswith("㏈_"):
        if len(s) == 35:
            return internals.EternalUID.from_base64(s[:24])
        elif len(s) == 24:
            return internals.EternalUID.from_base64(s)
        else:
            raise Exception("UID string is the wrong length")

        return None
    
def force_as_id(x):
    if isinstance(x, AtomRef):
        return origin_uid(x)
    if isinstance(x, str):
        ouid = maybe_parse_uid(x)
        if ouid is not None:
            return ouid
    if isinstance(x, AllIDs):
        return x
    if isinstance(x, FlatRefUID):
        return x
    return wrap_user_id(x)


def id_preference_pair(x: AllIDs, y: AllIDs) -> AllIDs:
    # Take the dominant id from two given ids: EternalUID >> Variable >> WishIDInternal

    assert isinstance(x, AllIDs) and isinstance(y, AllIDs)
    if isinstance(x, EternalUID) or isinstance(y, EternalUID):
        if isinstance(x, EternalUID) and isinstance(y, EternalUID) and x != y:
            raise Exception("Two different EUIDs are trying to be merged!")
        if isinstance(x, EternalUID):
            return x
        return y
    elif isinstance(x, Variable) or isinstance(y, Variable):
        if isinstance(x, Variable):
            return x
        return y
    else:
        return x

def id_preference(l: List[AllIDs]) -> AllIDs:
    out = l | reduce[id_preference_pair] | collect
    return out


def find_rae_in_target(global_uid, target):
    if isinstance(target, GraphSlice):
        # TODO: This migth not be good enough, if the gs is not the same as the
        # latest slice on the graph... but then why should it not be that?
        from ..graph_slice import get_instance_rae
        return get_instance_rae(global_uid, target)
    elif isinstance(target, FlatGraph):
        if global_uid in target:
            return target[global_uid]
        else:
            return None
    else:
        raise Exception(f"Don't understand target: {target}")


def find_value_node_in_target(val, glike):
    # This requires a bit of distinguishing between graphs and flat graphs,
    # because graphs need to serialize first.

    if isinstance(glike, GraphSlice):
        s = internals.val_as_serialized_if_necessary(val)
        return Graph(glike).get_value_node(s)
    elif isinstance(glike, FlatGraph):
        if val in glike:
            return glike[val]
        return None
        