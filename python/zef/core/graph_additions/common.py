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


from ..VT import *
from ..VT.rae_types import RAET_get_token, RAET_get_names

from .._ops import *

from ..zef_functions import func

from .types import *

# ID generation uses a counting integer to distinguish ids in the same set and a prefix (e.g. coming from a time) to try and distinguish between different runs
GenIDState = Tuple[String,Int]

##############################
# * General Utils

def get_uvt_type(uvi: UserValueInstance):
    from ..user_value_type import _user_value_type_registry
    return _user_value_type_registry[uvi._user_type_id]


def wrap_user_id(thing):
    return SymbolicExpression(thing)

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
    names = (RAET_get_names(raet)
             | map[match[
                 (WishID, identity),
                 (Any, wrap_user_id)
                 ]]
             | collect)
    return names
def bare_raet(raet):
    from .types import WishID
    # Note: need tuple as collect produces lists by default which are not what
    # is usually in absorbed.
    return raet._replace(absorbed=(RAET_get_token(raet),))


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
            cur_item = next(itr)
        else:
            cur_item = maybe_init

        yield cur_item
        for new_item in itr:
            comb_item = ()
            for cur,new,func in zip(cur_item, new_item, reducers):
                comb = func(cur, new)
                comb_item = (*comb_item, comb)
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

def ref_as_obj_notation(ref):
    raet = rae_type(ref)
    assert isinstance(raet, ET)
    return EntityValueInstance(raet, origin_uid(ref))

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
    assert isinstance(x, AllIDs)
    return x