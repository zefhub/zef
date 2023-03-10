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
from ..atom import get_all_ids, find_concrete_pointer

from .types import *

import builtins

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
    def __hash__(self):
        from ..VT.value_type import hash_frozen
        return hash_frozen(self.obj)

from .. import serialization
def serialize_originally_user_id(orig_user_id):
    return {
        "_zeftype": "OriginallyUserID",
        "item": serialization.serialize_internal(orig_user_id.obj),
    }
def deserialize_originally_user_id(d):
    return OriginallyUserID(serialization.deserialize_internal(d["item"]))
serialization.serialization_mapping[OriginallyUserID] = serialize_originally_user_id
serialization.deserialization_mapping["OriginallyUserID"] = deserialize_originally_user_id

@func
def maybe_unwrap_variable(x):
    return LazyValue(x) | match[
        (OriginallyUserID, get_field["obj"]),
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
    # from ..symbolic_expression import V
    if isinstance(thing, str):
        return UVT_ctor_opt(VariableOpt, thing)
    if isinstance(thing, ExtraUserAllowedIDs):
        return convert_extra_allowed_id(thing)
    return UVT_ctor_opt(VariableOpt, OriginallyUserID(thing))

def convert_extra_allowed_id(thing: ExtraUserAllowedIDs):
    if isinstance(thing, NamedZ):
        id = thing.root_node.arg2
    elif isinstance(thing, NamedAny):
        id = single(absorbed(thing))
    elif isinstance(thing, Variable):
        id = VariableOpt(thing.name)
    else:
        raise Exception("Shouldn't get here")
    # Tag this with OriginallyUserID so that we can unwrap it later on
    return wrap_user_id(id)

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

def not_implemented_error_opt(obj, text, *others):
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
    return UVT_ctor_opt(PleaseAssign,
                        dict(target=map_scalar_to_aet(scalar),
                             value=Val(scalar)))

def map_scalar_to_aet(x):
    if type(x) == bool: return AET.Bool
    elif type(x) == int: return AET.Int
    elif type(x) == float: return AET.Float
    elif type(x) == str: return AET.String
    elif is_a_Time(x): return AET.Time
    elif is_a_Enum(x): return make_enum_aet(x)
    elif is_a_QuantityFloat(x): make_qf_aet(x) 
    elif is_a_QuantityInt(x): make_qi_aet(x) 
    elif type(x) == ValueType_: return AET.Type
    elif type(x) == UserValueInstance_: return x._get_type()
    else: not_implemented_error(x, "TODO scalar type")

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

    from ..atom import _get_ref_pointer
    zz = g[origin_uid]
    if BT(zz) in {BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE}:
        out = get_instance_rae_opt(origin_uid, now(g))
        out = _get_ref_pointer(out)
        
    elif BT(zz) in {BT.ENTITY_NODE, BT.ATTRIBUTE_ENTITY_NODE, BT.RELATION_EDGE}:
        if zz | exists_at[now(g)] | collect:
            out = zz | in_frame[now(g)] | collect
        else:
            out = None
    elif BT(zz) in {BT.ROOT_NODE, BT.TX_EVENT_NODE}:
        out = zz | in_frame[now(g)] | collect
    else:
        raise RuntimeError("Unexpected option in most_recent_rae_on_graph")

    return out


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

def merge_nodups(x, y):
    shared_keys = set(keys(x)) & set(keys(y))
    assert len(shared_keys) == 0, f"Duplicates found in keys of dictionaries passed into merge_nodups: {shared_keys}"
    x.update(y)

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
    if type(x) ==  str:
        ouid = maybe_parse_uid(x)
        if ouid is not None:
            return ouid
    if is_a_AllID(x):
        return x
    # if isinstance(x, FlatRefUID):
    #     return x
    return wrap_user_id(x)


def id_preference_pair(x: AllIDs, y: AllIDs) -> AllIDs:
    # Take the dominant id from two given ids: EternalUID >> Variable >> WishIDInternal

    assert is_a_AllID(x) and is_a_AllID(y)
    if is_a_EternalUID(x) or is_a_EternalUID(y):
        if is_a_EternalUID(x) and is_a_EternalUID(y) and x != y:
            raise Exception("Two different EUIDs are trying to be merged!")
        if is_a_EternalUID(x):
            return x
        return y
    elif is_a_VariableOpt(x) or is_a_VariableOpt(y):
        if is_a_VariableOpt(x):
            return x
        return y
    else:
        return x

def id_preference(l: List[AllIDs]) -> AllIDs:
    from functools import reduce
    return reduce(id_preference_pair, l)


def find_rae_in_target(global_uid, target):
    if type(target) == GraphSlice_:
        # TODO: This migth not be good enough, if the gs is not the same as the
        # latest slice on the graph... but then why should it not be that?
        return get_instance_rae_opt(global_uid, target)
        # return get_instance_rae_opt(global_uid, target)
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
        



# Copy pasted speed ups

def get_instance_rae_opt(origin_uid, gs, allow_tombstone=False)->ZefRef:
    g = Graph(gs.tx)
    if origin_uid not in g:
        return None

    zz = g[origin_uid]
    if BT(zz) in {pymain.BT.FOREIGN_ENTITY_NODE, pymain.BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, pymain.BT.FOREIGN_RELATION_EDGE}:
        z_candidates = pyzo.traverse_in_node_multi(zz, pymain.BT.ORIGIN_RAE_EDGE)
        z_candidates = [pyzo.target(x) for x in z_candidates]
        if not allow_tombstone:
            z_candidates = [x for x in z_candidates if pyzo.exists_at(gs.tx)]
        if len(z_candidates) > 1:
            raise RuntimeError(f"Error: More than one instance alive found for RAE with origin uid {origin_uid}")
        elif len(z_candidates) == 1:
            out = pyzo.to_frame(z_candidates[0], gs.tx, allow_tombstone)
        else:
            out = None     # no instance alive at the moment
        
    elif BT(zz) in {pymain.BT.ENTITY_NODE, pymain.BT.ATTRIBUTE_ENTITY_NODE, pymain.BT.RELATION_EDGE, pymain.BT.TX_EVENT_NODE, pymain.BT.ROOT_NODE}:
        if allow_tombstone:
            from . import _ops
            out = pyzo.to_frame(zz, gs.tx, allow_tombstone)
        else:
            out = pyzo.to_frame(zz, gs.tx)
    else:
        raise RuntimeError(f"Unexpected option in get_instance_rae: {zz}")

    return out


def UVT_ctor_opt(typ, val):
    return UserValueInstance(typ._d["user_type_id"], val)

from ..internals import get_c_token

def GraphSlice_contains(gs, thing):
    from .. import internals

    if is_a_DelegateRef(thing):
        maybe_z = to_delegate(d, gs)
        return maybe_z is not None

    if is_a_Val(thing):
        val = internals.val_as_serialized_if_necessary(thing)
        maybe_z = Graph(gs.tx).get_value_node(val)
        if maybe_z is None:
            return False
        return pyzo.exists_at(maybe_z, gs.tx)

    assert is_a_EternalUID(thing)

    g = Graph(gs.tx)
    z = get_instance_rae_opt(thing, gs)
    return z is not None

def GraphSlice_getitem(gs, thing):
    from .. import internals

    if is_a_DelegateRef(thing):
        maybe_z = to_delegate(d, gs)
        if maybe_z is None:
            raise KeyError(f"Delegate {thing} not present in this timeslice")
        return maybe_z

    elif is_a_Val(thing):
        val = internals.val_as_serialized_if_necessary(thing)
        maybe_z = Graph(gs.tx).get_value_node(val)
        if maybe_z is None:
            raise KeyError(f"ValueNode {thing} doesn't exist on graph") 
        if not pyzo.exists_at(maybe_z, gs.tx):
            raise KeyError(f"ValueNode {thing} isn't alive in this timeslice") 
        return pyzo.to_frame(maybe_z, gs.tx)

    else:
        g = Graph(gs.tx)
        ezr = g[thing]
        # We magically transform any FOREIGN_ENTITY_NODE accesses to the real RAEs.
        # Accessing the low-level BTs can only be done through traversals
        if BT(ezr) in [BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE]:
            out = get_instance_rae_opt(uid(thing), gs)
            if out is None:
                raise KeyError("RAE doesn't have an alive instance in this timeslice")

        else:
            out = pyzo.to_frame(ezr, gs.tx)

    return out

def get_most_authorative_id_opt(atom: AtomClass):
    from ..atom import _get_atom_id
    atom_id = _get_atom_id(atom)
    if "global_uid" in atom_id:
        return atom_id["global_uid"]
    if "flatref_idx" in atom_id:
        return FlatRefUID(idx=atom_id["flatref_idx"], flatgraph=atom_id["frame_uid"])
    if "local_names" in atom_id:
        from ..VT.value_type import hash_frozen
        temp = sorted(atom_id["local_names"], key=hash_frozen)
        return temp[0]
    return None

def origin_uid_opt(z) -> EternalUID:
    if type(z) == Atom_:
        from ..atom import _get_ref_pointer
        z = _get_ref_pointer(z)
        assert z is not None
    assert internals.BT(z) in {internals.BT.ENTITY_NODE, internals.BT.ATTRIBUTE_ENTITY_NODE, internals.BT.RELATION_EDGE, internals.BT.TX_EVENT_NODE, internals.BT.ROOT_NODE}
    if internals.is_delegate(z):
        return to_delegate(z)
    if internals.BT(z) in {internals.BT.TX_EVENT_NODE, internals.BT.ROOT_NODE}:
        return pyzo.uid(pyzo.to_ezefref(z))
    origin_candidates = pyzo.traverse_out_node_multi(pyzo.traverse_in_edge(pyzo.to_ezefref(z), internals.BT.RAE_INSTANCE_EDGE), internals.BT.ORIGIN_RAE_EDGE)
    if len(origin_candidates) == 0:
        # z itself is the origin
        return pyzo.uid(pyzo.to_ezefref(z))
    assert len(origin_candidates) == 1
    z_or = origin_candidates[0]
    if internals.BT(z_or) in {internals.BT.FOREIGN_ENTITY_NODE, internals.BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, internals.BT.FOREIGN_RELATION_EDGE}:
        # the origin was from a different graph
        g_origin_uid = pyzo.base_uid(pyzo.traverses_out_node(z_or, internals.BT.ORIGIN_GRAPH_EDGE))
        return EternalUID(pyzo.base_uid(z_or), g_origin_uid)
    else:
        # z itself is not the origin, but the origin came from this graph.
        # The origin must have been terminated at some point and z is of the
        # same lineage
        return pyzo.uid(pyzo.to_ezefref(pyzo.target(z_or)))

# Hardcoded type checks

from ..user_value_type import UserValueInstance_
from ..symbolic_expression import SymbolicExpression_
from ..graph_slice import GraphSlice_
from ..VT.value_type import ValueType_
from ..atom import Atom_
from ...pyzef import main as pymain, zefops as pyzo
PInst_type_id = PleaseInstantiate._d["user_type_id"]
PAssign_type_id = PleaseAssign._d["user_type_id"]
PAlias_type_id = PleaseAlias._d["user_type_id"]
PTerminate_type_id = PleaseTerminate._d["user_type_id"]
PTag_type_id = PleaseTag._d["user_type_id"]
PRun_type_id = PleaseRun._d["user_type_id"]
PBeSource_type_id = PleaseBeSource._d["user_type_id"]
PBeTarget_type_id = PleaseBeTarget._d["user_type_id"]
PMustLive_type_id = PleaseMustLive._d["user_type_id"]
VariableOpt_type_id = VariableOpt._d["user_type_id"]
WishIDInternal_type_id = WishIDInternal._d["user_type_id"]
TagIDInternal_type_id = TagIDInternal._d["user_type_id"]
FlatRefUID_type_id = FlatRefUID._d["user_type_id"]

def is_a_PleaseInstantiate(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PInst_type_id: return False
    return True

def is_a_PleaseInstantiateRelation(x):
    if type(x) != dict: return False
    assert set(x.keys()) == {"rt", "source", "target"}
    assert is_a_AllID(x["source"])
    assert is_a_AllID(x["target"])
    assert is_a_RT(x["rt"])
    return True
        

def is_a_PleaseAlias(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PAlias_type_id: return False
    return True

def is_a_PleaseTerminate(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PTerminate_type_id: return False
    return True

def is_a_PleaseTag(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PTag_type_id: return False
    return True

def is_a_PleaseRun(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PRun_type_id: return False
    return True

def is_a_PleaseBeSource(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PBeSource_type_id: return False
    return True
def is_a_PleaseBeTarget(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PBeTarget_type_id: return False
    return True
def is_a_PleaseMustLive(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PMustLive_type_id: return False
    return True

def is_a_PleaseCommandLevel1(x):
    return (is_a_PleaseInstantiate(x)
            or is_a_PleaseAssignJustValue(x)
            or is_a_PleaseTerminate(x)
            or is_a_PleaseTagJustTag(x)
            or is_a_PleaseMustLive(x)
            or is_a_PleaseBeSource(x)
            or is_a_PleaseBeTarget(x)
            or is_a_PleaseAlias(x))

def is_a_PleaseAssign(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PAssign_type_id: return False
    return True

def is_a_PleaseAssignJustValue(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PAssign_type_id: return False
    if not is_a_AllID(x._value["target"]): return False
    return True

def is_a_PleaseAssignAlsoInstantiate(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PAssign_type_id: return False
    if is_a_AllID(x._value["target"]): return False
    return True

def is_a_PleaseTagJustTag(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != PTag_type_id: return False
    if not is_a_AllID(x._value["target"]): return False
    return True

def is_a_EternalUID(x):
    return type(x) == internals.EternalUID
def is_a_DelegateRef(x):
    return type(x) == internals.Delegate
def is_a_Val(x):
    return type(x) == internals.Val_
def is_a_AllID(x):
    return is_a_WishID(x) or is_a_EternalUID(x) or is_a_DelegateRef(x) or is_a_Val(x)

def is_a_WishID(x):
    return is_a_VariableOpt(x) or is_a_InternalID(x)
def is_a_Variable(x):
    if type(x) != SymbolicExpression_: return False
    if x.root_node is not None: return False
    return True
def is_a_InternalID(x):
    return is_a_WishIDInternal(x) or is_a_FlatRefUID(x) or is_a_TagIDInternal(x)

def is_a_VariableOpt(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != VariableOpt_type_id: return False
    return True
def is_a_WishIDInternal(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != WishIDInternal_type_id: return False
    return True
def is_a_TagIDInternal(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != TagIDInternal_type_id: return False
    return True
def is_a_FlatRefUID(x):
    if type(x) != UserValueInstance_: return False
    if x._user_type_id != FlatRefUID_type_id: return False
    return True


def is_a_PrimitiveValue(x):
    return type(x) in [int, str, float, bool, pymain.Time, internals.ZefEnumValue, pymain.QuantityInt, pymain.QuantityFloat]

def is_a_GraphWishValue(x):
    return is_a_PrimitiveValue(x) or is_a_Val(x)

def is_a_ET(x):
    if type(x) != ValueType_: return False
    if x._d["type_name"] != "ET": return False
    return True

def is_a_RT(x):
    if type(x) != ValueType_: return False
    if x._d["type_name"] != "RT": return False
    return True

def is_a_AET(x):
    if type(x) != ValueType_: return False
    if x._d["type_name"] != "AET": return False
    return True

def is_a_RAET(x):
    if type(x) != ValueType_: return False
    if x._d["type_name"] not in ["ET", "RT", "AET"]: return False
    return True

def is_a_Level2AtomClass(x):
    from ..atom import Atom_, _get_atom_id, _get_fields, _get_atom_type, _get_ref_pointer
    if type(x) != Atom_: return False

    atom_type = _get_atom_type(x)
    if not (is_a_RAET(atom_type) or atom_type in [BT.TX_EVENT_NODE, BT.ROOT_NODE, Val, None]): return False

    atom_id = _get_atom_id(x)
    # if "flatref_idx" in atom_id: return False
    if "global_uid" in atom_id:
        global_uid = atom_id["global_uid"]
        if not (is_a_EternalUID(global_uid) or is_a_DelegateRef(global_uid) or is_a_Val(global_uid)): return False
    if "frame_uid" in atom_id:
        # Urgh this is ugly. A flatref needs its flatgraph to be able to
        # uniquely identify itself. So we allow that, but not any other frame.
        if "flatref_idx" not in atom_id: return False

    fields = _get_fields(x)
    for name,(rel,val) in fields.items():
        if not (is_a_Level2AtomClass(rel) or is_a_RT(rel)): return False
        if type(val) != set: return False
        for v in val:
            if not (is_a_GraphWishValue(v) or is_a_Level2AtomClass(v) or is_a_WishID(v)): return False

    return True

def is_a_BlobPtr(x):
    return type(x) in [pymain.ZefRef, pymain.EZefRef]