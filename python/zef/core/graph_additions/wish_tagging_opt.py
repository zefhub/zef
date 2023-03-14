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
report_import("zef.core.graph_additions.wish_translation")

from .common_opt import *

def ensure_tag_atom(obj, gen_id_state):
    me = get_most_authorative_id_opt(obj)
    if me is not None:
        me2 = force_as_id(me)
        if me != me2:
            # A slightly weird syntax, as we need to first *remove* names.
            obj = obj.__replace__(atom_id={})
            # I would love to take out this call, but need to handle all cases
            # of types of "me2"
            obj = obj(me2)
            me = me2
        return obj,me,gen_id_state
        
    me,gen_id_state = gen_internal_id(gen_id_state)

    # Instead of going through the entire Atom machinery, we cheat a little
    from ..atom import _get_atom_id
    new_atom_id = dict(_get_atom_id(obj))
    new_atom_id["local_names"] = new_atom_id.get("local_names", []) + [me]
    obj = obj.__replace__(atom_id=new_atom_id)

    return obj,me,gen_id_state

def ensure_tag_primitive(obj, gen_id_state):
    obj = convert_scalar(obj)
    return ensure_tag_assign(obj, gen_id_state)

def ensure_tag_pure_et_aet(obj, gen_id_state):
    names = names_of_raet_opt(obj)
    if len(names) == 0:
        me,gen_id_state = gen_internal_id(gen_id_state)
        obj = obj[me]
    else:
        me = names[0]
    return obj,me,gen_id_state

def ensure_tag_delegate(obj, gen_id_state):
    obj = to_delegate(obj)
    return obj,obj,gen_id_state

def ensure_tag_assign(obj: PleaseAssign, gen_id_state):
    if is_a_PleaseAssignAlsoInstantiate(obj):
        new_target, me, gen_id_state = ensure_tag(obj.target, gen_id_state)
        if new_target != target:
            new_dict = dict(obj._value)
            new_dict["target"] = new_target
            obj = UVT_ctor_opt(PleaseAssign, new_dict)
    else:
        me = force_as_id(obj.target)
    return obj, me, gen_id_state

def ensure_tag_tag(obj: PleaseTag, gen_id_state):
    if isinstance(obj, PleaseTagAlsoInstantiate):
        new_target, me, gen_id_state = ensure_tag(obj.target, gen_id_state)
        if new_target != target:
            obj = PleaseTag(target=new_target, tag=obj.tag)
    else:
        me = force_as_id(obj.target)
    return obj, me, gen_id_state

def ensure_tag_OS_dict(obj: OldStyleDict, gen_id_state):
    main_obj = single(obj.keys())
    main_obj,obj_id,gen_id_state = ensure_tag(main_obj, gen_id_state)
    obj = {main_obj: single(obj.values())}
    return obj,obj_id,gen_id_state

def ensure_tag_rae_ref(obj: RAERef, gen_id_state):
    return obj,origin_uid(obj),gen_id_state

def ensure_tag_blob_ptr(obj: BlobPtr, gen_id_state):
    obj = Atom(obj)
    # BlobPtrs could be RAEs or other things like value nodes/delegates/txs, so
    # pass this back through to ensure_tag to dispatch on the right thing.
    return ensure_tag(obj, gen_id_state)
    # return obj,origin_uid(obj),gen_id_state

def ensure_tag_flatref(obj: FlatRef, gen_id_state):
    from ..flat_graph import FlatRef_maybe_uid
    fr_uid = FlatRef_maybe_uid(obj)
    if isinstance(fr_uid, EternalUID):
        me = fr_uid
    else:
        from ..atom import make_flatref_uid
        me = make_flatref_uid(obj)

    return obj,me,gen_id_state

def ensure_tag_pass_through(obj, gen_id_state):
    return obj,obj,gen_id_state

def ensure_tag_extra_user_id(obj: ExtraUserAllowedIDs, gen_id_state):
    obj = convert_extra_allowed_id(obj)
    return obj,obj,gen_id_state


tagging_rules = [
    (AtomClass, ensure_tag_atom),
    (PrimitiveValue, ensure_tag_primitive),
    (PureET | PureAET, ensure_tag_pure_et_aet),
    (Delegate, ensure_tag_delegate),
    (PleaseAssign, ensure_tag_assign),
    (PleaseTag, ensure_tag_tag),
    (RAERef, ensure_tag_rae_ref),
    (FlatRef, ensure_tag_flatref),
    (BlobPtr, ensure_tag_blob_ptr),
    (OldStyleDict, ensure_tag_OS_dict),
    (WrappedValue, ensure_tag_pass_through),
    (AllIDs, ensure_tag_pass_through),
    (ExtraUserAllowedIDs, ensure_tag_extra_user_id),
]
    
Taggable = Union[tagging_rules | map[first] | func[tuple] | collect]

def ensure_tag(obj: Taggable, gen_id_state: GenIDState) -> Tuple[Taggable, WishID, GenIDState]:

    if type(obj) == Atom_:
        return ensure_tag_atom(obj, gen_id_state)
    elif is_a_PrimitiveValue(obj):
        return ensure_tag_primitive(obj, gen_id_state)
    elif is_a_ET(obj) or is_a_AET(obj):
        return ensure_tag_pure_et_aet(obj, gen_id_state)
    elif is_a_DelegateRef(obj):
        return ensure_tag_delegate(obj, gen_id_state)
    elif is_a_PleaseAssign(obj):
        return ensure_tag_assign(obj, gen_id_state)
    elif is_a_PleaseTag(obj):
        return ensure_tag_tag(obj, gen_id_state)
    # elif is_a_RAERef(obj):
    #     return ensure_tag_rae_ref(obj, gen_id_state)
    # elif is_a_FlatRef(obj):
    #     return ensure_tag_flatref(obj, gen_id_state)
    elif is_a_BlobPtr(obj):
        return ensure_tag_blob_ptr(obj, gen_id_state)
    # elif is_a_OldStyleDict(obj):
    #     return ensure_tag_OS_dict(obj, gen_id_state)
    elif is_a_Val(obj):
        return ensure_tag_pass_through(obj, gen_id_state)
    elif is_a_AllID(obj):
        return ensure_tag_pass_through(obj, gen_id_state)
    elif is_a_ExtraUserAllowedID(obj):
        return ensure_tag_extra_user_id(obj, gen_id_state)
    else:
        not_implemented_error(obj, "Don't know how to ensure a tag for object")