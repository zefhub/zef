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

from .common import *

def ensure_tag_atom(obj, gen_id_state):
    me = get_most_authorative_id(obj)
    if me is not None:
        me2 = force_as_id(me)
        if me != me2:
            # A slightly weird syntax, as we need to first *remove* names.
            obj = obj.__replace__(atom_id={})
            obj = obj(me2)
            me = me2
        return obj,me,gen_id_state
        
    me,gen_id_state = gen_internal_id(gen_id_state)

    obj = obj(me)

    return obj,me,gen_id_state

def ensure_tag_primitive(obj, gen_id_state):
    obj = convert_scalar(obj)
    me,gen_id_state = gen_internal_id(gen_id_state)
    # Create new AETWithValue with the id included.
    obj = obj._get_type()(obj._value | insert["internal_ids"][[me]] | collect)
    return obj,me,gen_id_state

def ensure_tag_aet(obj: AETWithValue, gen_id_state):
    if "internal_ids" in obj and len(obj.internal_ids) > 0:
        me = obj.internal_ids[0]
    else:
        me,gen_id_state = gen_internal_id(gen_id_state)
        # Create new AETWithValue with the id included.
        obj = obj._get_type()(obj._value | insert["internal_ids"][[me]] | collect)
    return obj,me,gen_id_state

def ensure_tag_pure_et_aet(obj, gen_id_state):
    names = names_of_raet(obj)
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
    return obj, force_as_id(obj.target), gen_id_state

def ensure_tag_OS_dict(obj: OldStyleDict, gen_id_state):
    main_obj = single(obj.keys())
    main_obj,obj_id,gen_id_state = ensure_tag(main_obj, gen_id_state)
    obj = {main_obj: single(obj.values())}
    return obj,obj_id,gen_id_state

def ensure_tag_rae_ref(obj: RAERef, gen_id_state):
    return obj,origin_uid(obj),gen_id_state

def ensure_tag_blob_ptr(obj: BlobPtr, gen_id_state):
    obj = discard_frame(obj)
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
    (AETWithValue, ensure_tag_aet),
    (PureET | PureAET, ensure_tag_pure_et_aet),
    (Delegate, ensure_tag_delegate),
    (PleaseAssign, ensure_tag_assign),
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

    return (obj, gen_id_state) | match_rules[[
        *tagging_rules,
        (Any, not_implemented_error["Don't know how to ensure a tag for object"]),
    ]] | collect