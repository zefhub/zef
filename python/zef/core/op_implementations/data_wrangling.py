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

from .. import *
from ...ops import *
from ..VT import *


def make_idx_generator(n=0):
    def next_idx():
        nonlocal n
        n = n + 1
        return n
    return next_idx


def is_same_et(et1, et2):
    return without_absorbed(et1) == without_absorbed(et2)

def get_et_id(et):
    assert len(absorbed(et)) == 1, f"{et} didn't have excatly a single absorbed arg!"
    return absorbed(et)[0]

@func
def create_object(d: Dict, generator, idx_to_obj) -> Entity:

    def mapping(v):
        if isinstance(v, dict):
            return create_object(v, generator, idx_to_obj)
        elif isinstance(v, list):
            return [mapping(x) for x in v]
        elif isinstance(v, set):
            return {mapping(x) for x in v}
        else:
            return v
    idx = str(generator())
    if "type" in d:
        obj = ET(d['type'])[idx]
    else:
        obj = ET.ZEF_Unknown[idx]

    d = dict(d | items | map[lambda t: (t[0] , mapping(t[1]))] | collect)
    obj = obj(**d)
    idx_to_obj[idx] = obj
    return obj




def match_rule(rule, pattern):
    assert (isinstance(rule, list) or isinstance(rule, tuple)) and (isinstance(pattern, list) or isinstance(pattern, tuple))
    z_pos = - 1
    for i, (p_e,x_e) in enumerate(zip(rule, pattern)): 
        if p_e == Z:
            z_pos = i

        elif is_a(p_e, ET) and is_a(x_e, ET):
            if not is_same_et(p_e, x_e): return -1

        elif p_e != x_e and p_e != Any :
            return -1

    return z_pos



def flatten_object(obj: Entity) -> list[list[tuple, tuple]]:
    """
    Takes an object of type EntityValueInstance_ and flattens it out to pairs of tuples ((source, rt, target), (src_id?, None, trgt_id?)).
    It recursively calls itself if at any level a non-terminal object is found.
    """
    from zef.core.patching import EntityValueInstance_
    
    @func
    def flatten_to_tuples_with_ids(obj, rt, trgt) -> tuple:
        def generate_type_and_id_maybe(obj):
            return (obj._entity_type, get_et_id(obj._entity_type)) if isinstance(obj, EntityValueInstance_) else (obj, None)
        
        return ((obj, rt, trgt) | map[generate_type_and_id_maybe] | zip | collect) # -> # [(obj, t[0], t[1]), (id?, None, None)]

    @func
    def mapping(rt_and_trgt, obj):
        rt, trgt = rt_and_trgt
        if isinstance(trgt, EntityValueInstance_):
            flattened = flatten_object(trgt)
            return [flatten_to_tuples_with_ids(obj, rt, trgt), *flattened]
        
        elif isinstance(trgt, (list, set)):
            flattened = trgt | map[flatten_object] | concat | collect
            return [*(trgt | map[lambda trgt: flatten_to_tuples_with_ids(obj, rt, trgt)] | collect), *flattened]

        else:
            return [flatten_to_tuples_with_ids(obj, rt, trgt)]

    
    if not isinstance(obj, EntityValueInstance_): return []
    return obj._kwargs | items | map[mapping[obj]] | concat | collect


@func
def resolve_unknown(obj: Entity, rules: List, mapping: Dict) -> Entity:
    # For each path tries to match it against the rules set. If there is a match, the enity_type of the matched object is replaced.
    patterns_and_ids = flatten_object(obj) 
    for pattern, ids in patterns_and_ids:
        for rule, replacement in rules:
            Z_pos = match_rule(rule, pattern)
            # If there is a match and the object is not already of the correct type, the object is replaced.
            if Z_pos != -1 and mapping.get(ids[Z_pos], None) and is_same_et(mapping[ids[Z_pos]]._entity_type, ET.ZEF_Unknown):
                mapping[ids[Z_pos]]._entity_type = replacement[ids[Z_pos]]
                return obj

    return None


def validate_rules(rules: List) -> Bool:
    for rule,_ in rules:
        assert (rule | filter[lambda x: x == Z]  | length | equals[1] | collect), f"{rule} must contain exactly one Z!"
    return True


def infer_types(o: Dict|List, rules: List) -> Entity:
    """
    Given a dictionary (or a list of dicts) defining an object and a set of rules. The initial object with prefilled unknowns is iterated
    on until rules no longer match. The final object is returned.
    """
    validate_rules(rules)
    idx_to_obj = {}
    idx_generator = make_idx_generator()
    def create_and_resolve_single_dict(d: Dict):
        assert isinstance(d, dict), f"Expected a dict, got {d}"
        obj = create_object(d, idx_generator, idx_to_obj)
        LazyValue(obj) | iterate[resolve_unknown[rules][idx_to_obj]] | take_while[lambda x: x]  | collect
        return obj
    
    if isinstance(o, list):
        return o | map[create_and_resolve_single_dict] | collect
    elif isinstance(o, dict):
        return create_and_resolve_single_dict(o)
    else:
        raise Exception("Input must be a dictionary or a list of dictionaries.")

@func
def create_idx_to_obj_d(obj, idx_to_obj): 
    """
    Pure function that traverse the object and creates a dictionary mapping the internal ids of the objects to the objects themselves.
    """
    from zef.core.patching import EntityValueInstance_
    def traverse_nested(v):
        if isinstance(v, EntityValueInstance_):
            create_idx_to_obj_d(v, idx_to_obj)
        elif isinstance(v, (list, set)):
            [create_idx_to_obj_d(x, idx_to_obj) for x in v]

    if not isinstance(obj, EntityValueInstance_): return
    
    idx_to_obj[get_et_id(obj._entity_type)] = obj
    obj._kwargs | items | map[lambda t: traverse_nested(t[1])] | collect
   
    return idx_to_obj


@func
def identify_and_merge_step(obj, idx_to_obj, identification_rules):
    def retrieve_obj(et):
        return idx_to_obj[get_et_id(et)]
    
    def merge_identities(obj, other):
        # Remove overlapping obj from the dict
        idx_to_obj.pop(get_et_id(other._entity_type))
        
        # Overwrite the internal id of the other object with the internal id of the obj
        other._entity_type = obj._entity_type

        # Reset the overlapping object's internal dict
        other._kwargs = {}

    def try_identification(et, o1, o2):
        try:
            return identification_rules[et](o1, o2)
        except:
            return False
    if isinstance(obj, list):
        groups = obj | map[flatten_object] | concat | map[first | first] | func[set] | group_by[without_absorbed] | collect
    else:
        groups = flatten_object(obj) | map[first | first] | func[set] | group_by[without_absorbed] | collect
    for et, group in groups:
        if len(group) < 2: continue
        for o1, o2 in zip(group, group[1:]):
            o1, o2 = retrieve_obj(o1), retrieve_obj(o2)
            if try_identification(et, o1, o2):
                merge_identities(o1, o2)
                return obj
    
    return None


def deduplicate(obj_or_list, identification_rules: Dict) -> List[object]:
    idx_to_obj = {}
    if isinstance(obj_or_list, list):
        obj_or_list | for_each[create_idx_to_obj_d[idx_to_obj]]
    else:
        create_idx_to_obj_d(obj_or_list, idx_to_obj)
    LazyValue(obj_or_list) | iterate[identify_and_merge_step[idx_to_obj][identification_rules]]  | take_while[lambda x: x]  | collect
    return list(idx_to_obj.values())


# TODO merge with create_idx_to_obj_d
@func 
def generate_id_to_objs(obj, idx_to_objs):
    from zef.core.patching import EntityValueInstance_
    def traverse_nested(v):
        if isinstance(v, EntityValueInstance_):
            generate_id_to_objs(v, idx_to_objs)
        elif isinstance(v, (list, set)):
            [generate_id_to_objs(x, idx_to_objs) for x in v]

    if not isinstance(obj, EntityValueInstance_): return
    
    idx_to_objs[get_et_id(obj._entity_type)].add(obj)
    obj._kwargs | items | map[lambda t: traverse_nested(t[1])] | collect
   
    return idx_to_objs


def match_with_entity_and_replace_step(obj_list, idx_to_objs, identification_rules, gs):
    def try_retrieve_entity(obj):
        et = without_absorbed(obj._entity_type)
        try:
            return identification_rules[et](obj, gs)
        except:
            return None

    def replace_all_in_d(obj, ent):
        @func
        def over_write_obj(o, new_id): 
            o._entity_type = without_absorbed(o._entity_type)[new_id]
            o._kwargs = {}
            
        obj_id = get_et_id(obj._entity_type)
        entity_id = str(uid(ent))
        idx_to_objs[obj_id] | for_each[over_write_obj[entity_id]]

    for obj in obj_list:
        ent = try_retrieve_entity(obj)
        if ent: replace_all_in_d(obj, ent)



def identify_entities(obj_list: List[object], entity_identification_rules: Dict, gs: GraphSlice) -> List[object]:
    from collections import defaultdict
    idx_to_objs = defaultdict(set)
    obj_list | for_each[generate_id_to_objs[idx_to_objs]]
    match_with_entity_and_replace_step(obj_list, idx_to_objs, entity_identification_rules, gs)
    return obj_list