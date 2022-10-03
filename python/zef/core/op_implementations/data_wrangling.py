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


def idx_generator(n=0):
    def next_idx():
        nonlocal n
        n = n + 1
        return n
    return next_idx


def is_same_et(et1, et2):
    return et1._d['specific'] == et2._d['specific']


def create_object(d: Dict, generator, idx_to_obj) -> Entity:

    def mapping(v):
        if isinstance(v, dict):
            return create_object(v, generator, idx_to_obj)
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



@func
def resolve_unknown(obj: Entity, rules: List, mapping: Dict) -> Entity:
    from zef.core.patching import EntityValueInstance_
    """
    Takes an object of type EntityValueInstance_ and flattens it out to pairs of tuples ((source, rt, target), (src_id?, None, trgt_id?)).
    It recursively calls itself if at any level a non-terminal object is found.
    """
    def flatten_object(obj: Entity) -> list[list[tuple, tuple]]:
        def generate_type_and_id_maybe(obj):
            return (obj._entity_type, obj._entity_type._d['internal_id']) if isinstance(obj, EntityValueInstance_) else (obj, None)

        def mapping(t: tuple, obj) -> tuple:
            return  ((obj, t[0], t[1]) | map[generate_type_and_id_maybe] | zip | collect)
        
        if not isinstance(obj, EntityValueInstance_): return []
        return obj._kwargs | items | map[lambda t: [mapping(t,obj), *flatten_object(t[1])]] | concat | collect
    

    # For each path tries to match it against the rules set. If there is a match, the enity_type of the matched object is replaced.
    patterns_and_ids = flatten_object(obj) 
    for pattern, ids in patterns_and_ids:
        for rule, replacement in rules:
            Z_pos = match_rule(rule, pattern)
            # If there is a match and the object is not already of the correct type, the object is replaced.
            if Z_pos != -1 and is_same_et(mapping[ids[Z_pos]]._entity_type, ET.ZEF_Unknown):
                mapping[ids[Z_pos]]._entity_type = replacement[ids[Z_pos]]
                return obj
        
    return None


def validate_rules(rules: List) -> Bool:
    for rule,_ in rules:
        assert (rule | filter[lambda x: x == Z]  | length | equals[1] | collect), f"{rule} must contain exactly one Z!"
    return True


def to_object(d: Dict, rules: List) -> Entity:
    """
    Given a dictionary defining an object and a set of rules. The initial object with prefilled unknowns is iterated
    on until rules no longer match. The final object is returned.
    """
    validate_rules(rules)
    idx_to_obj = {}
    obj = create_object(d, idx_generator(), idx_to_obj)
    LazyValue(obj) | iterate[resolve_unknown[rules][idx_to_obj]] | take_while[lambda x: x]  | collect
    return obj