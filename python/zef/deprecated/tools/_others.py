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
    "update_items_from_json", 
    "update_items_convert_value",
    "AnyT",
    "zef_spec_check_relent",
    "zef_spec_check_relent_details",
    "eq",
    "noteq",
    "zef_spec_check_relation_details",
    "zef_spec_check_structure",
    "all_connected_entities",
    "cprint",
    "inject",
    "collect_subgraph",
    "timeit",
]

from ...core import *
from ...ops import *

from .time import calc_time_from_date

from typing import List, Dict, Set

class SimpleProblem(BaseException):
    def __init__(self, s):
        self.s = s


def update_items_from_json(data, g, et, fields,
                *,
                func_prefilter=lambda x: True,
                func_postfilter=lambda x: True,
                func_before_insert=lambda x: x,
                func_after_insert=lambda z,item, new_item: None):

    opts = [(key,old_key) for key,(old_key,new_type,purpose) in fields.items() if purpose == "key"]
    assert len(opts) == 1
    key_field, old_key_field = opts[0]

    problems = []
    changes = []

    with Transaction(g):
        entities = g | instances[now][et]
        for item_ind,orig_item in enumerate(data):
            try:
                if old_key_field not in orig_item:
                    raise SimpleProblem(f"Main key '{old_key_field}' not found in entry {item_ind}. Skipping")

                if not func_prefilter(orig_item):
                    continue
                item = {key: update_items_convert_value(g, orig_item, details)
                        for key,details in fields.items()}
                if not func_postfilter(item):
                    continue
                item = func_before_insert(item)

                # Find item
                if key_field not in item:
                    raise SimpleProblem(f"Main key '{key_field}' not found in entry {item_ind} after transformation. Skipping")

                entity_key = item[key_field]
                opts = entities | filter[lambda x: x >> RT(key_field) | value == entity_key]
                if len(opts) == 0:
                    entity = instantiate(et, g)
                    entities += ZefRefs([entity])
                    new_item = True
                elif len(opts) > 1:
                    raise SimpleProblem(f"Entry {item_ind}: unexpected multiple {et} with the same key {old_key_field} of {entity_key} found. Database problem detected.")
                else:
                    entity = opts | only
                    new_item = False
                    modified_field = False

                # Add fields in
                complained = False
                for key,details in fields.items():
                    old_key,new_type,purpose = details
                    try:
                        if purpose in ["static field", "dynamic field", "key"]:
                            val = item[key]
                            if new_item:
                                entity | fill_or_attach[RT(key), val]
                            else:
                                ae = entity >> O[RT(key)]
                                if ae is None or val != ae | value:
                                    modified_field = True
                                    if purpose in ["static field","key"] and not complained:
                                        problems.append(f'Static field "{key}" which should not change, has been updated to new value for item {et} with key {entity_key} - continuing with update anyway')
                                        complained = True
                                    entity | fill_or_attach[RT(key), val]
                        elif purpose == "calc":
                            # do nothing, this was just for postprocessing or filter or ...
                            pass
                        else:
                            raise Exception(f"Unknown purpose {purpose}")
                    except Exception as exc:
                        raise SimpleProblem(f"Unknown error occurred for key {key} in entry {item_ind}.")
                    

                ret = func_after_insert(entity, item, new_item)
                if ret is True:
                    modified_field = True

                if new_item:
                    changes.append(f"Added new {et} with key {entity_key}")
                elif modified_field:
                    changes.append(f"Modified existing {et} with key {entity_key}")
                else:
                    changes.append(f"Identical existing {et} with key {entity_key}")
                    

            except KeyboardInterrupt:
                raise
            except SimpleProblem as exc:
                problems.append(exc.s)
            except BaseException as exc:
                import traceback
                print(exc)
                print(traceback.format_exc())

                # Don't include the exception in the REST return
                # problems.append(f"Unknown exception: original key {orig_item[old_key_field]}, exc={exc}")
                problems.append(f"Unknown exception for entry {item_ind}")
                problems.append(f"Exception was {exc} with traceback {traceback.format_exc()}")

        return entities,problems,changes
    

        
def update_items_convert_value(g, item, details):
    old_key,new_type,purpose = details

    if old_key is None:
        # This must be handled in some post processing
        return None

    val = item[old_key]

    if new_type == str:
        if val is None:
            return ""
        else:
            return str(val)
    elif new_type == float:
        return float(val)
    elif new_type == int:
        return int(val)
    elif type(new_type) == str and new_type.startswith("EN."):
        en_type = new_type[len("ET."):]
        return EN(en_type, val)
    elif new_type == "date":
        if val is None:
            # If val is None then this will trigger an error later if it isn't handled in some post processing..
            return None
        return calc_time_from_date(g, val)
    elif new_type == "copy":
        return val
    else:
        raise Exception("Unknown new_type {new_type}")





######################################################
# * ZefSpec helper functions
#----------------------------------------------------

def AnyT(relent):
    if BT(relent) == BT.ENTITY_NODE:
        return ET(relent)
    elif BT(relent) == BT.ATOMIC_ENTITY_NODE:
        return AET(relent)
    elif BT(relent) == BT.RELATION_EDGE:
        return RT(relent)
    else:
        return BT(relent)


def zef_spec_check_relent(types, relent, errors, seen_relents):
    if AnyT(relent) not in types.keys():
        return
    if any(z == relent | to_ezefref for z in seen_relents):
        return
    seen_relents.append(relent | to_ezefref)

    relent_details = types[AnyT(relent)]
    zef_spec_check_relent_details(types, relent, relent_details, errors, seen_relents)


def zef_spec_check_relent_details(types, relent, relent_details, errors, seen_relents):
    relent_summary = f"relent {relent|uid} of type {AnyT(relent)}"
    to_check = list(relent | outs)
    additional = "no"

    for (key,rel_requirements) in relent_details.items():
        if type(key) == str:
            if key == "additional":
                assert additional in ["no", "allowed"]
                additional = rel_requirements
            elif key == "target":
                zef_spec_check_relation_details(types, relent, rel_requirements, errors, seen_relents)
                zef_spec_check_relent(types, relent | target, errors, seen_relents)
        elif type(key) == RelationType or type(key) == type(L) or type(key) == type(O):
            if type(key) == RelationType:
                rt = key
            elif type(key) == type(L):
                rt = key.data
            elif type(key) == type(O):
                rt = key.data
            opts = []
            for rel in list(to_check):
                if RT(rel) == rt:
                    opts.append(rel)
                    to_check.remove(rel)
            if type(key) == RelationType:
                if len(opts) > 1:
                    errors.append(f"Too many {key} for {relent_summary}")
                    continue
                elif len(opts) == 0:
                    errors.append(f"No {key} for {relent_summary}")
                    continue
            elif type(key) == type(O):
                if len(opts) > 1:
                    errors.append(f"Too many {key} for {relent_summary}")
                    continue

            for relation in opts:
                zef_spec_check_relation_details(types, relation, rel_requirements, errors, seen_relents)
                zef_spec_check_relent(types, relation | target, errors, seen_relents)

    if additional == "no":
        if len(to_check) > 0:
            extra_rts = set(RT(rel) for rel in to_check)
            errors.append(f"Extra unspecified relations on {relent_summary}: {extra_rts}")

    
    

            
def eq(a,b):
    if type(a) != type(b):
        return False
    return a == b


def noteq(a,b):
    return not eq(a,b)


def zef_spec_check_relation_details(types, relation, details, errors, seen_relents):
    import types as TYPES
    if type(details) not in [tuple,list]:
        details = [details]
    relation_summary = f"relation {relation|uid} of type {RT(relation)}"
    for requirement in details:
        if type(requirement) == TYPES.FunctionType:
            if not requirement(relation):
                errors.append(f"Function predicate failed for {relation_summary}")
        elif type(requirement) == dict:
            zef_spec_check_relent_details(types, relation, requirement, errors, seen_relents)
        else:
            if noteq(AnyT(relation | target), requirement):
                errors.append(f"Target should be ({requirement}) for {relation_summary} but was {AnyT(relation | target)}")
            

def zef_spec_check_structure(types, start_points, frame=None):
    errors = []
    seen_relents = []
    
    to_do = list(start_points)
    while to_do:
        item = to_do.pop()
        if type(item) == EntityType or type(item) == RelationType:
            if frame is None:
                raise TypeError("Need to pass a frame when giving ET/RT types.")
            to_do.extend(Graph(frame) | instances[frame][item])
        else:
            zef_spec_check_relent(types, item, errors, seen_relents)
    return len(errors) == 0, '\n'.join(errors)


################################
# * Merge helpers
#------------------------------
"""
    all_connected_entities(uzrs)    
Find all entities that would complete the set from `uzrs` such that no relation
has a source/target outside of the new set.
"""
def all_connected_entities(zrs):
    # FIXME: Working in a list for now, need to change to EZefRefs later
    all_relents = set(zrs)
    todo = list(zrs | filter[is_a[RT]] | collect)
    while todo:
        item = todo.pop()
        all_relents.add(item)
        if is_a(item, RT):
            todo += [source(item), target(item)]

    return ZefRefs(list(all_relents))


def add_connecting_relations(zs):
    todo = list(zs)
    full_set = set(zs)
    while todo:
        this = todo.pop()
        rels = this | ins_and_outs | filter[lambda z: z | source in full_set and z | target in full_set]
        for rel in rels:
            if rel not in full_set:
                full_set.add(rel)
                todo.append(rel)
                
    return ZefRefs(list(full_set))

def add_connected_entities(zs):
    full_set = set(zs)
    todo = list(ZefRefs(zs) | filter[lambda z: z | BT == BT.RELATION_EDGE])
    while todo:
        this = todo.pop()
        full_set.add(this)
        if this | BT == BT.RELATION_EDGE:
            todo.append(this | source)
            todo.append(this | target)
                
    return ZefRefs(list(full_set))
##################################
# * Import helpers
#--------------------------------


# More documentation needed here. Simple comments follow for the funcs:
#
# func_prefilter(raw) - passed the raw json entry for early filtering.
# func_postfilter(item) - passed the dictionary after automatic processing
# func_before_insert(item) - apply any modifications to the dictionary for manual processing (should return a dictionary)
# func_after_insert(zefref, item, new_item) - passed the entity, dictionary and bool "new_item" after this entry has been processed



##############################
# * Old things
#----------------------------

def cprint(x):
    os.write(1, str.encode(str(x) + '\n'))



##############################
# * Inject
#----------------------------
def instantiate_ae_from_val(val, g)->ZefRef:
    ae, = [val] | g | run
    return ae


def inject(d: any, g)->ZefRef:    
    """Function to import / inject existing data formats into a zefDB graph. 
    This may be extended to deal with data types beyond json (?).
    use as:
    d = {
        'zef_type': 'Cat',
        'zef_tag': 'small cat',
        'Name': 'Naomi',
        'Mother':{
            'zef_type': 'Cat',
            'zef_tag': 'big cat',
            'Name': 'Luna',
            'Age': 16,
            'favorite_foods': ['Tuna', 'Salmon', 42]
        },
        'Age': 15,
        'Weight': 4.5,
    }
    g = Graph()
    z = inject(d, g)    


    Note that we cannot express graph structures with multiple edges of the same type
    coming out of a single node as a dictionary: the keys have to be unique.
    """
    with Transaction(g):
        if type(d) in [str, float, bool, int]:
            return instantiate_ae_from_val(d, g)        
        
        if isinstance(d, list):
            my_list = instantiate(ET.List, g)
            prev_edge = None
            for el in d:                
                ed = instantiate(
                        my_list,
                        RT.ListElement,
                        inject(el, g),
                        g
                    )
                if prev_edge is not None:
                    instantiate(prev_edge, RT.Next, ed, g)
                prev_edge = ed                
            return my_list

        if isinstance(d, dict):
            et = ET(d.get('zef_type', '_unspecified'))
            z = instantiate(et, g)
            if 'zef_tag' in d:
                tag(z, d['zef_tag'])
            for rt, val in d.items():
                if rt not in ('zef_type', 'zef_tag'):
                    instantiate(
                        z,
                        RT(rt),
                        inject(val, g),
                        g
                    )            
            return z

######################################
# * Collect subgraph
#------------------------------------

def collect_subgraph(z_start, rels: List[Dict]):
    """
    z_start: elements in the initial subgraph, can be either ZefRefs or EZefRefs
    rels: [
            {'step': (ET.Schedule, RT.ScheduleCleaningTypeConnection, ET.CleaningType), 'step_direction': 'from_source'},
            {'step': (ET.ProcessOrder, RT.ProcessOrderShardableOperationInstanceConnection, ET.ShardableOperationInstance), 'step_direction': 'from_target', 'rel_predicate': lambda z: False},
          ]

    The first tuple means: If I am considering an entity of type ET.Schedule,
    walk outwards (step_direction) along RT.ScheduleCleaningTypeConnection and include the entity
    attached as a target in the set if it is of type ET.CleaningType.

    If 'step_direction'=='from_target' then we start walking from the target RAE (third element).
    If a 'rel_predicate' key is included, the associated function is applied to the relation being traversed.

    Starting from z_start, iteratively expand the set (subgraph)
    by adding RAE on the frontier connected by any of the relations
    specified in rels.
    """
    
    def to_zefrefs_or_uzefrefs(zs):
        if len(zs) == 0:
            raise RuntimeError(
                'set passed into extend_frontier is empty. Cannot determine whether it is a ZefRef or EZefRef')
        return ZefRefs(list(zs)) if isinstance(list(zs)[0], ZefRef) else EZefRefs(list(zs))

    def extend_frontier(frontier: Set, rels: List[Dict]) -> Set:
        """return both the source/target attached, but also the relations if a triple is to be included """

        out_rels = ([(to_zefrefs_or_uzefrefs(frontier) | filter[d['step'][0]] > L[(d['step'][1])]) | flatten | filter[
            lambda z: rae_type(z | target) == d['step'][2]] | filter[d.get('rel_predicate', lambda z: True)] for d in
                     rels if d['step_direction'] == 'from_source'] | ops.flatten())
        in_rels = ([(to_zefrefs_or_uzefrefs(frontier) | filter[d['step'][2]] < L[(d['step'][1])]) | flatten | filter[
            lambda z: rae_type(z | source) == d['step'][0]] | filter[d.get('rel_predicate', lambda z: True)] for d in
                    rels if d['step_direction'] == 'from_target'] | ops.flatten())
        return set(out_rels + in_rels + [z | target for z in out_rels] + [z | source for z in in_rels])

    current_set = set(z_start)
    frontier = set(z_start)
    # this is gonna be mutaty style for now :(
    while frontier:
        candidates = set(extend_frontier(frontier, rels))
        frontier = candidates - current_set  # only keep the ones we have never seen
        current_set.update(frontier)  # add all new elements by mutating the existing set

    return to_zefrefs_or_uzefrefs(current_set)



##############################
# * timeit
#----------------------------
def timeit(f):
    """use as decorator on any fct for a very simple print output measuring the execution duration"""
    def wrapped(*args, **kwargs):
        import time
        t0=time.time()
        res = f(*args, **kwargs)
        t1=time.time()
        print(f"zefdb.timeit - duration for single run: {t1-t0} seconds")
        return res
    return wrapped









