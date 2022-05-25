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
    "GraphDelta",
]
from typing import Any, Tuple, Callable
from functools import partial as P

from .. import pyzef
from ._core import *
from . import internals
from .zef_functions import func
from ._ops import *
from .op_structs import ZefOp, LazyValue
from .graph_slice import GraphSlice
from .abstract_raes import Entity, Relation, AtomicEntity, TXNode, Root

from abc import ABC
class ListOrTuple(ABC):
    pass
for t in [list, tuple]:
    ListOrTuple.register(t)




##############################
# * Description
#----------------------------

# Glossary:
#
# - commands: dictionaries in the constructed GraphDelta which match directly to low-level changes.
# - expressions: high-level things which match to user expectations. There are "complex" expressions and "primitive" expressions.
#
# The general flow to take a bunch of complex exprs, turn them into primitive
# exprs (may increase the number of exprs) and then turn these into commands.
#
# GraphDelta constructor is in three stages:
# First stage: verify input and translate to "simpler" or "fewer" base exprs.
# Second stage: convert exprs to commands and order them
# Orders them by iterating on to-do commands, sorts into bucket of can-be-done-now and to-do-later, applies can-be-done-now, iterate
#
# Logic flow of GraphDelta constructor is:
# - handles nested GraphDeltas (including reassigning unique temporary ids)
# - calls verify_internal_id on each expr (this builds a ID dict for the next step)
# - calls verify_input_el on each expr (this validates that all Z[...] are in the ID dict, as well as the exprs have the expected structure)
# - calls verify_relation_source_target (checks abstract Relations have all source/targets)
# - TODO: call verify no TX or root nodes
# - iteratively calls iteration_step (make_iteration_step)
# - calls verify_and_compact_commands - does ordering
#
# iteration_step is a dispatch on expr type to call functions with _cmds_for prefix (e.g. _cmds_for_instantiable)
# - takes in exprs-to-do ("user-expressions"), finished-commands ("commands") and seen-ids ("ids_present")
# - returns new exprs, new commands and new ids
# - eventually exprs list should "run out" and everything is now a command
# - another analogy: exprs is a "work queue", cmds/ids is an "output list", iteration_step takes from work queue, puts results into output and maybe more jobs into the work queue.
#
#
# Shorthand syntax:
#
# e.g. (a, (b,c,d)) = [ET.Entity, (z, RT.Something, 5)] | g | run
#
# If the user uses the shorthand syntax then the logic first passes through
# `encode`. The function encode only does two additional things to the
# GraphDelta constructor - it assigns IDs where necessary to be able to return
# appropriate results, and prevents a few unusual things (e.g. tagging a
# ZefRef). Most of this function is dedicated to handling relations to be able
# to unpack them into the right structures. Other nodes are left "as is" except
# for obtaining iids to unpack them.
#
#
#
# realise_single_node:
# Used by encode for every top level item and used by GraphDelta constructor and
# encode for pieces of relations. This function exists to both assign an ID and
# convert a complex expression to a primitive one. Only works on "single
# objects", e.g. entity, value, ZefRef, and not relations or tags.
#
# e.g. realise_single_node(ET.Entity) = ("tmp_id_1", ET.Entity["tmp_id_1"])
#
# 
# Performing GraphDelta transaction:
# - works through list of commands, applying then in a low-level transaction onto the graph
# - commands must be properly ordered (done in constructor) before this function is called
# - each command fills in ID dictionary to be returned as receipt.
# - ID dictionary is called "d_raes"
#

# types of commands:
# - instantiate
# - assign_value
# - merge
# - terminate
# - tag
#
# Commands should use keywords of "internal_id" or "origin_rae" to fill in ID dictionary.



########################################################
# * Entrypoint from user code
#------------------------------------------------------
def dispatch_ror_graph(g, x):
    from . import Effect, FX
    if isinstance(x, LazyValue):
        x = collect(x)
        # Note that this could produce a new LazyValue if the input was an
        # assign_value. This is fine.
    if any(isinstance(x, T) for T in {list, tuple, dict, EntityType, AtomicEntityType, ZefRef, EZefRef, ZefOp, QuantityFloat, QuantityInt, LazyValue, Entity, AtomicEntity, Relation}):
        unpacking_template, commands = encode(x)
        # insert "internal_id" with uid here: the unpacking must get to the RAEs from the receipt
        def insert_id_maybe(cmd: dict):
            if 'origin_rae' in cmd:
                if is_a(cmd['origin_rae'], Delegate):
                    internal_id = cmd['origin_rae']
                else:
                    internal_id = uid(cmd['origin_rae'])
                return {**cmd, 'internal_id': internal_id}
            return cmd

        commands_with_ids = [insert_id_maybe(c) for  c in commands]
        return Effect({
                "type": FX.TX.Transact,
                "target_graph": g,
                "commands": commands_with_ids,
                "unpacking_template": unpacking_template,
            })
    raise NotImplementedError(f"'x | g' for x of type {type(x)}")


from ..pyzef import main
main.Graph.__ror__ = dispatch_ror_graph




##############################
# * GraphDelta construction
#----------------------------
def construct_commands(elements) -> list:
    generate_id = make_generate_id()    # keeps an internal counter        

    # First extract any nested GraphDeltas commands out
    # Note: using native python filtering as the evaluation engine is too slow
    
    # TODO: These were commented out from old GraphDelta behavior. Do we still need to account for it?
    # nested_cmds = elements | filter[is_a[GraphDelta]] | map[lambda x: x.commands] | concat | collect
    # nested_cmds = [x for x in elements if type(x) == GraphDelta] | map[lambda x: x.commands] | concat | collect
    # elements = elements | filter[Not[is_a[GraphDelta]]] | collect
    # elements = [x for x in elements if not type(x) == GraphDelta]

    # The nested cmds need to be readjusted for their auto-generated ids 
    # I want to redo this as pure functional style but will do it with side effects for now
    mapping_dict = {}
    def update_ids(cmd, mapping_dict):
        new_cmd = dict(**cmd)
        if "internal_id" in cmd:
            if is_generated_id(cmd["internal_id"]):
                if cmd["internal_id"] not in mapping_dict:
                    mapping_dict[cmd["internal_id"]] = generate_id()
                new_cmd["internal_id"] = mapping_dict[cmd["internal_id"]]
        for key in ["source", "target"]:
            if key in cmd:
                if cmd[key] in mapping_dict:
                    new_cmd[key] = mapping_dict[cmd[key]]
        return new_cmd

    # nested_cmds = nested_cmds | map[P(update_ids, mapping_dict=mapping_dict)] | collect
    
    # handle any dict if found
    # elements = elements | map[handle_dict | first] | concat | collect
    # Obtain all user IDs
    id_definitions = elements | map[obtain_ids] | reduce[merge_no_overwrite][{}] | collect
    # Check that nothing needs a user ID that wasn't provided
    elements | for_each[verify_input_el[id_definitions]]
    elements | for_each[verify_relation_source_target[id_definitions]]

    state_initial = {
        'user_expressions': tuple(elements),
        'commands': [],
    }

    def make_debug_output():
        num_done = 0
        next_print = now()+5*seconds
        def debug_output(x):
            nonlocal num_done, next_print
            num_done += 1
            if now() > next_print:
                print(f"Up to iteration {num_done} - {len(x['user_expressions'])}, {len(x['commands'])}", now())
                next_print = now() + 5*seconds
        return debug_output
        
    # iterate until all user expressions and generated expressions have become commands
    state_final = (state_initial
                   | iterate[iteration_step[generate_id]]
                   # | map[tap[make_debug_output()]]
                   | take_until[lambda s: len(s['user_expressions'])==0]
                   | last
                   | collect
                   )
    return verify_and_compact_commands(state_final['commands'])



##############################
# * Validation
#----------------------------

def ensure_not_previously_defined(internal_id: str):
    if internal_id in id_definitions:
        raise KeyError(f"The internal id '{internal_id}' was already defined. Multiple definitions of an internal id are not allowed.")


@func
def obtain_ids(x) -> dict:
    # Return all user IDs for this part of the GraphDelta input.

    # The output of this func (and anytime it is used recursively) must go
    # through merge_no_overwrite to avoid ID clashes.

    ids = {}
    if isinstance(x, LazyValue):
        ids = obtain_ids(x.initial_val)
    elif is_a(x, Delegate):
        ids = {x: x}

    # this must be a triple, defining a relation
    elif isinstance(x, ListOrTuple):
        ids = x | map[obtain_ids] | reduce[merge_no_overwrite][{}] | collect

    elif isinstance(x, dict):
        entity, sub_d = list(x.items()) | single | collect # Will throw if there isn't a single key in the dict

        ids = obtain_ids(entity)
        for k,v in sub_d.items():
            ids = merge_no_overwrite(ids, obtain_ids(k))
            ids = merge_no_overwrite(ids, obtain_ids(v))

    elif type(x) in [Entity, AtomicEntity, Relation]:
        ids = {uid(x): x}
        if type(x) == Relation:
            if not is_a(x.d["type"][0], RT):
                ids = merge_no_overwrite(ids, obtain_ids(source(x)))
            if not is_a(x.d["type"][2], RT):
                ids = merge_no_overwrite(ids, obtain_ids(target(x)))

    elif type(x) in [ZefRef, EZefRef]:
        ids = {origin_uid(x): x}


    # This is an extra step on top of the previous checks
    if type(x) in [Entity, AtomicEntity, Relation, EntityType,
                    AtomicEntityType, RelationType, ZefRef, EZefRef]:
        # Need the lazy value for the RT possibility
        a_id = get_absorbed_id(LazyValue(x))
        if a_id is not None:
            ids = merge_no_overwrite(ids, {a_id: x})
    return ids


@func    
def verify_input_el(x, id_definitions, allow_rt=False, allow_scalar=False):
    # Check each input item to a GraphDelta to validate its form. Uses
    # id_definitions to assert that references to IDs are to things that are
    # defined in the GraphDelta.

    if isinstance(x, LazyValue):
        # This is checking for an (x <= value) format
        if is_a(x.el_ops, assign_value):
            return
        elif is_a(x.el_ops, terminate):
            return
        elif is_a(x.el_ops, tag):
            return
        raise Exception(f"A LazyValue must have come from (x | assign_value), (x | terminate) or (x | tag) only. Got {x}")

    # Note: just is_a(x, Z) will also mean ZefRefs will be hit
    elif is_a(x, ZefOp) and is_a(x, Z):
        some_id = get_curried_arg(x, 0)

        if some_id not in id_definitions:
            raise KeyError(f"The id '{some_id}' used in Z is not an internal id defined in the GraphDelta init list")
        return

    elif isinstance(x, ZefRef) or isinstance(x, EZefRef):
        return

    elif isinstance(x, dict):
        entity, sub_d = list(x.items()) | single | collect # Will throw if there isn't a single key in the dict
        verify_input_el(entity, id_definitions, False, False)

        for k,v in sub_d.items():
            verify_input_el(k, id_definitions, True, False)
            verify_input_el(v, id_definitions, False, True)

        return

    elif isinstance(x, ListOrTuple):                         
        if is_valid_relation_template(x):
            if len(x) == 3:
                verify_input_el(x[0], id_definitions, False, True)
                verify_input_el(x[1], id_definitions, True, False)
                verify_input_el(x[2], id_definitions, False, True)
                return
            else:
                return
        for item in x:
            verify_input_el(item, id_definitions, False, False)
        return
    elif isinstance(x, RelationType):
        if allow_rt:
            return
        else:
            raise ValueError(f"Bare RTs without source or target cannot be initialized. You tried to create a {x}.")                    
    elif type(x) in scalar_types:
        if not allow_scalar:
            raise Exception("Direct values are not allowed at the top level of a GraphDelta, as this is likely to indicate a typo, e.g. (ET.Machine, ET.ShouldBeRTNotET, 'name'). Please use explicit value assignment via (AET.String <= 'name') if you really want this behaviour.")
        return
    elif is_valid_single_node(x):
        return

    elif isinstance(x, ZefOp):
        if len(x) > 1:
            # This could be something like Z["asdf"] | assign_value[5]
            if LazyValue(x) | first | is_a[Z] | collect:
                return
        if length(LazyValue(x) | absorbed) != 1:
            raise Exception(f"ZefOp has more than one op inside of it. {x}")
        raise Exception(f"Not allowing ZefOps except for those starting with Z. Got {x}")

    elif is_a(x, Delegate):
        return

    elif type(x) in [Entity, AtomicEntity, Relation]:
        return

    else:
        raise ValueError(f"Unexpected type passed to init list of GraphDelta: {x} of type {type(x)}")

@func    
def verify_relation_source_target(x, id_definitions):
    if type(x) == Relation:
        assert all(u in id_definitions for u in x.d["uids"]), "A Relation doesn't have its corresponding source or target included in the GraphDelta. This is likely because you have an abstract Relation with another Relation as its source/target. These must be included explicitly into the GraphDelta."




##################################################
# * Expr -> cmd conversion
#------------------------------------------------

@func
def iteration_step(state: dict, gen_id)->dict:
    """[summary]

    Args:
        state (dict): of form 
        {
            'user_expressions': elements,
            'commands': (),
        }

    Returns:
        dict: [description]
    """

    # Note: we used to keep track of ids seen here to not merge the same thing
    # twice. However, the user can potentially give two different internal ids
    # to the same merged object, so we can't follow that logic anymore. Instead,
    # the nearly-identical duplicate merges will be contracted later on in the
    # compact commands function.

    exprs = state['user_expressions']
    if len(exprs) == 0:
        return state
    # expr = exprs[0]

    # WARNING: this mutates but is necessary for speed
    if not isinstance(exprs, list):
        exprs = list(exprs)
    expr = exprs.pop()

    new_exprs,new_cmds = dispatch_cmds_for(expr, gen_id)

    # Here are three ways to accomplish the same thing, although the last
    # (and fastest) mutates.

    # return {
    #         'user_expressions': (*exprs[1:], *new_exprs),
    #         'commands': (*state['commands'], *new_cmds),                 # for now this will be a dict to allow quick lookup: the key is the uid / internal id
    #         'ids_present': {*state['ids_present'], *new_ids}
    #     }

    # return {
    #         'user_expressions': exprs[1:] + list(new_exprs),
    #         'commands': state['commands'] + list(new_cmds),
    #         'ids_present': state['ids_present'] | set(new_ids)
    #     }


    exprs.extend(new_exprs)
    cmds = state["commands"]
    cmds.extend(new_cmds)
    return {
            'user_expressions': exprs,
            'commands': cmds,
    }

def dispatch_cmds_for(expr, gen_id):
    if isinstance(expr, LazyValue):
        expr = collect(expr)

    # If we have a lazy value the second time around, then this must be an actual action to perform.
    if isinstance(expr, LazyValue):
        if is_a(expr.el_ops, assign_value):
            return cmds_for_lv_assign_value(expr, gen_id)
        elif is_a(expr.el_ops, terminate):
            return cmds_for_lv_terminate(expr)
        elif is_a(expr.el_ops, tag):
            return cmds_for_lv_tag(expr, gen_id)
        else:
            raise Exception("LazyValue obtained which is not an assign_value or terminate")

    elif isinstance(expr, ZefOp):
        # If we have a chain beginning with a Z, convert to LazyValue
        if LazyValue(expr) | first | is_a[Z] | collect:
            return cmds_for_initial_Z(expr)

        raise RuntimeError(f'We should not have landed here, with expr={expr}')
    if is_a(expr, Delegate):
        return cmds_for_delegate(expr)

    d_dispatch = {
        EntityType: cmds_for_instantiable,
        AtomicEntityType: cmds_for_instantiable,

        ZefRef: cmds_for_mergable,
        EZefRef: cmds_for_mergable,
        Entity: cmds_for_mergable,
        AtomicEntity: cmds_for_mergable,
        Relation: cmds_for_mergable,
        TXNode: cmds_for_mergable,
        Root: cmds_for_mergable,

        tuple: P(cmds_for_tuple, gen_id=gen_id),
        list: P(cmds_for_tuple, gen_id=gen_id),

        dict: P(cmds_for_complex_expr, gen_id=gen_id),
    }
    if type(expr) not in d_dispatch:
        raise TypeError(f"transform_to_commands was called for type {type(expr)} value={expr}, but no handler in dispatch dict")            
    func = d_dispatch[type(expr)]

    return func(expr)



##################################################
# * Handlers for expr->cmd
#------------------------------------------------

def cmds_for_complex_expr(x, gen_id):
    iid,exprs = realise_single_node(x, gen_id)
    return exprs, []

def cmds_for_initial_Z(expr):
    assert LazyValue(expr) | first | is_a[Z] | collect

    expr = LazyValue(LazyValue(expr) | first | collect) | (LazyValue(expr) | skip[1] | as_pipeline | collect)
    return (expr,), ()


def cmds_for_instantiable(x):
    a_id = get_absorbed_id(x)
    raet = x | without_absorbed | collect
    cmd = {'cmd': 'instantiate', 'rae_type': raet}
    if a_id is not None:
        cmd['internal_id'] =  a_id
        
    return (), [cmd]

def cmds_for_mergable(x):
    origin = origin_rae(x)

    cmd = {"cmd": "merge",
           "origin_rae": origin,
           "internal_ids": []}

    a_id = get_absorbed_id(x)
    if a_id is not None:
        cmd['internal_ids'] += [a_id]

    if is_a(x, ZefRef) or is_a(x, EZefRef):
        if BT(x) in {BT.ENTITY_NODE, BT.TX_EVENT_NODE, BT.ROOT_NODE}:
            return (), [cmd]

        elif BT(x) == BT.ATOMIC_ENTITY_NODE:
            assign_val_cmds = [] if isinstance(x, EZefRef) else [{
                'cmd': 'assign_value', 
                'value': x | value | collect,
                'internal_id': uid(origin),
                'explicit': False,
                }]

            return (), [cmd] + assign_val_cmds

        elif BT(x) == BT.RELATION_EDGE:
            return (
                (x | source | to_ezefref | collect, x | target | to_ezefref | collect),
                [cmd],
            )
        else:
            raise NotImplementedError(f"Unknown ZefRef type for merging: {BT(x)}")
    elif is_a(x, Entity) or is_a(x, AtomicEntity):
        return (), [cmd]
    elif is_a(x, Relation):
        maybe_src_trg = []
        if not is_a(x.d["type"][0], RT):
            maybe_src_trg.append(source(x))
        if not is_a(x.d["type"][2], RT):
            maybe_src_trg.append(target(x))

        return (
            tuple(maybe_src_trg),
            [cmd],
        )
    else:
        raise NotImplementedError(f"Unknown type for mergable: {type(x)}")

def cmds_for_lv_tag(x, gen_id):
    assert isinstance(x, LazyValue)

    assert len(x | peel | collect) == 2
    target,op = x | peel | collect

    assert is_a(op, tag)
    tag_s = LazyValue(op) | absorbed | single | collect
    iid,exprs = realise_single_node(target, gen_id)
    cmd = {'cmd': 'tag',
           'tag_name': tag_s,
           'internal_id': iid}
    return exprs, [cmd]
    

def cmds_for_lv_assign_value(x, gen_id: Callable):
    assert isinstance(x, LazyValue)

    assert len(x | peel | collect) == 2
    target,op = x | peel | collect

    assert is_a(op, assign_value)
    val = LazyValue(op) | absorbed | single | collect
    iid,exprs = realise_single_node(target, gen_id)

    return exprs, [{'cmd': 'assign_value', 'value': val, 'explicit': True, "internal_id": iid}]

def cmds_for_lv_terminate(x):
    assert isinstance(x, LazyValue)

    assert len(x | peel | collect) == 2
    target,op = x | peel | collect

    assert is_a(op, terminate)
    cmd = {
        'cmd': 'terminate', 
        'origin_rae': origin_rae(target)
        }
    a_id = get_absorbed_id(LazyValue(op))
    if a_id is not None:
        cmd['internal_id'] = a_id

    return (), [cmd]

def cmds_for_delegate(x):                            
    internal_ids = []
    a_id = get_absorbed_id(x)
    if a_id is not None:
        internal_ids += [a_id]

    return (), [{
        'cmd': 'merge', 
        'origin_rae': x,
        'internal_ids': internal_ids,
        }]



def cmds_for_tuple(x: tuple, gen_id: Callable):
    new_exprs = []
    new_cmds = []

    if not is_valid_relation_template(x):
        raise Exception("Not allowing list/tuple nested in GraphDelta unless it belongs to a relation")

    if len(x) == 3:
        if isinstance(x[0], ListOrTuple) and isinstance(x[2], ListOrTuple):
            # Case 4 of is_valid_relation_template
            new_exprs += [(source, x[1], target) for source in x[0] for target in x[2]]
        elif isinstance(x[0], ListOrTuple):
            # Case 3 of is_valid_relation_template
            iid,exprs = realise_single_node(x[2], gen_id)
            new_exprs += exprs
            new_exprs += [(source, x[1], Z[iid]) for source in x[0]]
        elif isinstance(x[2], list):
            # Case 2 of is_valid_relation_template
            iid,exprs = realise_single_node(x[0], gen_id)
            new_exprs += exprs
            new_exprs += [(Z[iid], x[1], target) for target in x[2]]
        else:
            # Case 1 of is_valid_relation_template
            # it's a plain relation triple
            src_id, src_exprs = realise_single_node(x[0], gen_id)
            trg_id, trg_exprs = realise_single_node(x[2], gen_id)

            new_exprs += src_exprs
            new_exprs += trg_exprs

            # This is the only time the relation is created
            rel = x[1]
            if isinstance(rel, RelationType):
                a_id = get_absorbed_id(rel)
                raet = LazyValue(rel) | without_absorbed | collect
                cmd1 = {'cmd': 'instantiate', 'rae_type': raet}
                if a_id is not None:
                    cmd1['internal_id'] =  a_id
            elif isinstance(rel, ZefOp):
                cmd1 = {'cmd': 'instantiate',
                        'rae_type': rel.el_ops[0][1][0],
                        'internal_id': rel.el_ops[0][1][1]}
            cmd1['source'] = src_id
            cmd1['target'] = trg_id

            new_cmds += [cmd1]

    elif len(x) == 2:
        # Case 5 of is_valid_relation_template
        iid,exprs = realise_single_node(x[0], gen_id)
        new_exprs += exprs
        new_exprs += [(Z[iid], item[0], item[1]) for item in x[1]]
    else:
        raise Exception("Shouldn't get here")

    return new_exprs, new_cmds





##############################################################
# ** Complex->primitve expr utils
#------------------------------------------------------------

def is_valid_single_node(x):
    if type(x) in scalar_types:
        return True
    if isinstance(x, ZefRef) or isinstance(x, EZefRef):
        return True
    if is_a(x, Z):
        return True
    if isinstance(x, EntityType):
        return True
    if isinstance(x, RelationType):
        return True
    if isinstance(x, AtomicEntityType):
        return True
    if isinstance(x, Delegate):
        return True
    return False

def is_valid_relation_template(x):
    # Check for the relation templates:
    # Either:
    # 1. (a, RT.x, b)
    # 2. (a, RT.x, [b,c,d,e,...])
    # 3. ([a,b,c,...], RT.x, d)
    # 4. ([a,b,c,...], RT.x, [d,e,f,...])
    # 5. (a, [(RT.x, b), (RT.y, c)])
    # Note: tuple/list can be used interchangably
    # Note: 5. deliberately does not have its symmetric counterpart. This could be introduced later.
    if any(isinstance(item, RelationType) for item in x):
        # Cases 1-4 above
        if len(x) != 3:
            raise Exception(f"A list has an RT but isn't 3 elements long: {x}")
        if not isinstance(x[1], RelationType):
            raise Exception(f"A list has an RT but it isn't in the second position: {x}")
        # Note: if there are any lists involved, we cannot have a given ID as it
        # would have to be given to multiple instantiated relations.
        if not is_a(x[1], RT) and (isinstance(x[0], ListOrTuple) or isinstance(x[2], ListOrTuple)):
            raise Exception("An RT with an internal name is not allowed with multiple sources or targets")
        return True
    elif len(x) == 2 and isinstance(x[1], ListOrTuple):
        return all(isinstance(item, ListOrTuple) and len(item) == 2 and isinstance(item[0], RelationType) and is_valid_single_node(item[1]) for item in x[1])
    return False


@func
def realise_single_node(x, gen_id):
    # Take something that should refer to a single node, i.e. a RAE or a scalar
    # to be turned into a RAE, or a reference to a RAE, and return a version
    # with an explicit ID and the ID itself.
    #
    # For example
    #
    # realise_single_node(ET.Machine["a"])
    # will return
    # ("a", ET.Machine["a"])
    #
    # realise_single_node(ET.Machine)
    # might return
    # ("12323425346", ET.Machine["12323425346"])


    #     5 different cases we need to deal with:
    # 4) A ZefRef / EZefRef to an existing RAE is specified. Generate the merge command if it is not present yet
    # 5) Z['my_temp_id4']       The temp id may not exist yet (could come from another edge creation). Make sure it is checked to exist at the end
    # 3) e.g. if a plain value was specified, create an AE and assign the value
    # 1) e.g. if ET.Dog is specified as the source: definitely create it
    # 2) e.g. if ET.Dog['Rufus'] was specified, that will be there as a dict. It is the command to create it and register the temp id

    # First case of removing lazy values
    if isinstance(x, LazyValue):
        x = collect(x)

    # Now this is a check for whether we are an assign_value
    if isinstance(x, LazyValue):
        target,op = x | peel | collect
        if is_a(op, assign_value):
            iid,exprs = realise_single_node(target, gen_id)
            exprs = exprs + [LazyValue(Z[iid]) | op]
        elif is_a(op, terminate):
            iid = origin_uid(target)
            exprs = [x]
        elif is_a(op, tag):
            iid = origin_uid(target)
            exprs = [x]
        else:
            raise Exception(f"Don't understand LazyValue type: {op}")
    elif isinstance(x, EntityType) or isinstance(x, AtomicEntityType):
        a_id = get_absorbed_id(x)
        if a_id is None:
            iid = gen_id()
            exprs = [x[iid]]
        else:
            iid = a_id
            exprs = [x]
    elif isinstance(x, ZefRef) or isinstance(x, EZefRef):
        if internals.is_delegate(x):
            d = to_delegate(x)
            exprs = [d]
            iid = d
        else:
            exprs = [x]
            iid = origin_uid(x)
    elif type(x) in [Entity, AtomicEntity, Relation, TXNode, Root]:
        exprs = [x]
        iid = origin_uid(x)
    elif type(x) in scalar_types:
        iid = gen_id()
        aet = map_scalar_to_aet_type[type(x)](x)
        exprs = [aet[iid], LazyValue(Z[iid]) | assign_value[x]]
    elif isinstance(x, ZefOp):
        assert len(x) == 1
        params = LazyValue(x) | peel | first | second
        if is_a(x, Z):
            iid = params | first | collect
            # No expr to perform
            exprs = []
        else:
            raise NotImplementedError(f"Can't pass zefops to GraphDelta: for {x}")
    elif is_a(x, Delegate):
        iid = x
        exprs = [x]
    elif type(x) == dict:
        entity, sub_d = list(x.items()) | single | collect # Will throw if there isn't a single key in the dict
        iid, entity_exprs = realise_single_node(entity, gen_id)

        exprs = entity_exprs
        for k,v in sub_d.items():
            target_iid, target_exprs = realise_single_node(v, gen_id)
            exprs.extend(target_exprs)
            exprs.append((Z[iid], k, Z[target_iid]))
    else:
        raise TypeError(f'in GraphDelta encode step: for type(x)={type(x)}')

    return iid, exprs



##############################
# * Ordering commands
#----------------------------

def verify_and_compact_commands(cmds: tuple):                
    aes_with_explicit_assigns = (cmds
                                 | filter[lambda d: d["cmd"] == "assign_value"]
                                 | filter[lambda d: d["explicit"]]
                                 | map[lambda d: d["internal_id"]]
                                 | collect
                                 )
    def is_unnecessary_automatic_merge_assign(d):
        return (d["cmd"] == "assign_value"
                and not d["explicit"]
                and d["internal_id"] in aes_with_explicit_assigns)

    # make sure if multiple assignment commands exist for the same AE, that their values agree
    cmds = (cmds | group_by[get['cmd']]
            | map[match_apply[
                        (first | equals["assign_value"],
                         second | filter[Not[is_unnecessary_automatic_merge_assign]]
                         | validate_and_compress_unique_assignment),
                        (first | equals["merge"], second | combine_internal_ids_for_merges),
                        (first | equals["terminate"], second | combine_terminates),
                        # Just pack things back in for other cmd types
                        (always[True], second)
            ]]
            | concat
            | collect)

    sorted_cmds = tuple(cmds 
                        | sort[command_ordering_by_type] 
                        | collect
                        )

    def make_debug_output():
        num_done = 0
        next_print = now()+5*seconds
        def debug_output(x):
            nonlocal num_done, next_print
            num_done += 1
            if now() > next_print:
                print(f"Up to compacting {num_done} - {len(x['state']['input'])}, {len(x['state']['output'])}", now())
                next_print = now() + 5*seconds
        return debug_output

    state_initial = {
        'input': sorted_cmds,
        'output': (),
        'known_ids': set(),
    }
    state_final = (
        {"state": state_initial, "num_changed": -1}
        | iterate[resolve_dag_ordering_step] 
        # | tap[make_debug_output()]
        | take_until[lambda s: s["num_changed"] == 0]
        # | tap[lambda x: print("After take_until")]
        | last
        | get["state"]
        | collect
    )
    # print("Done state_final", now())
    if len(state_final['input']) > 0:
        import json
        print(json.dumps(state_final, indent=4, default=repr))
        raise NotImplementedError("Error constructing GraphDelta: instantiation order iteration did not converge. Probably there is a circular dependency in the required imperative instantiation order between inter-dependent relations. This is a valid GraphDelta in principle, but currently not implemented in zefDB.")
    ordered_cmds = state_final['output']
    return ordered_cmds

@func
def validate_and_compress_unique_assignment(cmds):
    @func
    def check_list_is_distinct(cmds):
        values = cmds | map[get["value"]] | distinct | collect
        if length(values) == 1:
            return cmds[0]
        raise ValueError(f'There may be at most one assignment commands for each AE. There were multiple for assignment to {get_id(cmds[0])!r} with values {values}')

    return (cmds
            | group_by[get_id]
            | map[second | check_list_is_distinct])
            

@func
def combine_internal_ids_for_merges(cmds):
    def combine_ids(x, y):
        assert x["origin_rae"] == y["origin_rae"]
        return {**x,
                "internal_ids": x["internal_ids"] + y["internal_ids"]}
    
    return (cmds
              # Convert each item to a dictionary using the origin_rae as key
            | map[lambda x: {x["origin_rae"]: x}]
              | reduce[merge_with[combine_ids]][{}]
              # Now we should have one big dict with each command
            | values)

@func
def combine_terminates(cmds):
    @func
    def check_list_is_distinct(cmds):
        # cmds = cmds | distinct | collect
        # if length(cmds) == 1:
        #     return cmds[0]
        if not cmds | map[equals[first(cmds)]] | all | collect:
            ids = cmds | map[get["internal_id"][None]] | collect
            raise ValueError(f'There may be at most one id for a terminated ZefRef. There were multiple for assignment to {cmds[0]["origin_rae"]!r} with ids {ids}')
        return cmds[0]

    return (cmds
            | group_by[get["origin_rae"]]
            | map[second | check_list_is_distinct])



def command_ordering_by_type(d_raes: dict) -> int:
    """we want some standardized order of the output to simplify value-based
        comparisons and other operations for graph deltas"""
        
    if d_raes['cmd'] == 'merge':
        if isinstance(d_raes['origin_rae'], Relation): return 0.5
        else: return 0
    if d_raes['cmd'] == 'instantiate':
        if isinstance(d_raes['rae_type'], EntityType): return 1
        if isinstance(d_raes['rae_type'], AtomicEntityType): return 2
        if isinstance(d_raes['rae_type'], RelationType): return 3
        return 4                                            # there may be {'cmd': 'instantiate', 'rae_type': AET.Bool}
    if d_raes['cmd'] == 'assign_value': return 5
    if d_raes['cmd'] == 'terminate' and isinstance(d_raes['origin_rae'], Relation): return 6
    if d_raes['cmd'] == 'terminate' and isinstance(d_raes['origin_rae'], Entity): return 7
    if d_raes['cmd'] == 'terminate' and isinstance(d_raes['origin_rae'], AtomicEntity): return 8
    if d_raes['cmd'] == 'terminate' and is_a(d_raes['origin_rae'], Delegate): return 9
    if d_raes['cmd'] == 'tag': return 10
    else: raise NotImplementedError(f"In Sort fct for {d_raes}")

def get_id(cmd):
    if "internal_ids" in cmd:
        raise Exception("Not allowed to ask for id of command that could have multiple ids")
    elif 'internal_id' in cmd:
        return cmd['internal_id']
    elif 'origin_rae' in cmd:
        if is_a(cmd['origin_rae'], Delegate):
            return cmd['origin_rae']
        return uid(cmd['origin_rae'])
    else:
        return None

def get_ids(cmd):
    ids = []
    if cmd['cmd'] == 'merge':
        ids += cmd["internal_ids"]
        if is_a(cmd['origin_rae'], Delegate):
            ids += [cmd['origin_rae']]
        else:
            ids += [uid(cmd['origin_rae'])]
    else:
        this_id = get_id(cmd)
        if this_id is not None:
            ids += [this_id]
    return ids

def resolve_dag_ordering_step(arg: dict)->dict:
    # arg is "state" + "num_changed"
    state = arg["state"]
    ids = state['known_ids']        
    
    def can_be_executed(cmd):        
        if cmd['cmd'] == 'instantiate' and isinstance(cmd['rae_type'], RelationType) and (cmd['source'] not in ids or cmd['target'] not in ids):
            return False
        if cmd['cmd'] == 'merge' and isinstance(cmd['origin_rae'], Relation) and (cmd['origin_rae'].d["uids"][0] not in ids or cmd['origin_rae'].d["uids"][2] not in ids):
            return False
        if cmd['cmd'] == 'terminate' and any(get_id(cmd) in get_ids(other) for other in state['input'] if other['cmd'] != 'terminate'):
            return False
        return True        
    (_,can), (_,cannot) = state['input'] | group_by[can_be_executed][[True, False]] | collect
    return {
        "state": {
            'input': tuple(cannot),
            'output': (*state['output'], *can),
            'known_ids': {*state['known_ids'], *(can | map[get_ids] | concat)},
        },
        "num_changed": len(can) > 0
    }
    
        

########################################################
# * Shorthand syntax handlers
#------------------------------------------------------



def encode(xx):
    """
    This function is invoked if one writes "[ET.Dog, (ET.Person, ET.Aardvark)] | g"
    It returns a GraphDelta with ids assigned, as well as a schema
    with the same structure as the nested input arrays, but the ids as placeholders

    Args:
        xx ([type]): [description]

    Returns:
        [type]: Tuple, 
    """
    gd_exprs = []
    
    gen_id = make_generate_id()

    # - encode works recursively
    # - step handles each recursive step
    #
    # logic:
    # - if step gets a valid relation - lots of logic to extract correct structure
    # - else if step gets a list/tuple - recurse on each item
    # - else must be a single item - use realise_single_node
    #
    # - plus some helpful error handling
    #
    # - note: heavy lifting is passed to GraphDelta constructor - this function just adds IDs, nothing else
    

    def step(x, allow_scalar):
        if isinstance(x, ListOrTuple):
            if is_valid_relation_template(x):
                if len(x) == 2:
                    ent = step(x[0], True)
                    triples = [step((Z[ent], *tup), False) for tup in x[1]]
                    doubles = [x[1:] for x in triples]

                    return (ent, doubles)

                if len(x) == 3:
                    if isinstance(x[0], ListOrTuple):
                        assert is_valid_single_node(x[2])
                        item = step(x[2], True)
                        triples = [step((source, x[1], Z[item]), False) for source in x[0]]
                        sources = [x[0] for x in triples]
                        # Note: have to return None for the relation, as there are actually multiple relations
                        return (sources, None, item)

                    if isinstance(x[2], ListOrTuple):
                        assert is_valid_single_node(x[0])
                        item = step(x[0], True)
                        triples = [step((Z[item], x[1], target), False) for target in x[2]]
                        targets = [x[2] for x in triples]
                        # Note: have to return None for the relation, as there are actually multiple relations
                        return (item, None, targets)

                    # it's a relation triple
                    src_id = step(x[0], True)
                    if get_absorbed_id(x[1]) is not None:
                        raise Exception("Not allowing ids in a shorthand syntax transaction.")
                    rel_id = gen_id()
                    trg_id = step(x[2], True)
                    gd_exprs.append( (Z[src_id], x[1][rel_id], Z[trg_id]) )

                    return (src_id, rel_id, trg_id)
                raise Exception("Shouldn't get here")
            else:
                # recurse: return isomorphic structure with IDs
                return tuple((step(el, False) for el in x))
        
        # These next few ifs are for checks on syntax only
        if type(x) in scalar_types:
            if not allow_scalar:
                raise Exception("Scalars are not allowed on their own to avoid accidental typos such as (ET.X, ET.Y, 'z') when (ET.X, RT.Y, 'z') is meant. If you want this behaviour, then create an explicit AET, i.e. (AET.String <= 'z').")
        
        iid,exprs = realise_single_node(x, gen_id)
        gd_exprs.extend(exprs)
        return iid

    step_res = step(xx, False)
    return step_res, construct_commands(gd_exprs)


def unpack_receipt(unpacking_template, receipt: dict):
    def step(x):
        if isinstance(x, tuple):
            return tuple((step(el) for el in x))
        if isinstance(x, list):
            return [step(el) for el in x]
        if isinstance(x, dict):
            return {k: step(v) for k,v in x.items()}
        return receipt[x] if isinstance(x, str) or is_a(x, uid) or is_a(x, Delegate) else x
    return step(unpacking_template)



##################################################
# * Performing transaction
#------------------------------------------------

def perform_transaction_commands(commands: list, g: Graph):
    d_raes = {}  # keep track of instantiated RAEs with temp ids            
    try:
        with Transaction(g) as tx_now:
            # TODO: Have to change the behavior of Transaction(g) later I suspect
            frame_now = GraphSlice(tx_now)
            d_raes['tx'] = tx_now
            for i,cmd in enumerate(commands):

                zz = None
                
                # print(f"{i}/{len(g_delta.commands)}: {g.graph_data.write_head * 16 / 1024 / 1024} MB")
                if cmd['cmd'] == 'instantiate' and type(cmd['rae_type']) in {EntityType, AtomicEntityType}:
                    zz = instantiate(cmd['rae_type'], g)
                
                elif cmd['cmd'] == 'instantiate' and type(cmd['rae_type']) in {RelationType}:
                    zz = instantiate(to_ezefref(d_raes[cmd['source']]), cmd['rae_type'], to_ezefref(d_raes[cmd['target']]), g) | in_frame[frame_now] | collect
                
                elif cmd['cmd'] == 'assign_value':
                    this_id = cmd['internal_id']
                    zz = d_raes.get(this_id, None)
                    if zz is None:
                        zz = most_recent_rae_on_graph(this_id, g)
                    assert zz is not None
                    if cmd['value'] is not  None:
                        if zz | value | collect != cmd['value']:
                            # print("Assigning value of ", cmd['value'], "to a", AET(z))
                            internals.assign_value_imp(zz, cmd['value'])
                    zz = now(zz)
                    
                elif cmd['cmd'] == 'merge':
                    # It is either an instance (with an 'origin_rae_uid' specified)
                    # or a delegate[...]
                    if is_a(cmd['origin_rae'], Delegate):
                        d = cmd['origin_rae']
                        zz = internals.delegate_to_ezr(d, g, True, 0)
                        zz = now(zz)
                    else:
                        candidate = most_recent_rae_on_graph(uid(cmd['origin_rae']), g)
                        if candidate is not None:
                            # this is already on the graph. Just assert and move on
                            assert abstract_type(cmd['origin_rae']) == abstract_type(candidate)
                            zz = candidate
                        else:
                            origin_rae_uid = uid(cmd['origin_rae'])
                            if isinstance(cmd['origin_rae'], Entity):
                                zz = internals.merge_entity_(
                                    g, 
                                    rae_type(cmd['origin_rae']), 
                                    origin_rae_uid.blob_uid,
                                    origin_rae_uid.graph_uid,
                                )
                            elif isinstance(cmd['origin_rae'], AtomicEntity):
                                zz = internals.merge_atomic_entity_(
                                    g, 
                                    rae_type(cmd['origin_rae']),
                                    origin_rae_uid.blob_uid,
                                    origin_rae_uid.graph_uid,
                                )
                            elif isinstance(cmd['origin_rae'], Relation):
                                src_origin_uid,_,trg_origin_uid = cmd['origin_rae'].d["uids"]
                                z_src = d_raes.get(src_origin_uid, most_recent_rae_on_graph(src_origin_uid, g))                                    
                                z_trg = d_raes.get(trg_origin_uid, most_recent_rae_on_graph(trg_origin_uid, g))                                    

                                assert z_src is not None
                                assert z_trg is not None                                    
                                zz = internals.merge_relation_(
                                    g, 
                                    rae_type(cmd['origin_rae']),
                                    to_ezefref(z_src),
                                    to_ezefref(z_trg),
                                    origin_rae_uid.blob_uid,
                                    origin_rae_uid.graph_uid,
                                )
                            # Special handling for TXNode and Root currently.
                            elif isinstance(cmd['origin_rae'], (TXNode, Root)):
                                raise Exception("Can only merge TXNode and Root onto the same graph")
                            else:
                                raise NotImplementedError

                        zz = now(zz)
                        if 'origin_rae' in cmd:
                            d_raes[uid(cmd['origin_rae'])] = zz
                
                elif cmd['cmd'] == 'terminate':
                    if uid(cmd['origin_rae']) not in g:
                        # raise KeyError(f"terminate {cmd['origin_rae']} called, but this RAE is not know to graph {uid(g)}")
                        # This is no longer an error, it should be ignored silently.
                        pass
                    else:
                        zz = most_recent_rae_on_graph(uid(cmd['origin_rae']), g)
                        if zz is not None:
                            from ..pyzef import zefops as pyzefops
                            pyzefops.terminate(zz)

                            zz = now(zz, allow_tombstone)
                elif cmd['cmd'] == 'tag':
                    if 'origin_rae' in cmd:
                        if uid(cmd['origin_rae']) not in g:
                            raise KeyError(f"tag {cmd['origin_rae']} called, but this RAE is not know to graph {g|uid}")
                        zz = most_recent_rae_on_graph(uid(cmd['origin_rae']), g)
                    else:
                        zz = d_raes[cmd['internal_id']]
                    # Go directly to the tagging instead of through another effect
                    pyzef.main.tag(zz, cmd['tag_name'], True)
                else:
                    raise RuntimeError(f'---------Unexpected case in performing graph delta tx: {cmd}')

                for this_id in get_ids(cmd):
                    d_raes[this_id] = zz

    except Exception as exc:        
        raise RuntimeError(f"Error executing graph delta transaction exc={exc}") from exc
        
    return d_raes    # the transaction receipt

################################
# * General utils
#------------------------------



scalar_types = {int, float, bool, str, Time, QuantityFloat, QuantityInt, ZefEnumValue}

def make_enum_aet(x):
    """ hacky work around function for now:
    e.g. given an enum value "EN.Color.White"
    we want to convert to the AET type expression: "AET.Enum.Color"
    """
    enum_typename: str = x.enum_type
    return getattr(AET.Enum, enum_typename)

def make_qf_aet(x):
    """ hacky work around function for now:
    e.g. given an enum value "QuantityFloat(2.1, EN.Unit.kilogram)"
    we want to convert to the AET type expression: "AET.QuantityFloat.kilogram"
    """
    quantity_unit: str = x.unit.enum_value
    return getattr(AET.QuantityFloat, quantity_unit)

def make_qi_aet(x):
    """ hacky work around function for now:
    e.g. given an enum value "QuantityInt(2.1, EN.Unit.kilogram)"
    we want to convert to the AET type expression: "AET.QuantityInt.kilogram"
    """
    quantity_unit: str = x.unit.enum_value
    return getattr(AET.QuantityInt, quantity_unit)

map_scalar_to_aet_type = {
    int:                lambda x: AET.Int,
    float:              lambda x: AET.Float,
    bool:               lambda x: AET.Bool,
    str:                lambda x: AET.String,
    Time:               lambda x: AET.Time,
    ZefEnumValue:       make_enum_aet,
    QuantityFloat:      make_qf_aet, 
    QuantityInt:        make_qi_aet, 
    }

def get_curried_arg(op, n):
    """ 
    utility function to get 
    e.g. get_curried_arg(Z['id1'][42], 0)       # => 'id1'
    e.g. get_curried_arg(Z['id1'][42], 1)       # => 42
    """
    # return op.el_ops[arg_indx][1][n]
    return LazyValue(op) | absorbed | nth[n] | collect



def most_recent_rae_on_graph(origin_uid: str, g: Graph)->ZefRef:
    """
    Will definitely not return a BT.ForeignInstance, always an instance.
    It could be that the node asked for has its origin on this graph 
    (the original rae may still be alive or it may be terminated)

    Args:
        origin_uid (str): the uid of the origin rae we are looking for
        g (Graph): on which graph are we looking?

    Returns:
        ZefRef: this graph knows about this: found instance
        None: this graph knows nothing about this RAE
    """
    if origin_uid not in g:
        return None     # this graph never knew about a RAE with this origin uid

    zz = g[origin_uid]
    if BT(zz) in {BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATOMIC_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE}:
        from .graph_slice import get_instance_rae
        return get_instance_rae(origin_uid, now(g))
        
    elif BT(zz) in {BT.ENTITY_NODE, BT.ATOMIC_ENTITY_NODE, BT.RELATION_EDGE}:
        if zz | exists_at[now(g)] | collect:
            return zz | in_frame[now(g)] | collect
        else:
            return None
    elif BT(zz) in {BT.ROOT_NODE, BT.TX_EVENT_NODE}:
        return zz | in_frame[now(g)] | collect
    else:
        raise RuntimeError("Unexpected option in most_recent_rae_on_graph")
        



# Generation of IDs and being able to detect whether an ID was auto-generated.

generated_prefix = "tmp_id_"
def make_generate_id():
    auto_id_counter = 0
    def generate_id():
        """count up"""
        nonlocal auto_id_counter
        auto_id_counter += 1
        return f"{generated_prefix}{auto_id_counter}"
    return generate_id

def is_generated_id(x):
    if not isinstance(x, str):
        return False
    return x.startswith(generated_prefix)

def filter_temporary_ids(receipt):
    return (receipt
            | items
            | filter[first | Not[is_generated_id]]
            | func[dict]
            | collect)



@func
def single_or(itr, default):
    itr = iter(itr)
    try:
        ret = next(itr)
        try:
            next(itr)
            raise Exception("single_or detected more than one item in iterator")
        except StopIteration:
            return ret
    except StopIteration:
        return default

@func
def get_absorbed_id(obj):
    # THIS SHOULDN"T BE NEEDED! FIX!
    if is_a(obj, RT) or is_a(obj, ZefOp):
        obj = LazyValue(obj)
    return obj | absorbed | single_or[None] | collect


@func
def merge_no_overwrite(a,b):
    d = {**a}

    for k,v in b.items():
        if k in d and d[k] != v:
            raise Exception("The internal id '{k}' refers to multiple objects, including '{d[k]}' and '{v}'. This is ambiguous and not allowed.")
        d[k] = v
    return d



################################################
# * Old not used anymore?
#----------------------------------------------



def verify_assign_values(assign_v, id_definitions):
    new_value = assign_v['new_value']

    # This function is called from 2 different locations. First inside the AssignValue_ constructor where we are able to cast a ZefRef to its rae_type
    # Secondly, inside the GraphDelta constructor where we check against uid inside the id_definitions dict.
    if "zr_type" in assign_v:
        type_of_assignedto = assign_v['zr_type']
    else:
        if assign_v['some_id'] in id_definitions: type_of_assignedto = id_definitions[assign_v['some_id']]
        else: type_of_assignedto = None

    # This comes first as _eq_ for ZefRef throws in the next if statement
    if isinstance(new_value, ZefRef) or isinstance(new_value, EZefRef) or isinstance(new_value, ZefRefs) or isinstance(new_value, EZefRefs):
        raise RuntimeError(f'The passed value can\'t be of type Zefref(s) or EZefRef(s)')

    # If no value was passed
    if new_value == None:
        raise RuntimeError(f'{assign_v} is missing a value to be assigned.')

    # If type of the value passed isn't part of this set of allowed types
    if type(new_value) in scalar_types:
        raise RuntimeError(f'The type of the value passed in {assign_v} isn\'t of the allowed types: str, int, float, bool, Time, QuantityFloat, QuantityInt, ZefEnumValue')

    if type_of_assignedto: 
        type_map = {
            str:           AET.String,
            ZefEnumValue:  AET.Enum,
            QuantityFloat: AET.QuantityFloat, 
            QuantityInt:   AET.QuantityInt, 
            Time:          AET.Time,
        }
        # If type of the rae that is being assigned isn't of an AET type i.e assigning a str to an ET.Cat
        if type_of_assignedto not in {AET.Int, AET.Float, AET.Bool, AET.String, AET.Enum, AET.QuantityFloat, AET.QuantityInt, AET.Time}:
            raise RuntimeError(f'The type_of_assignedto={type_of_assignedto} must be of the allowed types: AET.Int, AET.Float, AET.Bool, AET.String, AET.Enum, AET.QuantityFloat, AET.QuantityInt, AET.Time')

        # Verify cast-ability
        elif type(new_value) == int:
            if type_of_assignedto not in {AET.Int, AET.Float, AET.Bool}:
                raise RuntimeError(f'Can only assign values of type int to only AET.Int, AET.Float, AET.Bool')
            if type_of_assignedto == AET.Bool and new_value not in [0,1]:
                raise RuntimeError(f'Can\'t assign int value that isn\'t 0 or 1 to AET.Bool')
            
        elif type(new_value) == float:
            if type_of_assignedto not in {AET.Int, AET.Float}:
                raise RuntimeError(f'Can only assign values of type float to only AET.Int, AET.Float')
            import math
            if type_of_assignedto == AET.Int and math.fabs(new_value - round(new_value)) > 1e-8:
                raise RuntimeError(f'Can only assign values of type float to int if the double is numerically sufficiently close to make rounding safe.')

        elif type(new_value) == bool:    
            if type_of_assignedto not in {AET.Int, AET.Bool}:
                raise RuntimeError(f'Can only assign values of type bool to only AET.Int, AET.Bool')
            if type_of_assignedto == AET.Int and new_value not in [False, True]:
                raise RuntimeError(f'Can\'t assign bool value that isn\'t True or False to AET.Int')

        # These are only castable one-to-one
        elif type_map[type(new_value)] != type_of_assignedto:
            raise RuntimeError(f'Can\'t assign value of type {type(new_value)} to an AET of type {type_of_assignedto}')






