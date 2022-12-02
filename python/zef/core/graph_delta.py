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
report_import("zef.core.graph_delta")

__all__ = [
    "GraphDelta",
]
from typing import Tuple, Callable
from functools import partial as P

from .. import pyzef
from ._core import *
from . import internals, VT
from .zef_functions import func
from .VT import *
from ._ops import *
from .op_structs import ZefOp, LazyValue
from .graph_slice import GraphSlice
# from .abstract_raes import Entity, Relation, AttributeEntity, TXNode, Root
from ..pyzef.zefops import SerializedValue
from .logger import log
from .internals import instantiate_value_node_imp

from .VT import make_VT

NamedAny = VT.insert_VT("NamedAny", ValueType & Is[without_absorbed | equals[Any]] & Is[absorbed | length | greater_than[0]])
# Temporary deprecated naming
def delayed_check_namedz(x):
    if not isinstance(x, ZExpression):
        return False
    if x.root_node._entity_type != ET.GetItem:
        return False
    if not x.root_node._kwargs['arg1']== ET.Z:
        return False
    return True
NamedZ = VT.insert_VT("NamedZ", Is[delayed_check_namedz])
# NamedZ = VT.insert_VT("NamedZ", ZExpression & Is[_ops.get_field["root_node"] | _ops.And[_ops.is_a[EntityValueInstance]][_ops.get_field["arg1"] | _ops.equals[ET.Z]]])

PleaseInstantiate = UserValueType("PleaseInstantiate", Dict, Pattern[{"raet": RAET}])
PleaseTerminate = UserValueType("PleaseInstantiate", Dict, Pattern[{
    "target": RAE | ZefOp[Z],
    "internal_id": String | Nil,
}])
PleaseAssign = UserValueType("PleaseAssign",
                              Dict,
                              # Pattern[{"target": AttributeEntity,
                              # Pattern[{"target": Any,
                              Pattern[{"target": AttributeEntity | ZefOp[Z] | NamedAny | NamedZ | AET,
                                       "value": Any}])
PleaseCommand = PleaseInstantiate | PleaseAssign | PleaseTerminate


from abc import ABC
class ListOrTuple(ABC):
    pass
for t in [list, tuple]:
    ListOrTuple.register(t)


import os
gd_timing = "ZEFDB_GRAPH_DELTA_TIMING" in os.environ

##############################
# * Description
#----------------------------

# Glossary:
#
# - commands: dictionaries in the constructed GraphDelta which match directly to low-level changes.
# - expressions: high-level things which match to user expectations. There are "complex" expressions and "primitive" expressions.
#

# 1. Entrypoints:
# There are two entrypoints to the code in this file. Either:
#
# receipt = [... list of expressions ...] | transact[g] | run
# or
# unpacked_template = template | g | run
#
# The first is a "GraphDelta" whereas the second is the shorthand syntax.
#
# The shorthand syntax goes through `encode` which will first assign IDs to any
# parts necessary for unpacking the result into the same template structure. It
# then passes the result to `construct_commands`
#
# The `transact[g]` route goes directly to `construct_commands`
#
# 2. `construct_commands`
# Each expressions given by the user is passed to the cmds_for_* functions to
# convert it to either a) an equivalent command, and/or b) additional expressions.
#
# To make this work more easily, `realise_single_node` can take any expression
# which should result in a "single node" (e.g. an ET.Something, or a
# {ET.Something: ...} syntax) and returns a simplified set of expressions to
# realise this, along with the ID to refer to that "single node"
#
# Pretty much the only object that cannot be passed to `realise_single_node` is
# a relation triple. This is handled in `cmds_for_tuple`.
#
# Many expressions passed in are of a LazyValue type. These are consider the
# same as values, and special forms such as assign, set_field or tag are just
# LazyValues that evaluate to themselves.
#
# 3. Flow
#
# (maybe) `encode`
# `construct_commands`:
#     (pre-construct verification)
#     - `obtain_ids` 
#     - `verify_input_el`
#     - `verify_relation_source_target`
#     (construction)
#     - `iteration_step` -> `dispatch_cmds_for` -> `cmds_for_*` -> `realise_single_node`
#     - `verify_and_compact_commands`
# `perform_transaction_commands`
#
# 4. IDs
#
# The IDs must be consistently unique throughout each GraphDelta. A `gen_id`
# object is passed around the functions and produces ids beginning with
# "tmp_id". This is exploited to filter out automatically-generated IDs in the
# receipt.
#
# IDs are how the individual commands are linked to one another. In the end, any
# command that references an ID must come after the ID which creates that item.
# IDs refer to "internal IDs" which do not exist on the graph after the
# transaction finishes. However they are returned in the receipt, allowing user
# code to identify these objects later.



# WARNING!!!
# WARNING!!!
# WARNING!!!
#
# Much of the logic flow in this file is written in an immutable functional
# style. However, for speed many of these functions have been changed to a
# mutating version. This can lead to confusion when debugging.
#
# WARNING!!!
# WARNING!!!
# WARNING!!!


########################################################
# * Entrypoint from user code
#------------------------------------------------------
def dispatch_ror_graph(g, x):
    from . import Effect, FX, Val
    if isinstance(x, LazyValue):
        x = collect(x)
        # Note that this could produce a new LazyValue if the input was an
        # assign_value. This is fine.

    # Because LazyValue has some special behaviour for warning about incorrect
    # type checking, but we have already handled that above, we must put
    # LazyValue first.
    if isinstance(x, (LazyValue, list, tuple, dict, ZefRef, EZefRef, ZefOp, QuantityFloat, QuantityInt, Entity, AttributeEntity, Relation, Val, PleaseCommand, EntityValueInstance, ET, AET)):
        unpacking_template, commands = encode(x)
        # insert "internal_id" with uid here: the unpacking must get to the RAEs from the receipt
        def insert_id_maybe(cmd: dict):
            if 'internal_id' in cmd or 'internal_ids' in cmd:
                return cmd
            if 'origin_rae' in cmd:
                if is_a(cmd['origin_rae'], Delegate | Val):
                    internal_id = cmd['origin_rae']
                else:
                    internal_id = uid(cmd['origin_rae'])
                return {**cmd, 'internal_id': internal_id}
            return cmd

        commands_with_ids = [insert_id_maybe(c) for  c in commands]
        return {
                "type": FX.Graph.Transact,
                "target_graph": g,
                "commands": commands_with_ids,
                "unpacking_template": unpacking_template,
            }
    raise NotImplementedError(f"'x | g' for x of type {type(x)}")


from ..pyzef import main
main.Graph.__ror__ = dispatch_ror_graph




##############################
# * GraphDelta construction
#----------------------------
def construct_commands(elements, generate_id=None) -> list:
    if generate_id is None:
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
    if gd_timing:
        log.debug("Got id_definitions")
    # Check that nothing needs a user ID that wasn't provided
    elements | for_each[verify_input_el[id_definitions]]
    if gd_timing:
        log.debug("Verified input_el")
    elements | for_each[verify_relation_source_target[id_definitions]]
    if gd_timing:
        log.debug("Verified relation_source_target")

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
                log.debug("Construct: Up to", num_done=num_done, num_expr=len(x['user_expressions']), num_cmd=len(x['commands']))
                next_print = now() + 5*seconds
        return debug_output
        
    # iterate until all user expressions and generated expressions have become commands
    state_final = (state_initial
                   | iterate[iteration_step[generate_id]]
                   | (map[tap[make_debug_output()]] if gd_timing else identity)
                   | take_until[lambda s: len(s['user_expressions'])==0]
                   # | map[tap[print]]
                   | last
                   | collect
                   )
    return verify_and_compact_commands(state_final['commands'])



##############################
# * Validation
#----------------------------

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

    elif isinstance(x, (EntityRef, AttributeEntityRef, RelationRef)):
        ids = {uid(x): x}
        if isinstance(x, RelationRef):
            if not is_a(x.d["type"][0], RT):
                ids = merge_no_overwrite(ids, obtain_ids(source(x)))
            if not is_a(x.d["type"][2], RT):
                ids = merge_no_overwrite(ids, obtain_ids(target(x)))

    elif isinstance(x, (ZefRef, EZefRef)):
        ids = {origin_uid(x): x}

    elif isinstance(x, Val):
        no_iid = Val(x.arg)
        ids = {no_iid: x.arg}
        if x.iid is not None:
            ids[x.iid] = x.arg

    elif isinstance(x, EntityValueInstance):
        id = get_absorbed_id(x._entity_type)
        if id is not None:
            ids = {id: x}

        for item in x._kwargs.values():
            ids = merge_no_overwrite(ids, obtain_ids(item))

    elif isinstance(x, PleaseAssign):
        ids = merge_no_overwrite(ids, obtain_ids(x.target))

    # This is an extra step on top of the previous checks
    # if type(x) in [Entity, AttributeEntity, Relation, EntityType,
    #                 AttributeEntityType, RelationType, ZefRef, EZefRef]:
    #     # Need the lazy value for the RT possibility
    #     a_id = get_absorbed_id(LazyValue(x))
    #     if a_id is not None:
    #         ids = merge_no_overwrite(ids, {a_id: x})

    # This is an extra step on top of the previous checks
    # if isinstance(x, (Entity, AttributeEntity, Relation, ET[Any],
    #                 AttributeEntityType, RelationType, ZefRef, EZefRef)):
    if isinstance_lv_safe(x, (RAERef, (ValueType & (ET | RT | AET)),
                    ZefRef, EZefRef)):
        # Need the lazy value for the RT possibility
        a_id = get_absorbed_id(LazyValue(x))
        if a_id is not None:
            ids = merge_no_overwrite(ids, {a_id: x})
    return ids


@func    
def verify_input_el(x, id_definitions, allow_rt=False, allow_scalar=False):
    from . import Val
    # Check each input item to a GraphDelta to validate its form. Uses
    # id_definitions to assert that references to IDs are to things that are
    # defined in the GraphDelta.

    if isinstance(x, LazyValue):
        # This is checking for an (x <= value) format
        if False:
            pass
        # if is_a(x.el_ops, assign):
        #     return
        # elif is_a(x.el_ops, terminate):
        #     return
        elif is_a(x.el_ops, ZefOp[tag]):
            return
        elif is_a(x.el_ops, ZefOp[fill_or_attach]):
            return
        elif is_a(x.el_ops, ZefOp[set_field]):
            return
        raise Exception(f"A LazyValue must have come from (x | fill_or_attach) or (x | tag) only. Got {x}")

    # Note: just is_a(x, Z) will also mean ZefRefs will be hit
    elif is_a(x, ZefOp[Z]):
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
            if isinstance(v, list):
                [verify_input_el(e, id_definitions, False, True) for e in v]
            else:
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
            verify_input_el(item, id_definitions, False, allow_scalar)
        return

    elif is_a(x, Delegate):
        return

    elif isinstance(x, RAERef):
        return

    elif isinstance(x, RT):
        if allow_rt:
            return
        else:
            raise ValueError(f"Bare RTs without source or target cannot be initialized. You tried to create a {x}.")                    
    elif isinstance(x, scalar_types):
        if not allow_scalar:
            raise Exception("Direct values are not allowed at the top level of a GraphDelta, as this is likely to indicate a typo, e.g. (ET.Machine, ET.ShouldBeRTNotET, 'name'). Please use explicit value assignment via (AET.String <= 'name') if you really want this behaviour.")
        return
    elif is_valid_single_node(x):
        return

    elif isinstance(x, ZefOp):
        if len(x) > 1:
            # This could be something like Z["asdf"] | assign[5]
            if LazyValue(x) | first | is_a[ZefOp[Z]] | collect:
                return
        if length(LazyValue(x) | absorbed) != 1:
            raise Exception(f"ZefOp has more than one op inside of it. {x}")
        raise Exception(f"Not allowing ZefOps except for those starting with Z. Got {x}")

    elif isinstance(x, Val):
        return

    elif isinstance(x, EntityValueInstance):
        return

    elif isinstance(x, PleaseAssign):
        verify_input_el(x.target, id_definitions, False, True)
        return

    elif isinstance(x, PleaseCommand):
        return

    elif isinstance(x, NamedZ):
        return

    elif isinstance(x, NamedAny):
        return

    else:
        raise ValueError(f"Unexpected type passed to init list of GraphDelta: {x} of type {type(x)}")

@func    
def verify_relation_source_target(x, id_definitions):
    if isinstance_lv_safe(x, RelationRef):
        assert id_from_ref(x.d["source"]) in id_definitions, "A Relation doesn't have its corresponding source included in the GraphDelta."
        assert id_from_ref(x.d["target"]) in id_definitions, "A Relation doesn't have its corresponding source included in the GraphDelta."




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
    expr = exprs.pop(0)

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
    from . import Val
    if isinstance(expr, LazyValue):
        expr = collect(expr)

    # If we have a lazy value the second time around, then this must be an actual action to perform.
    if isinstance(expr, LazyValue):
        if False:
            pass
        # elif is_a(expr.el_ops, assign):
        #     return cmds_for_lv_assign(expr, gen_id)
        # elif is_a(expr.el_ops, terminate):
        #     return cmds_for_lv_terminate(expr)
        elif is_a(expr.el_ops, ZefOp[set_field]):
            return cmds_for_lv_set_field(expr, gen_id)
        elif is_a(expr.el_ops, ZefOp[fill_or_attach]):
            return cmds_for_complex_expr(expr, gen_id)
        elif is_a(expr.el_ops, ZefOp[tag]):
            return cmds_for_lv_tag(expr, gen_id)
        else:
            raise Exception("LazyValue obtained which is not known")

    elif isinstance(expr, ZefOp):
        # If we have a chain beginning with a Z, convert to LazyValue
        if LazyValue(expr) | first | is_a[ZefOp[Z]] | collect:
            return cmds_for_initial_Z(expr)

        raise RuntimeError(f'We should not have landed here, with expr={expr}')


    # d_dispatch = {
    #     EntityType: cmds_for_instantiable,
    #     AttributeEntityType: cmds_for_instantiable,

    #     ZefRef: cmds_for_mergable,
    #     EZefRef: cmds_for_mergable,
    #     Entity: cmds_for_mergable,
    #     AttributeEntity: cmds_for_mergable,
    #     Relation: cmds_for_mergable,
    #     TXNode: cmds_for_mergable,
    #     Root: cmds_for_mergable,

    #     tuple: P(cmds_for_tuple, gen_id=gen_id),
    #     list: P(cmds_for_tuple, gen_id=gen_id),

    #     dict: P(cmds_for_complex_expr, gen_id=gen_id),

    #     Val: cmds_for_value_node,
    #     TaggedVal: cmds_for_value_node,
    # }
    # if type(expr) not in d_dispatch:
    #     raise TypeError(f"transform_to_commands was called for type {type(expr)} value={expr}, but no handler in dispatch dict")            
    # func = d_dispatch[type(expr)]

    def error_handler(x):
        raise TypeError(f"transform_to_commands was called for type {type(expr)} value={expr}, but no handler in dispatch dict")
    return expr | match[
        (Delegate,               cmds_for_delegate),
        (ValueType & (ET | AET), cmds_for_instantiable),
        (Tuple | List,           P(cmds_for_tuple, gen_id=gen_id)),
        (Dict,                   P(cmds_for_complex_expr, gen_id=gen_id)),
        (ZefRef | EZefRef,       cmds_for_mergable),
        (RAERef,                 cmds_for_mergable),
        (Val,                    cmds_for_value_node),
        (EntityValueInstance,    P(cmds_for_complex_expr, gen_id=gen_id)),
        (PleaseAssign,           P(cmds_for_please_assign, gen_id=gen_id)),
        (PleaseTerminate,        cmds_for_please_terminate),
        (Any,                    error_handler),
    ] | collect



##################################################
# * Handlers for expr->cmd
#------------------------------------------------

def cmds_for_complex_expr(x, gen_id):
    iid,exprs = realise_single_node(x, gen_id)
    return exprs, []

def cmds_for_initial_Z(expr):
    assert LazyValue(expr) | first | is_a[ZefOp[Z]] | collect

    expr = LazyValue(LazyValue(expr) | first | collect) | (LazyValue(expr) | skip[1] | to_pipeline | collect)
    return (expr,), ()

def cmds_for_value_node(x):
    cmd = {'cmd': 'merge',
           'origin_rae': x,
           'internal_ids': []}
    
    if x.iid is not None:
        cmd['internal_ids'] += [x.iid]
        
    return (), [cmd]


def cmds_for_instantiable(x):
    a_id = get_absorbed_id(x)
    # TODO
    # raet = x | without_absorbed | collect
    raet = x
    cmd = {'cmd': 'instantiate', 'rae_type': raet}
    if a_id is not None:
        cmd['internal_id'] =  a_id
        
    return (), [cmd]

def cmds_for_mergable(x):
    origin = discard_frame(x)

    cmd = {"cmd": "merge",
           "origin_rae": origin,
           "internal_ids": []}

    a_id = get_absorbed_id(x)
    if a_id is not None:
        cmd['internal_ids'] += [a_id]

    if is_a(x, ZefRef) or is_a(x, EZefRef):
        if isinstance(x, (BT.ENTITY_NODE, BT.TX_EVENT_NODE, BT.ROOT_NODE, BT.VALUE_NODE)):
            return (), [cmd]

        elif isinstance(x, BT.ATTRIBUTE_ENTITY_NODE):
            cmds = [cmd]
            if isinstance(x, ZefRef):
                val = x | value | collect
                if val is not None:
                    cmds += [{
                        'cmd': 'assign', 
                        'value': val,
                        'internal_id': uid(origin),
                        'explicit': False,
                }]

            return (), cmds

        elif isinstance(x, BT.RELATION_EDGE):
            return (
                (x | source | to_ezefref | collect, x | target | to_ezefref | collect),
                [cmd],
            )
        else:
            raise NotImplementedError(f"Unknown ZefRef type for merging: {BT(x)}")
    elif is_a(x, EntityRef) or is_a(x, AttributeEntityRef):
        return (), [cmd]
    elif is_a(x, RelationRef):
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

    assert is_a(op, ZefOp[tag])
    tag_s = LazyValue(op) | absorbed | single | collect
    iid,exprs = realise_single_node(target, gen_id)
    cmd = {'cmd': 'tag',
           'tag_name': tag_s,
           'internal_id': iid}
    return exprs, [cmd]
    

def cmds_for_please_assign(x, gen_id: Callable):
    from . import Val
    assert isinstance(x, PleaseAssign)

    target = x.target
    val = x.value
        
    iid,exprs = realise_single_node(target, gen_id)

    cmd = {'cmd': 'assign', 'explicit': True, "internal_id": iid}

    if isinstance(val, Val):
        val = val.arg
    elif isinstance(val, BlobPtr & BT.VALUE_NODE):
        val = value(val)

    cmd['value'] = val

    return exprs, [cmd]

def cmds_for_please_terminate(x):
    assert isinstance(x, PleaseTerminate)

    target = x.target
    a_id = x.internal_id

    cmd = {
        'cmd': 'terminate', 
        'origin_rae': discard_frame(target)
        }
    if a_id is not None:
        cmd['internal_ids'] = [a_id]

    return (), [cmd]

def cmds_for_lv_set_field(x, gen_id):
    assert isinstance(x, LazyValue)

    assert len(x | peel | collect) == 2
    source,op = x | peel | collect

    # assert is_a(source, ZefOp) and is_a(source, Z), f"fill_or_attach reached cmd creation with an incorrect input type: {source}"
    assert is_a(op, ZefOp[set_field])
    rt,assignment,incoming = LazyValue(op) | absorbed | collect
    assert isinstance(rt, RT)

    iid,exprs = realise_single_node(source, gen_id)

    cmd = {
        'cmd': 'set_field', 
        'source_id': iid,
        'rt': rt,
        'incoming': incoming,
    }

    if isinstance(assignment, scalar_types):
        cmd['value'] = assignment 
    else:
        target_iid,target_exprs = realise_single_node(assignment, gen_id)
        cmd['target_id'] = target_iid
        exprs.extend(target_exprs)

    if len(absorbed(rt)) > 0:
        cmd['internal_id'] = single(absorbed(rt))

    return exprs, [cmd]

def cmds_for_delegate(x):                            
    internal_ids = []
    a_id = get_absorbed_id(x)
    if a_id is not None:
        internal_ids += [a_id]

    # The to_delegate below takes care of whether x is a DelegateRef or a ZefRef
    return (), [{
        'cmd': 'merge', 
        'origin_rae': to_delegate(x),
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
            if isinstance(rel, RT):
                a_id = get_absorbed_id(rel)
                # raet = LazyValue(rel) | without_absorbed | collect
                cmd1 = {'cmd': 'instantiate', 'rae_type': rel}
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
    if isinstance(x, scalar_types):
        return True
    if isinstance(x, ZefRef) or isinstance(x, EZefRef):
        return True
    if is_a(x, ZefOp[Z]):
        return True
    if isinstance(x, ET):
        return True
    if isinstance(x, RT):
        return True
    if isinstance(x, AET):
        return True
    if isinstance(x, Delegate):
        return True
    return False

def is_RT_token(x):
    from .VT.value_type import is_type_name_
    return is_type_name_(x, "RT")

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
    if any(is_RT_token(item) for item in x):
        # Cases 1-4 above
        if len(x) != 3:
            raise Exception(f"A list has an RT but isn't 3 elements long: {x}")
        if not isinstance(x[1], RT):
            raise Exception(f"A list has an RT but it isn't in the second position: {x}")
        # Note: if there are any lists involved, we cannot have a given ID as it
        # would have to be given to multiple instantiated relations.
        if not is_RT_token(x[1]) and (isinstance(x[0], ListOrTuple) or isinstance(x[2], ListOrTuple)):
            raise Exception("An RT with an internal name is not allowed with multiple sources or targets")
        return True
    elif len(x) == 2 and isinstance(x[1], ListOrTuple):
        return all(isinstance(item, ListOrTuple) and len(item) == 2 and is_RT_token(item[0]) and is_valid_single_node(item[1]) for item in x[1])
    return False


@func
def realise_single_node(x, gen_id):
    from . import Val
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

    from .VT import ZExpression
    from .VT.value_type import type_name

    # First case of removing lazy values
    if isinstance(x, LazyValue):
        x = collect(x)

    # Now this is a check for whether we are an assign
    if isinstance(x, LazyValue):
        target,op = x | peel | collect
        # if is_a(op, terminate) or is_a(op, tag):
        if is_a(op, ZefOp[tag]):
            iid,exprs = realise_single_node(target, gen_id)
            exprs = [x]
        # elif is_a(op, assign):
        #     iid,exprs = realise_single_node(target, gen_id)
        #     exprs = exprs + [LazyValue(Z[iid]) | op]
        elif is_a(op, ZefOp[fill_or_attach]):
            # fill_or_attach behaviour is now basically set_field except when the target is not a value
            iid,exprs = realise_single_node(target, gen_id)
            rt,assignment = LazyValue(op) | absorbed | collect
            if isinstance(assignment, scalar_types):
                exprs = exprs + [LazyValue(Z[iid]) | set_field[rt][assignment]]
            else:
                exprs = exprs + [(Z[iid], rt, assignment)]
        elif is_a(op, ZefOp[set_field]):
            iid,exprs = realise_single_node(target, gen_id)
            exprs = exprs + [LazyValue(Z[iid]) | op]
        else:
            raise Exception(f"Don't understand LazyValue type: {op}")
    elif isinstance(x, PleaseAssign):
        target = x.target
        val = x.value
        iid,exprs = realise_single_node(target, gen_id)
        exprs = exprs + [PleaseAssign(target=Z[iid], value=val)]
    elif isinstance(x, PleaseTerminate):
        target = x.target
        iid = x.internal_id
        if iid is None:
            iid = gen_id()
            new_terminate = PleaseTerminate(insert(x._value, "internal_id", iid))
            exprs = [new_terminate]
        else:
            exprs = [x]
    elif isinstance(x, ValueType) and issubclass(x, (ET,AET)):
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
        elif isinstance(x, BT.VALUE_NODE):
            iid,exprs = realise_single_node(Val(value(x)), gen_id)
        else:
            exprs = [x]
            iid = origin_uid(x)
    elif isinstance(x, RAERef):
        exprs = [x]
        iid = origin_uid(x)
    elif isinstance(x, Val):
        if x.iid is None:
            iid = x
        else:
            iid = x.iid
        exprs = [x]
    elif isinstance(x, shorthand_scalar_types):
        iid = gen_id()
        aet = map_scalar_to_aet_type(x)
        exprs = [aet[iid], LazyValue(Z[iid]) | assign[x]]
    elif isinstance(x, scalar_types):
        raise Exception("A value of type {type(x)} is not allowed to be given in a GraphDelta in the shorthand syntax as it is ambiguous. You might want to explicitly create an AET and assign, or a value node, or a custom AET.")
    elif isinstance(x, ZefOp):
        if len(x) == 1:
            if is_a(x, ZefOp[Z]):
                iid = LazyValue(x) | peel | first | second | first | collect
                # No expr to perform
                exprs = []
            else:
                raise NotImplementedError(f"Can't pass zefops to GraphDelta: for {x}")
        else:
            ops = LazyValue(x) | peel | collect
            first_op = ops[0]
            rest = ops[1:]
            if is_a(first_op, ZefOp[Z]):
                new_op = LazyValue(first_op) | to_pipeline(rest)
                iid,exprs = realise_single_node(new_op, gen_id)
            else:
                raise NotImplementedError(f"Can't pass zefops to GraphDelta: for {x}")
    elif isinstance(x, NamedZ):
        iid = x.root_node.arg2
        # No expr to perform
        exprs = []
    elif isinstance(x, NamedAny):
        names = names_of_any(x)
        assert len(names) == 1, f"Too many names: {names} in {x}"
        iid = names[0]
        # No expr to perform
        exprs = []
    elif is_a(x, Delegate):
        iid = x
        exprs = [x]
    elif type(x) == dict:
        entity, sub_d = list(x.items()) | single | collect # Will throw if there isn't a single key in the dict
        iid, entity_exprs = realise_single_node(entity, gen_id)

        exprs = entity_exprs
        for k,v in sub_d.items():
            if isinstance(v, list):
                # for e in v:
                #     assert type(e) not in scalar_types, "Can't have multiple scalars attached in a dictionary"
                #     exprs.append((Z[iid], k, e))
                raise Exception("Not allowed to use lists inside of a dictionary syntax anymore")
            else:
                exprs.append(Z[iid] | set_field[k][v])
    elif isinstance(x, EntityValueInstance):
        exprs = expand_helper(x, gen_id)
        # TODO: Make the iid be returned explicitly.
        iid = get_absorbed_id(exprs[0])
    else:
        raise TypeError(f'in GraphDelta encode step: for type(x)={type(x)}')

    return iid, exprs



##############################
# * Ordering commands
#----------------------------

def verify_and_compact_commands(cmds: tuple):                
    aes_with_explicit_assigns = (cmds
                                 | filter[lambda d: d["cmd"] == "assign"]
                                 | filter[lambda d: d["explicit"]]
                                 | map[lambda d: d["internal_id"]]
                                 | collect
                                 )
    def is_unnecessary_automatic_merge_assign(d):
        return (d["cmd"] == "assign"
                and not d["explicit"]
                and d["internal_id"] in aes_with_explicit_assigns)

    # make sure if multiple assignment commands exist for the same AE, that their values agree
    cmds = (cmds | group_by[get['cmd']]
            | map[match[
                        (Is[first | equals["assign"]],   second | filter[Not[is_unnecessary_automatic_merge_assign]] | validate_and_compress_unique_assignment),
                # TODO: Validate fill_or_attach assignments - but note that
                # these could also conflict with regular assignments but it's
                # hard to tell that without playing out the assignments first.
                        (Is[first | equals["merge"]],          second | combine_internal_ids_for_merges),
                        (Is[first | equals["terminate"]],      second | combine_terminates), 
                        (Is[first | equals["instantiate"]],    second | combine_instantiates), 
                        (Any, second),                                 # Just pack things back in for other cmd types
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
                log.debug("Compacting:", num_done=num_done, num_input=len(x['state']['input']), num_output=len(x['state']['output']))
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
        | (tap[make_debug_output()] if gd_timing else identity)
        | take_until[lambda s: s["num_changed"] == 0]
        # | map[tap[print]]
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
            

def combine_ids(x, y):
    assert x["origin_rae"] == y["origin_rae"]
    return {**x,
            "internal_ids": x["internal_ids"] + y["internal_ids"]}

@func
def combine_internal_ids_for_merges(cmds):
    return (cmds
              # Convert each item to a dictionary using the origin_rae as key
            | map[lambda x: {x["origin_rae"]: x}]
              | reduce[merge_with[combine_ids]][{}]
              # Now we should have one big dict with each command
            | values)

# @func
# def combine_terminates(cmds):
#     @func
#     def check_list_is_distinct(cmds):
#         # cmds = cmds | distinct | collect
#         # if length(cmds) == 1:
#         #     return cmds[0]
#         if not cmds | map[equals[first(cmds)]] | all | collect:
#             ids = cmds | map[get["internal_id"][None]] | collect
#             raise ValueError(f'There may be at most one id for a terminated ZefRef. There were multiple for assignment to {cmds[0]["origin_rae"]!r} with ids {ids}')
#         return cmds[0]

#     return (cmds
#             | group_by[get["origin_rae"]]
#             | map[second | check_list_is_distinct])
@func
def combine_terminates(cmds):
    return (cmds
            # Convert each item to a dictionary using the origin_rae as key
            | map[lambda x: {x["origin_rae"]: x}]
            | reduce[merge_with[combine_ids]][{}]
            # Now we should have one big dict with each command
            | values)

@func
def combine_instantiates(cmds):
    @func
    def check_list_is_distinct(arg):
        iid,cmds = arg
        if iid is None:
            return cmds
        if not cmds | map[equals[first(cmds)]] | all | collect:
            raets = cmds | map[get["rae_type"]] | collect
            raise ValueError(f'There may be at most one RAET for an instantiated ZefRef. Found: {raets!r} for id {cmds[0]["internal_id"]}')
        return [cmds[0]]

    return (cmds
            | group_by[get["internal_id"][None]]
            | map[check_list_is_distinct]
            | concat)



def command_ordering_by_type(d_raes: dict) -> int:
    """we want some standardized order of the output to simplify value-based
        comparisons and other operations for graph deltas"""
        
    if d_raes['cmd'] == 'merge':
        if isinstance(d_raes['origin_rae'], Relation): return 0.5
        else: return 0
    if d_raes['cmd'] == 'instantiate':
        if isinstance(d_raes['rae_type'], ET): return 1
        if isinstance(d_raes['rae_type'], AET): return 2
        if isinstance(d_raes['rae_type'], RT): return 3
        return 4                                            # there may be {'cmd': 'instantiate', 'rae_type': AET.Bool}
    if d_raes['cmd'] == 'assign': return 5
    if d_raes['cmd'] == 'set_field': return 5.5
    if d_raes['cmd'] == 'terminate' and isinstance(d_raes['origin_rae'], RelationRef): return 6
    if d_raes['cmd'] == 'terminate' and isinstance(d_raes['origin_rae'], EntityRef): return 7
    if d_raes['cmd'] == 'terminate' and isinstance(d_raes['origin_rae'], AttributeEntityRef): return 8
    if d_raes['cmd'] == 'terminate' and isinstance(d_raes['origin_rae'], Delegate): return 9
    if d_raes['cmd'] == 'tag': return 10
    else: raise NotImplementedError(f"In Sort fct for {d_raes}")

def get_id(cmd):
    if "internal_ids" in cmd:
        raise Exception("Not allowed to ask for id of command that could have multiple ids")
    elif 'internal_id' in cmd:
        return cmd['internal_id']
    elif 'origin_rae' in cmd:
        if isinstance(cmd['origin_rae'], Delegate):
            return cmd['origin_rae']
        return uid(cmd['origin_rae'])
    else:
        return None

def get_ids(cmd):
    ids = []
    if cmd['cmd'] in ['merge', "terminate"]:
        ids += cmd.get("internal_ids", [])
        ids += [id_from_ref(cmd['origin_rae'])]
        # if isinstance(cmd['origin_rae'], Delegate):
        #     ids += [cmd['origin_rae']]
        # else:
        #     ids += [uid(cmd['origin_rae'])]
    else:
        this_id = get_id(cmd)
        if this_id is not None:
            ids += [this_id]
    return ids

def id_from_ref(obj):
    if isinstance(obj, DelegateRef):
        return obj
    elif isinstance(obj, Val):
        # Get rid of an iid if it has one
        return Val(obj.arg)
    elif isinstance(obj, AtomRef):
        return origin_uid(obj)

    raise Exception(f"Shouldn't get here: {obj}")

def resolve_dag_ordering_step(arg: dict)->dict:
    # arg is "state" + "num_changed"
    state = arg["state"]
    ids = state['known_ids']        
    input = state['input']
    
    def can_be_executed(cmd):        
        if cmd['cmd'] == 'instantiate':
            # If we are creating an RT, wait until both source/target exist
            return not isinstance(cmd['rae_type'], RT) or (cmd['source'] in ids and cmd['target'] in ids)
        if cmd['cmd'] == 'merge':
            # If the merge is of a relation, we need both source and target to exist already
            return not isinstance(cmd['origin_rae'], Relation) or (id_from_ref(cmd['origin_rae'].d["source"]) in ids and id_from_ref(cmd['origin_rae'].d["target"]) in ids)
        if cmd['cmd'] == 'terminate':
            # Don't terminate if there an upcoming operation that will create this item
            return all(my_id not in get_ids(other) for other in input for my_id in get_ids(cmd) if other['cmd'] != 'terminate')
        if cmd['cmd'] == 'assign':
            # The object to be assigned needs to exist.
            return get_id(cmd) in ids
        if cmd['cmd'] == 'set_field':
            # This is where things get tricky - the behaviour of set_field
            # can change if there is another command that creates a relation of
            # the same type.
            #
            # TODO: this properly! I can see issues with assignments to the same
            # object via different IDs causing massive headaches here. Need to
            # consider aliasing and also consider multiple set_field commands.
            #
            # For now, just make sure the source is alive and run with that.
            return cmd['source_id'] in ids
        if cmd['cmd'] == 'tag':
            return get_id(cmd) in ids
        raise Exception(f"Don't know how to decide DAG ordering for command {cmd['cmd']}")
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
        if isinstance_lv_safe(x, shorthand_scalar_types):
            if not allow_scalar:
                raise Exception("Scalars are not allowed on their own to avoid accidental typos such as (ET.X, ET.Y, 'z') when (ET.X, RT.Y, 'z') is meant. If you want this behaviour, then create an explicit AET, i.e. (AET.String <= 'z').")
        
        iid,exprs = realise_single_node(x, gen_id)
        gd_exprs.extend(exprs)
        return iid

    step_res = step(xx, False)
    return step_res, construct_commands(gd_exprs, gen_id)


def unpack_receipt(unpacking_template, receipt: dict):
    def step(x):
        if isinstance(x, tuple):
            return tuple((step(el) for el in x))
        if isinstance(x, list):
            return [step(el) for el in x]
        if isinstance(x, dict):
            return {k: step(v) for k,v in x.items()}
        return receipt[x] if isinstance(x, (str, UID, Delegate, Val)) else x
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

            next_print = now()+5*seconds
            for i,cmd in enumerate(commands):
                if gd_timing:
                    if now() > next_print:
                        log.debug("Perform", i=i, total=len(commands))
                        next_print = now() + 5*seconds

                zz = None
                
                # print(f"{i}/{len(g_delta.commands)}: {g.graph_data.write_head * 16 / 1024 / 1024} MB")
                if cmd['cmd'] == 'instantiate' and (is_a(cmd['rae_type'], ET) or is_a(cmd['rae_type'], AET)):
                    maybe_token = internals.get_c_token(cmd['rae_type'])
                    if isinstance(maybe_token, ValueType):
                        zz = instantiate(internals.AET[maybe_token], g)
                    else:
                        zz = instantiate(maybe_token, g)
                
                elif cmd['cmd'] == 'instantiate' and is_a(cmd['rae_type'], RT):
                    zz = instantiate(to_ezefref(d_raes[cmd['source']]), internals.get_c_token(cmd['rae_type']), to_ezefref(d_raes[cmd['target']]), g) | in_frame[frame_now] | collect
                
                elif cmd['cmd'] == 'assign':
                    this_id = cmd['internal_id']
                    zz = d_raes.get(this_id, None)
                    if zz is None:
                        zz = most_recent_rae_on_graph(this_id, g)
                    assert zz is not None
                    if 'value' in cmd:
                        if zz | value | collect != cmd['value']:
                            # print("Assigning value of ", cmd['value'], "to a", AET(z))
                            internals.assign_value_imp(zz, cmd['value'])
                    else:
                        raise Exception("Assignment without an value")
                            
                    zz = now(zz)

                elif cmd['cmd'] == 'set_field':
                    z_source = d_raes.get(cmd['source_id'], None)
                    if z_source is None:
                        raise KeyError(f"set_field called with unknown source {cmd['source_id']}")

                    if 'value' in cmd:
                        # AET path
                        aet = map_scalar_to_aet_type(cmd['value'])
                    else:
                        # Entity path
                        z_target = d_raes.get(cmd['target_id'], None)
                        if z_target is None:
                            raise KeyError("set_field called with entity that is not known {cmd['target_id']}")

                    rt = cmd['rt']
                    rt_token = internals.get_c_token(cmd['rt'])
                    if cmd['incoming']:
                        opts = z_source | in_rels[rt] | collect
                    else:
                        opts = z_source | out_rels[rt] | collect
                    if len(opts) == 2:
                        raise Exception(f"Can't set_field to {z_source} because it has two or more relations of kind {rt}")
                    elif len(opts) == 1:
                        zz = single(opts)
                        if 'value' in cmd:
                            # AE path - overwrite value
                            if cmd['incoming']:
                                ae = source(zz)
                            else:
                                ae = target(zz)
                            if aet != rae_type(ae):
                                raise Exception(f"Can't fill or attach {ae} because it is the wrong type for value {cmd['value']}")
                            if value(ae) != cmd['value']:
                                internals.assign_value_imp(ae, cmd['value'])
                        else:
                            # Entity path - replace the relation
                            from ..pyzef import zefops as pyzefops
                            pyzefops.terminate(zz)
                            if cmd['incoming']:
                                zz = instantiate(z_target, rt_token, z_source, g)
                            else:
                                zz = instantiate(z_source, rt_token, z_target, g)
                    else:
                        if 'value' in cmd:
                            # AE path
                            aet_token = internals.get_c_token(aet)
                            ae = instantiate(aet_token, g)
                            internals.assign_value_imp(ae, cmd['value'])
                            if cmd['incoming']:
                                zz = instantiate(ae, rt_token, z_source, g)
                            else:
                                zz = instantiate(z_source, rt_token, ae, g)
                        else:
                            # Entity path
                            if cmd['incoming']:
                                zz = instantiate(z_target, rt_token, z_source, g)
                            else:
                                zz = instantiate(z_source, rt_token, z_target, g)
                    # This zz is the relation that connects the source/target
                    zz = now(zz)
                    
                elif cmd['cmd'] == 'merge':
                    # It is either an instance (with an 'origin_rae_uid' specified)
                    # or a delegate[...]
                    if is_a(cmd['origin_rae'], Delegate):
                        d = cmd['origin_rae']
                        zz = internals.delegate_to_ezr(d, g, True, 0)
                        zz = now(zz)
                    elif is_a(cmd['origin_rae'], Val):
                        zz = instantiate_value_node_imp(cmd['origin_rae'].arg, g)
                        zz = now(zz)
                    else:
                        candidate = most_recent_rae_on_graph(id_from_ref(cmd['origin_rae']), g)
                        if candidate is not None:
                            # this is already on the graph. Just assert and move on
                            assert abstract_type(cmd['origin_rae']) == abstract_type(candidate), f"Abstract types don't match: {abstract_type(cmd['origin_rae'])!r} != {abstract_type(candidate)!r}"
                            zz = candidate
                        else:
                            origin_rae_uid = origin_uid(cmd['origin_rae'])
                            if isinstance(cmd['origin_rae'], EntityRef):
                                zz = internals.merge_entity_(
                                    g, 
                                    internals.get_c_token(rae_type(cmd['origin_rae'])),
                                    origin_rae_uid.blob_uid,
                                    origin_rae_uid.graph_uid,
                                )
                            elif isinstance(cmd['origin_rae'], AttributeEntityRef):
                                zz = internals.merge_atomic_entity_(
                                    g, 
                                    internals.get_c_token(rae_type(cmd['origin_rae'])),
                                    origin_rae_uid.blob_uid,
                                    origin_rae_uid.graph_uid,
                                )
                            elif isinstance(cmd['origin_rae'], RelationRef):
                                src_origin_uid = id_from_ref(cmd['origin_rae'].d["source"])
                                trg_origin_uid = id_from_ref(cmd['origin_rae'].d["target"])
                                z_src = d_raes.get(src_origin_uid, most_recent_rae_on_graph(src_origin_uid, g))                                    
                                z_trg = d_raes.get(trg_origin_uid, most_recent_rae_on_graph(trg_origin_uid, g))                                    

                                assert z_src is not None
                                assert z_trg is not None                                    
                                zz = internals.merge_relation_(
                                    g, 
                                    internals.get_c_token(rae_type(cmd['origin_rae'])),
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

        # There is a weird edge case here - if a node is asked to be merged
        # in, but already existed on the graph AND there were no changes,
        # causing the transaction to be rolled-back, then the reference
        # frame of the ZefRefs will be wrong.

        # We check this by seeing if the index of the tx zefref is invalid.
        if index(d_raes['tx']) > g.graph_data.read_head:
            # Signal this by setting the tx to none (i.e. there was no tx)
            d_raes['tx'] = None

            # Update all ZefRefs to be in the latest frame instead of the previously created tx.
            reset_frame = in_frame[now(g)][allow_tombstone]
            for key,val in d_raes.items():
                if key == 'tx': continue

                # A terminated non-existent entity can return None
                if val is not None:
                    d_raes[key] = reset_frame(val)

    except Exception as exc:        
        raise RuntimeError(f"Error executing graph delta transaction exc={exc}") from exc
        
    return d_raes    # the transaction receipt

################################
# * General utils
#------------------------------

scalar_types = (int, float, bool, str, Time, QuantityFloat, QuantityInt, Enum, SerializedValue, EntityTypeToken, AttributeEntityTypeToken, RelationTypeToken)
shorthand_scalar_types = (int, float, bool, str, Time, QuantityFloat, QuantityInt, Enum, SerializedValue)

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

# map_scalar_to_aet_type = {
#     int:                lambda x: AET.Int,
#     float:              lambda x: AET.Float,
#     bool:               lambda x: AET.Bool,
#     str:                lambda x: AET.String,
#     Time:               lambda x: AET.Time,
#     ZefEnumValue:       make_enum_aet,
#     QuantityFloat:      make_qf_aet, 
#     QuantityInt:        make_qi_aet, 
#     ValueType_:         lambda x: AET.Type,
#     }
def map_scalar_to_aet_type(x):
    return LazyValue(x) | match[
        (PyBool, always[AET.Bool]),
        (Int, always[AET.Int]),
        (Float, always[AET.Float]),
        (String, always[AET.String]),
        (Time, always[AET.Time]),
        (Enum, make_enum_aet),
        (QuantityFloat, make_qf_aet), 
        (QuantityInt, make_qi_aet), 
        (ValueType, always[AET.Type]),
    ] | collect


def get_curried_arg(op, n):
    """ 
    utility function to get 
    e.g. get_curried_arg(Z['id1'][42], 0)       # => 'id1'
    e.g. get_curried_arg(Z['id1'][42], 1)       # => 42
    """
    # return op.el_ops[arg_indx][1][n]
    return LazyValue(op) | absorbed | nth[n] | collect

def most_recent_rae_on_graph(id, g: Graph)->ZefRef|Nil:
    if isinstance(id, EternalUID):
        return most_recent_rae_on_graph_uidonly(id, g)
    elif isinstance(id, DelegateRef):
        return to_delegate(id, g)
    elif isinstance(id, Val):
        val = id.arg
        if not isinstance(val, scalar_types):
            val = SerializedValue.serialize(val)
        return internals.search_value_node(val, g)

    raise Exception(f"Shouldn't get here: {id}, {type(id)}")
            
        
def most_recent_rae_on_graph_uidonly(origin_uid: str, g: Graph)->ZefRef|Nil:
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
    if BT(zz) in {BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE}:
        from .graph_slice import get_instance_rae
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
def get_absorbed_id(obj):
    # THIS SHOULDN"T BE NEEDED! FIX!
    # if is_a(obj, RT) or is_a(obj, ZefOp):
    #     obj = LazyValue(obj)
    # if isinstance(obj, (ET, RT, AET)):
    from .VT.rae_types import RAET_get_names
    if isinstance(obj, DelegateRef):
        return None
    elif isinstance(obj, ValueType) and issubclass(obj, (ET, RT, AET)):
        # return obj | absorbed | single_or[None] | collect
        return RAET_get_names(obj) | single_or[None] | collect
    else:
        return LazyValue(obj) | absorbed | single_or[None] | collect


def equal_identity(a, b):
    if type(a) != type(b):
        return False

    # Things that don't produce the same object even though their python objects
    # are the same
    # if type(a) in [EntityType, RelationType, AttributeEntityType, ZefEnumValue]:
    #     return False

    return a == b

# @func
# def merge_no_overwrite(a,b):
#     d = {**a}

#     for k,v in b.items():
#         if k in d and not equal_identity(d[k], v):
#             raise Exception(f"The internal id '{k}' refers to multiple objects, including '{d[k]}' and '{v}'. This is ambiguous and not allowed.")
#         d[k] = v
#     return d

@func
def merge_no_overwrite(a,b):
    # This version is mutating because otherwise it's too slow
    for k,v in b.items():
        if k in a and not equal_identity(a[k], v):
            raise Exception(f"The internal id '{k}' refers to multiple objects, including '{a[k]!r}' and '{v!r}'. This is ambiguous and not allowed.")
        a[k] = v
    return a




# Temporary copy paste
def expand_helper(x, gen_id):
    if type(x) in {str, int, float, bool}:
        return (x, )

    elif isinstance(x, EntityValueInstance):
        res = []
        assert isinstance(x, EntityValueInstance)
        et = x._entity_type
        ent_id = get_absorbed_id(et)
        if ent_id is None:
            ent_id = gen_id()
            et = et[ent_id]
        me = Z[ent_id]
        res.append(et)

        for k, v in x._kwargs.items():
            if isinstance(v, EntityValueInstance):
                sub_obj_instrs = expand_helper(v, gen_id)
                res.append( (me, RT(k), sub_obj_instrs[0]) )
                res.extend(sub_obj_instrs[1:])
            
            elif isinstance(v, set):
                for el in v:
                    sub_obj_instrs = expand_helper(el, gen_id)
                    res.append( (me, RT(k), sub_obj_instrs[0]) )
                    res.extend(sub_obj_instrs[1:])
            
            elif type(v) in {list, tuple}:
                list_id = gen_id()
                res.append( (me, RT(k), ET.ZEF_List[list_id]) )

                # generate ids for each relation, that we can inter-connect them
                list_ids = [gen_id() for _ in range(len(v))]
                res.extend(list_ids 
                      | sliding[2] 
                      | map[lambda p: (Z[p[0]], RT.ZEF_NextElement, Z[p[1]])]
                      | collect
                    )
                for el, edge_id in zip(v, list_ids):
                    sub_obj_instrs = expand_helper(el, gen_id)
                    res.append( (ET.ZEF_List[list_id], RT.ZEF_ListElement[edge_id], sub_obj_instrs[0]) )
                    res.extend(sub_obj_instrs[1:])
            else:
                res.append( (me, RT(k), v) )
        return res
    
def expand_object_to_instructions(x, id_generator=None):
    def make_gen_id():
        c = 0
        while True:
            yield f"_id_{c}"
            c += 1
    
    return expand_helper(x, make_gen_id())

def isinstance_lv_safe(instance, types):
    # This is required to get around the warning checks by ValueTypes for lazy
    # values. This function should be used after considering whether the input
    # argument could be a lazy value, that means it can't be used with tag or
    # set_field at the moment.
    return not isinstance(instance, LazyValue) and isinstance(instance, types)

def names_of_any(a):
    assert isinstance(a, NamedAny)

    return absorbed(a)