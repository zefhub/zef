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

from .common import *

from .wish_tagging import ensure_tag

def evaluate_chain(val, el_ops, gen_id_state):
    state = dict(obj=val,
                 # ops_itr=iter(el_ops),
                 focused_field=None,
                 emitted_cmds=[],
                 gen_id_state=gen_id_state)

    final_state = list(el_ops) | reduce[apply_op][state] | collect

    return final_state

@func
def apply_op(state, op):
    @func
    def op_kind(op):
        return RT[peel(op)[0][0]]
        
    return (op,state) | match_ops[
        (RT.Field, apply_field),
        (RT.Assign, apply_assign),
        (RT.SetField, apply_set_field),
        (RT.Terminate, apply_terminate),
    ] | collect

@func
def match_ops(input, rules):
    op,state = input
    op_rt = RT[peel(op)[0][0]]

    for rule in rules:
        if op_rt == rule[0]:
            return rule[1](op,state)

    raise Exception(f"Shouldn't get here: no rule found for op {op_rt}")
        

    

def apply_field(op, state):
    rt = single(absorbed(op))

    assert state["focused_field"] is None
    return state | insert["focused_field"][rt] | collect
    
def apply_assign(op, state):
    val = single(absorbed(op))

    gen_id_state = state["gen_id_state"]
    new_cmds = []

    if isinstance(state["obj"], AtomClass):
        # assert isinstance(state["focused_field"], RT)
        if state["focused_field"] is None:
            assert isinstance(state["obj"], AttributeEntityAtom)
            new_obj,me,gen_id_state = ensure_tag(state["obj"], gen_id_state)
            # Make sure that we don't include the frame with the atom now, since we
            # are assigning a value to it.
            from .._ops import discard_frame
            new_obj = discard_frame(new_obj)
            new_cmds += [PleaseAssign(target=me, value=Val(val))]
        else:
            new_obj = state["obj"](**{token_name(state["focused_field"]) : val })

        return (state
                | merge[{
                    "focused_field": None,
                    "obj": new_obj,
                    "gen_id_state": gen_id_state,
                    "emitted_cmds": state["emitted_cmds"] + new_cmds,
                }]
                | collect)
    else:
        raise NotImplementedError("TODO: non-atoms in assign")

def apply_set_field(op, state):
    set_target = state["obj"]

    if not isinstance(set_target, AtomClass):
        set_target = Atom(set_target)

    rt,val = absorbed(op)

    new_obj = set_target(**{token_name(rt): val})

    return state | insert["obj"][new_obj] | collect

def apply_terminate(op, state):
    if len(absorbed(op)) > 0:
        raise Exception(f"Terminate should take no additional arguments: {absorbed(op)}.")
    obj = state["obj"]
    if not isinstance(obj, AtomClass):
        raise Exception("Terminate can't understand anything other than an Atom")
    from ..atom import _get_fields
    assert len(_get_fields(obj)) == 0
    id = force_as_id(get_most_authorative_id(obj))
        
    new_obj = PleaseTerminate(target=id)
    return (state
            | insert["obj"][new_obj]
            | collect)




def cast_symbolic_expression_as_lazyvalue(se):
    assert isinstance(se.root_node, ET.Or)

    as_list = extract_binary_tree(se.root_node)

    lv = LazyValue(as_list[0])
    return lv | to_pipeline(as_list[1:])

def extract_binary_tree(fr):
    # Treat this like a binary tree and we have to extract the ordered list
    if isinstance(fr, ET.Or):
        left = extract_binary_tree(fr | Out[RT.Arg1] | collect)
        right = extract_binary_tree(fr | Out[RT.Arg2] | collect)
        return left + right
    elif rae_type(fr) == BT.VALUE_NODE:
        return [value(fr)]
    else:
        raise NotImplementedError(f"TODO: {fr}")
