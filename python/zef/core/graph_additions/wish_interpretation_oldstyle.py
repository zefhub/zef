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
report_import("zef.core.graph_additions.wish_translation_oldstyle")

from ..VT import *
from .common import *
from ..symbolic_expression import V

from ..zef_functions import func

from . import wish_interpretation
from .wish_interpretation import Lvl2Context

from ..VT.rae_types import RAET_get_names

# NOTE: everything in this file uses "OS" to mean "old style"

# This is what we will use to interface with the newer style and map variables
# back and forth.
oldstyle_prefix = "__oldstyle_"

# These rules try to mimic the behaviour of graph_delta.py

def gen_mapping_id(prev_state: GenIDState) -> Tuple[Variable]:
    prefix,prev_id = prev_state
    next_id = prev_id + 1
    id_str = getattr(V, f"{oldstyle_prefix}{prefix}_{next_id}")
    next_state = (prefix, next_id)
    return id_str, next_state

def OS_names_of_raet(context, raet):
    # Anything that's not a ValueType is a name - integers, strings, Vs, etc...
    names = RAET_get_names(raet)
    # We map these to Vs, so we can pass them to the rest of the framework
    V_names = []
    for name in names:
        context,V_name = OS_add_name_mapping(context, name)
        V_names += [V_name]

    return context, V_names

def OS_bare_raet(raet):
    return raet._replace(absorbed=(RAET_get_token(raet),))

def OS_add_name_mapping(context, name):
    if isinstance(name, WishID):
        return context, name

    # Using mutations for speed, could change this later. If made immutable,
    # need to make sure the context dict is recreated with these new items.
    custom = context.setdefault("custom", {})
    mapping = custom.setdefault("name_mapping", {})

    if name in mapping:
        return context, mapping[name]

    gen_id_state = context["gen_id_state"]
    id,gen_id_state = gen_mapping_id(gen_id_state)

    mapping[name] = id

    context = context | insert["gen_id_state"][gen_id_state] | collect

    return context, id


@func
def OS_lvl2cmds_for_ETorAET(input: ET|AET, context: Lvl2Context):
    context, names = OS_names_of_raet(context, input)
    bare_raet = OS_bare_raet(input)

    d = {"atom": bare_raet}
    if len(names) > 0:
        d["internal_ids"] = names
    return [PleaseInstantiate(d)], [], context

@func
def OS_lvl2cmds_for_relation_triple(input: RelationTriple, context: Lvl2Context):
    print("Deprecation warning: graph wishes using dictionary inputs will be disallowed in the future.")

    context, names = OS_names_of_raet(context, input[1])
    bare_rt = OS_bare_raet(input[1])

    d = {
        "atom": {
            "rt": bare_rt,
            "source": input[0],
            "target": input[2],
        }
    }
    if len(names) > 0:
        d["internal_ids"] = names

    return [PleaseInstantiate(d)], [], context


# This is to be replaced with a way to be self-referential
OSTaggable_Cheat = Any
DictSyntax = Dict[PureET][Dict[PureRT][OSTaggable_Cheat]] & Is[length | equals[1]]
OS_Taggable = wish_interpretation.Taggable | DictSyntax

@func
def OS_lvl2cmds_for_dict(input: DictSyntax, context: Lvl2Context):
    # This must be of the form:
    # {ET.Something: {RT.One: 5, RT.Two: z}}
    assert isinstance(input, DictSyntax)
    assert len(input) == 1

    gen_id_state = context["gen_id_state"]

    res = []

    source_input, rel_dict = single(input.items())

    source_input, me, gen_id_state = OS_ensure_tag(source_input, gen_id_state)
    res += [source_input]

    for k,v in rel_dict.items():
        v,v_id,gen_id_state = OS_ensure_tag(v, gen_id_state)
        res += [v]
        res += [(me, k, v_id)]

    context = context | insert["gen_id_state"][gen_id_state] | collect
    return [], res, context
        

def OS_ensure_tag(obj: OS_Taggable, gen_id_state: GenIDState) -> Tuple[OS_Taggable, WishID, GenIDState]:
    if isinstance(obj, DictSyntax):
        main_obj = single(obj.keys())
        new_main_obj,me,gen_id_state = OS_ensure_tag(main_obj, gen_id_state)
        if new_main_obj != main_obj:
            obj = {new_main_obj: single(obj.values())}
    else:
        # Fallback to regular version
        obj,me,gen_id_state = wish_interpretation.ensure_tag(obj, gen_id_state)

    return obj,me,gen_id_state



OS_interpretation_rules = [
    # Unaffected
    (AETWithValue, wish_interpretation.lvl2cmds_for_aet_with_value),
    (ObjectNotation, wish_interpretation.lvl2cmds_for_object_notation),
    (PleaseCommandLevel1, wish_interpretation.pass_through),
    (PleaseCommandLevel2, not_implemented_error["TODO"]),
    # Slight changes
    (PureET | PureAET, OS_lvl2cmds_for_ETorAET),
    (RelationTriple, OS_lvl2cmds_for_relation_triple),
    # Things not in base version
    (Dict, OS_lvl2cmds_for_dict),
]



def OS_post_transact_rule(receipt: GraphWishReceipt, lvl2_custom: Dict[Any][WishID]):
    if lvl2_custom is None:
        return receipt
    print("Deprecation warning: graph wishes using names other than `V.x` variables will be disallowed in future versions.")

    # Translate any oldstyle variable names into what they were originally
    new_items = {}
    to_remove = []
    flipped_mapping = {val: key for key,val in lvl2_custom["name_mapping"].items()}
    for key in receipt:
        if key.name.startswith(oldstyle_prefix):
            to_remove += [key]
            new_items[flipped_mapping[key]] = receipt[key]

    return receipt | without[to_remove] | merge[new_items] | collect
            
    