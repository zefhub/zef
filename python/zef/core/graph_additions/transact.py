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
report_import("zef.core.graph_additions.transact")

# This is the interface for the user

# Notes:
# - rules for interpretation (input -> lvl2 cmds) are acted upon in transact.
# - rules for transaltion (lvl2 -> lvl1) are acted upon in the effect.
# - lvl1 has no rules. It is the lowest-lvl commands that map 1-to-1 to the
#   operations applied to a graph.

# To better handle sending graph wishes over the wire, while zef functions are
# not serializable, we will use a string "default" for the translation rules.


from .common import *


@func
def transact_dispatch(input, onto, *others):
    if isinstance(onto, (Graph, GraphRef)):
        return transact_graph(input, onto, *others)
    elif isinstance(onto, FlatGraph):
        from ..op_implementations import fg_insert_imp
        return fg_insert_imp(onto, input)
    else:
        raise Exception(f"Don't understand kind of object to transact onto: {onto}")

@func
def transact_graph(input: GraphWishInput | List[GraphWishInput], g, interpretation_rules=None, translation_rules="default", post_transact_rule=None):
    assert isinstance(input, GraphWishInput | List[GraphWishInput])
    if isinstance(input, GraphWishInput):
        input = [input]

    if interpretation_rules is None:
        from .wish_interpretation import default_interpretation_rules
        interpretation_rules = default_interpretation_rules
        
    from .wish_interpretation import generate_level2_commands
    cmds = generate_level2_commands(input, interpretation_rules)

    from ..fx import FX
    return {
        "type": FX.Graph.Transact,
        "target_graph": GraphRef(g),
        "level2_commands": cmds,
        "translation_rules": translation_rules,
        "post_transact_rule": post_transact_rule,
    }
    

@func
def encode_test(input: GraphWishInput | List[GraphWishInput], g):
    gen_id_state = generate_initial_state("encode")
    cmds, template, gen_id_state = encode_cmd(input, gen_id_state)

    #return out_cmds, out_template

    t_effect = cmds | transact[g] | collect

    return (t_effect
            | insert["unpacking_template"][template]
            | insert["keep_internal_ids"][True]
            | collect)

def dispatch_ror_graph(g, x):
    return x | encode_test[g]

from ...pyzef import main
main.Graph.__ror__ = dispatch_ror_graph


def encode_cmd(obj, gen_id_state: GenIDState):
    from .wish_interpretation import tagging_rules as default_rules

    all_ensure_tag_types = Union[tuple(default_rules | map[first] | collect)]

    encode_rules = [
        (RelationTriple, encode_relation_triple),
        (List, encode_list),
        # TODO: other relation forms

        # Fallback to ensure tag
        (all_ensure_tag_types, encode_ensure_tag_fallback[default_rules]),
    ]

    return (obj, gen_id_state) | match_rules[[
        *encode_rules,
        (Any, not_implemented_error["Don't know how to encode object for shorthand syntax"]),
    ]] | collect

def encode_relation_triple(obj, gen_id_state):
    s,rt,t = obj

    s_cmds, s_tag, gen_id_state = encode_cmd(s, gen_id_state)
    t_cmds, t_tag, gen_id_state = encode_cmd(t, gen_id_state)

    names = names_of_raet(rt)
    if len(names) == 0:
        me,gen_id_state = gen_internal_id(gen_id_state)
        rt = rt[me]
    else:
        me = names[0]

    cmds = s_cmds + t_cmds + [(s_tag, rt, t_tag)]
    tags = [s_tag, me, t_tag]
    return cmds, tags, gen_id_state

def encode_list(obj, gen_id_state):
    out_cmds = []
    out_tags = []
    for item in obj:
        this_cmds,this_tag,gen_id_state = encode_cmd(item, gen_id_state)
        out_cmds += this_cmds
        out_tags += [this_tag]
    return out_cmds, out_tags, gen_id_state

@func
def encode_ensure_tag_fallback(obj, tagging_rules, gen_id_state):
    obj,id,gen_id_state = (obj, gen_id_state) | match_rules[[
        *tagging_rules,
        (Any, not_implemented_error["Don't know how to ensure a tag for object"]),
    ]] | collect

    return [obj], id, gen_id_state
    


def unpack_receipt(template, receipt, gs):
    if isinstance(template, List):
        return tuple((unpack_receipt(el, receipt) for el in template))
    if isinstance(template, WishID):
        return receipt[template]
    if isinstance(template, EternalUID):
        from ..graph_slice import get_instance_rae
        return get_instance_rae(template, gs)
    raise Exception(f"Should not get here - unknown type in unpacking template: {template}")