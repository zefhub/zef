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

from .common import *

@func
def encode(input: GraphWishInput | List[GraphWishInput], g):
    gen_id_state = generate_initial_state("encode")
    cmds, template, gen_id_state = encode_cmd(input, gen_id_state)

    #return out_cmds, out_template

    t_effect = cmds | transact[g] | collect

    return (t_effect
            | insert["unpacking_template"][template]
            | insert["keep_internal_ids"][True]
            | collect)

def dispatch_ror_graph(g, x):
    return x | encode[g]

from ...pyzef import main
main.Graph.__ror__ = dispatch_ror_graph


def encode_cmd(obj, gen_id_state: GenIDState):
    from .wish_tagging import tagging_rules as default_rules

    all_ensure_tag_types = Union[tuple(default_rules | map[first] | collect)]

    encode_rules = [
        (RelationTriple, encode_relation_triple),
        (OldStyleRelationTriple, encode_OS_relation_triple),
        (List, encode_list),
        (PleaseTerminate, encode_terminate),

        # Fallback to ensure tag
        (all_ensure_tag_types, encode_ensure_tag_fallback[default_rules]),
    ]

    return (obj, gen_id_state) | match_rules[[
        *encode_rules,
        (Any, not_implemented_error["Don't know how to encode object for shorthand syntax"]),
    ]] | collect

def make_name_for_rt(rt, gen_id_state):
    names = names_of_raet(rt)
    if len(names) == 0:
        me,gen_id_state = gen_internal_id(gen_id_state)
        rt = rt[me]
    else:
        me = names[0]

    return rt,me,gen_id_state

def encode_relation_triple(obj, gen_id_state):
    s,rt,t = obj

    s_cmds, s_tag, gen_id_state = encode_cmd(s, gen_id_state)
    t_cmds, t_tag, gen_id_state = encode_cmd(t, gen_id_state)

    rt,me,gen_id_state = make_name_for_rt(rt, gen_id_state)

    cmds = s_cmds + t_cmds + [(s_tag, rt, t_tag)]
    tags = [s_tag, me, t_tag]
    return cmds, tags, gen_id_state

def encode_OS_relation_triple(obj, gen_id_state):
    if len(obj) == 3:
        s,rt,t = obj

        s_cmds, s_tags, gen_id_state = encode_cmd(s, gen_id_state)
        t_cmds, t_tags, gen_id_state = encode_cmd(t, gen_id_state)

        cmds = s_cmds + t_cmds
        tags = (s_tags, None, t_tags)

        if not isinstance(s_tags, List):
            s_tags = [s_tags]
        if not isinstance(t_tags, List):
            t_tags = [t_tags]
        for s_tag in s_tags:
            for t_tag in t_tags:
                cmds += [(s_tag, rt, t_tag)]

        return cmds, tags, gen_id_state
    elif len(obj) == 2:
        s,rels = obj
        s_cmds, s_tag, gen_id_state = encode_cmd(s, gen_id_state)

        cmds = s_cmds

        rel_names = []
        for rt,t in rels:
            rt,rt_name,gen_id_state = make_name_for_rt(rt, gen_id_state)
            t_cmds, t_tag, gen_id_state = encode_cmd(t, gen_id_state)
            rel_names += [(rt_name, t_tag)]
            cmds += t_cmds
            cmds += [(s_tag, rt, t_tag)]

        tags = (s_tag, rel_names)
            
        return cmds, tags, gen_id_state
    else:
        raise Exception("Shouldn't get here")

def encode_list(obj, gen_id_state):
    out_cmds = []
    out_tags = []
    for item in obj:
        this_cmds,this_tag,gen_id_state = encode_cmd(item, gen_id_state)
        out_cmds += this_cmds
        out_tags += [this_tag]
    return out_cmds, out_tags, gen_id_state

def encode_terminate(obj, gen_id_state):
    return [obj], force_as_id(obj.target), gen_id_state

@func
def encode_ensure_tag_fallback(obj, tagging_rules, gen_id_state):
    obj,id,gen_id_state = (obj, gen_id_state) | match_rules[[
        *tagging_rules,
        (Any, not_implemented_error["Don't know how to ensure a tag for object"]),
    ]] | collect

    return [obj], id, gen_id_state
    


def unpack_receipt(template, receipt, glike_frame):
    if isinstance(glike_frame, GraphSlice):
        return unpack_receipt_graphslice(template, receipt, glike_frame)
    elif isinstance(glike_frame, FlatGraph):
        return unpack_receipt_flatgraph(template, receipt, glike_frame)
    else:
        raise Exception(f"Unexpected frame-like: {glike_frame}")

def unpack_receipt_flatgraph(template, receipt, fg):
    if isinstance(template, List):
        return tuple((unpack_receipt_flatgraph(el, receipt, fg) for el in template))
    if isinstance(template, WishID):
        if isinstance(template, Variable) and isinstance(template.name, OriginallyUserID):
            template = template.name.obj
        # This should probably be an atom already in the receipt
        # return Atom(receipt[template])
        return fg[template]
    if isinstance(template, EternalUID):
        return fg[template]
    if isinstance(template, Nil):
        return None
    if isinstance(template, DelegateRef):
        return fg[template]
    if isinstance(template, WrappedValue):
        return fg[template]
    raise Exception(f"Should not get here - unknown type in unpacking template: {template}")

def unpack_receipt_graphslice(template, receipt, gs):
    if isinstance(template, List):
        return tuple((unpack_receipt(el, receipt, gs) for el in template))
    if isinstance(template, WishID):
        if isinstance(template, Variable) and isinstance(template.name, OriginallyUserID):
            template = template.name.obj
        # This should probably be an atom already in the receipt
        return Atom(receipt[template])
    if isinstance(template, EternalUID):
        from ..graph_slice import get_instance_rae
        out = get_instance_rae(template, gs, allow_tombstone=True)
        assert out is not None, f"Out is None! Coming from {template}"
        return Atom(out)
    if isinstance(template, Nil):
        return None
    if isinstance(template, DelegateRef):
        out = to_delegate(template, gs)
        assert out is not None
        return out
    if isinstance(template, WrappedValue):
        val = internals.val_as_serialized_if_necessary(template)
        out = Graph(gs).get_value_node(val)
        assert out is not None
        return out | in_frame[gs] | collect
    raise Exception(f"Should not get here - unknown type in unpacking template: {template}")