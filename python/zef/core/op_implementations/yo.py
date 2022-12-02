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

from typing import Tuple
from .._ops import *
from datetime import datetime, timezone, timedelta
from functional import seq
from ..internals import is_delegate, root_node_blob_index, BlobType
from .._core import *
from .. import internals
from ..VT import *

def yo_implementation(x, display=True):
    import inspect
    from ..fx.fx_types import FXElement, _group_types
    from ..fx import _effect_handlers
    
    if display:
        import sys
        file = sys.stdout
    else:
        import io
        file = io.StringIO()

    if is_a(x, (EZefRef | ZefRef) & BT.TX_EVENT_NODE):
        print(tx_view(x), file=file)
    elif is_a(x, EZefRef):
        if is_delegate(x):
            print("\n\n\n**************  delegate EZefRef ********************\n\n", file=file)
        else:
            print(eternalist_view(x), file=file)
    elif is_a(x, ZefRef):
        if is_delegate(x):
            print("\n\n\n**************  delegate ZefRef ********************\n\n", file=file)
        else:
            print(eternalist_view(x), file=file)
    elif is_a(x, Graph):
        print(graph_info(x), file=file)
    elif "pyzef.Graph" in str(type(x)):
        # This is because of monkeypatching
        print(graph_info(x), file=file)

    elif is_a(x, GraphSlice):
        return yo_implementation(x | to_tx | collect, display)

    elif is_a(x, ZefOp):
        if len(x.el_ops) == 1:
            from .dispatch_dictionary import _op_to_functions
            if len(x.el_ops) == 1:
                f = _op_to_functions[x.el_ops[0][0]][0]
                doc = inspect.getdoc(f)
                doc = doc if doc else f"No docstring found for given {x}!"
                print(doc, file=file)
        else:
            from .yo_ascii import make_op_chain_ascii_output
            print(make_op_chain_ascii_output(x), file=file)
    
    elif type(x) == FXElement:
        handler = _effect_handlers.get(x.d, None)
        if handler:
            doc = inspect.getdoc(handler)
            doc = doc if doc else f"No docstring found for given {x}!"
            print(doc, file=file)
        else:
            print(x, file=file)
    elif type(x) in _group_types:
        summary = [f"Summary of {x._name} group Class:"]
        for k,v in _effect_handlers.items():
            if k[0] == x._name and k[1] in x.__dir__()| filter[lambda x: not x.startswith("_")] | collect:
                summary.append(f"{fill_str_to_length(k[1], 20)} =>  {v}")
        print('\n'.join([str(i) for i in summary]), file=file)
        
    else:
        print(x, file=file)

    if display:
        return None
    else:
        return file.getvalue()

def yo_type_info(op, curr_type):
    return String



##############################################################
# * Implementation details below
#------------------------------------------------------------
def tx_view(zr_or_uzr) -> str:
    uzr = to_ezefref(zr_or_uzr)

    def value_assigned_string_view(lst):
        def get_aet_values(x):
            return f"    {fill_str_to_length(value_previous_of_aet(x, uzr), 25)} ({uid(x)})        {value_of_aet_at_tx(x, uzr)}"
        return f"{len(lst)}x:     {zr_type(lst[0])}\n" + "\n".join(seq(lst).map(get_aet_values)) + "\n\n"

    def instantiated_or_terminated_string_view(lst):
        return (f"{len(lst)}x:     ({zr_type(lst[0])})\n"
                + "\n".join(seq(lst).map(lambda x: f"    ({uid_or_value_hash(x)})"))
                + "\n\n")

    def tx_block_view(uzrs, fn):
        return ("".join(
                seq(uzrs)
                .group_by(lambda z: zr_type(z))
                .map(lambda x: x[1])
                .map(fn)))

    return f"""
======================================================================================================================
================================================== Transaction View ==================================================
================================= Transaction Time: {readable_datetime_from_tx_uzr(zr_or_uzr)} ==================================
======================================================================================================================

uid:                    {uid(uzr)}
blob index:             {index(uzr)}
current owning graphs:  {uid(Graph(uzr))} {f", name tags: ({','.join(Graph(uzr).graph_data.tag_list)})"
    if Graph(uzr).graph_data.tag_list else ""}
total affected:         {length(uzr | events)}
total instantiations:   {length(uzr | events[Instantiated])}
total assignments:      {length(uzr | events[Assigned])}
total terminations:     {length(uzr | events[Terminated])}
\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Instantiations ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
{tx_block_view(uzr | events[Instantiated] | map[absorbed | first] | collect, instantiated_or_terminated_string_view)} 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Value Assignments ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
{tx_block_view(uzr | events[Assigned]  | map[absorbed | first] | collect, value_assigned_string_view)} 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Terminations ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
{tx_block_view(uzr | events[Terminated] | map[absorbed | first] | collect, instantiated_or_terminated_string_view)} 
"""

def uid_or_value_hash(x):
    if BT(x) == BT.VALUE_NODE:
        return f"hash: {internals.value_hash(value(x))})"
    if internals.has_uid(to_ezefref(x)):
        return uid(x)
    raise Exception(f"Don't know how to represent UID of object ({x})")


def indent_lines(s: str, indent_amount=4):
    m = ' ' * indent_amount
    return m + s.replace('\n', '\n' + m)


def fill_str_to_length(s: str, target_length):
    if s is None:
        s = '/'
    return s + ' ' * max(0, target_length - len(s))

def maybe_elide(s: str, max_lines=10):
    # Shorten to maximum number of lines if too big and short_output mode is on
    if not zwitch.short_output():
        return s

    if s.count('\n') <= max_lines:
        return s

    def indexnth(s, c, n, ind=0):
        ind = s.index(c, ind)
        if n == 1:
            return ind
        else:
            return indexnth(s, c, n-1, ind+1)
    def rindexnth(s, c, n, ind=None):
        ind = s.rindex(c, 0, ind)
        if n == 1:
            return ind
        else:
            # Note: end=ind means it doesn't include ind in the next search
            return rindexnth(s, c, n-1, ind)

    start = indexnth(s, '\n', max_lines // 2)
    end = rindexnth(s, '\n', max_lines // 2)

    return s[:start] + "\n......<snip>......\n" + s[end+1:]

    


def zr_type(uzr):
    return repr(rae_type(uzr))


def value_previous_of_aet(aet, tx) -> str:
    try:
        if BT(aet) == BT.ATTRIBUTE_ENTITY_NODE:
            prev_tx = tx << BT.NEXT_TX_EDGE | collect
            val = aet | value[prev_tx] | collect  # If prev_tx == BT.ROOT_NODE it will be caught by except clause
            return f" [previous val: {'/' if not val else str(val)}]"
        else:
            return ''
    except:
        return str(f" [previous val: NA]")


def value_of_aet_at_tx(aet, tx) -> str:
    latest_or_current = "latest" if (tx is now or len(tx | Outs[BT.NEXT_TX_EDGE] | collect) == 0) else "current"
    try:
        if tx is now:
            tx = Graph(aet) | now | collect
        if BT(aet) == BT.ATTRIBUTE_ENTITY_NODE:
            val = aet | value[tx] | collect 
            return f" [{latest_or_current} val: {'/' if val is None else str(val)}]"
        else:
            return ''
    except:
        return str(f" [{latest_or_current} val: NA]")


def readable_datetime_from_tx_uzr(uzr_tx) -> str:
    uzr_tx = to_ezefref(uzr_tx)
    if index(uzr_tx) == root_node_blob_index():
        return '/'
    return f'GraphSlice {str(graph_slice_index(to_graph_slice(uzr_tx)))}: {str(uzr_tx | time | collect)}'



def eternalist_view(zr_or_uzr) -> str:
    reference_frame = zr_or_uzr | frame | collect if isinstance(zr_or_uzr, ZefRef) else Graph(zr_or_uzr) | now | collect
    reference_tx = reference_frame | to_tx | collect
    uzr = to_ezefref(zr_or_uzr)
    return f"""
======================================================================================================================
================================================== {'Historical ' if isinstance(zr_or_uzr, ZefRef) else 'Most Recent'} View ==================================================
===================================== {'Seen from' if isinstance(zr_or_uzr, ZefRef) else 'Up to timeslice'}: {readable_datetime_from_tx_uzr(reference_tx)}  =================================
======================================================================================================================

uid:                    {uid(uzr)}
blob index:             {index(uzr)}
type:                   {zr_type(uzr)}
current owning graphs:  {Graph(uzr) | uid | collect} {f", name tags: ({','.join(Graph(uzr).graph_data.tag_list)})"
    if Graph(uzr).graph_data.tag_list else ""}
other graphs viewing:   /
instantiation:          {readable_datetime_from_tx_uzr(uzr | instantiation_tx | collect)}
termination:            {readable_datetime_from_tx_uzr(uzr | termination_tx | collect) if not exists_at(uzr, reference_frame) else '/'}
{src_dst_info(uzr) if BT(uzr) == BT.RELATION_EDGE else ''}
""" + \
           indent_lines(relations_view(zr_or_uzr)) + "\n" + \
           indent_lines(maybe_elide(timeline_view(zr_or_uzr), 30))


def src_dst_info(uzr) -> str:
    return f"""
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Source and Target ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    ({uzr | source | uid | collect}: {zr_type(uzr | source | collect)})-----(z)----->({uzr | target | uid | collect}: {zr_type(uzr | target | collect)})
"""


def relations_view(zr_or_uzr) -> str:
    reference_frame = zr_or_uzr | frame | collect if isinstance(zr_or_uzr, ZefRef) else Graph(zr_or_uzr) | now | collect
    reference_tx = reference_frame | to_tx | collect
    def compose_triple_name(rel, z):
        if to_ezefref(rel | source) == to_ezefref(z):
            return ('outs', zr_type(rel), zr_type(rel | target | collect))
        else:
            return ('ins', zr_type(rel), zr_type(rel | source | collect))

    edges_to_show = ( 
        (zr_or_uzr | in_rels[RT], zr_or_uzr | out_rels[RT]) 
        | concat 
        | (identity if isinstance(zr_or_uzr, ZefRef) else filter[is_zefref_promotable])
        | collect
        )                     

    list_of_and_and_outs = (seq(edges_to_show)
                            .group_by(lambda rel: compose_triple_name(rel, zr_or_uzr))
                            .sorted(lambda rel_group: len(rel_group[1])))  # the second element is the list of zefrefs

    def wrap_to_fixed_length(in_str, total_length=51):
        m = total_length - len(in_str)
        return f"{'-' * round(m / 2 - 0.1)}-({in_str})-{'-' * round(m / 2 + 0.1)}"  # hacky wey to get ceil / floor

    def single_relation_type_view(triple_type_and_list) -> str:
        l_symb = '<' if triple_type_and_list[0][0] == 'ins' else '-'
        r_symb = '-' if triple_type_and_list[0][0] == 'ins' else '>'
        newline = '\n'
        return f"""\
{len(triple_type_and_list[1])}x:     (z:{zr_type(zr_or_uzr)}) {l_symb}---{wrap_to_fixed_length(triple_type_and_list[0][1])}--{r_symb} ({triple_type_and_list[0][2]})
{maybe_elide(''.join([f"            (z) {l_symb}---({ro | to_ezefref | uid | collect})---{r_symb} ({ro | to_ezefref | (source if l_symb == '<' else target) | uid | collect}{value_of_aet_at_tx(ro | (source if l_symb == '<' else target) | collect,  reference_tx | to_ezefref | collect)}){newline}" for ro in triple_type_and_list[1]]))}
"""

    return f"""\
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Incoming & Outgoing Relations ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

{''.join([single_relation_type_view(trip) for trip in list_of_and_and_outs])}
"""


def timeline_view(zr_or_uzr) -> str:
    uzr = to_ezefref(zr_or_uzr)
    reference_tx = to_tx(frame(zr_or_uzr)) if isinstance(zr_or_uzr, ZefRef) else Graph(zr_or_uzr) | now | to_tx | collect

    # Function: visually list all transactions of affecting a certain entity
    # traverse the low level graph here to gather instantiation, termination and value assignment edges
    def generate_low_level_description(low_level_edge_uzr):
        if BT(low_level_edge_uzr) == BT.INSTANTIATION_EDGE:
            return f'=========  <---- Instantiation'
        # if low_level_edge_uzr | BT == BT.CLONE_REL_ENT_EDGE:
        #     return f'<<<<<<<<<  <---- Cloned: ....'
        if BT(low_level_edge_uzr) == BT.ATOMIC_VALUE_ASSIGNMENT_EDGE:
            return f'    x      <---- Value assignment: {value_of_aet_at_tx(uzr,low_level_edge_uzr | source | collect)}'  # get the value: pass the respective tx
        if BT(low_level_edge_uzr) == BT.ATTRIBUTE_VALUE_ASSIGNMENT_EDGE:
            return f'    x      <---- Value assignment: {value_of_aet_at_tx(uzr,low_level_edge_uzr | source | collect)}'  # get the value: pass the respective tx
        if BT(low_level_edge_uzr) == BT.TERMINATION_EDGE:
            return f'=========  <---- Termination'
        raise Exception(f"To be fixed with new lineage system ({BT(low_level_edge_uzr)})")

    # Function: visually list all details about a single relation.
    # Output is affected by the "in" or "out" direction and also the state of the rt_uzr.
    def generate_rt_description(rt_uzr: EZefRef, direction: str, state: str) -> str:
        connected_et_or_aet = rt_uzr | source | collect if direction == "in" else rt_uzr | target | collect 
        sign = "<<-" if direction == "in" else "->>"
        instantiation_or_termination_tx = rt_uzr | instantiation_tx | collect if state == "Instantiated" else rt_uzr | termination_tx | collect
        return (f'    |            {fill_str_to_length(f"{state} Relation: ",50)}[{readable_datetime_from_tx_uzr(instantiation_or_termination_tx)}]\n'
                f'    |            {fill_str_to_length(f"({zr_type(rt_uzr)})",50)}({zr_type(connected_et_or_aet)}) {value_of_aet_at_tx(connected_et_or_aet,reference_tx|to_ezefref | collect)}\n'
                f'    |{sign if direction == "in" else "-"}---------({rt_uzr|uid|collect})---------------{sign if direction == "out" else "-"}({connected_et_or_aet | uid | collect})')

    # Appends directed relations to the all_edges list which is in the scope of this function
    def add_directed_rt_to_list(edges, direction: str) -> None:
        for e in edges:
            all_edges.append((e, direction, "Instantiated"))
            if e | termination_tx | is_a[BT.TX_EVENT_NODE] | collect:
                all_edges.append((e, direction, "Terminated"))

    # Returns the time of the a transaction depending on the BT type and the edge_state.
    def edge_time_by_type(edge_info_triple: tuple) -> Time:
        edge, _, edge_state = edge_info_triple
        if BT(edge) == BT.RELATION_EDGE:
            return (edge | instantiation_tx | time | collect) if edge_state == "Instantiated" else (edge | termination_tx | time | collect)
        return edge | source | time | collect

    reference_tx_gs_index = graph_slice_index(to_graph_slice(reference_tx))
    rel_ent_inst_edge = uzr | in_rel[BT.RAE_INSTANCE_EDGE]
    all_edges = [(e, "low_lvl", "") for e in rel_ent_inst_edge | in_rels[BT] | collect]
    add_directed_rt_to_list(uzr | in_rels[RT] | filter[lambda z: BT(z) != BT.RAE_INSTANCE_EDGE] | collect, "in")
    add_directed_rt_to_list(uzr | out_rels[RT] | collect, "out")
    all_edges.sort(key=edge_time_by_type)
    descriptions_and_txs = []
    termination_edge_description = [""]
    # Loop over all edges and filter based on edge_type and on the reference_tx value
    for e in all_edges:
        edge, edge_type, edge_state = e
        if edge_type == "low_lvl" and edge | source | to_graph_slice | graph_slice_index | collect <= reference_tx_gs_index:
            edge_description = (f"{fill_str_to_length(generate_low_level_description(edge), 65)}  "
                                f"[{readable_datetime_from_tx_uzr(edge | source | collect)}]", edge)
            if BT(edge) == BT.TERMINATION_EDGE:
                termination_edge_description = edge_description
            else:
                descriptions_and_txs.append(edge_description)
        else:
            if (edge_state == "Instantiated" and graph_slice_index(edge | instantiation_tx | frame | collect) <= reference_tx_gs_index) or\
                    (edge_state == "Terminated" and graph_slice_index(edge | termination_tx | frame | collect) <= reference_tx_gs_index):
                descriptions_and_txs.append(
                    (f"{fill_str_to_length(generate_rt_description(edge, edge_type, edge_state), 54)}", edge))

    filler = '\n    |\n    |\n'
    return f"""\
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Timeline ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

{termination_edge_description[0]}
    ^
   ^^^
    |
{filler.join([x[0] for x in descriptions_and_txs[1:][::-1]])}
    |
    |
{descriptions_and_txs[0][0]}
-----------------------------------------------------------------------------------------------------------------\n
"""


def graph_info(g) -> str:
    bl = blobs(g)
    grouped_by_bts =  dict(
        bl 
        | filter[Not[is_delegate]]
        | group_by[BT] 
        | filter[lambda x: x[0] in {BT.TX_EVENT_NODE, BT.ENTITY_NODE, BT.ATTRIBUTE_ENTITY_NODE, BT.RELATION_EDGE}] 
        | collect
    )
    grouped_by_bts[BT.TX_EVENT_NODE] = grouped_by_bts.get(BT.TX_EVENT_NODE, [])
    grouped_by_bts[BT.ENTITY_NODE] = grouped_by_bts.get(BT.ENTITY_NODE, [])
    grouped_by_bts[BT.ATTRIBUTE_ENTITY_NODE] = grouped_by_bts.get(BT.ATTRIBUTE_ENTITY_NODE, [])
    grouped_by_bts[BT.RELATION_EDGE] = grouped_by_bts.get(BT.RELATION_EDGE, [])

    def simple_lengths(g) -> str:
        return (f"{g.graph_data.write_head} blobs, "
                + str(length(grouped_by_bts[BT.TX_EVENT_NODE])) + " TXs, "
                + str(length(grouped_by_bts[BT.ENTITY_NODE])) + " ETs, "
                + str(length(grouped_by_bts[BT.ATTRIBUTE_ENTITY_NODE])) + " AETs, "
                + str(length(grouped_by_bts[BT.RELATION_EDGE])) + " RTs"
                )

    from builtins import round
    return f"""
======================================================================================================================
=============================================== Graph {uid(g)} ===============================================
===================================== Seen from: {str(now())} =========================================
======================================================================================================================

instantiation:          {readable_datetime_from_tx_uzr(g | all[TX] | first | collect) if length(g | all[TX] | collect) > 0 else "NA"}
last change:            {readable_datetime_from_tx_uzr(g | all[TX] | last | collect)  if len(g | all[TX] | collect) > 0 else "NA"}
current tags:           {g.graph_data.tag_list}
summary:                {simple_lengths(g)}
size:                   {round((g.graph_data.write_head * 16) / 1E6, 3)}MB

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Atomic Entities ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
{type_summary_view(grouped_by_bts[BT.ATTRIBUTE_ENTITY_NODE], g, BT.ATTRIBUTE_ENTITY_NODE)}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Entities ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
{type_summary_view(grouped_by_bts[BT.ENTITY_NODE], g, BT.ENTITY_NODE)}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^  Relations ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
{type_summary_view(grouped_by_bts[BT.RELATION_EDGE], g, BT.RELATION_EDGE)}
"""


def type_summary_view(bl, g: Graph, bt_filter: BlobType) -> str:
    def aet_et_rt_string_view(summary: Tuple[int, int, tuple]) -> str:
        aet_or_et, total, alive = summary[0], summary[1], summary[2]
        return f"[{fill_str_to_length(str(f'{total} total, {alive} alive] '), 30)}{aet_or_et}\n"

    def triple_string_view(summary: Tuple[int, int, tuple]) -> str:
        triple, total, alive = summary[0], summary[1], summary[2]
        return (f"       [{fill_str_to_length(str(f'{total} total, {alive} alive] '), 30)}"
                f"({triple[0]!r}, {triple[1]!r}, {triple[2]!r})\n")

    def find_alive_count_of_triple(triple: Tuple[str]) -> int:
        d_top = delegate_of(triple[1], g)
        return (d_top
         | Outs[BT.TO_DELEGATE_EDGE]
         | filter[And[source | rae_type | equals[triple[0]]]
                     [target | rae_type | equals[triple[2]]]]
         | map[now | all | length]
         | sum
         | collect)

    def rt_block_view(rt: str, total_count: int) -> str:
        rt = RT(rt)
        total_alive = 0
        def find_triple_count_and_inc(trip):
            nonlocal total_alive
            alive = find_alive_count_of_triple(trip)
            total_alive += alive
            return alive

        triples_str_views = (
            grouped_triples
            | filter[lambda x: x[0][1] == rt]
            | map[lambda x: (x[0], len(x[1]), find_triple_count_and_inc(x[0]))]
            | map[triple_string_view]
            | join[""]
            | collect
        )
        return aet_et_rt_string_view((repr(rt), total_count, total_alive)) + triples_str_views

    filtered_blobs = bl | filter[lambda b: not is_a(b, Delegate)]
    if bt_filter == BT.RELATION_EDGE:
        grouped_triples = filtered_blobs | map[lambda z: (rae_type(z | source | collect), rae_type(z), rae_type(z | target | collect))] | group_by[lambda x: x] | collect
    else: grouped_triples = None

    return (
        [z for z in filtered_blobs]
        | group_by[zr_type]
        # | map[lambda x: (x[0], len(x[1]), len(x[1][0] | delegate_of | now | all | collect))]
        | map[lambda x: (x[0], len(x[1]), len(x[1][0] | delegate_of | match[
            (Nil, always[[]]),
            (Any, now | all | filter[bt_filter])
        ] | collect))]
        | map[lambda x: rt_block_view(x[0][3:], x[1]) if bt_filter == BT.RELATION_EDGE else aet_et_rt_string_view(x)]
        | join[""]
        | collect
    )
# +\
# indent_lines(relations_view(zr_or_uzr)) + "\n" +\
# indent_lines(timeline_view(zr_or_uzr))
# def src_dst_info(uzr) -> str:
#     return f"""
#     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Source and Target ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#     ({uzr | source | uid}: {zr_type(uzr | source)})-----(z)----->({uzr | target | uid}: {zr_type(uzr | target)})
# """
