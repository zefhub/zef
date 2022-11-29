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

from . import *
from ..core import *
from ..ops import *

# -----------------------Table View------------------------------------------
def generate_mapping_for_et(et, g):
    z = delegate_of(et, g)
    connected_rels = z | out_rels[RT] | collect
    mapping =  connected_rels | map[rae_type] | collect
    # TODO, should this be removed and handled with returning empty table?
    if len(mapping) < 1: raise Exception(f"Couldn't find any out RTs from {et}'s delegate.")
    return mapping

@func
def value_or_type(node):
    if is_a(node, AttributeEntity):
        val =  node | value | collect
        if isinstance(val, bool):
            return ["❌","✅"][val]
        return val
    else:
        default = f"({repr(rae_type(node))})"
        if len(Outs(node, RT.Name)) == 1:
            return f"{node | Out[RT.Name]  | value | collect}{default}"
        
        return default

@func
def traverse_rt(rt, et, traverse_op = Outs):
    outs_or_ins = et | traverse_op[rt] | collect
    if len(outs_or_ins) == 1:
        return outs_or_ins[0] | value_or_type | func[str] | collect
    elif len(outs_or_ins) > 1:
        return outs_or_ins | map[value_or_type | func[str]] | join[','] | collect
    else:
        return "-" 

def generate_rows(nodes, mapping, et_type = None):
    if et_type: return nodes | map[lambda et: mapping | map[traverse_rt[et]] | prepend[et_type] | func[tuple] | collect] | collect
    return nodes | map[lambda et: mapping | map[traverse_rt[et]] | func[tuple] | collect] | collect


@func
def generate_table_for_single_type_zrs(selected_et, zrs, expand, limit):
    g = Graph(zrs[0])
    mapping = generate_mapping_for_et(selected_et, g)
    mapping_values = [str(m) for m in mapping]

    zrs = zrs | take[limit] | collect
    padding = [(0,0,0,0),(1,1,1,1)][expand]
    

    colors = ["#ff7e74","#ffe596","#81d76d","#57acf9","#baaee1"]
    row_styles  = ["", "dim"]

    rows = generate_rows(zrs, mapping)
    columns = [ Column(Text(c,justify = "center"), header_style = Style(background_color="#2a3240", color = colors[i%len(colors)]), style = Style(color = colors[i%len(colors)]))
                for i,c in enumerate(mapping_values)]

    title = Frame(Text(f" A List of {len(zrs)} {repr(selected_et)}", bold= True, justify="center"), box ="simple_head")
    table = Table(
        show_edge=False,
        expand=True,     
        padding = padding,
        show_header=True,
        box = 'simple_head',        
        rows = rows,
        cols = columns,
        row_styles=row_styles,
    )  

    return VStack([
        title,
        table
    ])

@func
def generate_table_for_zrs(groups, expand, limit):
    if len(groups) == 0: return Frame()
    if len(groups) == 1: return generate_table_for_single_type_zrs(*groups[0], expand, limit)


    g = Graph(groups[0][1][0])
    mapping = groups | map[lambda tup: generate_mapping_for_et(tup[0], g)] | concat  | collect
    
    padding = [(0,0,0,0),(1,1,1,1)][expand]

    colors = ["#ff7e74","#ffe596","#81d76d","#57acf9","#baaee1"]
    row_styles  = ["", "dim"]

    rows =  groups | map[lambda tup: generate_rows(tup[1], mapping, str(tup[0]))] | concat | take[limit] | collect

    mapping = ["ET"] + mapping
    mapping_values = [str(m) for m in mapping]
    columns = [ Column(Text(c,justify = "center"), header_style = Style(background_color="#2a3240", color = colors[i%len(colors)]), style = Style(color = colors[i%len(colors)])) for i,c in enumerate(mapping_values)]

    title = Frame(Text(f"Table of {','.join([str(g[0]) for g in groups])}", bold= True, justify="center"), box ="simple_head")
    table = Table(
        show_edge=False,
        expand=True,     
        padding = padding,
        show_header=True,
        box = 'simple_head',        
        rows = rows,
        cols = columns,
        row_styles=row_styles,
    )  

    return VStack([
        title,
        table
    ])

@func
def generate_table_from_query(query, expand = False, limit=20):
    groups = query | filter[is_a[Entity]] | group_by[rae_type] |  collect
    table = generate_table_for_zrs(groups, expand, limit)
    return VStack([table])


#------------------------Card View------------------------------
def generate_rows_for_node(node, is_out = True):
    traverse_op, traverse_op_end = [(in_rels, Ins), (out_rels, Outs)][is_out]
    return node | traverse_op[RT] | map[rae_type] | func[set] | map[lambda rt: (str(rt), traverse_rt(rt, node, traverse_op_end))] | collect

def generate_rts_table(zr, is_out, expand):
    colors = ["#ff7e74","#ffe596","#81d76d","#57acf9","#baaee1"]
    padding = [(0,0,0,0),(1,1,1,1)][expand]


    rows = generate_rows_for_node(zr, is_out)
    if not rows: return ""
    row_styles  = [colors[i%len(colors)] for i in range(len(rows))]

    column_headers = ["", [" Ins 👈🏻", " Outs 👉🏻"][is_out]]
    columns = [Column(Text(c,justify = "left", style = Style(color = colors[1]))) for i,c in enumerate(column_headers)]

    return Table(
        expand=True,     
        padding= padding,
        box = 'simple_head',        
        rows = rows,
        cols = columns,
        row_styles=row_styles,
    )  

@func
def generate_card(zr, expand = False):
    title = Text(f"{(repr(rae_type(zr)))}", bold= True)
    subtitle = Text(f"({uid(zr)})", bold= True)

    outs_table = generate_rts_table(zr, True, expand)
    ins_table = generate_rts_table(zr, False, expand)

    return Frame(HStack([outs_table, ins_table], expand= True), title = title, subtitle=subtitle,  box="horizontals") 


#--------------------------Ops--------------------------------------
to_table =  generate_table_from_query
to_card  =  generate_card