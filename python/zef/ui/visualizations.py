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

def generate_mapping_for_et(et, g):
    z = delegate_of(et, g)
    connected_rels = z | out_rels[RT] | collect
    mapping =  connected_rels | map[lambda r: (rae_type(r), str(rae_type(r)))] | func[dict] | collect
    return mapping


def generate_rows(nodes, mapping):
    @func
    def value_or_type(node):
        if is_a(node, AET):
            val =  node | value | collect
            if isinstance(val, bool):
                return ["❌","✅"][val]
            return val
        else:
            default = repr(rae_type(node))
            if len(Outs(node, RT.Name)) == 1:
                return node | Out[RT.Name]  | value | collect
            
            return default
    
    @func
    def traverse_rt(rt, et):
        outs = et | Outs[rt] | collect
        if len(outs) == 1:
            return outs[0] | value_or_type | func[str] | collect
        elif len(outs) > 1:
            return outs | map[value_or_type | func[str]] | join[','] | collect
        else:
            return "❔"

    return nodes | map[lambda et: list(mapping.keys()) | map[traverse_rt[et]] | func[tuple] | collect] | collect


@func
def generate_table_for_single_type_zrs(selected_et, zrs, limit = 10):
    if len(zrs) < 0: return Frame()

    g = Graph(zrs[0])
    mapping = generate_mapping_for_et(selected_et, g)
    zrs = zrs[:limit]

    colors = ["#ff7e74","#ffe596","#81d76d","#57acf9","#baaee1"]
    row_styles  = ["", "dim"]

    rows = generate_rows(zrs, mapping)
    columns = [ Column(Text(c,justify = "center"), header_style = Style(background_color="#2a3240", color = colors[i%len(colors)]), style = Style(color = colors[i%len(colors)])) for i,c in enumerate(mapping.values())]

    title = Frame(Text(f" A List of {len(zrs)} {repr(selected_et)}", bold= True, justify="center"), box ="head")
    table = Table(
        show_edge=False,
        expand=True,     
        show_header=True,
        padding= (1,1,1,1),
        box = 'simple_head',        
        rows = rows,
        cols = columns,
        row_styles=row_styles,
    )  

    return VStack([
        title,
        table
    ])


def generate_table_from_query(query):
    groups = query | filter[is_a[ET]] | group_by[rae_type] |  collect
    tables = groups | map[unpack[generate_table_for_single_type_zrs]] | collect
    return VStack(tables)