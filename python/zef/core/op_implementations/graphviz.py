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

from .. import *
from .._ops import *
from ..VT import Is, Any, Val
from ..internals import VRT

def graphviz_imp(zz, *flags):
    """
    Returns a GraphViz visualization of the graph passed in.
    By default it returns an error if more than 1000 RAEs are to be shown.

    Zef graphs are meta-graphs: relations can themselves have outgoing relations
    which allows mimicing the behavior of property graphs (with nodes/relations 
    allowed to contain "objects" / dictionaries).

    GraphViz does not allow plotting relations out of relations (please let us
    know if we're wrong here and you know a way!). Therefore we need to 
    map the meta-graph onto a larger true graph (each relation becoming a node)
    and subsequently plot this. The downside is the visual clarity:
    It may be hard to see which edges/lines are the true edges and which lines
    are other incoming/outgoing edges to the actual edge.
    
    As a slight improvement, each relation's color is based on it's type: this 
    allows identifying the actual relation as the one with and incoming and
    outgoing arrow of the same color in case of ambiguity.

    ---- Examples ----
    >>> g | now | all | graph_viz | collect
    >>> g | now | all | graph_viz[simplified] | collect     # No extra nodes introduced for relations. Hence: can't show relations out of relations.
    >>> g | all | graph_viz | collect                       # show the full eternal graph


    ---- Signature ----
    List[EZefRef] -> Image
    List[ZefRef] -> Image
    (List[ZefRef], ZefOp) -> Image
    """
    if is_a(zz, FlatGraph): 
        g = Graph()
        zz | transact[g] | run 
        contains_del = any([is_a(b[1], Delegate) for b in zz.blobs])
        zz = g | now | all[RAE] | collect
        if contains_del: zz = zz + (g | blueprint[True] | collect)
    from functools import lru_cache
    try:
        import graphviz
    except:
        msg = """ 
        Error importing the Python 'GraphViz' package.
        Two steps are required:
            1) GraphViz on the system needs to be installed:
                e.g. on Ubuntu 'sudo apt install graphviz'
                e.g. on Mac 'brew install graphviz'
                google otherwise :)
            2) Python package: e.g. 'pip3 install graphviz'
        """
        raise RuntimeError(msg)

    def make_color_generator():
        """
        returns a generator that first picks from a 
        predefined list of colors and switches to generating
        random colors once the list exhausts.
        """
        import random
        color_list = (
            '#115A63',
            '#78C25C',
            '#6f42b3',
            '#2fb6fc',
            '#3e4337',
            '#31BBA5',
            '#4342d5',
            '#5edfcb',
            '#078f01',
            '#2c3167',
            '#402667',
            '#515252',
            '#604862',
        )
        yield from color_list        
        alphabet="0123456789ABCDEF"
        while True:
            r = random.SystemRandom()
            yield '#' + ''.join([r.choice(alphabet) for i in range(6)])
    
    
    def make_color_generator_edges():
        """
        returns a generator that first picks from a 
        predefined list of colors and switches to generating
        random colors once the list exhausts.
        """
        import random
        color_list = (
            '#00171f',
            '#777777',
            '#99374F',
            '#124763',
            '#402667',
            '#604862',
        )
        yield from color_list        
        alphabet="0123456789ABCDEF"
        while True:
            r = random.SystemRandom()
            yield '#' + ''.join([r.choice(alphabet) for i in range(6)])

    color_gen = make_color_generator()
    color_gen_edges = make_color_generator_edges()

    @lru_cache(maxsize=10000)
    def hex_color_for_type(some_type):
        return next(color_gen)      

    @lru_cache(maxsize=10000)
    def hex_color_for_type_edges(some_type):
        return next(color_gen_edges)       
    
    graph_ctor_kwargs = ({} 
        | If[Is[lambda _: KW.neato in flags]][insert['engine']['neato']][identity]
        | collect
    )


    G = graphviz.Digraph('G', **graph_ctor_kwargs)
    if isinstance(zz, Graph): zz = all(zz)
    zz = list(zz)
    if len(zz) == 0:
        return G            # exit early if there is nothing to plot
    if (isinstance(zz, list) and isinstance(zz[0], ZefRef)):
        plotting_eternal_graph = False
    else:
        plotting_eternal_graph = True

    edge_colors = {}
    def nice_name(z: EZefRef, compact_view = False) -> str:
        def val_to_str(z):
            v = value(z)
            if v is None: return 'None'
            elif isinstance(v, str): return f'"{v}"' if len(v) < 20 else f'"{v[:18]}..."'
            else: return str(v)
        def val_to_str_html(z):
            v = value(z)
            if v is None: return 'None'
            elif isinstance(v, str):
                v = f'"{v}"' if len(v) < 20 else f'"{v[:18]}..."'
            else:
                v = str(v)
            import html
            v = html.escape(str(v))
            return v

        if BT(z)==BT.ENTITY_NODE:
            if compact_view and not internals.is_delegate(z): 
                rts = z | out_rels | filter[lambda z: BT(target(z)) == BT.ATTRIBUTE_ENTITY_NODE and not internals.is_delegate(target(z))] | collect
                title = f"{ET(z)!r}"
                return f"""<<TABLE TITLE='{title}' CELLPADDING='0' CELLSPACING='0'>
                <TR><TD>{title}</TD></TR>
                {''.join([f"<TR><TD ALIGN='LEFT'><FONT POINT-SIZE='10'><B>{RT(rt)!r}</B>: {val_to_str_html(target(rt))}</FONT></TD></TR>" for rt in rts])}
                </TABLE>
                >""" 
            return f"{ET(z)!r}"    

        if BT(z)==BT.TX_EVENT_NODE:
            return f"TX"
        if BT(z)==BT.ATTRIBUTE_ENTITY_NODE:
            if internals.is_delegate(z):
                return f"{VRT(z)!r}"        
            else:
                val_maybe = (f"\n▷{val_to_str(z)}") if isinstance(z, ZefRef) else ''
                return f"{AET(z)!r}{val_maybe}"        
        if BT(z)==BT.RELATION_EDGE:
            if compact_view: 
                rts = z | out_rels | filter[lambda z: BT(target(z)) == BT.ATTRIBUTE_ENTITY_NODE] | collect
                title = f"{RT(z)!r}"
                return f"""<<TABLE TITLE='{title}' CELLPADDING='0' CELLSPACING='0' BORDER='0'>
                <TR><TD>{title}</TD></TR>
                {''.join([f"<TR><TD ALIGN='LEFT'><FONT POINT-SIZE='10'>{RT(rt)!r}: {val_to_str_html(target(rt))}</FONT></TD></TR>" for rt in rts])}
                </TABLE>
                >""" 
            return f"{RT(z)!r}"        
        if BT(z)==BT.VALUE_NODE:
            return f"Val({val_to_str(z)})"
        return f"{BT(z)}"[:-5]
    
    def nice_color(z: EZefRef)->str:
        if BT(z) == BT.ENTITY_NODE:
            return hex_color_for_type(ET(z))
        if BT(z) == BT.ATTRIBUTE_ENTITY_NODE:
            if internals.is_delegate(z):
                return hex_color_for_type(VRT(z))
            else:
                return hex_color_for_type(AET(z))
            
        if BT(z) in {BT.TX_EVENT_NODE, BT.NEXT_TX_EDGE}:
            return "#991155"
        if BT(z) == BT.RELATION_EDGE:
            return "#000000" if plotting_eternal_graph else hex_color_for_type_edges(RT(z))
        else:            
            return hex_color_for_type(BT(z))

    def nice_shape(z, compact_view = False):
        if BT(z) == BT.TX_EVENT_NODE:
            return 'triangle'
        elif BT(z) == BT.ENTITY_NODE:
            if compact_view: return 'note'
            return 'oval'            
        elif BT(z) == BT.ATTRIBUTE_ENTITY_NODE:
            return 'rectangle'
        else:
            return 'diamond'

    def nice_style(z):
        if BT(z) in {BT.ENTITY_NODE, BT.ATTRIBUTE_ENTITY_NODE}:
            return 'dashed' if internals.is_delegate(z) else 'filled'
        else:
            return ''
    
    def is_node(z):
        return str(z | func[BT] | collect)[-5:] == '_NODE'
    
    @func
    def add_node(z, compact_view=False):
        G.node(str(index(z)),  label=nice_name(z, compact_view), fontsize='12px', color=nice_color(z), shape=nice_shape(z,compact_view), style=nice_style(z))

    @func 
    def add_edge_as_gv_node(z, compact_view=False):
        col = nice_color(z)
        edge_colors[index(z)] = col
        G.node(str(index(z)), label=nice_name(z, compact_view), color=col, shape='none', style='', fontsize='10px', fontcolor=col)
        
    def add_gv_edges(z):
        # ------- hacky / dirty way for now: just try going to source / target and swallow error if anything fails :| --------
        try:
            col = edge_colors[index(z)]   # this was determined above
            additional = {"style": "dashed"} if is_a(z, Delegate) else {}
            G.edge(str(index(z | source | collect)), str(index(z)), color=col, dir="back", arrowtail="dot", **additional)
            G.edge(str(index(z)), str(index(z | target | collect)), color=col, dir="forward", arrowhead="normal", **additional)
        except Exception as e:
            #print(f"add_gv_edges failed for {z} e={e}")
            pass

    nodes, edges = zz | group_by[is_node][(True,False)] | map[second] |  collect
    if 'expand' in flags or plotting_eternal_graph:
        nodes | for_each[add_node]
        edges | for_each[add_edge_as_gv_node]
    else:
        nodes = nodes | filter[lambda z: BT(z) not in  {BT.ATTRIBUTE_ENTITY_NODE,BT.VALUE_NODE}] | collect
        nodes | for_each[add_node[True]]
        edges  = edges | filter[lambda z: BT(target(z)) != BT.ATTRIBUTE_ENTITY_NODE] | collect
        edges | for_each[add_edge_as_gv_node[True]]


    edges | for_each[add_gv_edges]

    # Convert the Graphviz Graph to svg string and then to an Image
    return Image(G.pipe(format='svg'))


def graphviz_tp(x):
    return VT.Any           # return a VT.Image in future?
