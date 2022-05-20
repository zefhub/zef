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
    if isinstance(zz, FlatGraph): 
        g = Graph()
        zz | transact[g] | run 
        zz = g | now | all[RAE] | collect
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
        | if_then_else_apply[lambda _: KW.neato in flags][insert['engine']['neato']][identity]
        | collect
    )


    G = graphviz.Digraph('G', **graph_ctor_kwargs)
    if isinstance(zz, Graph): zz = all(zz)
    zz = list(zz)
    if len(zz) == 0:
        return G            # exit early if there is nothing to plot
    if isinstance(zz, ZefRefs) or (isinstance(zz, list) and isinstance(zz[0], ZefRef)):
        plotting_eternal_graph = False
    else:
        plotting_eternal_graph = True

    edge_colors = {}
    def nice_name(z: EZefRef) -> str:
        if BT(z)==BT.ENTITY_NODE:
            return f"ET.{str(ET(z))}"        
        if BT(z)==BT.TX_EVENT_NODE:
            return f"TX"
        if BT(z)==BT.ATOMIC_ENTITY_NODE:
            def val_to_str(z):
                v = value(z)
                if v is None: return 'None'
                elif isinstance(v, str): return f'"{v}"' if len(v) < 20 else f'"{v[:18]}..."'
                else: return str(v)
            val_maybe = '' if isinstance(z, EZefRef) else (f"\n▷{val_to_str(z)}")
            return f"AET.{AET(z)}{val_maybe}"        
        if BT(z)==BT.RELATION_EDGE:
            return f"RT.{str(RT(z))}"        
        else:
            return f"BT.{str(BT(z))}"[:-5]
    
    def nice_color(z: EZefRef)->str:
        if BT(z) == BT.ENTITY_NODE:
            return hex_color_for_type(ET(z))
        if BT(z) == BT.ATOMIC_ENTITY_NODE:
            return hex_color_for_type(AET(z))
            
        if BT(z) in {BT.TX_EVENT_NODE, BT.NEXT_TX_EDGE}:
            return "#991155"
        if BT(z) == BT.RELATION_EDGE:
            return "#000000" if plotting_eternal_graph else hex_color_for_type_edges(RT(z))
        else:            
            return hex_color_for_type(BT(z))

    def nice_shape(z):
        if BT(z) == BT.TX_EVENT_NODE:
            return 'triangle'
        elif BT(z) == BT.ENTITY_NODE:
            return 'oval'            
        elif BT(z) == BT.ATOMIC_ENTITY_NODE:
            return 'rectangle'
        else:
            return 'diamond'

    def nice_style(z):
        if BT(z) in {BT.ENTITY_NODE, BT.ATOMIC_ENTITY_NODE}:
            return '' if internals.is_delegate(z) else 'filled'
        else:
            return ''
    
    def is_node(z):
        return str(z | BT | collect)[-5:] == '_NODE'
    
    def add_node(z):
        G.node(str(index(z)),  label=nice_name(z), fontsize='12px', color=nice_color(z), shape=nice_shape(z), style=nice_style(z))
        
    def add_edge_as_gv_node(z):
        col = nice_color(z)
        edge_colors[index(z)] = col
        G.node(str(index(z)), label=nice_name(z), color=col, shape='none', style='', fontsize='10px', fontcolor=col)
        
    def add_gv_edges(z):
        # ------- hacky / dirty way for now: just try going to source / target and swallow error if anything fails :| --------
        try:
            col = edge_colors[index(z)]   # this was determined above
            G.edge(str(index(z | source | collect)), str(index(z)), color=col, dir="back", arrowtail="dot")
            G.edge(str(index(z)), str(index(z | target | collect)), color=col, dir="forward", arrowhead="normal")
        except Exception as e:
            #print(f"add_gv_edges failed for {z} e={e}")
            pass
        
    nodes, edges = zz | group_by[is_node][(True,False)] | map[second] |  collect
    nodes | for_each[add_node]
    edges | for_each[add_edge_as_gv_node]
    edges | for_each[add_gv_edges]

    # Convert the Graphviz Graph to svg string and then to an Image
    return Image(G.pipe(format='svg'))


def graphviz_tp(x):
    return VT.Any           # return a VT.Image in future?
