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
from ..VT import Int, Float, String, Bool, Nil, List, Dict


def handle_int(x: Int, name: str):
    return [AET.Int[name] <= x]


def handle_float(x: Float, name: str):
    return [AET.Float[name] <= x]


def handle_string(x: String, name: str):
    return [AET.String[name] <= x]


def handle_bool(x: Bool, name: str):
    return [AET.Bool[name] <= x]


def handle_nil(x: Nil, name: str):
    return [ET.Nil[name]]


def handle_dict(dd: dict, name: str):    

    def generate_random_name() -> str:
        from random import randint
        return f"_{randint(0, 1000000000)}"

    def make_delta(rt_and_trg):
        rel_name, targ = rt_and_trg
        rt = RT(to_pascal_case(rel_name))
        new_name = generate_random_name()
        new_delta = to_deltas(targ, new_name)
        return [
            *new_delta,
            (Z[name], rt, Z[new_name]),
        ]

    try:
        type_name = to_pascal_case(dd['_type'])     
    except KeyError:
        type_name = 'ZEF_Unspecified'

    return [
    ET(type_name)[name],
    *(dd 
     | items 
     | filter[lambda p: p[0] != '_type']
     | map[make_delta]
     | concat     
     | collect
    )
    ]


def handle_list(x: List, name: str):
    def generate_random_name() -> str:
        from random import randint
        return f"_{randint(0, 100000000)}"

    list_edge_count = 0
    ed_name = lambda m: f"ed_{name}_{m}"
    def make_delta_sublist(list_el):
        new_name = generate_random_name()
        new_delta = to_deltas(list_el, new_name)
        nonlocal list_edge_count
        list_edge_count += 1
        return [
            *new_delta,
            (Z[name], RT.ZEF_ListElement[ed_name(list_edge_count-1)], Z[new_name]),
        ]

    res = [
        ET.ZEF_List[name],
        *(x
          | map[make_delta_sublist]
          | concat
          | collect
        )
    ]
    list_order_rels = (
        range(list_edge_count)
        | sliding[2]
        | map[lambda p: (Z[ed_name(p[0])], RT.ZEF_NextElement, Z[ed_name(p[1])])]
        | collect
    )
    return [*res, *list_order_rels]




def to_deltas(x, name: str) -> List:
    d_dispatch = {
        int: handle_int,
        float: handle_float,
        str: handle_string,
        bool: handle_bool,
        type(None): handle_nil,
        dict: handle_dict,
        list: handle_list,
        tuple: handle_list,
    }
    return d_dispatch[type(x)](x, name=name)



def to_flatgraph_imp(d: dict):
    """
    An operator that allows transforming plain Zef values (dicts, lists, 
    scalars and nested forms thereof) to FlatGraphs in a standardized form.

    Dicts: if a key "_type" is specified, this is not attached as child,
    but the associated value is assumed to be a string, which is used 
    as the type name. All other keys are attached as children.

    Lists: These are converted to ZefLists, and the elements are attached.

    ---- Examples -----
    >>> d = {
    ... '_type': 'paragraph',
    ... 'foo': 42,
    ... 'bar': [1, 2, 'bye', False]
    ... }
    ... d | to_flatgraph     # returns a FlatGraph
        
    ---- Signature ----
    (Bool | Int | Float | String | Dict | List) -> FlatGraph

    ---- Tags ----
    - operates on: List
    - operates on: Dict

    """
    fg = FlatGraph(
        to_deltas(d, name='root')
    )
    # keep this function pure
    # we don't want the random names leaking out
    fg.key_dict = select_keys(fg.key_dict, 'root')    
    return fg

