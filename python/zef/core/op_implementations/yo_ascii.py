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
from .. import VT
import random

##############################################################
# * ZefOp chain yo output implementations
def pad_side(s: str, padding: int, left: bool, desired_length: int) -> str:
    n = desired_length - len(s)
    if n<0:
        raise RuntimeError('pad_side is only well defined if the desired length is larger than the length of the string to pad')
    if left:
        return f"{' ' * padding}{s}{' ' * (desired_length - len(s) - padding)}"
    else:
        return f"{' ' * (desired_length - len(s) - padding)}{s}{' ' * padding}"


def inject_in_line(line: list, title: str):
    if not title: return line
    if len(title) > len(line) - 6: title = title[:len(line) - 10] + "..."
    title = f" {title} "
    return [*line[:3] , title, *line[3+len(title):]] 


@func
def make_box(content, label = ""):
    max_width = content | map[length] | max | collect
    width  = max_width + 30
    padding_v = 2
    padding_h = 8

    up     = ("─" | repeat |  take[width] | prepend["┌"] | append["┐"] | collect)  
    up     = inject_in_line(up, label) | join | collect

    middle = " " | repeat |  take[width] | prepend["│"] | append["│"] | join | repeat | take[padding_v] | join["\n"] | collect
    
    middle_content = content | map[lambda l: f"|{pad_side(l, padding_h, True, width)}|"] | join["\n"] | collect
    
    bottom = "─"  | repeat |  take[width] | join | prepend["└"] | append["┘"] | join | collect
    return [up, middle, middle_content, middle, bottom]  | join["\n"] | collect

@func
def make_box_details(op_chain, out_types, op_purity):
    return f"""
!! This is an incomplete/incorrect output !!
Operator 🔗:    {op_chain}
Signature:      {out_types[0]} -> {out_types[-1]}
Purity:         {random.SystemRandom().choice(['🌿 pristenly pure', '👹 possibly mutating'])}\n\n"""


@func
def make_operator_repr(op, out_type, op_purity):
    op          = ZefOp((op, ))
    purity      = f"│ {op_purity}"
    operator    = f"│ {str(op)}"
    out_type    = f"{out_type}"
    return [purity, operator, "▼", out_type]


@func
def chain_output(op: ZefOp, out_types: list, op_purity: list) -> list:
    initial_type = first(out_types)
    content = (op.el_ops, out_types[1:], op_purity) | zip | for_each[lambda x: make_operator_repr(*x)] | concat | prepend[str(initial_type)] | collect
    return content


def make_op_chain_ascii_output(op_chain):
    out_types = [VT.Any for _ in range(len(op_chain.el_ops) + 1)]
    op_purity = [random.SystemRandom().choice('🌿👹') for _ in range(len(op_chain.el_ops))]


    return (
        LazyValue(op_chain) 
        | chain_output[out_types][op_purity]
        | make_box["Data pipeline"] 
        | prepend[make_box_details(op_chain, out_types, op_purity)]
        | collect
    )