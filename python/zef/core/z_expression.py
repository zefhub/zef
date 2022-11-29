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

"""
Z expressions are useful notation for succinctly declaring sets of things.
e.g. (Z > 10) denotes the set of all things greater than 10.

This can be chained though: zef functions and all kinds of operators can be absorbed.
e.g. IsJohn = (Z.first_name | to_uppercase = 'JOHN')

Z expressions sit right at the top of the food chain when it comes to operator binding.
A Z-expression gobbles up anything to become a new Z-Expression.

So how can we use them to declare sets of things? As soon as a Z expression encounters
a binary comparison operator ('=', '>', '<', '>=', '<=', '!=') it will stop gobbling
up operators and instead become a ValueType.
"""

from . import VT
from .VT import ET, Is
from .zef_functions import func

# from .VT import FlatGraph, Pattern, Any, SetOf, Val,  RT, AET
# from ._ops import match, collect, insert, split, get, filter, map, Z

class ZExpression_:
    def __init__(self, root_node=None):
        self.root_node = root_node    # if this is None, then it is a bare variable. This also qualifies as a symbolic expression.

    def __repr__(self):
        # print(f'repr called for {self.root_node}')
        # raise NotImplementedError
        if self.root_node is None:            
            return f"ZZ"
        else:
            # return "Z Expression output: todo"#self.root_node.fg | graphviz | collect
            return str(f"ZExpression({self.root_node})")

    def __hash__(self):
        return (hash(self.root_node)+78566354676)


    def __getattr__(self, other):
        return compose_zexpr(ET.Dot, self, other)

    def __getitem__(self, other):
        return compose_zexpr(ET.GetItem, self, other)

    def __add__(self, other):
        return compose_zexpr(ET.Add, self, other)

    def __radd__(self, other):
        return compose_zexpr(ET.Add, other, self)

    def __sub__(self, other):
        return compose_zexpr(ET.Subtract, self, other)

    def __rsub__(self, other):
        return compose_zexpr(ET.Subtract, other, self) 

    def __mul__(self, other):
        return compose_zexpr(ET.Multiply, self, other)

    def __rmul__(self, other):
        return compose_zexpr(ET.Multiply, other, self)

    def __truediv__(self, other):
        return compose_zexpr(ET.Divide, self, other)
    
    def __rtruediv__(self, other):
        return compose_zexpr(ET.Divide, other, self)

    def __mod__(self, other):
        return compose_zexpr(ET.Modulo, self, other)

    def __rmod__(self, other):
        return compose_zexpr(ET.Modulo, other, self)
    
    def __pow__(self, other):
        return compose_zexpr(ET.Power, self, other)

    def __rpow__(self, other):
        return compose_zexpr(ET.Power, other, self)
    
    def __or__(self, other):
        return compose_zexpr(ET.Or, self, other)
    
    def __ror__(self, other):
        return compose_zexpr(ET.Or, other, self)
    
    def __and__(self, other):
        return compose_zexpr(ET.And, self, other)
    
    def __rand__(self, other):
        return compose_zexpr(ET.And, other, self)
    
    def __xor__(self, other):
        return compose_zexpr(ET.Xor, self, other)

    def __rxor__(self, other):
        return compose_zexpr(ET.Xor, other, self)
        

    def __eq__(self, other):
        return z_expression_to_vt(ET.Equals, self, other)

    def __req__(self, other):
        return z_expression_to_vt(ET.Equals, other, self)

    def __ne__(self, other):
        return z_expression_to_vt(ET.NotEquals, self, other)

    def __rne__(self, other):
        return z_expression_to_vt(ET.NotEquals, other, self)

    def __gt__(self, other):
        return z_expression_to_vt(ET.GreaterThan, self, other)
    
    def __rgt__(self, other):
        return z_expression_to_vt(ET.GreaterThan, other, self)
    
    def __ge__(self, other):
        return z_expression_to_vt(ET.GreaterThanOrEquals, self, other)

    def __rge__(self, other):
        return z_expression_to_vt(ET.GreaterThanOrEquals, other, self)
    
    def __lt__(self, other):
        return z_expression_to_vt(ET.LessThan, self, other)

    def __rlt__(self, other):
        return z_expression_to_vt(ET.LessThan, other, self)

    def __le__(self, other):
        return z_expression_to_vt(ET.LessThanOrEquals, self, other)
    
    def __rle__(self, other):
        return z_expression_to_vt(ET.LessThanOrEquals, other, self)
    


def unwrap_zexpr(x):
    if isinstance(x, ZExpression_):
        if x.root_node is None:
            return ET.Z
        else:
            return x.root_node
    else:
        return x


def compose_zexpr(op_type: ET, arg1, arg2):
    return ZExpression_(root_node=op_type(arg1=unwrap_zexpr(arg1), arg2=unwrap_zexpr(arg2)))
   


import operator as o
_op_translation = {
    ET.Add: o.add,
    ET.Subtract: o.sub,
    ET.Multiply: o.mul,
    ET.Divide: o.truediv,
    ET.Modulo: o.mod,
    ET.Power: o.pow,
    ET.Or: o.or_,
    ET.And: o.and_,
    ET.Xor: o.xor,

    ET.GetItem: o.getitem,
    ET.Dot: lambda x, k: x.__getattr__(k),

    ET.GreaterThan: o.gt,
    ET.GreaterThanOrEqual: o.ge,
    ET.LessThan: o.lt,
    ET.LessThanOrEqual: o.le,
    ET.Equals: o.eq,
    ET.NotEquals: o.ne,
} 


def compose_fct(op_type: ET, g1, g2):
    import zef
    res = _op_translation[op_type](g1, g2)
    if isinstance(res, zef.core.op_structs.LazyValue):
        # res2 = res()
        # print(f'Compose {op_type} with {g1} and {g2} -> {res} -> {res2}')
        return res()
    else:
        # print(f'Compose {op_type} with {g1} and {g2} -> {res}')
        return res


def eval_if_lazy(x):
    import zef
    if isinstance(x, zef.core.op_structs.LazyValue):
        return x()
    else:
        return x


def z_expression_to_vt(op_type: ET, arg1, arg2) -> VT:
    """
    This function is hit when a binary comparison operator is encountered in a ZExpression.
    It returns a value type, which wraps a predicate function.
    """
       
    def step(expr) -> func:
        import zef
        is_lambda = lambda x: callable(x) and not isinstance(x, zef.core.op_structs.ZefOp_)

        # exit early if this is the main variable (ET.Z  => identity function)
        # or a constant expression, i.e. not another nested expression in the form of an EntityValueInstance_
        if expr == ET.Z:
            return lambda x: x

        if not isinstance(expr, zef.core.patching.EntityValueInstance_):
            return expr   # normal value

        # once we're here, it must be an EntityValueInstance_: recurse into its children
        f1 = step(eval_if_lazy(expr.arg1))
        f2 = step(eval_if_lazy(expr.arg2))

        # print(f"step will dispatch on: {op_type} {f1}    {f2}  {callable(f1)} {callable(f2)}")
        # one extra lambda layer to prevent evaluation of the first term
        return {
            (False, False): lambda:           compose_fct(expr._entity_type, f1, f2),
            (True, False):  lambda: lambda x: compose_fct(expr._entity_type, f1(x), f2),
            (False, True):  lambda: lambda x: compose_fct(expr._entity_type, f1, f2(x)),
            (True, True):   lambda: lambda x: compose_fct(expr._entity_type, f1(x), f2(x)),
        }[is_lambda(f1), is_lambda(f2)]()

    a = op_type(arg1=unwrap_zexpr(arg1), arg2=unwrap_zexpr(arg2))
    # print(f"\n\n----\n{a=}")
    return Is[step(a)]
 


ZZ = ZExpression_() # TODO: rename this to "Z" once the "Z" zefop can be fully replaced with this

from .VT import make_VT
ZExpression = make_VT("ZExpression", pytype=ZExpression_)