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
SymbolicVariables are a special kind of SymbolicExpression. They are a primitive building block.
Which variable they are is simply stored as a string, which makes them values without an identity
other than this string.

There are also other kinds of SymbolicExpressions: they can be composed to form composite expressions.
Internally, the structure of the expression is stored as a FlatGraph (this is a more suitable
data structure than a tree).

Generally, SymbolicExpressions follow strict value semantics though. If a new expression is expressed
in terms of another SymbolicExpression, the original expression is never mutated.

It is generally up to the outer scope to decide what to do with a SymbolicExpression. They are just
a value, potentially a complicated one.

They are useful in Queries, expressing various interconnected constraints, truth maintenance systems,
logic programs that are built up incrementally.

A very similar structure is used for ReactiveVariables. These are a different type and have a different
use case though.




----------------------------- Basic Usage -----------------------------------
>>> x = SV('x')
>>> y = x*x + 42     # y is a composite SymbolicExpression

Many operators are supported


----------------------------- Use Cases -----------------------------------
# z is a SE: serve as the body of a Zef Lambda expression.
Lambda[z*z + 42]

p, a = SVs('a, p')
Query[
    {
        p: ET.Person,
        a: ET.Dog | ET.Cat,
    },
    RP(p, RT.Owns, a),
]


Query[
    z,
    z[1]*z[1] + z[2]*z[2] == z[3]*z[3]
]

or
Query[
    (x1, x2, x3),               # do we want this? Not entirely consistent with the Dict notation.
    x1*x1 + x2*x2 == x3*x3,
]


----------------------------- Sample Code -----------------------------------
>>> from zef.core.symbolic_expression import SymbolicExpression_, SV, unwrap_vars
>>> 
>>> x1 = SV('x1')
>>> x2 = SV('x2')
>>> expr = (x1 + 5)**x2
>>> expr.root_node.fg | unwrap_vars | graphviz | collect   # temporary hack: shows graphviz representation of the expression

----------------------------- Open Questions -----------------------------------
- What should "List[x2]" do? Simply Absorb the SymbolicExpression or should this itself be a SymbolicExpression?
- What is the grammar for SEs interacting with ReactiveVariables?
- if we expose `z`, should z['a'] be identical to SV('a')?

----------------------------- TODOs -----------------------------------
- add SymbolicExpression to VTs and implement in is_a
- serialization
- overload some unary operators: e.g. __pos__, __neg__, ...?
- 41 | add[1] | x | multiply[2]          # advanced use case: should this also be a SE? SEs can themselves be used to do logic with ZefOps.

"""


from .VT import FlatGraph, Pattern, Any, SetOf, Val, ET, RT, AET
from ._ops import match, collect, insert, split, get, filter, map, Z
from .zef_functions import func

def merge_flatgraphs(g1, g2) -> FlatGraph:
    """
    Given two graphs, make a new FlatGraph that contains both.

    """
    def shift(el, d):
        """
        depending on the element type in the FlatGraph blobs, 
        we may have to perform different types of shifts.
        """
        return el | match[
         (Pattern[[Any, AET]], lambda el: (el[0]+d, el[1], [n+d if n>=0 else n-d for n in el[2]], el[3], el[4])),
         (Pattern[[Any, RT]], lambda el: (el[0]+d, el[1], [n+d if n>=0 else n-d for n in el[2]], el[3], el[4]+d, el[5]+d)),
         (Any, lambda el: (el[0]+d, el[1], [n+d if n>=0 else n-d for n in el[2]], el[3],)),
        ] | collect
    

    g = FlatGraph()
    delta_n = len(g1.blobs)    # how much to shift all indexes of g2
    g.blobs = (
        *g1.blobs,
        *(shift(el, delta_n) for el in g2.blobs),
    )
    g.key_dict = {**g1.key_dict, **{k: v+delta_n for k,v in g2.key_dict.items()}}
    return g




class SymbolicExpression_:
    """Symbolic Variable class"""
    def __init__(self, name=None, root_node=None):
        self.name = name
        self.root_node = root_node    # if this is None, then it is a bare variable. This also qualifies as a symbolic expression.

    def __repr__(self):
        if self.name is not None:
            # if the name is specified: it is a SV only
            return f"V.{self.name}"            
        else:
            return "Composed SymbolicExpression (todo: expression output)"

    def __hash__(self):
        return (hash(self.name)+435677842)^(hash(self.root_node)+3424242)


    def __getitem__(self, k):
        if self.root_node is not None:
            raise RuntimeError("a composite SymbolicExpression cannot absorb a value")
        return SymbolicExpression_(name=self.name)

    def __getattr__(self, other):
        return compose_se(ET.Dot, self, other)

    def __add__(self, other):
        return compose_se(ET.Add, self, other)

    def __radd__(self, other):
        return compose_se(ET.Add, other, self)

    def __sub__(self, other):
        return compose_se(ET.Subtract, self, other)

    def __rsub__(self, other):
        return compose_se(ET.Subtract, other, self)

    def __mul__(self, other):
        return compose_se(ET.Multiply, self, other)

    def __rmul__(self, other):
        return compose_se(ET.Multiply, other, self)

    def __truediv__(self, other):
        return compose_se(ET.Divide, self, other)

    def __rtruediv__(self, other):
        return compose_se(ET.Divide, other, self)

    def __eq__(self, other):
        return compose_se(ET.Equals, self, other)

    def __req__(self, other):
        return compose_se(ET.Equals, other, self)

    def __ne__(self, other):
        return compose_se(ET.NotEquals, self, other)

    def __rne__(self, other):
        return compose_se(ET.NotEquals, other, self)

    def __gt__(self, other):
        return compose_se(ET.GreaterThan, self, other)

    def __rgt__(self, other):
        return compose_se(ET.GreaterThan, other, self)

    def __ge__(self, other):
        return compose_se(ET.GreaterOrEqualThan, self, other)

    def __rge__(self, other):
        return compose_se(ET.GreaterOrEqualThan, other, self)

    def __lt__(self, other):
        return compose_se(ET.LessThan, self, other)

    def __rlt__(self, other):
        return compose_se(ET.LessThan, other, self)

    def __le__(self, other):
        return compose_se(ET.LessOrEqualThan, self, other)

    def __rle__(self, other):
        return compose_se(ET.LessOrEqualThan, other, self)

    def __mod__(self, other):
        return compose_se(ET.Modulus, self, other)

    def __rmod__(self, other):
        return compose_se(ET.Modulus, other, self)

    def __or__(self, other):
        return compose_se(ET.Or, self, other)

    def __ror__(self, other):
        return compose_se(ET.Or, other, self)

    def __and__(self, other):
        return compose_se(ET.And, self, other)

    def __rand__(self, other):
        return compose_se(ET.And, other, self)

    def __xor__(self, other):
        return compose_se(ET.Xor, self, other)

    def __rxor__(self, other):
        return compose_se(ET.Xor, other, self)

    def __pow__(self, other):
        return compose_se(ET.Power, self, other)

    def __rpow__(self, other):
        return compose_se(ET.Power, other, self)



def SV(name: str):
    """acts like a constructor. SymbolicVariable is the `primitive`, 
    atomic building block  of any symbolic expression. The latter
    can be a larger composed form."""
    if not isinstance(name, str):
        raise TypeError("name must be a string")    
    return SymbolicExpression_(name=name)


def SVs(names: str):
    """
    initialize multiple SVs together:
    >>> x1, x2, *_ = SVs('x1, x2, x3, x4')
    Space or comma indicates a new variable.
    """
    if not isinstance(names, str):
        raise TypeError("name must be a string")    

    return (names 
        | split[lambda c: c==' ' or c==','] 
        | filter[~SetOf['',' ', ',']] 
        | map[SV]
        | collect
        )



def compose_se(op_type, arg1, arg2):
    """
    either arg1 or arg2 will be a SymbolicExpression.
    Otherwise the magic method would not forward to this function.
    """
    fg = FlatGraph()

    is_composite_se = lambda x: isinstance(x, SymbolicExpression_) and x.name == None
    arg1_composite_se = is_composite_se(arg1)
    arg2_composite_se = is_composite_se(arg2)
    # neither side is a composite SE: start with fresh graph
    if (not arg1_composite_se) and (not arg2_composite_se):
        res = (fg 
            | insert[op_type['root'], RT.Arg1, Val(arg1)]
            | insert[Z['root'], RT.Arg2, Val(arg2)]
            | get['root']
            | collect
        )
        return SymbolicExpression_(root_node=res)
    
    # both sides are composite SEs: merge
    elif arg1_composite_se and arg2_composite_se:
        g1 = arg1.root_node.fg
        g2 = arg2.root_node.fg
        g_merged = merge_flatgraphs(g1, g2)
        # some manual hacking here. g2's blob list is appended to g1's blob list.
        g_merged.key_dict['_arg1'] = arg1.root_node.fg.key_dict['root']
        g_merged.key_dict['_arg2'] = arg2.root_node.fg.key_dict['root'] + len(arg1.root_node.fg.blobs)
        res = (g_merged
            | insert[op_type['root'], RT.Arg1, Z['_arg1']]
            | insert[Z['root'], RT.Arg2, Z['_arg2']]
            | get['root']
            | collect
        )
        return SymbolicExpression_(root_node=res)
        

    elif arg1_composite_se:
        res = (arg1.root_node.fg
            | insert[op_type['root'], RT.Arg1, arg1.root_node]
            | insert[Z['root'], RT.Arg2, Val(arg2)]
            | get['root']
            | collect
        )
        return SymbolicExpression_(root_node=res)
        
    elif arg2_composite_se:
        res = (arg2.root_node.fg
            | insert[op_type['root'], RT.Arg2, arg2.root_node]
            | insert[Z['root'], RT.Arg1, Val(arg1)]
            | get['root']
            | collect
        )
        return SymbolicExpression_(root_node=res)
        
    raise RuntimeError('We should not be here')



class VExpression_():
    """
    A helper class for a shorthand way to 
    construct a variable called "x2": v.x2
    """
    def __getattr__(self, name):
        return SV(name)

# Helper type that we can write
# V.x2 instead of SV('x2')
V = VExpression_()


