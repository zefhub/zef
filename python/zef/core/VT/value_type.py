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
In Zef the types of Zef Values are values themselves: ValueType_.
These are distinct from the host language's implementation type
of the value instance.

In contrast to most programming languages's type system, the 
Zef Type system is based on set theory and has a stronger focus 
on succinctly being able to express complex domains and associative
ontolgies.

ValueTypes fall into the category of Zef Values that can absorb other
Zef Values, which is useful in constructing query expressions (as native
data structures) and graph commands. 
It also allows them to be flexibly used by the user to build custom DSLs.

---------------------- internal structure ---------------------------
It also has "absorbed": a tuple of other values


----------- Nesting Order ----------------
TODO: Do we want to keep this? Probably not.


What is "Union"? A basic value type.
Union[Int, Union[Float, String]]

Union[Int,Float]
Union[Int,Float,String]

This can be distinguished from Tuples as a type
Union[(Int,Float,String),]


----------- Constructor Overloading ----------------
Int(3.4) can have overloaded behavior. The user 
provides a function.

------------ Square Bracket Overloading --------
Should Union be of type ZefType? Or just return a ZefType?


Intersection
Union
Is

There is also a Union ValueType_, which is returned.
This custom structure may rearrange and parse terms.


---------- User Defined Types ----------
needed?
"""
from ..error import Error
# For certain value types, the user may want to call e.g. Float(4.5).
# This looks up the associated function 
_value_type_constructor_funcs = {}

class ValueType_:
    """ 
    Zef ValueTypes are Values themselves.
    """
    def __init__(self, type_name: str, absorbed=(), constructor_func=None):
            self.d = {
                'type_name': type_name,
                'absorbed': absorbed,
            }
            if constructor_func is not None:
                _value_type_constructor_funcs[type_name] = constructor_func

        
    def __repr__(self):
        return self.d['type_name'] + (
            '' if self.d['absorbed'] == () 
            else ''.join([ f"[{repr(el)}]" for el in self.d['absorbed'] ])
        )


    def __call__(self, *args, **kwargs):
        try:
            f = _value_type_constructor_funcs[self.d["type_name"]]
        except KeyError:
            return Error(f'{self.d["type_name"]}(...) was called, but no constructor function was registered for this type')
        return f(*args, **kwargs)


    def __getitem__(self, x):
        return ValueType_(self.d["type_name"], absorbed=(*self.d['absorbed'], x))


    def __eq__(self, other):
        if not isinstance(other, ValueType_): return False
        return self.d['type_name'] == other.d['type_name'] and self.d['absorbed'] == other.d['absorbed']


    def __hash__(self):
        return hash(self.d['type_name']) ^ hash(self.d['absorbed'])


    def __or__(self, other):
        from ..op_structs import ZefOp
        if isinstance(other, ValueType_):
            return simplify_value_type(ValueType_(type_name='Union', absorbed=(self, other,)))
        elif isinstance(other, ZefOp):
            return other.__ror__(self)
        else:
            raise Exception(f'"ValueType_`s "|" called with unsupported type {type(other)}')
    
    def __and__(self, other):
        if isinstance(other, ValueType_):
            return simplify_value_type(ValueType_(type_name='Intersection', absorbed=(self, other,)))
        else:
            raise Exception(f'"ValueType_`s "&" called with unsupported type {type(other)}')

    def __invert__(self):
        return ValueType_(type_name='Complement', absorbed=(self,))
    



class UnionClass:
    def __getitem__(self, x):
        from ..op_structs import ZefOp
        if isinstance(x, tuple):
            return ValueType_(type_name='Union', absorbed=x)
        elif isinstance(x, ValueType_):
            return ValueType_(type_name='Union', absorbed=(x,))
        elif isinstance(x, ZefOp):
            return ValueType_(type_name='Union', absorbed=(x,))
        else:
            raise Exception(f'"Union[...]" called with unsupported type {type(x)}')
            

class IntersectionClass:
    def __getitem__(self, x):
        from ..op_structs import ZefOp
        if isinstance(x, tuple):
            return ValueType_(type_name='Intersection', absorbed=x)
        elif isinstance(x, ValueType_):
            return ValueType_(type_name='Intersection', absorbed=(x,))
        elif isinstance(x, ZefOp):
            return ValueType_(type_name='Intersection', absorbed=(x,))
        else:
            raise Exception(f'"Intersection[...]" called with unsupported type {type(x)}')
            


class ComplementClass:
    def __getitem__(self, x):
        if isinstance(x, ValueType_):
            return ValueType_(type_name='Complement', absorbed=(x,))
        else:
            raise Exception(f'"Complement[...]" called with unsupported type {type(x)}')
   

class IsClass:
    def __getitem__(self, x):
        from ..op_structs import ZefOp
        if isinstance(x, tuple):
            return ValueType_(type_name='Is', absorbed=x)
        elif isinstance(x, ValueType_):
            return ValueType_(type_name='Is', absorbed=(x,))
        elif isinstance(x, ZefOp):
            return ValueType_(type_name='Is', absorbed=(x,))
        else:
            raise Exception(f'"Is[...]" called with unsupported type {type(x)}')
         

class SetOfClass:
    def __getitem__(self, x):
        # TODO: make sure that x is a zef value. No other python objects that we can't serialize etc.
        return ValueType_(type_name='SetOf', absorbed=(x, ))
    def __call__(self, *x):
        """
        calling SetOf(5,6,7) is a more convenient shorthand notation than 
        SetOf[5][6][7]. But the former expression evaluates to the latter.

        We can't use `SetOf[5,6,7]` here, since Python's treatment of
        the [...] operator converts this to a tuple `SetOf[(5,6,7)]`,
        which itself is a valid expression.
        """
        return ValueType_(type_name='SetOf', absorbed = x)



def make_distinct(v):
    """
    utility function to replace the 'distinct'
    zefop and preserve order.
    Writing this on a plane and can't use zefops,
    since I forgot the environment variable to
    allow offline mode.
    """
    seen = set()
    for el in v:
        if el not in seen:
            yield el
            seen.add(el)


def simplify_value_type(x):
    """
    Only simplifies nested Union and Intersection.
    It does not change any absorbed values, but
    only rearranges on the outer level of Zef Values.

    Union and Intersection behave analogous to + and *.

    I[U[A, B], C] = I[U[A,C], U[B,C]]              # True
    I[U[Ev, Od], Pos] = I[U[Ev,Pos], U[Odd,Pos]]   # True
    (A+B)*C = A*C + B*C
    
    U[I[Ev, Od], Pos] = U[I[Ev,Pos], I[Odd,Pos]]   # This is False!!!!


    """
    def is_a_union(y):
        try:
            return y.d['type_name'] == 'Union'
        except:
            return False
    def is_a_intersection(y):
        try:
            return y.d['type_name'] == 'Intersection'
        except:
            return False

    if is_a_union(x):
        # flatten out unions: Union[Union[A][B]][C]  == Union[A][B][C]
        old_abs = x.d['absorbed']
        absorbed = tuple((el.d['absorbed'] if is_a_union(el) else (el,) for el in old_abs))  # flatten this out
        return ValueType_(type_name='Union', absorbed=tuple(make_distinct((a2 for a1 in absorbed for a2 in a1))))
    elif is_a_intersection(x):
        # flatten out Intersections: Intersection[Intersection[A][B]][C]  == Intersection[A][B][C]
        old_abs = x.d['absorbed']
        absorbed = tuple((el.d['absorbed'] if is_a_intersection(el) else (el,) for el in old_abs))  # flatten this out
        return ValueType_(type_name='Intersection', absorbed=tuple(make_distinct((a2 for a1 in absorbed for a2 in a1))))
    else:
        return x
