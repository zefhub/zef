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
# from ..error import Error
from .._core import *
from .. import internals


# For certain value types, the user may want to call e.g. Float(4.5).
# This looks up the associated function 
_value_type_constructor_funcs = {}
_value_type_get_item_funcs = {}
_value_type_attr_funcs = {}
_value_type_is_a_funcs = {}
_value_type_is_subtype_funcs = {}
_value_type_simplify_type_funcs = {}
_value_type_str_funcs = {}
_value_type_pytypes = {}

class ValueType_:
    """ 
    Zef ValueTypes are Values themselves.
    """
    def __init__(self, type_name:str = None, absorbed=(), constructor_func=None, get_item_func=None, pass_self=None, attr_funcs=(None,None,None), is_a_func=None, is_subtype_func=None, pytype=None, str_func=None, fill_dict=None, template=None):
            if type_name is not None:
                assert template is None
                self._d = {
                    'type_name': type_name,
                    'absorbed': absorbed,
                    'alias': None,
                }

                if constructor_func is not None:
                    _value_type_constructor_funcs[type_name] = constructor_func
                if get_item_func is not None:
                    _value_type_get_item_funcs[type_name] = get_item_func
                if is_a_func is not None:
                    _value_type_is_a_funcs[type_name] = is_a_func
                if is_subtype_func is not None:
                    _value_type_is_subtype_funcs[type_name] = is_subtype_func
                if str_func is not None:
                    _value_type_str_funcs[type_name] = str_func
                if pytype is not None:
                    _value_type_pytypes[type_name] = pytype
                if attr_funcs != (None, None, None):
                    _value_type_attr_funcs[type_name] = attr_funcs
                self.__pass_self = False if pass_self is None else pass_self

            elif template is not None:
                assert type_name is None
                assert constructor_func is None
                assert get_item_func is None
                assert is_a_func is None
                assert is_subtype_func is None
                assert str_func is None
                assert pytype is None
                assert pass_self is None
                assert attr_funcs is (None,None,None)
                self._d = dict(template._d)
                self.__pass_self = template.__pass_self

            else:
                raise Exception("Need type_name or template provided")

            if fill_dict is not None:
                self._d.update(fill_dict)

    def __instancecheck__(self, instance):
        return is_a_(instance, self)
    def __subclasscheck__(self, subclass):
        return is_subtype(subclass, self) is True

    # def __repr__(self):
    def __get_nice_name(self):
        if self._d['alias'] != None:
            return self._d['alias']
        return self._d['type_name']

    def __str__(self):
        str_func = _value_type_str_funcs.get(self._d["type_name"], None)
        if str_func is None:
            return self.__get_nice_name() + (
                '' if self._d['absorbed'] == () 
                else ''.join([ f"[{repr(el)}]" for el in self._d['absorbed'] ])
            )
        return str_func(self)

    def __repr__(self):
        return str(self)


    def __call__(self, *args, **kwargs):
        try:
            f = _value_type_constructor_funcs[self._d["type_name"]]
        except KeyError:
            try:
                f = _value_type_pytypes[self._d["type_name"]]
            except KeyError:
                from .. import Error
                raise Exception(f'{self._d["type_name"]}(...) was called, but no constructor function was registered for this type')
                return Error(f'{self._d["type_name"]}(...) was called, but no constructor function was registered for this type')
        if self.__pass_self: return f(self, *args, **kwargs)
        return f(*args, **kwargs)


    def __getitem__(self, x):
        try:
            f = _value_type_get_item_funcs[self._d["type_name"]]
        except KeyError:
            return ValueType_(self._d["type_name"], absorbed=(*self._d['absorbed'], x))
        if self.__pass_self: return f(self, x)
        return f(self, x)


    def __eq__(self, other):
        if isinstance(other, type) and other in [internals.ZefRef, internals.EZefRef]:
            import traceback
            traceback.print_stack()
            print(f"""Warning, you tried to compare == between a '{other}' and a ValueType. This is likely because you wrote `type(x) == SomeValueType`. Instead you should write `isinstance(x, SomeValueType)` or `representation_type(x) == SomeValueType`.""")
        if not isinstance(other, ValueType_): return False
        # return self._d['type_name'] == other._d['type_name'] and self._d['absorbed'] == other._d['absorbed']
        return self._d == other._d


    def __hash__(self):
        return hash(self._d['type_name']) ^ hash(self._d['absorbed'])


    def __or__(self, other):
        if isinstance(other, ValueType_):
            return simplify_value_type(ValueType_(type_name='Union', absorbed=(self, other,)))
        else:
            return NotImplemented

    def __ror__(self, other):
        # Is | commutative? Going to assume it isn't
        if isinstance(other, ValueType_):
            return simplify_value_type(ValueType_(type_name='Union', absorbed=(other, self,)))
        else:
            return NotImplemented
    
    def __and__(self, other):
        if isinstance(other, ValueType_):
            return simplify_value_type(ValueType_(type_name='Intersection', absorbed=(self, other,)))
        else:
            return NotImplemented

    def __rand__(self, other):
        # Is & commutative? Going to assume it isn't
        if isinstance(other, ValueType_):
            return simplify_value_type(ValueType_(type_name='Intersection', absorbed=(other, self,)))
        else:
            return NotImplemented

    def __invert__(self):
        return ValueType_(type_name='Complement', absorbed=(self,))


    def __contains__(self, x):
        """
        Allows checking membership in form
        >>> 4 in Int
        """
        from ..op_implementations.implementation_typing_functions import is_a_implementation
        return is_a_implementation(x, self)

    def __getattribute__(self, name):
        if name.startswith("_"):
            return object.__getattribute__(self, name)

        get_attr_func, set_attr_func, dir_func = _value_type_attr_funcs.get(self._d["type_name"], (None,None,None))
        if get_attr_func is None:
            raise AttributeError(name)
        return get_attr_func(self, name)

    def __setattribute__(self, name, value):
        if name.startswith("_"):
            return object.__getattribute__(self, name)

        get_attr_func, set_attr_func, dir_func = _value_type_attr_funcs.get(self._d["type_name"], (None,None,None))
        if set_attr_func is None:
            raise AttributeError
        return set_attr_func(self, name, value)

    def __dir__(self):
        get_attr_func, set_attr_func, dir_func = _value_type_attr_funcs.get(self._d["type_name"], (None,None,None))
        if get_attr_func is None:
            return object.__dir__(self)
        return dir_func(self)
    

def make_distinct(v):
    """
    utility function to replace the 'distinct'
    zefop and preserve order.
    """
    seen = set()
    for el in v:
        if isinstance(el, set):
            # we can't import the following at the top of file or even outside this if
            # block: this function executes upon import of zef and leads to 
            # circular dependencies. We do really want to use the value_hash zefop though.
            from ..op_implementations.implementation_typing_functions import value_hash
            h = value_hash(el)
        else:
            h = el
        if h not in seen:
            yield el
            seen.add(h)


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
            return y._d['type_name'] == 'Union'
        except:
            return False
    def is_a_intersection(y):
        try:
            return y._d['type_name'] == 'Intersection'
        except:
            return False

    if is_a_union(x):
        # flatten out unions: Union[Union[A][B]][C]  == Union[A][B][C]
        old_abs = x._d['absorbed']
        absorbed = tuple((el._d['absorbed'] if is_a_union(el) else (el,) for el in old_abs))  # flatten this out
        return ValueType_(type_name='Union', absorbed=tuple(make_distinct((a2 for a1 in absorbed for a2 in a1))))
    elif is_a_intersection(x):
        # flatten out Intersections: Intersection[Intersection[A][B]][C]  == Intersection[A][B][C]
        old_abs = x._d['absorbed']
        absorbed = tuple((el._d['absorbed'] if is_a_intersection(el) else (el,) for el in old_abs))  # flatten this out
        return ValueType_(type_name='Intersection', absorbed=tuple(make_distinct((a2 for a1 in absorbed for a2 in a1))))
    else:
        return x


def is_type(typ):
    return type(typ) == ValueType_

def is_a_(obj, typ):
    assert is_type(typ), f"Can't do a is_a_ on a non-ValueType '{typ}'"
    if typ._d["type_name"] in _value_type_is_a_funcs:
        return _value_type_is_a_funcs[typ._d["type_name"]](obj, typ)
    elif typ._d["type_name"] in _value_type_pytypes:
        return isinstance(obj, _value_type_pytypes[typ._d["type_name"]])
    else:
        raise Exception(f"ValueType '{typ._d['type_name']}' has no is_a implementation")

def is_subtype(typ1, typ2):
    assert is_type(typ1), f"is_subtype got a non-type: {typ1}"
    assert is_type(typ2), f"is_subtype got a non-type: {typ2}"

    if typ2._d["type_name"] in _value_type_is_subtype_funcs:
        return _value_type_is_subtype_funcs[typ2._d["type_name"]](typ1, typ2)
    elif typ1._d["type_name"] == typ2._d["type_name"]:
        if len(typ2._d["absorbed"]) == 0 and len(typ1._d["absorbed"]) > 0:
            return True
        else:
            # TODO: Default covariant or other subtyping behaviour here.
            return "maybe"
    else:
        # raise Exception(f"ValueType '{typ1._d['type_name']}' has no is_subtype implementation")
        return False

def is_strict_subtype(typ1, typ2):
    res = is_subtype(typ1, typ2)

    if typ1 == typ2:
        return False
    elif res is True:
        return True
    elif res is False:
        return False
    elif res == "maybe":
        return False
    
def is_empty(typ):
    typ = simplify_type(typ)
    from . import SetOf
    return typ == SetOf



def simplify_type(typ):
    if typ._d["type_name"] in _value_type_simplify_type_funcs:
        return _value_type_simplify_type_funcs[typ._d["type_name"]](typ)
    return typ
        
    