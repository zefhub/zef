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
# from .._error import Error
from .._core import *
from .. import internals


# For certain value types, the user may want to call e.g. Float(4.5).
# This looks up the associated function 
_value_type_constructor_funcs = {}
_value_type_attr_funcs = {}
_value_type_is_a_funcs = {}
_value_type_is_subtype_funcs = {}
# These funcs are for the VERY limited group that combines sets together.
_value_type_override_subtype_funcs = {}
_value_type_simplify_type_funcs = {}
_value_type_str_funcs = {}
_value_type_pytypes = {}

class ValueType_:
    """ 
    Zef ValueTypes are Values themselves.
    """
    def __init__(self, type_name:str, absorbed=(), pytype=None, constructor_func=None, pass_self=False, attr_funcs=(None,None,None), is_a_func=None, is_subtype_func=None, override_subtype_func=None, simplify_type_func=None, str_func=None):
            self._d = {
                'type_name': type_name,
                'absorbed': absorbed,
                'alias': None,
            }

            if constructor_func is not None:
                assert type_name not in _value_type_constructor_funcs
                _value_type_constructor_funcs[type_name] = (constructor_func, pass_self)
            if is_a_func is not None:
                assert type_name not in _value_type_is_a_funcs
                _value_type_is_a_funcs[type_name] = is_a_func
            if is_subtype_func is not None:
                assert type_name not in _value_type_is_subtype_funcs
                _value_type_is_subtype_funcs[type_name] = is_subtype_func
            if override_subtype_func is not None:
                assert type_name not in _value_type_override_subtype_funcs
                _value_type_override_subtype_funcs[type_name] = override_subtype_func
            if str_func is not None:
                assert type_name not in _value_type_str_funcs
                _value_type_str_funcs[type_name] = str_func
            if simplify_type_func is not None:
                assert type_name not in _value_type_simplify_type_funcs
                _value_type_simplify_type_funcs[type_name] = simplify_type_func
            if pytype is not None:
                assert type_name not in _value_type_pytypes
                _value_type_pytypes[type_name] = pytype
            if attr_funcs != (None, None, None):
                assert type_name not in _value_type_attr_funcs
                _value_type_attr_funcs[type_name] = attr_funcs

    def _replace(self, **kwargs):
        new_vt = ValueType_(self._d["type_name"])
        new_vt._d = dict(self._d)
        new_vt._d.update(kwargs)
        return new_vt


    def __instancecheck__(self, instance):
        return is_a_(instance, self)
    def __subclasscheck__(self, subclass):
        return is_subtype_(subclass, self) is True

    # def __repr__(self):
    def __get_nice_name(self):
        if self._d['alias'] != None:
            return self._d['alias']
        return self._d['type_name']

    def __str__(self):
        str_func = _value_type_str_funcs.get(self._d["type_name"], None)
        if str_func is None:
            if self._d["alias"] is not None:
                return self._d["alias"]
            return self.__get_nice_name() + (
                ''.join([ f"[{repr(el)}]" for el in self._d['absorbed'] ])
            )
        return str_func(self)

    def __repr__(self):
        return str(self)


    def __call__(self, *args, **kwargs):
        try:
            f,pass_self = _value_type_constructor_funcs[self._d["type_name"]]
        except KeyError:
            try:
                f = _value_type_pytypes[self._d["type_name"]]
                pass_self = False
            except KeyError:
                from .. import Error
                raise Exception(f'{self.__get_nice_name()}(...) was called, but no constructor function was registered for this type')
                return Error(f'{self.__get_nice_name()}(...) was called, but no constructor function was registered for this type')
        if pass_self:
            out = f(self, *args, **kwargs)
        else:
            out = f(*args, **kwargs)
        if out is NotImplemented:
            raise Exception("Constructor cannot be called for this instance of value type {self.__get_nice_name()}")
        return out


    def __getitem__(self, x):
        new_absorbed = self._d["absorbed"] + (x,)
        return self._replace(absorbed=new_absorbed)

    def __eq__(self, other):
        if isinstance(other, type) and other in [internals.ZefRef, internals.EZefRef]:
            import traceback
            traceback.print_stack()
            print(f"""Warning, you tried to compare == between a '{other}' and a ValueType. This is likely because you wrote `type(x) == SomeValueType`. Instead you should write `isinstance(x, SomeValueType)` or `representation_type(x) == SomeValueType`.""")
        if not isinstance(other, ValueType_): return False
        # return self._d['type_name'] == other._d['type_name'] and self._d['absorbed'] == other._d['absorbed']
        return self._d == other._d


    def __hash__(self):
        # return hash(self._d['type_name']) ^ hash(self._d['absorbed'])
        return hash_frozen(self._d)


    def __or__(self, other):
        from . import Union
        if isinstance(other, ValueType_):
            return simplify_type(Union[self, other])
        else:
            return NotImplemented

    def __ror__(self, other):
        # Is | commutative? Going to assume it isn't
        if isinstance(other, ValueType_):
            from . import Union
            return simplify_type(Union[other, self])
        else:
            return NotImplemented
    
    def __and__(self, other):
        if isinstance(other, ValueType_):
            from . import Intersection
            return simplify_type(Intersection[self, other])
        else:
            return NotImplemented

    def __rand__(self, other):
        # Is & commutative? Going to assume it isn't
        if isinstance(other, ValueType_):
            from . import Intersection
            return simplify_type(Intersection[other, self])
        else:
            return NotImplemented

    def __invert__(self):
        from . import Complement
        return Complement[self]

    def __contains__(self, x):
        """
        Allows checking membership in form
        >>> 4 in Int
        """
        return isinstance(x, self)

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


def simplify_type(typ):
    if typ._d["type_name"] in _value_type_simplify_type_funcs:
        return _value_type_simplify_type_funcs[typ._d["type_name"]](typ)
    return typ
        
    
# Internal use only
def is_type_(typ):
    return type(typ) == ValueType_

def is_a_(obj, typ):
    assert is_type_(typ), f"Can't do a is_a_ on a non-ValueType '{typ}'"
    if typ._d["type_name"] in _value_type_is_a_funcs:
        out = _value_type_is_a_funcs[typ._d["type_name"]](obj, typ)
        if out is not NotImplemented:
            return out
    if typ._d["type_name"] in _value_type_pytypes:
        from .helpers import remove_names, absorbed
        # If something is absorbed, then we don't know how to handle it so error
        abs = remove_names(absorbed(typ))
        if len(abs) > 0:
            raise Exception(f"Absorbed arguments in a {type_name(typ)} cannot be used (have {abs}), as it is a native-python type domainted ValueType")
        return isinstance(obj, _value_type_pytypes[typ._d["type_name"]])
    else:
        raise Exception(f"ValueType '{typ._d['type_name']}' has no is_a implementation")

def is_subtype_(typ1, typ2):
    assert is_type_(typ1), f"is_subtype got a non-type: {typ1}"
    assert is_type_(typ2), f"is_subtype got a non-type: {typ2}"

    if typ1._d["type_name"] in _value_type_override_subtype_funcs:
        result = _value_type_override_subtype_funcs[typ1._d["type_name"]](typ1, typ2)
        if result is True or result is False:
            return result
        # Otherwise, "maybe" and continue on, in the hopes the other type knows how.

    if typ2._d["type_name"] in _value_type_is_subtype_funcs:
        return _value_type_is_subtype_funcs[typ2._d["type_name"]](typ1, typ2)

    # Fallback
    if typ1._d["type_name"] == typ2._d["type_name"]:
        # Going to default to nonvariant
        return typ1._d == typ2._d
    else:
        # raise Exception(f"ValueType '{typ1._d['type_name']}' has no is_subtype implementation")
        return False

def is_strict_subtype_(typ1, typ2):
    res = is_subtype_(typ1, typ2)

    if typ1 == typ2:
        return False
    elif res is True:
        return True
    elif res is False:
        return False
    elif res == "maybe":
        return False
    
def is_empty_(typ):
    typ = simplify_type(typ)
    from . import SetOf
    return typ == SetOf


def is_type_name_(typ, name):
    return isinstance(typ, ValueType_) and typ._d["type_name"] == name
    

def absorbed(typ):
    return typ._d["absorbed"]
def type_name(typ):
    return typ._d["type_name"]

# Temporary hash, probably needs to be merged into other code

def hash_frozen(obj):
    if type(obj) == dict:
        h = hash("dict")
        for key in sorted(obj):
            h ^= hash(key)
            h ^= hash_frozen(obj[key])
        return h
    elif type(obj) == set:
        all_hs = [hash_frozen(x) for x in obj]
        h = hash("set")
        for h_i in sorted(all_hs):
            h ^= h_i
        return h
    elif type(obj) == list:
        h = hash("list")
        for x in obj:
            h ^= hash_frozen(x)
        return h
    elif type(obj) == tuple:
        h = hash("tuple")
        for x in obj:
            h ^= hash_frozen(x)
        return h
            
    return hash(obj)

