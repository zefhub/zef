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


# from .value_type import *
from . import ValueType, make_VT, PyDict, PyList, PyTuple
from .helpers import remove_names, absorbed, type_name

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
            #
            # TODO: Move this to pure_utils
            from ..op_implementations.implementation_typing_functions import value_hash
            h = value_hash(el)
        else:
            h = el
        if h not in seen:
            yield el
            seen.add(h)

def is_union_VT(y):
    return isinstance(y, ValueType) and y._d['type_name'] == 'Union'
def is_intersection_VT(y):
    return isinstance(y, ValueType) and y._d['type_name'] == 'Intersection'


def get_union_intersection_subtypes(typ):
    subtypes = remove_names(absorbed(typ))[0]
    if not isinstance(subtypes, PyTuple):
        subtypes = (subtypes,)
    return subtypes


def union_validation(typ):
    abs = remove_names(absorbed(typ))
    if len(abs) != 1:
        raise Exception(f"Union needs exactly 1 absorbed type or tuple of types (has {abs})")
    subtypes = abs[0]
    if not isinstance(subtypes, PyTuple):
        subtypes = (subtypes,)
    assert all(isinstance(el, ValueType) for el in subtypes), "Union requires ValueTypes"
    return True

def union_is_a(val, typ):
    assert union_validation(typ)
    subtypes = get_union_intersection_subtypes(typ)
    return any(isinstance(val, subtyp) for subtyp in subtypes)

def union_override_subtype(union, typ):
    assert union_validation(union)
    subtypes = get_union_intersection_subtypes(union)
    return all(issubclass(x, typ) for x in subtypes)

def union_is_subtype(other, union):
    assert union_validation(union)
    subtypes = get_union_intersection_subtypes(union)
    return any(issubclass(other, x) for x in subtypes)

def union_simplify(x):
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
    # flatten out unions: Union[Union[A][B]][C]  == Union[A][B][C]
    old_subtypes = get_union_intersection_subtypes(x)
    types = tuple(get_union_intersection_subtypes(el) if is_union_VT(el) else (el,) for el in old_subtypes)  # flatten this out
    flattened = list(make_distinct(a2 for a1 in types for a2 in a1))
    supers = []
    for i,x in enumerate(flattened):
        for j,y in enumerate(flattened):
            if j != i:
                if issubclass(x,y):
                    break
        else:
            supers.append(x)
    if len(supers) == 1:
        return supers[0]
    return Union[tuple(supers)]


Union = make_VT('Union',
                is_a_func=union_is_a,
                is_subtype_func=union_is_subtype,
                # simplify_type_func=union_simplify,
                override_subtype_func=union_override_subtype)

def intersection_validation(typ):
    abs = remove_names(absorbed(typ))
    if len(abs) != 1:
        raise Exception("Intersection needs exactly 1 absorbed type or tuple of types")
    subtypes = abs[0]
    if not isinstance(subtypes, PyTuple):
        subtypes = (subtypes,)
    assert all(isinstance(el, ValueType) for el in subtypes), "Intersection requires ValueTypes"
    return True

def intersection_is_a(val, typ):
    assert intersection_validation(typ)
    subtypes = get_union_intersection_subtypes(typ)
    return all(isinstance(val, subtyp) for subtyp in subtypes)

warned_about_intersection = False
def intersection_override_subtype(intersection, typ):
    assert intersection_validation(intersection)
    subtypes = get_union_intersection_subtypes(intersection)
    # raise NotImplementedError("This is tricky!")

    # An intersection X is a subtype of Y, if the intersection of X and ~Y is
    # empty. Since we can't prove this yet, go for a cop-out.
    #
    # For now do the easy cases and return "maybe" otherwise
    result = any(issubclass(x, typ) for x in subtypes)
    if result is False:
        global warned_about_intersection
        if not warned_about_intersection:
            print("WARNING: using intersection subtype override is not rigorous yet")
            print("WARNING: using intersection subtype override is not rigorous yet")
            print("WARNING: using intersection subtype override is not rigorous yet")
            print("WARNING: using intersection subtype override is not rigorous yet")
            print("WARNING: using intersection subtype override is not rigorous yet")
            warned_about_intersection = True

        result = "maybe"
    return result


def intersection_simplify(x):
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
    # flatten out Intersections: Intersection[Intersection[A][B]][C]  == Intersection[A][B][C]
    old_subtypes = get_union_intersection_subtypes(x)
    types = tuple(get_union_intersection_subtypes(el) if is_intersection_VT(el) else (el,) for el in old_subtypes)  # flatten this out
    flattened = list(make_distinct(a2 for a1 in types for a2 in a1))
    supers = []
    for i,x in enumerate(flattened):
        for j,y in enumerate(flattened):
            if j != i:
                if issubclass(y,x):
                    break
        else:
            supers.append(x)

    if len(supers) == 1:
        return supers[0]
    # TODO: If there is no overlap between any of the individual sets, this should return the empty set
    return Intersection[tuple(supers)]

Intersection = make_VT('Intersection',
                       is_a_func=intersection_is_a,
                       # simplify_type_func=intersection_simplify,
                       override_subtype_func=intersection_override_subtype)

def complement_validation(typ):
    abs = remove_names(absorbed(typ))
    if len(abs) != 1:
        raise Exception(f"Complement requires a single type not {abs}")
    assert isinstance(abs[0], ValueType), f"Complement require a ValueType not {abs[0]}"

def complement_is_a(val, typ):
    complement_validation(typ)
    subtype = remove_names(absorbed(typ))[0]
    return not isinstance(val, subtype)


make_VT('Complement',
        is_a_func=complement_is_a)

def is_validation(typ):
    abs = remove_names(absorbed(typ))
    if len(abs) != 1:
        raise Exception(f"Is requires a single predicate not {abs}")

    subtype = abs[0]
    
    # TODO: I want to change this to just "callables" as otherwise the different
    # behaviour could lead to unexpected problems
    from typing import Callable
    from ..op_structs import ZefOp
    from .. import func
    if isinstance(subtype, (ZefOp, Callable)):
        return True
    raise Exception(f'"Is[...]" called with unsupported type {subtype}')

def is_is_a(el, typ):
    is_validation(typ)
    predicate = remove_names(absorbed(typ))[0]
    try:
        if not predicate(el): return False
    except:
        return False
    return True

make_VT('Is',
        is_a_func=is_is_a)

def setof_ctor(self, *args):
    """
    Can be called with either SetOf[5,6,7] or SetOf(5,6,7).
    When calling with square brackets, a tuple must always be 
    passed implicitly or explicitly.
    SetOf[42] is NOT valid
    SetOf[(42,)] is valid
    SetOf[42,] is valid    
    SetOf[42, 43] is valid    
    """
    abs = remove_names(absorbed(self))
    if len(abs) > 0:
        return NotImplemented
    return self[args]

def setof_validation(typ):
    assert type_name(typ) == "SetOf"
    items = remove_names(absorbed(typ))
    if len(items) >= 2:
        raise Exception("SetOf can't have multiple absorbed arguments")
    return True

def setof_get_items(typ):
    setof_validation(typ)
    items = remove_names(absorbed(typ))
    if len(items) == 0:
        return ()
    return items[0]

def setof_is_a(x, typ):
    abs = setof_get_items(typ)
    return x in abs

def setof_override_subtype(setof, typ):
    return all(isinstance(x, typ) for x in setof_get_items(setof))

make_VT('SetOf',
        constructor_func=setof_ctor,
        pass_self=True,
        is_a_func=setof_is_a,
        override_subtype_func=setof_override_subtype)



def pattern_validation(self):
    abs = remove_names(absorbed(self))
    if len(abs) != 1:
        raise Exception(f"Pattern requires a single absorbed item not {abs}")
        
    pattern = abs[0]
    if not isinstance(pattern, PyDict | PyList):
        raise Exception(f"Pattern takes either a dictionary or a list not {pattern}")

    return True

def pattern_vt_matching(x, typ):
    assert pattern_validation(typ)
    p = remove_names(absorbed(typ))[0]

    class Sentinel: pass
    sentinel = Sentinel() 

    # Note: by this point, we only have access to PyDict and not Dict
    if not ((isinstance(x, PyDict) and isinstance(p, PyDict)) or
            (isinstance(x, PyList) and isinstance(p, PyList))):
        return False
    if isinstance(x, PyDict):
        for k, v in p.items():            
            r = x.get(k, sentinel)
            if r is sentinel: return False
            if not isinstance(v, ValueType): raise ValueError(f"The pattern passed didn't have a ValueType but rather {v}")
            if not isinstance(r, v): return False  
        return True
    elif isinstance(x, PyList):
        for p_e, x_e in zip(p, x): # Creates tuples of pairwise elements from both lists
            if not isinstance(p_e, ValueType):
                raise ValueError(f"The pattern passed didn't have a ValueType but rather {p_e}")
            if not isinstance(x_e, p_e): return False  
        return True

    raise NotImplementedError(f"Pattern ValueType isn't implemented for {x}")


make_VT('Pattern', is_a_func=pattern_vt_matching)

