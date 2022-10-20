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
from . import ValueType, make_VT, PyDict, PyList

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



def union_getitem(self, x):
    if "types" in self._d:
        raise Exception("Can't absorb types twice for a Union")
    if not isinstance(x, tuple):
        x = (x,)
    assert all(isinstance(el, ValueType) for el in x), "Union requires ValueTypes"
    return self._replace(types=x)

def union_is_a(val, typ):
    if "types" not in typ._d:
        return NotImplemented
    return any(isinstance(val, subtyp) for subtyp in typ._d["types"])

def union_override_subtype(union, typ):
    if "types" not in typ._d:
        return NotImplemented
    return all(issubclass(x, typ) for x in union._d["types"])

def union_str(self):
    s = "Union"
    if "types" in self._d:
        s += "[" + ", ".join(str(x) for x in self._d["types"]) + "]"
    if len(self._d["absorbed"]) > 0:
        s += f"[{self._d['absorbed'][0]}]"
    return s

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
    if "types" not in x._d:
        return x
    old_abs = x._d['types']
    types = tuple(el._d['types'] if is_union_VT(el) else (el,) for el in old_abs)  # flatten this out
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
                get_item_func=union_getitem,
                is_a_func=union_is_a,
                simplify_type_func=union_simplify,
                override_subtype_func=union_override_subtype,
                str_func=union_str)

def intersection_getitem(self, x):
    if "types" in self._d:
        raise Exception("Can't absorb types twice for an Intersection")
    if not isinstance(x, tuple):
        x = (x,)
    assert all(isinstance(el, ValueType) for el in x), "Intersection requires ValueTypes"
    return self._replace(types=x)

def intersection_is_a(val, typ):
    if "types" not in typ._d:
        return NotImplemented
    return all(isinstance(val, subtyp) for subtyp in typ._d["types"])

warned_about_intersection = False
def intersection_override_subtype(intersection, typ):
    if "types" not in intersection._d:
        return NotImplemented
    # raise NotImplementedError("This is tricky!")
    # print("WARNING: using intersection subtype override is not rigorous yet")
    # TODO: basically want to add the new type into the intersection and then
    # check if the resultant set is provably empty
    #
    # For now do the easy cases and return "maybe" otherwise
    result = any(issubclass(x, typ) for x in intersection._d["types"])
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
    if "types" not in x._d:
        return x
    old_abs = x._d['types']
    types = tuple(el._d['types'] if is_intersection_VT(el) else (el,) for el in old_abs)  # flatten this out
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
                       get_item_func=intersection_getitem,
                       is_a_func=intersection_is_a,
                       simplify_type_func=intersection_simplify,
                       override_subtype_func=intersection_override_subtype)

def complement_getitem(self, x):
    if "type" in self._d:
        return NotImplemented
    if isinstance(x, ValueType):
        return self._replace(type=(x,))
    else:
        raise Exception(f'"Complement[...]" called with unsupported type {type(x)}')

def complement_is_a(val, typ):
    return not isinstance(val, typ._d['type'])


make_VT('Complement',
        get_item_func=complement_getitem,
        is_a_func=complement_is_a)

def is_getitem(self, x):
    if "predicate" in self._d:
        raise Exception("Can't absorb a second predicate into an Is")

    # TODO: I want to change this to just "callables" as otherwise the different
    # behaviour could lead to unexpected problems
    from typing import Callable
    from ..op_structs import ZefOp
    from .. import func
    if isinstance(x, tuple):
        return self._replace(predicate=x)
    elif isinstance(x, ZefOp):
        return self._replace(predicate=(x,))
    elif isinstance(x, Callable):
        return self._replace(predicate=(func[x],))
    else:
        raise Exception(f'"Is[...]" called with unsupported type {type(x)}')

def is_is_a(el, typ):
    if "predicate" not in typ._d:
        return NotImplemented
    from typing import Callable
    from . import ZefOp
    for t in typ._d['predicate']:
        if isinstance(t, ValueType):
            # return Error.ValueError(f"A ValueType_ was passed to Is but it only takes predicate functions. Try wrapping in is_a[{t}]")
            raise Exception(f"A ValueType_ was passed to Is but it only takes predicate functions. Try wrapping in is_a[{t}]")
        elif isinstance(t, Callable) or isinstance(t, ZefOp):
            try:
                if not t(el): return False
            except:
                return False
        else:
            # return Error.ValueError(f"Expected a predicate function or a ZefOp type inside Is but got {t} instead.")
            return Exception(f"Expected a predicate function or a ZefOp type inside Is but got {t} instead.")
    return True

make_VT('Is',
        get_item_func=is_getitem,
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
    if "instances" in self._d:
        return NotImplemented
    return self._replace(instances=args)

def setof_is_a(x, typ):
    if "instances" not in typ._d:
        return NotImplemented
    return x in typ._d["instances"]

def setof_override_subtype(setof, typ):
    if "instances" not in setof._d:
        return NotImplemented
    return all(isinstance(x, typ) for x in setof._d["instances"])

def setof_str(self):
    s = self._d["type_name"]
    if "instances" in self._d:
        s += "[" + ", ".join(repr(x) for x in self._d['instances']) + "]"
    s += ''.join(f"[{x!r}]" for x in self._d["absorbed"])
    return s

make_VT('SetOf',
        constructor_func=setof_ctor,
        pass_self=True,
        is_a_func=setof_is_a,
        str_func=setof_str,
        override_subtype_func=setof_override_subtype)



def pattern_getitem(self, x):
    if "pattern" in self._d:
        raise Exception("Pattern can't take a second pattern")
    return self._replace(pattern=x)

def pattern_vt_matching(x, typ):
    class Sentinel: pass
    sentinel = Sentinel() 
    p = typ._d["pattern"]

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
    elif isinstance(x, List):
        for p_e, x_e in zip(p, x): # Creates tuples of pairwise elements from both lists
            if not isinstance(p_e, ValueType):
                raise ValueError(f"The pattern passed didn't have a ValueType but rather {p_e}")
            if not isinstance(x_e, p_e): return False  
        return True

    raise NotImplementedError(f"Pattern ValueType isn't implemented for {x}")


make_VT('Pattern', is_a_func=pattern_vt_matching,
        get_item_func=pattern_getitem)

