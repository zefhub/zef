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

from . import make_VT, Error, ZefGenerator, PyList, PySet, PyTuple, PyDict, ValueType
from .value_type import is_type_name_
from .helpers import remove_names, absorbed, generic_subtype_validate, generic_subtype_get, generic_covariant_is_subtype, type_name



# TODO: Change this to a proper isa
def tuple_validation(tup):
    items = remove_names(absorbed(tup))
    if len(items) == 0:
        return True
    if len(items) >= 2:
        raise Exception("Tuple can't have more than one curried item")
    tup_params = items[0]
    assert all(isinstance(x, ValueType) for x in tup_params)
    return True

def tuple_get_params(tup):
    assert tuple_validation(tup)
    items = remove_names(absorbed(tup))
    if len(items) == 0:
        return ()
    return items[0]
    
def tuple_override_subtype(tup, typ):
    assert tuple_validation(tup)
    if is_type_name_(typ, "List"):
        return True
    return "maybe"

def tuple_is_subtype(x, tup):
    assert tuple_validation(tup)
    if type_name(x) != "Tuple":
        # TODO: Lists etc should be allowed
        return False
    assert tuple_validation(x)
    tup_params = tuple_get_params(tup)
    x_params = tuple_get_params(x)

    return all(issubclass(a,b) is True for a,b in zip(x_params, tup_params))

make_VT('Tuple', pytype=tuple, override_subtype_func=tuple_override_subtype,
        is_subtype_func=tuple_is_subtype)

def list_is_a(x, typ):
    assert generic_subtype_validate(typ)
    import sys
    from typing import Generator
    if not isinstance(x, (PyList, PyTuple, Generator, ZefGenerator)):
        return False
    subtype = generic_subtype_get(typ)
    if subtype is None:
        return True
    if isinstance(x, (Generator, ZefGenerator)):
        raise NotImplementedError()

    return all(isinstance(item, subtype) for item in x)

make_VT('List',
        constructor_func=tuple,
        is_a_func=list_is_a,
        is_subtype_func=generic_covariant_is_subtype)

def set_is_a(x, typ):
    assert generic_subtype_validate(typ)
    import sys
    if not isinstance(x, PySet):
        return False
    subtype = generic_subtype_get(typ)
    if subtype is None:
        return True

    return all(isinstance(item, subtype) for item in x)
make_VT('Set', pytype=set, is_a_func=set_is_a,
        is_subtype_func=generic_covariant_is_subtype)

def dict_validate(typ):
    # Should validation return True/False? Or True/String? Or True and throw?
    items = remove_names(absorbed(typ))
    if len(items) == 0:
        return True
    if len(items) != 2:
        raise Exception(f"Dict validation failed: there should be two subtypes for the keys and values ({items})")
    key_type, value_type = items
    if not isinstance(key_type, ValueType):
        raise Exception(f"Dict validation failed: key type is not a ValueType ({key_type})")
    if not isinstance(value_type, ValueType):
        raise Exception(f"Dict validation failed: value type is not a ValueType ({value_type})")
    return True

def dict_is_a(x, typ):
    assert dict_validate(typ)
    import sys
    if not isinstance(x, PyDict):
        return False
    abs = remove_names(absorbed(typ))
    if len(abs) == 0:
        return True

    T1, T2 = abs
    return all(isinstance(key, T1) and isinstance(val, T2) for key,val in x.items())
make_VT('Dict', pytype=dict, is_a_func=dict_is_a,
        is_subtype_func=generic_covariant_is_subtype)