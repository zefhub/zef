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

from . import make_VT, Error, ZefGenerator, PyList, PySet, PyTuple, PyDict
from .value_type import is_type_name_, generic_subtype_get_item, generic_subtype_str, generic_covariant_is_subtype



# TODO: Change this to a proper isa
def tuple_override_subtype(tup, typ):
    if is_type_name_(typ, "List"):
        return True
    return "maybe"
make_VT('Tuple', pytype=tuple, override_subtype_func=tuple_override_subtype,
        get_item_func=generic_subtype_get_item,
        str_func=generic_subtype_str,
        is_subtype_func=generic_covariant_is_subtype)

def list_is_a(x, typ):
    import sys
    from typing import Generator
    if not isinstance(x, (PyList, PyTuple, Generator, ZefGenerator)):
        return False
    if 'subtype' not in typ._d:
        return True
    ab = typ._d['subtype']
    if isinstance(x, (Generator, ZefGenerator)):
        raise NotImplementedError()

    return all(isinstance(item, ab) for item in x)

make_VT('List',
        constructor_func=tuple,
        is_a_func=list_is_a,
        get_item_func=generic_subtype_get_item,
        str_func=generic_subtype_str,
        is_subtype_func=generic_covariant_is_subtype)

def set_is_a(x, typ):
    import sys
    if not isinstance(x, PySet):
        return False
    if 'subtype' not in typ._d:
        return True
    ab = typ._d['subtype']

    return all(isinstance(item, ab) for item in x)
make_VT('Set', pytype=set, is_a_func=set_is_a,
        get_item_func=generic_subtype_get_item,
        str_func=generic_subtype_str,
        is_subtype_func=generic_covariant_is_subtype)


def dict_is_a(x, typ):
    import sys
    if not isinstance(x, PyDict):
        return False
    if 'subtype' not in typ._d:
        return True
    ab = typ._d['subtype']

    if (len(ab)!=2):    # Dict must contain a type in one [] and that must be a pair
        print(f'Something went wrong in `is_a[Dict[T1, T2]]`: exactly two subtypes must be given. Got {ab}', file=sys.stderr)
        return Error('Error matching Dict[...]')

    T1, T2 = ab
    return all(isinstance(key, T1) and isinstance(val, T2) for key,val in x.items())
make_VT('Dict', pytype=dict, is_a_func=dict_is_a,
        get_item_func=generic_subtype_get_item,
        str_func=generic_subtype_str,
        is_subtype_func=generic_covariant_is_subtype)