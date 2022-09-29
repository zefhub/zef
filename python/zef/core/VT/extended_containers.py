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



# TODO: Change this to a proper isa
make_VT('Tuple', pytype=tuple)

def list_is_a(x, typ):
    import sys
    from typing import Generator
    if not isinstance(x, (PyList, PyTuple, Generator, ZefGenerator)):
        return False
    ab = typ._d['absorbed']
    if ab is None:
        return True

    if isinstance(x, (Generator, ZefGenerator)):
        raise NotImplementedError()
    if len(ab)!=1:    # List takes only one Type argument
        print('Something went wrong in `is_a[List[...]]`: multiple args curried into ', file=sys.stderr)

    return all(isinstance(item, ab[0]) for item in x)
make_VT('List', constructor_func=tuple, is_a_func=list_is_a)

def set_is_a(x, typ):
    import sys
    if not isinstance(x, PySet):
        return False
    ab = typ._d['absorbed']
    if ab is None:
        return True

    if len(ab)!=1:    # List takes only one Type argument
        print(f'Something went wrong in `is_a[Set[T1]]`: Set takes exactly one subtype, but got {x}', file=sys.stderr)
    return all(isinstance(item, ab[0]) for item in x)
make_VT('Set', pytype=set, is_a_func=set_is_a)


def dict_is_a(x, typ):
    import sys
    if not isinstance(x, PyDict):
        return False
    ab = typ._d['absorbed']
    if ab is None:
        return True

    if (len(ab)!=1 or len(ab[0])!=2):    # Dict must contain a type in one [] and that must be a pair
        print(f'Something went wrong in `is_a[Dict[T1, T2]]`: exactly two subtypes must be given. Got {ab}', file=sys.stderr)
        return Error('Error matching Dict[...]')

    T1, T2 = ab[0]
    return all(isinstance(key, T1) and isinstance(val, T2) for key,val in x.items())
make_VT('Dict', pytype=dict, is_a_func=dict_is_a)