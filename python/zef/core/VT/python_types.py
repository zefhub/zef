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

from . import make_VT

# Show of these are not python types but are very close and fit into the category of scalar primitives

make_VT('Nil', pytype=type(None))
make_VT('Any',
              is_a_func=lambda x,typ: True,
              is_subtype_func=lambda x,typ: True)

def numeric_is_a(x, typ):
    from .value_type import _value_type_pytypes
    python_type = _value_type_pytypes[typ._d['type_name']]
    try:
        return isinstance(x, python_type) or python_type(x) == x
    except:
        return False

PyBool = make_VT("PyBool", pytype=bool)
Bool = make_VT("Bool", pytype=bool, is_a_func=numeric_is_a)

PyInt = make_VT("PyInt", pytype=int)
Int = make_VT("Int", pytype=int, is_a_func=numeric_is_a)

PyFloat = make_VT("PyFloat", pytype=float)
Float = make_VT("Float", pytype=float, is_a_func=numeric_is_a)

String = make_VT('String', pytype=str)
PyBytes = make_VT('PyBytes', pytype=bytes)

from frozendict import frozendict
PyList = make_VT('PyList', pytype=list)
def PyDict_is_a(x, typ):
    return isinstance(x, (dict, frozendict))
PyDict = make_VT('PyDict', pytype=dict, is_a_func=PyDict_is_a)
PySet = make_VT('PySet', pytype=set)

PyTuple = make_VT('PyTuple', pytype=tuple)

