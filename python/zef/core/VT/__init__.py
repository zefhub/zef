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

from ... import report_import
report_import("zef.core.VT")

from functools import partial
from .value_type import ValueType_, is_type_name_
from .. import internals
from ... import pyzef

__all__ = []

def insert_VT(name, vt):
    assert name not in globals()
    if vt._d["type_name"] != name:
        vt = vt._replace(alias=name)
    globals()[name] = vt
    global __all__
    __all__.append(name)
    return vt

def make_VT(name, **kwargs):
    vt = ValueType_(type_name=name, **kwargs)
    return insert_VT(name, vt)


def ValueType_validate(vt):
    from .value_type import absorbed
    assert type(vt) == ValueType_
    abs = absorbed(vt)
    if len(abs) == 0:
        return True
    elif len(abs) == 1:
        if not isinstance(abs[0], ValueType):
            raise Exception(f"Absorbed parameter in ValueType is itself not a ValueType: {abs}")
        return True
    else:
        raise Exception(f"ValueType has multiple absorbed things: {abs}")
def ValueType_is_a(x, vt):
    assert ValueType_validate(vt)
    if type(x) != ValueType_:
        return False
    from .value_type import absorbed, type_name
    assert len(absorbed(vt)) <= 1
    if len(absorbed(vt)) == 0:
        return True
    subtype = absorbed(vt)[0]
    return type_name(x) == type_name(subtype)
make_VT("ValueType", pytype=ValueType_, is_a_func=ValueType_is_a)

# I think these should become ValueTypeParmeters instead
# T          = ValueType_(type_name='T',  constructor_func=None)
# T1         = ValueType_(type_name='T1',  constructor_func=None)
# T2         = ValueType_(type_name='T2',  constructor_func=None)
# T3         = ValueType_(type_name='T3',  constructor_func=None)


# Bootstrap types
from . import python_types

# TODO: This should be replaced with the proper symbolic variables
Variable = String

from . import sets

from . import cond

from . import libzef_types

from . import rae_types

from . import external

from . import later

from . import codebase_query
# extended_containers relies on Error and ZefGenerator so must be imported later

# This is a type defined in internals, so can include it here
# TODO: Move this around to be somewhere sensible
from ..internals import Val_
make_VT("Val", pytype=Val_)
