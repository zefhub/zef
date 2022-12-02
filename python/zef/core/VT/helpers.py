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



# Functions that are useful for implementing other type functions

# Note: must be imported after python_types
from .python_types import PyTuple
from . import ValueType
from .value_type import absorbed, type_name, is_subtype_

def remove_names(absorbed: PyTuple):
    from . import Variable
    # TODO: Replace with early ops if possible
    return tuple(x for x in absorbed if not isinstance(x, Variable))

def names_of(typ: ValueType):
    from . import Variable
    # TODO: Replace with early ops if possible
    return tuple(x for x in absorbed(typ) if isinstance(x, Variable))


##############################
# * Subtype VTs

# These VTs have a single curried type which is used for representing them and
# for membership tests.
#----------------------------

# def generic_subtype_str(self):
#     s = self._d["type_name"]
#     if "subtype" in self._d:
#         s += f"[{self._d['subtype']}]"
#     s += ''.join(f"[{x!r}]" for x in self._d["absorbed"])
#     return s

def generic_subtype_validate(typ):
    # Should validation return True/False? Or True/String? Or True and throw?
    items = remove_names(absorbed(typ))
    if len(items) == 0:
        return True
    if len(items) > 2:
        raise Exception(f"Generic subtype validation failed: there are two or more absorbed subtypes ({items})")
    subtype = items[0]
    if not isinstance(subtype, ValueType):
        raise Exception(f"Generic subtype validation failed: subtype is not a ValueType ({subtype})")
    return True

def generic_subtype_get(typ):
    assert generic_subtype_validate(typ)
    items = remove_names(absorbed(typ))
    if len(items) == 0:
        return None
    else:
        return items[0]


################################################
# * Covariant subtype VTs

# These subtype VTs allow for multiple parameters and the membership test is
# performed covariantly (that is, the individual parameters are compared and
# combined with AND)
#----------------------------------------------

def generic_covariant_validate(typ):
    # Should validation return True/False? Or True/String? Or True and throw?
    items = remove_names(absorbed(typ))
    for item in items:
        if not isinstance(item, ValueType):
            raise Exception(f"Generic covariant type validation failed: item is not a ValueType ({item})")
    return True

def generic_covariant_parameters(typ):
    assert generic_covariant_validate(typ)
    return remove_names(absorbed(typ))

def generic_covariant_is_subtype(x, super):
    if type_name(x) != type_name(super):
        return False
    super_params = generic_covariant_parameters(super)
    x_params = generic_covariant_parameters(x)

    return all(is_subtype_(a,b) is True for a,b in zip(x_params, super_params))