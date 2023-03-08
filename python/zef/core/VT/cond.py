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


# This file implements Cond[TestVT][SecondaryVT]
#
# It is equivalent to an implication, that if the first applies, so must the
# second.
#
# Alternatively, *either* TestVT doesn't apply, or SecondaryVT must apply, i.e.
# ~TestVT | SecondaryVT.
#
# The rest of this code is just a convoluted way to express:
# Cond[X][Y] = ~X | Y

from . import make_VT, ValueType

from .helpers import remove_names, absorbed

def cond_validation(typ):
    abs = remove_names(absorbed(typ))
    if len(abs) != 2:
        raise Exception(f"Cond needs exactly one absorbed argument, have {abs}")
    for x in abs:
        if not isinstance(x, ValueType):
            raise Exception(f"Cond requires ValueTypes. Found {x}")
    return True

def cond_is_a(x, typ):
    assert cond_validation(typ)
    
    test,follow_up = remove_names(absorbed(typ))

    # This test is like a logic implication so we can replace it with:
    equivalent = ~test | follow_up

    return isinstance(x, equivalent)

Cond = make_VT("Cond",
               is_a_func=cond_is_a)