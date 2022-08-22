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


def c_value_type_check(val, s_typ):
    """Don't call this explicitly. Only for the zefdb core."""

    from ...pyzef.zefops import SerializedValue
    from .._ops import is_a

    typ = s_typ.deserialize()
    if type(val) == SerializedValue:
        val = val.deserialize()

    return is_a(val, typ)

def register_value_type_check():
    from ...pyzef import internals
    internals.register_value_type_check(c_value_type_check)
