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


def c_determine_primitive_type(aet):
    """Don't call this explicitly. Only for the zefdb core."""

    from ...pyzef.internals import AttributeEntityType, VRT
    return VRT.Any

def register_determine_primitive_type():
    from ...pyzef import internals
    internals.register_determine_primitive_type(c_determine_primitive_type)
