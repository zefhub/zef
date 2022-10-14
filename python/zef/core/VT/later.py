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

# the following functions add a layer of indirection to prevent circular imports

def same_as_get_item(y):
    from . import Is
    from ..op_implementations.implementation_typing_functions import discard_frame_imp
    return Is[lambda x: discard_frame_imp(x) == discard_frame_imp(y)]


SameAs      = make_VT('SameAs', get_item_func = same_as_get_item)
# TODO Add or remove these once decided their importance
# Tagged     = make_VT('Tagged'   )
# LazyValue  = make_VT('LazyValue')
# Function   = make_VT('Function' )
# GraphDelta = make_VT('GraphDelta')
