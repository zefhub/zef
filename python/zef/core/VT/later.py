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

from .helpers import remove_names, absorbed

def same_as_validation(typ):
    abs = remove_names(absorbed(typ))
    if len(abs) != 1:
        raise Exception(f"SameAs needs exactly one absorbed argument, have {abs}")
    return True

def same_as_is_a(x, typ):
    assert same_as_validation(typ)
    
    thing = remove_names(absorbed(typ))[0]
    return discard_frame_imp(x) == discard_frame_imp(thing)


SameAs      = make_VT('SameAs', is_a_func = same_as_is_a)
# TODO Add or remove these once decided their importance
# Tagged     = make_VT('Tagged'   )
# LazyValue  = make_VT('LazyValue')
# Function   = make_VT('Function' )
# GraphDelta = make_VT('GraphDelta')
