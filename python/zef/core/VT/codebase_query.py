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

def rp_getitem(self, x):
    if not isinstance(x, tuple) or len(x)!=3:
        raise TypeError(f"`RP`[...]  must be initialized with a triple to match on. Got x={x}")
    return self._replace(absorbed = (x,))

def operates_on_ctor(x):
    from .._ops import operates_on, contains
    from . import Is
    return Is[operates_on | contains[x]]

def related_ops_ctor(x):
    from . import Is
    from .._ops import related_ops, contains
    return Is[related_ops | contains[x]]

def used_for_ctor(x):
    from . import Is
    from .._ops import used_for, contains
    return Is[used_for | contains[x]]



RP          = make_VT('RP', get_item_func=rp_getitem)
HasValue    = make_VT('HasValue')   # TODO Remove this later
Query      = make_VT('Query'    )
OperatesOn  = make_VT('OperatesOn',   constructor_func = operates_on_ctor,  get_item_func = operates_on_ctor)
RelatedOps  = make_VT('RelatedOps',   constructor_func = related_ops_ctor,  get_item_func = related_ops_ctor)
UsedFor     = make_VT('UsedFor',      constructor_func = used_for_ctor, get_item_func =used_for_ctor)



