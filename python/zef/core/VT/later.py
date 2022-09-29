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



# the following functions add a layer of indirection to prevent circular imports

def rp_getitem(self, x):
    if not isinstance(x, tuple) or len(x)!=3:
        raise TypeError(f"`RP`[...]  must be initialized with a triple to match on. Got x={x}")
    return ValueType_(type_name='RP', absorbed=(x,))

def same_as_get_item(y):
    from ..op_implementations.implementation_typing_functions import discard_frame_imp
    return Is[lambda x: discard_frame_imp(x) == discard_frame_imp(y)]

RP             = ValueType_(type_name='RP',                  constructor_func=None,             get_item_func=rp_getitem)
HasValue       = ValueType_(type_name='HasValue',            constructor_func=None)
SameAs         = ValueType_(type_name='SameAs',                                                 get_item_func = same_as_get_item)

TX         = ValueType_(type_name='TX',         constructor_func=None)

def delegate_is_a(val, typ):
    # TODO Need to make some tricky decisions here
    # For now, any abstract delegate + any delegate zefref counts
    if isinstance(val, AbstractDelegate):
        return True
    if isinstance(val, (ZefRef, EZefRef)):
        return internals.is_delegate(val)
    return False

Delegate         = ValueType_(type_name='Delegate',   constructor_func=None, is_a_func=delegate_is_a)


# Instantiated = ValueType_(type_name='Instantiated', constructor_func=None)
# Terminated = ValueType_(type_name='Terminated', constructor_func=None)
# Assigned   = ValueType_(type_name='Assigned',   constructor_func=None)
Tagged     = ValueType_(type_name='Tagged',     constructor_func=None)

LazyValue  = ValueType_(type_name='LazyValue',  constructor_func=None)
Awaitable  = ValueType_(type_name='Awaitable',  constructor_func=None)


Function   = ValueType_(type_name='Function',   constructor_func=None)

GraphDelta = ValueType_(type_name='GraphDelta', constructor_func=None)
Query      = ValueType_(type_name='Query',      constructor_func=None)
Effect     = ValueType_(type_name='Effect',     constructor_func=None)
Val        = ValueType_(type_name='Val',        constructor_func=None, pytype=internals.Val)

def operates_on_ctor(x):
    from .._ops import operates_on, contains
    return Is[operates_on | contains[x]]

def related_ops_ctor(x):
    from .._ops import related_ops, contains
    return Is[related_ops | contains[x]]

def used_for_ctor(x):
    from .._ops import used_for, contains
    return Is[used_for | contains[x]]

OperatesOn     = ValueType_(type_name='OperatesOn',   constructor_func = operates_on_ctor,  get_item_func = operates_on_ctor)
RelatedOps     = ValueType_(type_name='RelatedOps',   constructor_func = related_ops_ctor,  get_item_func = related_ops_ctor)
UsedFor        = ValueType_(type_name='UsedFor',      constructor_func = used_for_ctor, get_item_func =used_for_ctor)



