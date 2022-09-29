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

from functools import partial
from .value_type import *


# the following functions add a layer of indirection to prevent circular imports

def graph_ctor(*args, **kwargs):
    # Go to experimental load path for cases it supports
    if len(kwargs) == 0 and len(args) == 1 and type(args[0]) == str:
        from ...experimental.repl_interface import load_graph
        return load_graph(args[0])
    else:
        from ... import core
        return core.Graph(*args, **kwargs)



def time_ctor(*args, **kwargs):
    from ... import pyzef
    return pyzef.main.Time(*args, **kwargs)


def bytes_ctor(*args, **kwargs):
    # from ... import core
    from ...core.bytes import Bytes_
    # return core.bytes.Bytes_(*args, **kwargs)
    return Bytes_(*args, **kwargs)


def decimal_ctor(*args, **kwargs):
    # from ... import core
    from ...core.decimal import Decimal_
    # return core.bytes.Bytes_(*args, **kwargs)
    return Decimal_(*args, **kwargs)


def flatgraph_ctor(*args, **kwargs):
    from ...core.flat_graph import FlatGraph_
    return FlatGraph_(*args)


def union_getitem(x):
    from ..op_structs import ZefOp
    if isinstance(x, tuple):
        return ValueType_(type_name='Union', absorbed=x)
    elif isinstance(x, ValueType_):
        return ValueType_(type_name='Union', absorbed=(x,))
    elif isinstance(x, ZefOp):
        return ValueType_(type_name='Union', absorbed=(x,))
    else:
        raise Exception(f'"Union[...]" called with unsupported type {type(x)}')

def intersection_getitem(x):
    from ..op_structs import ZefOp
    if isinstance(x, tuple):
        return ValueType_(type_name='Intersection', absorbed=x)
    elif isinstance(x, ValueType_):
        return ValueType_(type_name='Intersection', absorbed=(x,))
    elif isinstance(x, ZefOp):
        return ValueType_(type_name='Intersection', absorbed=(x,))
    else:
        raise Exception(f'"Intersection[...]" called with unsupported type {type(x)}')

def complement_getitem(x):
    if isinstance(x, ValueType_):
        return ValueType_(type_name='Complement', absorbed=(x,))
    else:
        raise Exception(f'"Complement[...]" called with unsupported type {type(x)}')

def is_getitem(x):
    from typing import Callable
    from ..op_structs import ZefOp
    from .. import func
    if isinstance(x, tuple):
        return ValueType_(type_name='Is', absorbed=x)
    elif isinstance(x, ValueType_):
        return ValueType_(type_name='Is', absorbed=(x,))
    elif isinstance(x, ZefOp):
        return ValueType_(type_name='Is', absorbed=(x,))
    elif isinstance(x, Callable):
        return ValueType_(type_name='Is', absorbed=(func[x],))
    else:
        raise Exception(f'"Is[...]" called with unsupported type {type(x)}')



def setof_ctor(*args):
    """
    Can be called with either SetOf[5,6,7] or SetOf(5,6,7).
    When calling with square brackets, a tuple must always be 
    passed implicitly or explicitly.
    SetOf[42] is NOT valid
    SetOf[(42,)] is valid
    SetOf[42,] is valid    
    SetOf[42, 43] is valid    
    """
    return ValueType_(type_name='SetOf', absorbed = (args, ))



def setof_getitem(x):
    if not isinstance(x, tuple):
        raise TypeError(f"`SetOf[...]` must be called with a tuple, either explicitly or implicitly. e.g. SetOf[3,4], SetOf[(3,4)]. When wrapping one value, use SetOf[42,]. It was called with {x}. ")
    return ValueType_(type_name='SetOf', absorbed=(x, ))




def rp_getitem(x):
    if not isinstance(x, tuple) or len(x)!=3:
        raise TypeError(f"`RP`[...]  must be initialized with a triple to match on. Got x={x}")
    return ValueType_(type_name='RP', absorbed=(x,))


def same_as_get_item(y):
    from ..op_implementations.implementation_typing_functions import discard_frame_imp
    return Is[lambda x: discard_frame_imp(x) == discard_frame_imp(y)]



Nil        = ValueType_(type_name='Nil',        constructor_func=None)
Any        = ValueType_(type_name='Any',        constructor_func=None)
Bool       = ValueType_(type_name='Bool',       constructor_func=bool)
Int        = ValueType_(type_name='Int',        constructor_func=int)
Float      = ValueType_(type_name='Float',      constructor_func=float)
String     = ValueType_(type_name='String',     constructor_func=str)
Bytes      = ValueType_(type_name='Bytes',      constructor_func=bytes_ctor)
Decimal    = ValueType_(type_name='Decimal',    constructor_func=decimal_ctor)
List       = ValueType_(type_name='List',       constructor_func=tuple)
Dict       = ValueType_(type_name='Dict',       constructor_func=dict)
Set        = ValueType_(type_name='Set',        constructor_func=set)
EZefRef    = ValueType_(type_name='EZefRef',    constructor_func=None)    
ZefRef     = ValueType_(type_name='ZefRef',     constructor_func=None)     
Graph      = ValueType_(type_name='Graph',      constructor_func=graph_ctor)     
GraphSlice = ValueType_(type_name='GraphSlice', constructor_func=None)     
FlatGraph  = ValueType_(type_name='FlatGraph',  constructor_func=flatgraph_ctor)     
ZefOp      = ValueType_(type_name='ZefOp',      constructor_func=None)     
Stream     = ValueType_(type_name='Stream',     constructor_func=None)     
TX         = ValueType_(type_name='TX',         constructor_func=None)     
Time       = ValueType_(type_name='Time',       constructor_func=time_ctor)
Error      = ValueType_(type_name='Error',      constructor_func=None)
Image      = ValueType_(type_name='Image',      constructor_func=None)
ValueType  = ValueType_(type_name='ValueType',  constructor_func=ValueType_)
T          = ValueType_(type_name='T',  constructor_func=None)
T1         = ValueType_(type_name='T1',  constructor_func=None)
T2         = ValueType_(type_name='T2',  constructor_func=None)
T3         = ValueType_(type_name='T3',  constructor_func=None)


# QuantityInt= ValueType_(type_name='QuantityInt',constructor_func=None)
# QuantityFloat= ValueType_(type_name='QuantityFloat',constructor_func=None)

UID        = ValueType_(type_name='UID',        constructor_func=None)       
BaseUID    = ValueType_(type_name='BaseUID',    constructor_func=None)       
ZefRefUID  = ValueType_(type_name='ZefRefUID',  constructor_func=None)        
EternalUID = ValueType_(type_name='EternalUID', constructor_func=None)       

Instantiated = ValueType_(type_name='Instantiated', constructor_func=None)       
Terminated = ValueType_(type_name='Terminated', constructor_func=None)       
Assigned   = ValueType_(type_name='Assigned',   constructor_func=None)       
Tagged     = ValueType_(type_name='Tagged',     constructor_func=None)       

LazyValue  = ValueType_(type_name='LazyValue',  constructor_func=None) 
Awaitable  = ValueType_(type_name='Awaitable',  constructor_func=None) 



AET        = ValueType_(type_name='AET',        constructor_func=None)
ET         = ValueType_(type_name='ET',         constructor_func=None)
RT         = ValueType_(type_name='RT',         constructor_func=None)
BT         = ValueType_(type_name='BT',         constructor_func=None)
Enum       = ValueType_(type_name='Enum',       constructor_func=None)
Tuple      = ValueType_(type_name='Tuple',     constructor_func=None)       
Function   = ValueType_(type_name='Function',   constructor_func=None)
GraphDelta = ValueType_(type_name='GraphDelta', constructor_func=None)     
Query      = ValueType_(type_name='Query',      constructor_func=None) 
Effect     = ValueType_(type_name='Effect',     constructor_func=None) 
DataFrame  = ValueType_(type_name='DataFrame',  constructor_func=None)     
# EZefRefs   = ValueType_(type_name='EZefRefs', constructor_func=None)     
# EZefRefss  = ValueType_(type_name='EZefRefss', constructor_func=None)     
# ZefRefs    = ValueType_(type_name='ZefRefs', constructor_func=None)     
# ZefRefss   = ValueType_(type_name='ZefRefss', constructor_func=None)     

Pattern        = ValueType_(type_name='Pattern',             constructor_func=None)
Union          = ValueType_(type_name='Union',               constructor_func=None,             get_item_func=union_getitem)
Intersection   = ValueType_(type_name='Intersection',        constructor_func=None,             get_item_func=intersection_getitem)
Is             = ValueType_(type_name='Is',                  constructor_func=None,             get_item_func=is_getitem)
SetOf          = ValueType_(type_name='SetOf',               constructor_func=setof_ctor,       get_item_func=setof_getitem)
Complement     = ValueType_(type_name='Complement',          constructor_func=None,             get_item_func=complement_getitem)
RP             = ValueType_(type_name='RP',                  constructor_func=None,             get_item_func=rp_getitem)
HasValue       = ValueType_(type_name='HasValue',            constructor_func=None)
SameAs         = ValueType_(type_name='SameAs',                                                 get_item_func = same_as_get_item)




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
