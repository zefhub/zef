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

from .value_type import *


# the following functions add a layer of indirection to prevent circular imports

def graph_ctor(*args, **kwargs):
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

# def error_ctor(*args, **kwargs):
#     from ... import core
#     return core.error._Error.Error(*args, **kwargs)



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
FlatGraph  = ValueType_(type_name='FlatGraph',  constructor_func=None)     
ZefOp      = ValueType_(type_name='ZefOp',      constructor_func=None)     
Stream     = ValueType_(type_name='Stream',     constructor_func=None)     
TX         = ValueType_(type_name='TX',         constructor_func=None)     
Time       = ValueType_(type_name='Time',       constructor_func=time_ctor)
Error      = ValueType_(type_name='Error',      constructor_func=None)
Image      = ValueType_(type_name='Image',      constructor_func=None)
ValueType  = ValueType_(type_name='ValueType',  constructor_func=ValueType_)


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
Record     = ValueType_(type_name='Record',     constructor_func=None)       
Function   = ValueType_(type_name='Function',   constructor_func=None)
GraphDelta = ValueType_(type_name='GraphDelta', constructor_func=None)     
Query      = ValueType_(type_name='Query',      constructor_func=None) 
DeltaQuery = ValueType_(type_name='DeltaQuery', constructor_func=None) 
Effect     = ValueType_(type_name='Effect',     constructor_func=None) 
DataFrame  = ValueType_(type_name='DataFrame', constructor_func=None)     
EZefRefs   = ValueType_(type_name='EZefRefs', constructor_func=None)     
EZefRefss  = ValueType_(type_name='EZefRefss', constructor_func=None)     
ZefRefs    = ValueType_(type_name='ZefRefs', constructor_func=None)     
ZefRefss   = ValueType_(type_name='ZefRefss', constructor_func=None)     


Pattern = ValueType_(type_name='Pattern', constructor_func=None)
# These are special classes: using them with [...] returns a ValueType_ though
Union = UnionClass()
Intersection = IntersectionClass()
Is = IsClass()
SetOf = SetOfClass()
Complement = ComplementClass()
