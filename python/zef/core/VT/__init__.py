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


# def error_ctor(*args, **kwargs):
#     from ... import core
#     return core.error._Error.Error(*args, **kwargs)


Error.RuntimeError('abcd')

Nil        = ValueType(type_name='Nil',        constructor_func=None)
Any        = ValueType(type_name='Any',        constructor_func=None)
Bool       = ValueType(type_name='Bool',       constructor_func=bool)
Int        = ValueType(type_name='Int',        constructor_func=int)
Float      = ValueType(type_name='Float',      constructor_func=float)
String     = ValueType(type_name='String',     constructor_func=str)
List       = ValueType(type_name='List',       constructor_func=tuple)
Dict       = ValueType(type_name='Dict',       constructor_func=dict)
Set        = ValueType(type_name='Set',        constructor_func=set)
EZefRef    = ValueType(type_name='EZefRef',    constructor_func=None)    
ZefRef     = ValueType(type_name='ZefRef',     constructor_func=None)     
Graph      = ValueType(type_name='Graph',      constructor_func=graph_ctor)     
GraphSlice = ValueType(type_name='GraphSlice', constructor_func=None)     
FlatGraph  = ValueType(type_name='FlatGraph',  constructor_func=None)     
ZefOp      = ValueType(type_name='ZefOp',      constructor_func=None)     
Stream     = ValueType(type_name='Stream',     constructor_func=None)     
TX         = ValueType(type_name='TX',         constructor_func=None)     
Time       = ValueType(type_name='Time',       constructor_func=time_ctor)
Error      = ValueType(type_name='Error',      constructor_func=None)
Image      = ValueType(type_name='Image',      constructor_func=None)

UID        = ValueType(type_name='UID',        constructor_func=None)       
BaseUID    = ValueType(type_name='BaseUID',    constructor_func=None)       
ZefRefUID  = ValueType(type_name='ZefRefUID',  constructor_func=None)        
EternalUID = ValueType(type_name='EternalUID', constructor_func=None)       

LazyValue  = ValueType(type_name='LazyValue',  constructor_func=None) 
Awaitable  = ValueType(type_name='Awaitable',  constructor_func=None) 



AET        = ValueType(type_name='AET',        constructor_func=None)
ET         = ValueType(type_name='ET',         constructor_func=None)
RT         = ValueType(type_name='RT',         constructor_func=None)
BT         = ValueType(type_name='BT',         constructor_func=None)
Enum       = ValueType(type_name='Enum',       constructor_func=None)
Record     = ValueType(type_name='Record',     constructor_func=None)       
Function   = ValueType(type_name='Function',   constructor_func=None)
GraphDelta = ValueType(type_name='GraphDelta', constructor_func=None)     
Query      = ValueType(type_name='Query',      constructor_func=None) 
DeltaQuery = ValueType(type_name='DeltaQuery', constructor_func=None) 
Effect     = ValueType(type_name='Effect',     constructor_func=None) 
DataFrame  = ValueType(type_name='DataFrame', constructor_func=None)     
EZefRefs   = ValueType(type_name='EZefRefs', constructor_func=None)     
EZefRefss  = ValueType(type_name='EZefRefss', constructor_func=None)     
ZefRefs    = ValueType(type_name='ZefRefs', constructor_func=None)     
ZefRefss   = ValueType(type_name='ZefRefss', constructor_func=None)     



# These are special classes: using them with [...] returns a ValueType though
Union = UnionClass()
Intersection = IntersectionClass()
SetOf = SetOfClass()
