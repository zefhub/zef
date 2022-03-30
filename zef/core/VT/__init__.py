from .value_type import *


# Elementary value types
Nil        = ValueType('Nil',        nesting_order=0)
Any        = ValueType('Any',        nesting_order=0)
Int        = ValueType('Int',        nesting_order=0)
Float      = ValueType('Float',      nesting_order=0)
String     = ValueType('String',     nesting_order=0)
Bool       = ValueType('Bool',       nesting_order=0)
Time       = ValueType('Time',       nesting_order=0)
List       = ValueType('List',       nesting_order=1)
Set        = ValueType('Set',        nesting_order=1)
ZefRef     = ValueType('ZefRef',     nesting_order=1)
EZefRef    = ValueType('EZefRef',    nesting_order=1)
Dict       = ValueType('Dict',       nesting_order=2)
AET        = ValueType('AET',        nesting_order=1)
ET         = ValueType('ET',         nesting_order=1)
RT         = ValueType('RT',         nesting_order=1)
TX         = ValueType('TX',         nesting_order=1)
BT         = ValueType('BT',         nesting_order=1)
Enum       = ValueType('Enum',       nesting_order=1)
ZefOp      = ValueType('ZefOp',      nesting_order=0)    # what is the substructure here?
LazyValue  = ValueType('LazyValue',  nesting_order=1) 
Awaitable  = ValueType('Awaitable',  nesting_order=1) 
Graph      = ValueType('Graph',      nesting_order=0) 
Time       = ValueType('Time',       nesting_order=0) 
# TimeSlice  = ValueType('TimeSlice',  nesting_order=0) 
GraphSlice = ValueType('GraphSlice', nesting_order=0) 
GraphDelta = ValueType('GraphDelta', nesting_order=0) 
Query      = ValueType('Query',      nesting_order=0) 
DeltaQuery = ValueType('DeltaQuery', nesting_order=0) 
Effect     = ValueType('Effect',     nesting_order=0) 
Function   = ValueType('Function',   nesting_order=2)
Record     = ValueType('Record',     nesting_order=1)       # Use this instead of "Tuple" to clarify the difference from a List?

Error      = ValueType('Error',      nesting_order=1)


UID        = ValueType('UID',        nesting_order=0)       # UID is an alias for Or[BaseUID, ZefRefUID, EternalUID]
BaseUID    = ValueType('BaseUID',    nesting_order=0)       # Should these coexist as independent types on this level? UID just acts as a superset?
ZefRefUID  = ValueType('ZefRefUID',  nesting_order=0)        
EternalUID = ValueType('EternalUID', nesting_order=0)       

Stream     = ValueType('Stream',     nesting_order=1)       # Stream[T] should be an alias for Awaitable[List[T]]


ZefRef     = ValueType('ZefRef',     nesting_order=1)
ZefRefs    = ValueType('ZefRefs',    nesting_order=1)
ZefRefss   = ValueType('ZefRefss',   nesting_order=1)
EZefRef    = ValueType('EZefRef',    nesting_order=1)
EZefRefs   = ValueType('EZefRefs',   nesting_order=1)
EZefRefss  = ValueType('EZefRefss',  nesting_order=1)

DataFrame = ValueType('DataFrame', nesting_order=0)       