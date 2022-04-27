from .value_type import *


Nil = ValueType(type_name='Nil', constructor_func=None)
Any = ValueType(type_name='Any', constructor_func=None)
Bool = ValueType(type_name='Bool', constructor_func=bool)
Int = ValueType(type_name='Int', constructor_func=int)
Float = ValueType(type_name='Float', constructor_func=float)
String = ValueType(type_name='String', constructor_func=str)
List = ValueType(type_name='List', constructor_func=tuple)
Dict = ValueType(type_name='Dict', constructor_func=dict)
Set = ValueType(type_name='Set', constructor_func=set)
EZefRef = ValueType(type_name='EZefRef', constructor_func=None)     # TODO: ctor
EZefRefs = ValueType(type_name='EZefRefs', constructor_func=None)     # TODO: ctor
EZefRefss = ValueType(type_name='EZefRefss', constructor_func=None)     # TODO: ctor
ZefRef = ValueType(type_name='ZefRef', constructor_func=None)     # TODO: ctor
ZefRefs = ValueType(type_name='ZefRefs', constructor_func=None)     # TODO: ctor
ZefRefss = ValueType(type_name='ZefRefss', constructor_func=None)     # TODO: ctor
Graph = ValueType(type_name='Graph', constructor_func=None)     # TODO: ctor
GraphSlice = ValueType(type_name='GraphSlice', constructor_func=None)     # TODO: ctor
FlatGraph = ValueType(type_name='FlatGraph', constructor_func=None)     # TODO: ctor
ZefOp = ValueType(type_name='ZefOp', constructor_func=None)     # TODO: ctor
Stream = ValueType(type_name='Stream', constructor_func=None)     # TODO: ctor
TX = ValueType(type_name='TX', constructor_func=None)     # TODO: ctor
DataFrame = ValueType(type_name='DataFrame', constructor_func=None)     # TODO: ctor
GraphDelta = ValueType(type_name='GraphDelta', constructor_func=None)     # TODO: ctor

Union = UnionClass()
Intersection = IntersectionClass()
SetOf = SetOfClass()