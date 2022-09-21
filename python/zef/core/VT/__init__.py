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
from .. import internals
from ... import pyzef


# the following functions add a layer of indirection to prevent circular imports

def graph_ctor(*args, **kwargs):
    # Go to experimental load path for cases it supports
    if len(kwargs) == 0 and len(args) == 1 and type(args[0]) == str:
        from ...experimental.repl_interface import load_graph
        return load_graph(args[0])
    else:
        from ... import core
        return internals.Graph(*args, **kwargs)



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

def is_getitem(_, x):
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



def setof_getitem(_, x):
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


def wrap_attr_readonly(other, ret_type):
    def this_get_attr(_, name):
        out = getattr(other, name)
        if ret_type is not None:
            out = ret_type[out]
        return out
    def this_dir(_):
        return dir(other)
    return (this_get_attr, None, this_dir)



Nil        = ValueType_(type_name='Nil',        constructor_func=None, is_a_func=lambda x,typ: x is None)
Any        = ValueType_(type_name='Any',        constructor_func=None, is_a_func=lambda x,typ: True, is_subtype_func=lambda x,typ: True)
Bool       = ValueType_(type_name='Bool',       constructor_func=bool, is_a_func=lambda x,typ: type(x) == bool)
Int        = ValueType_(type_name='Int',        constructor_func=int)
Float      = ValueType_(type_name='Float',      constructor_func=float)
String     = ValueType_(type_name='String',     constructor_func=str)
Bytes      = ValueType_(type_name='Bytes',      constructor_func=bytes_ctor)
Decimal    = ValueType_(type_name='Decimal',    constructor_func=decimal_ctor)
List       = ValueType_(type_name='List',       constructor_func=tuple)
Dict       = ValueType_(type_name='Dict',       constructor_func=dict)
Set        = ValueType_(type_name='Set',        constructor_func=set)
EZefRef    = ValueType_(type_name='EZefRef',    constructor_func=None, pytype=pyzef.main.EZefRef)
ZefRef     = ValueType_(type_name='ZefRef',     constructor_func=None, pytype=pyzef.main.ZefRef)
Graph      = ValueType_(type_name='Graph',      constructor_func=graph_ctor, pytype=pyzef.main.Graph)
GraphRef   = ValueType_(type_name='GraphRef',   constructor_func=None, pytype=pyzef.main.GraphRef)
GraphSlice = ValueType_(type_name='GraphSlice', constructor_func=None)
FlatGraph  = ValueType_(type_name='FlatGraph',  constructor_func=flatgraph_ctor)
ZefOp      = ValueType_(type_name='ZefOp',      constructor_func=None)
Stream     = ValueType_(type_name='Stream',     constructor_func=None)
TX         = ValueType_(type_name='TX',         constructor_func=None)
Time       = ValueType_(type_name='Time',       constructor_func=time_ctor)
from ..error import _Error, _ErrorType
Error      = ValueType_(type_name='Error',      constructor_func=_Error, attr_funcs=wrap_attr_readonly(_Error, None), pytype=_ErrorType)
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

AttributeEntityType = ValueType_(type_name='AttET', constructor_func=None)
# EntityType          = ValueType_(type_name='EntT',  constructor_func=None)
RelationType        = ValueType_(type_name='RelT',  constructor_func=None)
BlobType            = ValueType_(type_name='BlobT',  constructor_func=None)


def BT_call(x):
    return BlobType[pyzef.internals.BT(x)]

AET        = ValueType_(type_name='AETLookup',  constructor_func=None, attr_funcs=wrap_attr_readonly(internals.AET, AttributeEntityType))
# ET         = ValueType_(type_name='ETLookup',   constructor_func=None, attr_funcs=wrap_attr_readonly(internals.ET, EntityType), is_a_func=lambda x,typ: is_type(x) and is_subtype(x, EntityType))
RT         = ValueType_(type_name='RTLookup',   constructor_func=None, attr_funcs=wrap_attr_readonly(internals.RT, RelationType), is_a_func=lambda x,typ: is_type(x) and is_subtype(x, RelationType))
BT         = ValueType_(type_name='BTLookup',   constructor_func=BT_call, attr_funcs=wrap_attr_readonly(internals.BT, BlobType))
Enum       = ValueType_(type_name='Enum',       constructor_func=None)
Tuple      = ValueType_(type_name='Tuple',      constructor_func=None)
Function   = ValueType_(type_name='Function',   constructor_func=None)
GraphDelta = ValueType_(type_name='GraphDelta', constructor_func=None)
Query      = ValueType_(type_name='Query',      constructor_func=None)
Effect     = ValueType_(type_name='Effect',     constructor_func=None)
DataFrame  = ValueType_(type_name='DataFrame',  constructor_func=None)
# EZefRefs   = ValueType_(type_name='EZefRefs', constructor_func=None)
# EZefRefss  = ValueType_(type_name='EZefRefss', constructor_func=None)
# ZefRefs    = ValueType_(type_name='ZefRefs', constructor_func=None)
# ZefRefss   = ValueType_(type_name='ZefRefss', constructor_func=None)

def is_matching(el, typ):
    from typing import Callable
    for t in typ._d['absorbed']:
        if isinstance(t, ValueType_):
            return Error.ValueError(f"A ValueType_ was passed to Is but it only takes predicate functions. Try wrapping in is_a[{t}]")
        elif isinstance(t, Callable) or is_a(t, ZefOp):
            try:
                if not t(el): return False
            except:
                return False
        else: return Error.ValueError(f"Expected a predicate function or a ZefOp type inside Is but got {t} instead.")
    return True

Pattern        = ValueType_(type_name='Pattern',             constructor_func=None)
Union          = ValueType_(type_name='Union',               constructor_func=None,             get_item_func=union_getitem)
Intersection   = ValueType_(type_name='Intersection',        constructor_func=None,             get_item_func=intersection_getitem)
Is             = ValueType_(type_name='Is',                  constructor_func=None,             get_item_func=is_getitem, is_a_func=is_matching)
SetOf          = ValueType_(type_name='SetOf',               constructor_func=setof_ctor,       get_item_func=setof_getitem, is_a_func=lambda x,typ: x in typ._d["absorbed"][0])
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





def wrap_attr_readonly_ET():
    def this_get_attr(self, name):
        if "specific" in self._d:
            raise AttributeError(name)
        out = getattr(internals.ET, name)
        return ET[out]
    def this_dir(_):
        if "specific" in self._d:
            return []
        return dir(internals.ET)
    return (this_get_attr, None, this_dir)

def ET_is_a(x, typ):
    from .._ops import is_a
    if "specific" in typ._d:
        if not is_a(x, ZefRef) or is_a(x, EZefRef):
            return False
        if internals.BT(x) != internals.BT.ENTITY_NODE:
            return False
        # TODO: EntityTypeToken
        if is_type(typ._d["specific"]):
            return is_a(internals.ET(x), typ._d["specific"])
        return internals.ET(x) == typ._d["specific"]
    else:
        if is_type(x) and is_strict_subtype(x, ET[Any]):
            return True
        return False

def ET_subtype(other, this):
    from .._ops import is_a
    # This seems generic and could be extracted out
    if other._d["type_name"] != "ET":
        return False
    if "specific" not in other._d:
        return "specific" not in this._d
    if "specific" not in this._d:
        return False
    if is_type(this._d["specific"]):
        return is_a(other._d["specific"], this._d["specific"])
    else:
        return is_a(other._d["specific"], SetOf(this._d["specific"]))

def ET_getitem(self, thing):
    from .._ops import is_a, insert
    if isinstance(thing, str):
        if "internal_id" in self._d:
            raise Exception("Can't assign a new internal_id an existing ET with internal_id.")
        return ValueType_(fill_dict=insert(self._d, "internal_id", thing))

    if "specific" in self._d:
        raise Exception("Can't assign a new ET token to an existing ET with token.")
    if is_a(thing, EntityTypeToken):
        return ValueType_(fill_dict=insert(self._d, "specific", thing))
    # Allow arbitrary types, so long as they can contain EntityTypeTokens
    if type(thing) == ValueType_ and is_strict_subtype(EntityTypeToken, thing):
        return ValueType_(fill_dict=insert(self._d, "specific", thing))
    raise Exception(f"ET can only contain an EntityTypeToken or an internal id, not {thing}")

def ET_str(self):
    from .._ops import is_a
    s = "ET"
    if "specific" in self._d:
        if is_a(self._d["specific"], EntityTypeToken):
            s += "." + str(self._d["specific"][0])
        else:
            s += "[" + str(self._d["specific"]) + "]"
    if "internal_id" in self._d:
        s += f"['{self._d['internal_id']}']"
    return s

EntityTypeToken = ValueType_(type_name='EntityTypeToken',
                             constructor_func=None,
                             pytype=internals.EntityType,
                             )

ET = ValueType_(type_name='ET',
                constructor_func=None,
                attr_funcs=wrap_attr_readonly_ET(),
                is_a_func=ET_is_a,
                is_subtype_func=ET_subtype,
                get_item_func=ET_getitem,
                str_func=ET_str,
                )