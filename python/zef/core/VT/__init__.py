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


def union_getitem(self, x):
    from ..op_structs import ZefOp
    if isinstance(x, tuple):
        return ValueType_(type_name='Union', absorbed=x)
    elif isinstance(x, ValueType_):
        return ValueType_(type_name='Union', absorbed=(x,))
    elif isinstance(x, ZefOp):
        return ValueType_(type_name='Union', absorbed=(x,))
    else:
        raise Exception(f'"Union[...]" called with unsupported type {type(x)}')

def intersection_getitem(self, x):
    from ..op_structs import ZefOp
    if isinstance(x, tuple):
        return ValueType_(type_name='Intersection', absorbed=x)
    elif isinstance(x, ValueType_):
        return ValueType_(type_name='Intersection', absorbed=(x,))
    elif isinstance(x, ZefOp):
        return ValueType_(type_name='Intersection', absorbed=(x,))
    else:
        raise Exception(f'"Intersection[...]" called with unsupported type {type(x)}')

def complement_getitem(self, x):
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




def rp_getitem(self, x):
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

def is_is_a(el, typ):
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

def union_is_a(val, typ):
    return any(is_a_(val, subtyp) for subtyp in typ._d["absorbed"])

def intersection_is_a(val, typ):
    return all(is_a_(val, subtyp) for subtyp in typ._d["absorbed"])


def pattern_vt_matching(x, typ):
    class Sentinel: pass
    from .._ops import absorbed, single, collect
    sentinel = Sentinel() 
    p = typ | absorbed | single | collect
    assert (
        (isinstance(x, Dict) and isinstance(p, Dict)) or
        (type(x) in {list, tuple} and type(p) in {list, tuple}) 
    )
    if isinstance(x, Dict):
        for k, v in p.items():            
            r = x.get(k, sentinel)
            if r is sentinel: return False
            if not isinstance(v, ValueType): raise ValueError(f"The pattern passed didn't have a ValueType but rather {v}")
            if not isinstance(r, v): return False  
        return True
    elif isinstance(x, list) or isinstance(x, tuple):
        for p_e, x_e in zip(p, x): # Creates tuples of pairwise elements from both lists
            if not isinstance(p_e, ValueType):
                raise ValueError(f"The pattern passed didn't have a ValueType but rather {p_e}")
            if not isinstance(x_e, p_e): return False  
        return True

    raise NotImplementedError(f"Pattern ValueType isn't implemented for {x}")


Pattern        = ValueType_(type_name='Pattern',             constructor_func=None, is_a_func=pattern_vt_matching)
Union          = ValueType_(type_name='Union',               constructor_func=None,             get_item_func=union_getitem, is_a_func=union_is_a)
Intersection   = ValueType_(type_name='Intersection',        constructor_func=None,             get_item_func=intersection_getitem, is_a_func=intersection_is_a)
Is             = ValueType_(type_name='Is',                  constructor_func=None,             get_item_func=is_getitem, is_a_func=is_is_a)
SetOf          = ValueType_(type_name='SetOf',               constructor_func=setof_ctor,       get_item_func=setof_getitem, is_a_func=lambda x,typ: x in typ._d["absorbed"][0])
Complement     = ValueType_(type_name='Complement',          constructor_func=None,             get_item_func=complement_getitem)
RP             = ValueType_(type_name='RP',                  constructor_func=None,             get_item_func=rp_getitem)
HasValue       = ValueType_(type_name='HasValue',            constructor_func=None)
SameAs         = ValueType_(type_name='SameAs',                                                 get_item_func = same_as_get_item)


def numeric_is_a(x, typ):
    from .value_type import _value_type_pytypes
    python_type = _value_type_pytypes[typ._d['type_name']]
    try:
        return isinstance(x, python_type) or python_type(x) == x
    except:
        return False

Nil        = ValueType_(type_name='Nil',        constructor_func=None, pytype=type(None))
Any        = ValueType_(type_name='Any',        constructor_func=None, is_a_func=lambda x,typ: True, is_subtype_func=lambda x,typ: True)
Bool       = ValueType_(type_name='Bool',       constructor_func=None, pytype=bool, is_a_func=numeric_is_a)
Int        = ValueType_(type_name='Int',        constructor_func=None, pytype=int, is_a_func=numeric_is_a)
Float      = ValueType_(type_name='Float',      constructor_func=None, pytype=float, is_a_func=numeric_is_a)
String     = ValueType_(type_name='String',     constructor_func=None, pytype=str)
Bytes      = ValueType_(type_name='Bytes',      constructor_func=bytes_ctor, pytype=bytes)
Decimal    = ValueType_(type_name='Decimal',    constructor_func=decimal_ctor)
# TODO: Change this to a proper isa
List       = ValueType_(type_name='List',       constructor_func=tuple, is_a_func=lambda x,typ: isinstance(x, (tuple, list)))
Dict       = ValueType_(type_name='Dict',       constructor_func=None, pytype=dict)
Set        = ValueType_(type_name='Set',        constructor_func=None, pytype=set)
EZefRef    = ValueType_(type_name='EZefRef',    constructor_func=None, pytype=pyzef.main.EZefRef)
ZefRef     = ValueType_(type_name='ZefRef',     constructor_func=None, pytype=pyzef.main.ZefRef)
Graph      = ValueType_(type_name='Graph',      constructor_func=graph_ctor, pytype=pyzef.main.Graph)
GraphRef   = ValueType_(type_name='GraphRef',   constructor_func=None, pytype=pyzef.main.GraphRef)
GraphSlice = ValueType_(type_name='GraphSlice', constructor_func=None)
FlatGraph  = ValueType_(type_name='FlatGraph',  constructor_func=flatgraph_ctor)
ZefOp      = ValueType_(type_name='ZefOp',      constructor_func=None)
Stream     = ValueType_(type_name='Stream',     constructor_func=None)
TX         = ValueType_(type_name='TX',         constructor_func=None)
Time       = ValueType_(type_name='Time',       constructor_func=time_ctor, pytype=pyzef.main.Time)
from ..error import _Error, _ErrorType
Error      = ValueType_(type_name='Error',      constructor_func=_Error, attr_funcs=wrap_attr_readonly(_Error, None), pytype=_ErrorType)
Image      = ValueType_(type_name='Image',      constructor_func=None)
ValueType  = ValueType_(type_name='ValueType',  constructor_func=None, pytype=ValueType_)
T          = ValueType_(type_name='T',  constructor_func=None)
T1         = ValueType_(type_name='T1',  constructor_func=None)
T2         = ValueType_(type_name='T2',  constructor_func=None)
T3         = ValueType_(type_name='T3',  constructor_func=None)


def delegate_is_a(val, typ):
    # TODO Need to make some tricky decisions here
    # For now, any abstract delegate + any delegate zefref counts
    if isinstance(val, AbstractDelegate):
        return True
    if isinstance(val, (ZefRef, EZefRef)):
        return internals.is_delegate(val)
    return False

AbstractDelegate = ValueType_(type_name='AbstractDelegate', constructor_func=None, pytype=pyzef.internals.Delegate)
Delegate         = ValueType_(type_name='Delegate',   constructor_func=None, is_a_func=delegate_is_a)


# QuantityInt= ValueType_(type_name='QuantityInt',constructor_func=None)
# QuantityFloat= ValueType_(type_name='QuantityFloat',constructor_func=None)

# UID        = ValueType_(type_name='UID',        constructor_func=None)
BaseUID    = ValueType_(type_name='BaseUID',    constructor_func=None, pytype=pyzef.internals.BaseUID)
ZefRefUID  = ValueType_(type_name='ZefRefUID',  constructor_func=None, pytype=pyzef.internals.ZefRefUID)
EternalUID = ValueType_(type_name='EternalUID', constructor_func=None, pytype=pyzef.internals.EternalUID)
UID = BaseUID | ZefRefUID | EternalUID

# Instantiated = ValueType_(type_name='Instantiated', constructor_func=None)
# Terminated = ValueType_(type_name='Terminated', constructor_func=None)
# Assigned   = ValueType_(type_name='Assigned',   constructor_func=None)
Tagged     = ValueType_(type_name='Tagged',     constructor_func=None)

LazyValue  = ValueType_(type_name='LazyValue',  constructor_func=None)
Awaitable  = ValueType_(type_name='Awaitable',  constructor_func=None)

Enum       = ValueType_(type_name='Enum',       constructor_func=None, pytype=pyzef.main.ZefEnumValue)
# TODO: Change this to a proper isa
Tuple      = ValueType_(type_name='Tuple',      constructor_func=None, pytype=tuple)
Function   = ValueType_(type_name='Function',   constructor_func=None)
GraphDelta = ValueType_(type_name='GraphDelta', constructor_func=None)
Query      = ValueType_(type_name='Query',      constructor_func=None)
Effect     = ValueType_(type_name='Effect',     constructor_func=None)
DataFrame  = ValueType_(type_name='DataFrame',  constructor_func=None)
# EZefRefs   = ValueType_(type_name='EZefRefs', constructor_func=None)
# EZefRefss  = ValueType_(type_name='EZefRefss', constructor_func=None)
# ZefRefs    = ValueType_(type_name='ZefRefs', constructor_func=None)
# ZefRefss   = ValueType_(type_name='ZefRefss', constructor_func=None)
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





def wrap_attr_readonly_token(orig):
    def this_get_attr(self, name):
        if "specific" in self._d:
            # This is just for AETs
            if isinstance(self._d["specific"], AET_QFloat):
                out = getattr(orig.QuantityFloat, name)
            elif isinstance(self._d["specific"], AET_QInt):
                out = getattr(orig.QuantityInt, name)
            elif isinstance(self._d["specific"], AET_Enum):
                out = getattr(orig.Enum, name)
            else:
                raise AttributeError(name)
        else:
            out = getattr(orig, name)
        return self[out]
    def this_dir(self):
        if "specific" in self._d:
            # This is just for AETs
            if isinstance(self._d["specific"], AET_QFloat):
                return dir(orig.QuantityFloat)
            elif isinstance(self._d["specific"], AET_QInt):
                return dir(orig.QuantityInt)
            elif isinstance(self._d["specific"], AET_Enum):
                return dir(orig.Enum)
            else:
                return []
        return dir(orig)
    return (this_get_attr, None, this_dir)

def token_subtype(other, this):
    from .._ops import is_a
    # This seems generic and could be extracted out
    if other._d["type_name"] != this._d["type_name"]:
        return False
    if "specific" not in other._d:
        return "specific" not in this._d
    if "specific" not in this._d:
        return True
    if is_type(this._d["specific"]):
        return is_a(other._d["specific"], this._d["specific"])
    else:
        return is_a(other._d["specific"], SetOf(this._d["specific"]))

def token_getitem(self, thing, token_type):
    my_name = self._d["type_name"]
    from .._ops import is_a, insert
    if isinstance(thing, str):
        if "internal_id" in self._d:
            raise Exception(f"Can't assign a new internal_id an existing {my_name} with internal_id.")
        return ValueType_(template=self, fill_dict={"internal_id": thing})

    # Allow arbitrary types, so long as they can contain EntityTypeTokens
    # if is_a(thing, token_type) or (type(thing) == ValueType_ and is_strict_subtype(token_type, thing)):
    if is_a(thing, token_type) or (type(thing) == ValueType_ and not is_empty(token_type & thing)):
        if "specific" in self._d and not isinstance(self._d["specific"], (AET_QFloat, AET_QInt, AET_Enum)):
            raise Exception(f"Can't assign a new {my_name} token to an existing {my_name} with token.")
        return ValueType_(template=self, fill_dict={"specific": thing})

    raise Exception(f"{my_name} can only contain an {token_type} or an internal id, not {thing}. Note: subtypes must be determinable.")

def token_str(self):
    my_name = self._d["type_name"]
    from .._ops import is_a
    s = my_name
    if "specific" in self._d:
        if isinstance(self._d["specific"], str):
            s += "[" + str(self._d["specific"]) + "]"
        else:
            s += "." + str(self._d["specific"])
    if "internal_id" in self._d:
        s += f"['{self._d['internal_id']}']"
    return s

def ET_is_a(x, typ):
    from .._ops import is_a
    if "specific" in typ._d:
        if not (is_a(x, ZefRef) or is_a(x, EZefRef)):
            return False
        if internals.BT(x) != internals.BT.ENTITY_NODE:
            return False
        if internals.is_delegate(x):
            return False
        if is_type(typ._d["specific"]):
            return is_a(internals.ET(x), typ._d["specific"])
        return internals.ET(x) == typ._d["specific"]
    else:
        if is_a(x, ZefRef) or is_a(x, EZefRef):
            return (internals.BT(x) == internals.BT.ENTITY_NODE
                    and not internals.is_delegate(x))
        return is_type(x) and x._d["type_name"] == "ET"
def AET_is_a(x, typ):
    from .._ops import is_a
    if "specific" in typ._d:
        if not (is_a(x, ZefRef) or is_a(x, EZefRef)):
            return False
        if internals.BT(x) != internals.BT.ATTRIBUTE_ENTITY_NODE:
            return False
        if internals.is_delegate(x):
            return False
        if is_type(typ._d["specific"]):
            return is_a(internals.AET(x), typ._d["specific"])
        return internals.AET(x) == typ._d["specific"]
    else:
        if is_a(x, ZefRef) or is_a(x, EZefRef):
            return (internals.BT(x) == internals.BT.ATTRIBUTE_ENTITY_NODE
                    and not internals.is_delegate(x))
        return is_type(x) and x._d["type_name"] == "AET"
def RT_is_a(x, typ):
    from .._ops import is_a
    if "specific" in typ._d:
        if not (is_a(x, ZefRef) or is_a(x, EZefRef)):
            return False
        if internals.BT(x) != internals.BT.RELATION_EDGE:
            return False
        if internals.is_delegate(x):
            return False
        # TODO: EntityTypeToken
        if is_type(typ._d["specific"]):
            return is_a(internals.RT(x), typ._d["specific"])
        return internals.RT(x) == typ._d["specific"]
    else:
        if is_a(x, ZefRef) or is_a(x, EZefRef):
            return (internals.BT(x) == internals.BT.RELATION_EDGE
                    and not internals.is_delegate(x))
        return is_type(x) and x._d["type_name"] == "RT"
def BT_is_a(x, typ):
    from .._ops import is_a
    if "specific" not in typ._d:
        if is_a(x, ZefRef) or is_a(x, EZefRef):
            return True
        return is_type(x) and x._d["type_name"] == "BT"
    else:
        c_bt = typ._d["specific"]
        if not isinstance(c_bt, BlobTypeToken):
            raise Exception("TODO")
        if is_a(x, ZefRef) or is_a(x, EZefRef):
            return c_bt == internals.BT(x)
        if c_bt == internals.BT.RELATION_EDGE:
            if isinstance(x, RT):
                return True
        if c_bt == internals.BT.ENTITY_NODE:
            if isinstance(x, ET):
                return True
        if c_bt == internals.BT.ATTRIBUTE_ENTITY_NODE:
            if isinstance(x, AET):
                return True
        return False

EntityTypeToken = ValueType_(type_name='EntityTypeToken',
                             constructor_func=None,
                             pytype=internals.EntityType,
                             )

ET = ValueType_(type_name='ET',
                constructor_func=lambda x: ET[internals.ET(x)],
                attr_funcs=wrap_attr_readonly_token(internals.ET),
                is_a_func=ET_is_a,
                is_subtype_func=token_subtype,
                get_item_func=lambda self,thing: token_getitem(self, thing, EntityTypeToken),
                str_func=token_str,
                )

AttributeEntityTypeToken = ValueType_(type_name='AttributeEntityTypeToken',
                             constructor_func=None,
                             pytype=internals.AttributeEntityType,
                             )
AET_QFloat = ValueType_(type_name='AET_QFloat',
                        constructor_func=None,
                        pytype=internals.AttributeEntityTypeStruct_QuantityFloat,
                        )
AET_QInt = ValueType_(type_name='AET_QInt',
                      constructor_func=None,
                      pytype=internals.AttributeEntityTypeStruct_QuantityInt,
                      )
AET_Enum = ValueType_(type_name='AET_Enum',
                      constructor_func=None,
                      pytype=internals.AttributeEntityTypeStruct_Enum,
                      )

def AET_ctor(self, x):
    if type(x) == str:
        return getattr(self, x)
    if "specific" in self._d:
        raise Exception("Can't get AET from item when a specific type of AET is given")
    return AET[internals.AET(x)]

AET = ValueType_(type_name='AET',
                 constructor_func=AET_ctor,
                 pass_self=True,
                attr_funcs=wrap_attr_readonly_token(internals.AET),
                is_a_func=AET_is_a,
                is_subtype_func=token_subtype,
                get_item_func=lambda self,thing: token_getitem(self, thing, AttributeEntityTypeToken | AET_QFloat | AET_QInt | AET_Enum),
                str_func=token_str,
                )

RelationTypeToken = ValueType_(type_name='RelationTypeToken',
                             constructor_func=None,
                             pytype=internals.RelationType,
                             )
RT = ValueType_(type_name='RT',
                constructor_func=lambda x: RT[internals.RT(x)],
                attr_funcs=wrap_attr_readonly_token(internals.RT),
                is_a_func=RT_is_a,
                is_subtype_func=token_subtype,
                get_item_func=lambda self,thing: token_getitem(self, thing, RelationTypeToken),
                str_func=token_str,
                )


BlobTypeToken = ValueType_(type_name='BlobTypeToken',
                             constructor_func=None,
                             pytype=internals.BlobType,
                             )
BT = ValueType_(type_name='BT',
                constructor_func=lambda x: BT[internals.BT(x)],
                attr_funcs=wrap_attr_readonly_token(internals.BT),
                is_a_func=BT_is_a,
                is_subtype_func=token_subtype,
                get_item_func=lambda self,thing: token_getitem(self, thing, BlobTypeToken),
                str_func=token_str,
                )

# BT         = ValueType_(type_name='BT',   constructor_func=pyzef.internals.BT, attr_funcs=wrap_attr_readonly(internals.BT, None), pytype=internals.BlobType)