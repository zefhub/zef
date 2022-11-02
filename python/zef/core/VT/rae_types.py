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

from . import make_VT, insert_VT, ValueType, SetOf, BlobPtr
from .value_type import is_empty_, is_type_name_
from .. import internals

# These pop up a lot as special cases
AET_QFloat = make_VT('AET_QFloat', pytype=internals.AttributeEntityTypeStruct_QuantityFloat)
AET_QInt = make_VT('AET_QInt', pytype=internals.AttributeEntityTypeStruct_QuantityInt)
AET_Enum = make_VT('AET_Enum', pytype=internals.AttributeEntityTypeStruct_Enum)

def wrap_attr_readonly_token(orig):
    def this_get_attr(self, name):
        if "token" in self._d:
            # This is just for AETs
            if isinstance(self._d["token"], AET_QFloat):
                out = getattr(orig.QuantityFloat, name)
            elif isinstance(self._d["token"], AET_QInt):
                out = getattr(orig.QuantityInt, name)
            elif isinstance(self._d["token"], AET_Enum):
                out = getattr(orig.Enum, name)
            else:
                raise AttributeError(name)
        else:
            out = getattr(orig, name)
        return self[out]
    def this_dir(self):
        if "token" in self._d:
            # This is just for AETs
            if isinstance(self._d["token"], AET_QFloat):
                return dir(orig.QuantityFloat)
            elif isinstance(self._d["token"], AET_QInt):
                return dir(orig.QuantityInt)
            elif isinstance(self._d["token"], AET_Enum):
                return dir(orig.Enum)
            else:
                return []
        return dir(orig)
    return (this_get_attr, None, this_dir)

def token_subtype(other, this):
    # This seems generic and could be extracted out as "covariance test"
    if other._d["type_name"] != this._d["type_name"]:
        return False
    if "token" not in other._d:
        return "token" not in this._d
    if "token" not in this._d:
        return True
    if isinstance(this._d["token"], ValueType):
        return isinstance(other._d["token"], this._d["token"])
    else:
        return isinstance(other._d["token"], SetOf(this._d["token"]))

def token_getitem(self, thing, token_type):
    my_name = self._d["type_name"]

    # Allow arbitrary types, so long as they can contain EntityTypeTokens
    #
    # Or should we simply add the intersection automatically and allow null sets?
    # if is_a(thing, token_type) or (type(thing) == ValueType_ and is_strict_subtype(token_type, thing)):
    if isinstance(thing, token_type) or (isinstance(thing, ValueType) and not is_empty_(token_type & thing)):
        if "token" in self._d and not isinstance(self._d["token"], (AET_QFloat, AET_QInt, AET_Enum)):
            raise Exception(f"Can't assign a new {my_name} token to an existing {my_name} with token.")
        return self._replace(token=thing)

    raise Exception(f"{my_name} can only contain an {token_type} or an internal id, not {thing}. Note: subtypes must be determinable.")

def token_str(self):
    my_name = self._d["type_name"]
    s = my_name
    if "token" in self._d:
        if isinstance(self._d["token"], (EntityTypeToken, RelationTypeToken, AttributeEntityTypeToken)):
            s += "." + self._d["token"].name
        elif isinstance(self._d["token"], BlobTypeToken):
            s += "." + str(self._d["token"])
        else:
            s += "[" + str(self._d["token"]) + "]"
    if len(self._d["absorbed"]) > 0:
        s += f"['{self._d['absorbed'][0]}']"
    return s

def ET_is_a(x, typ):
    from . import DelegateRef
    if "token" in typ._d:
        if isinstance(x, DelegateRef):
            return x.item == typ._d["token"]
        elif isinstance(x, BlobPtr):
            if internals.BT(x) != internals.BT.ENTITY_NODE:
                return False
            if isinstance(typ._d["token"], ValueType):
                return isinstance(internals.ET(x), typ._d["token"])
            return internals.ET(x) == typ._d["token"]
        else:
            return False
    else:
        if isinstance(x, DelegateRef):
            return isinstance(x.item, EntityTypeToken)
        elif isinstance(x, BlobPtr):
            return internals.BT(x) == internals.BT.ENTITY_NODE
        return isinstance(x, ValueType) and x._d["type_name"] == "ET"
def AET_is_a(x, typ):
    from . import DelegateRef
    if "token" in typ._d:
        token = typ._d["token"]

        if isinstance(x, DelegateRef):
            x_aet = x.item
        elif isinstance(x, BlobPtr):
            if internals.BT(x) != internals.BT.ATTRIBUTE_ENTITY_NODE:
                return False
            x_aet = internals.AET(x)
        elif isinstance(x, ValueType) and x._d["type_name"] == "AET":
            if "token" not in x._d:
                return False
            x_aet = x._d["token"]
        elif isinstance(x, (AttributeEntityTypeToken, AET_QFloat, AET_QInt, AET_Enum)):
            x_aet = x
        else:
            return False

        if isinstance(token, ValueType):
            return isinstance(x_aet, token)

        if token == x_aet:
            return True

        if isinstance(token, AttributeEntityTypeToken) and token.complex_value is not None:
            raise Exception(f"Checking isinstance on complex AETs (got {typ}) is not yet implemented. Coming soon!")
        if isinstance(x_aet, AttributeEntityTypeToken) and x_aet.complex_value is not None:
            raise Exception(f"Checking isinstance on complex AETs (got {x_aet}) is not yet implemented. Coming soon!")

        if isinstance(token, (AET_QFloat, AET_QInt, AET_Enum)):
            if isinstance(x_aet, AttributeEntityTypeToken):
                if isinstance(token, AET_QFloat):
                    return internals.is_vrt_a_quantity_float(x_aet.rep_type)
                if isinstance(token, AET_QInt):
                    return internals.is_vrt_a_quantity_int(x_aet.rep_type)
                if isinstance(token, AET_QEnum):
                    return internals.is_vrt_a_enum(x_aet.rep_type)
        return False
    else:
        if isinstance(x, DelegateRef):
            return isinstance(x.item, AttributeEntityTypeToken)
        elif isinstance(x, BlobPtr):
            return internals.BT(x) == internals.BT.ATTRIBUTE_ENTITY_NODE
        return is_type_name_(x, "AET")
def RT_is_a(x, typ):
    from . import DelegateRef
    if "token" in typ._d:
        if isinstance(x, DelegateRef):
            if type(x.item) == internals.DelegateRelationTriple:
                return x.item.rt == typ._d["token"]
            else:
                return x.item == typ._d["token"]
        elif isinstance(x, BlobPtr):
            if internals.BT(x) != internals.BT.RELATION_EDGE:
                return False
            # TODO: EntityTypeToken
            if isinstance(typ._d["token"], ValueType):
                return isinstance(internals.RT(x), typ._d["token"])
            return internals.RT(x) == typ._d["token"]
        else:
            return False
    else:
        if isinstance(x, DelegateRef):
            if type(x.item) == internals.DelegateRelationTriple:
                return isinstance(x.item.rt, RelationTypeToken)
            return isinstance(x.item, RelationTypeToken)
        elif isinstance(x, BlobPtr):
            # TODO: not was removed from the second part of this condition. In case something breaks check here!
            return internals.BT(x) == internals.BT.RELATION_EDGE
        return isinstance(x, ValueType) and x._d["type_name"] == "RT"
def BT_is_a(x, typ):
    if "token" not in typ._d:
        if isinstance(x, BlobPtr):
            return True
        return isinstance(x, ValueType) and x._d["type_name"] == "BT"
    else:
        c_bt = typ._d["token"]
        if not isinstance(c_bt, BlobTypeToken):
            raise Exception("TODO")
        if isinstance(x, BlobPtr):
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


def ET_ctor(self, *args, **kwargs):
    if "token" in self._d:
        return EntityValueInstance(self, *args, **kwargs)
    else:
        return ET[internals.ET(*args, **kwargs)]
    
# TODO: Move this somewhere
from ..patching import EntityValueInstance_
EntityValueInstance = make_VT('EntityValueInstance', pytype=EntityValueInstance_)
EntityTypeToken = make_VT('EntityTypeToken', pytype=internals.EntityType)

ET = make_VT('ET',
             constructor_func=ET_ctor,
             pass_self=True,
             attr_funcs=wrap_attr_readonly_token(internals.ET),
             is_a_func=ET_is_a,
             is_subtype_func=token_subtype,
             get_item_func=lambda self,thing: token_getitem(self, thing, EntityTypeToken),
             str_func=token_str)

AttributeEntityTypeToken = make_VT('AttributeEntityTypeToken', pytype=internals.AttributeEntityType)
def AET_ctor(self, x):
    if type(x) == str:
        return getattr(self, x)
    if "token" in self._d:
        return NotImplemented
    return AET[internals.AET(x)]

AET = make_VT('AET',
              constructor_func=AET_ctor,
              pass_self=True,
              attr_funcs=wrap_attr_readonly_token(internals.AET),
              is_a_func=AET_is_a,
              is_subtype_func=token_subtype,
              get_item_func=lambda self,thing: token_getitem(self, thing, AttributeEntityTypeToken | AET_QFloat | AET_QInt | AET_Enum),
              str_func=token_str)

RelationTypeToken = make_VT('RelationTypeToken', pytype=internals.RelationType)
RT = make_VT('RT',
             constructor_func=lambda x: RT[internals.RT(x)],
             attr_funcs=wrap_attr_readonly_token(internals.RT),
             is_a_func=RT_is_a,
             is_subtype_func=token_subtype,
             get_item_func=lambda self,thing: token_getitem(self, thing, RelationTypeToken),
             str_func=token_str)


BlobTypeToken = make_VT('BlobTypeToken', pytype=internals.BlobType)
BT = make_VT('BT',
             constructor_func=lambda x: BT[internals.BT(x)],
             attr_funcs=wrap_attr_readonly_token(internals.BT),
             is_a_func=BT_is_a,
             is_subtype_func=token_subtype,
             get_item_func=lambda self,thing: token_getitem(self, thing, BlobTypeToken),
             str_func=token_str)

# BT         = ValueType_(type_name='BT',   constructor_func=pyzef.internals.BT, attr_funcs=wrap_attr_readonly(internals.BT, None), pytype=internals.BlobType)


def tx_is_a(x, typ):
    return isinstance(x, BT.TX_EVENT_NODE)

TX = make_VT("TX", is_a_func=tx_is_a)

RAET = insert_VT("RAET", ET | RT | AET)