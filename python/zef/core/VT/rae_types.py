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
from .helpers import absorbed, type_name
from .. import internals

# These pop up a lot as special cases
AET_QFloat = make_VT('AET_QFloat', pytype=internals.AttributeEntityTypeStruct_QuantityFloat)
AET_QInt = make_VT('AET_QInt', pytype=internals.AttributeEntityTypeStruct_QuantityInt)
AET_Enum = make_VT('AET_Enum', pytype=internals.AttributeEntityTypeStruct_Enum)

VRT_QFloat = make_VT('VRT_QFloat', pytype=internals.ValueRepTypeStruct_QuantityFloat)
VRT_QInt = make_VT('VRT_QInt', pytype=internals.ValueRepTypeStruct_QuantityInt)
VRT_Enum = make_VT('VRT_Enum', pytype=internals.ValueRepTypeStruct_Enum)

# Helpers

def RAET_get_token(typ, convert_complex=True):
    # This allows anything in the absorbed and takes just the first likely item
    # This is far too lenient, it will likely break in the future
    opts = [x for x in absorbed(typ) if isinstance(x, (EntityTypeToken, RelationTypeToken, BlobTypeToken, AttributeEntityTypeToken, ValueRepTypeToken,
                                                       AET_QFloat, AET_QInt, AET_Enum,
                                                       ValueType))]
    if len(opts) == 0:
        return None
    if isinstance(opts[0], ValueType) and convert_complex:
        # Need to do something a bit more special with this. This should only make sense for AET[<complex type>].
        assert isinstance(typ, ValueType[AET])
        return internals.AttributeEntityType(internals.SerializedValue.serialize(opts[0]))
    return opts[0]

def RAET_get_names(typ):
    token = RAET_get_token(typ, convert_complex=False)
    if token is not None:
        names = list(absorbed(typ))
        names.remove(token)
        return tuple(names)
    else:
        return absorbed(typ)

def RAET_without_names(typ):
    token = RAET_get_token(typ, convert_complex=False)
    if token is None:
        return typ._replace(absorbed=())
    else:
        return typ._replace(absorbed=(token,))

def print_or_error(text):
    # This is a way to toggle the deprecation of the checks isinstance(some_atom, ET) for isinstance(some_atom, Entity)
    if True:
        print(text)
    else:
        raise Exception(text)
        
    
def wrap_attr_readonly_token(orig):
    def this_get_attr(self, name):
        token = RAET_get_token(self)
        if token is None:
            if name.endswith("_"):
                raise AttributeError("Tokens cannot end with '_'.")
            out = getattr(orig, name)
        else:
            # This is just for AETs
            if isinstance(token, AET_QFloat):
                out = getattr(orig.QuantityFloat, name)
            elif isinstance(token, AET_QInt):
                out = getattr(orig.QuantityInt, name)
            elif isinstance(token, AET_Enum):
                out = getattr(orig.Enum, name)
            else:
                raise AttributeError(name)
        assert len(RAET_get_names(self)) == 0, "Going to lose names"
        base_token_type = self._replace(absorbed=())
        return base_token_type[out]
    def this_dir(self):
        token = RAET_get_token(self)
        if token is not None:
            # This is just for AETs
            if isinstance(token, AET_QFloat):
                return dir(orig.QuantityFloat)
            elif isinstance(token, AET_QInt):
                return dir(orig.QuantityInt)
            elif isinstance(token, AET_Enum):
                return dir(orig.Enum)
            else:
                return []
        return dir(orig)
    return (this_get_attr, None, this_dir)

def token_subtype(other, this):
    if type_name(other) != type_name(this):
        return False
    other_token = RAET_get_token(other)
    this_token = RAET_get_token(this)
    if other_token is None:
        return this_token is None
    if this_token is None:
        return True
    if isinstance(this_token, ValueType):
        if isinstance(other_token, ValueType):
            return issubclass(other_token, this_token)
        return isinstance(other_token, this_token)
    return other_token == this_token

def token_validation(self, token_type):
    # my_name = type_name(self)

    # abs = remove_names(absorbed(self))
    # if len(abs) == 0:
    #     return True
    # if len(abs) >= 2:
    #     raise Exception(f"Should only have one token absorbed into a {my_name}")
    # thing = abs[0]

    # if not isinstance(thing, token_type):
    #     raise Exception(f"A {my_name} doesn't contain a {token_type} but has a {thing} instead")

    # Anything is allowed now
    return True

def token_str(self):
    my_name = self._d["type_name"]
    s = my_name
    token = RAET_get_token(self)
    if token is not None:
        if isinstance(token, (EntityTypeToken, RelationTypeToken, AttributeEntityTypeToken)):
            s += "." + token.name
        elif isinstance(token, BlobTypeToken):
            s += "." + str(token)
        else:
            s += "[" + str(token) + "]"

    for thing in RAET_get_names(self):
        s += f"[{thing!r}]"
    return s

def ET_is_a(x, typ):
    from . import DelegateRef, EntityRef, FlatRef, AtomClass
    token = RAET_get_token(typ)
    if token is None:
        # Note: ET check needs to happen first, as the others recurse into this
        if isinstance(x, ValueType):
            return type_name(x) == "ET"
        elif isinstance(x, DelegateRef):
            print_or_error(" ⚠️  Warning, `ET` was used as the type in an isinstance comparison for an `Entity`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., Entity) instead. If you want to only catch e.g. isinstance(ET.X, ET) then until the deprecation occurs, you should use isinstance(ET.X, ValueType & ET). ⚠ ️")
            return isinstance(x.item, EntityTypeToken)
        elif isinstance(x, BlobPtr):
            print_or_error(" ⚠ ️ Warning, `ET` was used as the type in an isinstance comparison for an `Entity`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., Entity) instead. If you want to only catch e.g. isinstance(ET.X, ET) then until the deprecation occurs, you should use isinstance(ET.X, ValueType & ET). ⚠ ️")
            return internals.BT(x) == internals.BT.ENTITY_NODE
        elif isinstance(x, EntityRef):
            print_or_error(" ⚠ ️ Warning, `ET` was used as the type in an isinstance comparison for an `Entity`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., Entity) instead. If you want to only catch e.g. isinstance(ET.X, ET) then until the deprecation occurs, you should use isinstance(ET.X, ValueType & ET). ⚠ ️")
            return True
        elif isinstance(x, AtomClass):
            print_or_error(" ⚠ ️ Warning, `ET` was used as the type in an isinstance comparison for an `Entity`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., Entity) instead. If you want to only catch e.g. isinstance(ET.X, ET) then until the deprecation occurs, you should use isinstance(ET.X, ValueType & ET). ⚠ ️")
            from .._ops import rae_type
            return isinstance(rae_type(x), ET)

        return False
    else:
        if isinstance(x, DelegateRef):
            return x.item == token
        elif isinstance(x, FlatRef):
            from .._ops import rae_type
            return RAET_get_token(rae_type(x)) == token
        elif isinstance(x, BlobPtr):
            if internals.BT(x) != internals.BT.ENTITY_NODE:
                return False
            if isinstance(token, ValueType):
                return isinstance(internals.ET(x), token)
            return internals.ET(x) == token
        elif isinstance(x, EntityRef):
            from .._ops import rae_type
            return RAET_get_token(rae_type(x)) == token
        elif isinstance(x, AtomClass):
            from .._ops import rae_type
            return RAET_get_token(rae_type(x)) == token
        else:
            return False

def AET_is_a(x, typ):
    from . import DelegateRef, AttributeEntityRef, FlatRef, AtomClass
    token = RAET_get_token(typ)
    if token is None:
        # Note: AET check needs to happen first, as the others recurse into this
        if isinstance(x, ValueType):
            return type_name(x) == "AET"
        elif isinstance(x, DelegateRef):
            print_or_error(" ⚠ ️ Warning, `AET` was used as the type in an isinstance comparison for an `AttributeEntity`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., AttributeEntity) instead. If you want to only catch e.g. isinstance(AET.Int, AET) then until the deprecation occurs, you should use isinstance(AET.Int, ValueType & AET). ⚠ ️")
            return isinstance(x.item, AttributeEntityTypeToken)
        elif isinstance(x, BlobPtr):
            print_or_error(" ⚠ ️ Warning, `AET` was used as the type in an isinstance comparison for an `AttributeEntity`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., AttributeEntity) instead. If you want to only catch e.g. isinstance(AET.Int, AET) then until the deprecation occurs, you should use isinstance(AET.Int, ValueType & AET). ⚠ ️")
            return internals.BT(x) == internals.BT.ATTRIBUTE_ENTITY_NODE
        elif isinstance(x, AttributeEntityRef):
            print_or_error(" ⚠ ️ Warning, `AET` was used as the type in an isinstance comparison for an `AttributeEntity`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., AttributeEntity) instead. If you want to only catch e.g. isinstance(AET.Int, AET) then until the deprecation occurs, you should use isinstance(AET.Int, ValueType & AET). ⚠ ️")
            return True
        elif isinstance(x, AtomClass):
            print_or_error(" ⚠ ️ Warning, `AET` was used as the type in an isinstance comparison for an `AttributeEntity`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., AttributeEntity) instead. If you want to only catch e.g. isinstance(AET.Int, AET) then until the deprecation occurs, you should use isinstance(AET.Int, ValueType & AET). ⚠ ️")
            from .._ops import rae_type
            return isinstance(rae_type(x), AET)
        return False
    else:
        if isinstance(x, DelegateRef):
            x_aet = x.item
        elif isinstance(x, FlatRef):
            from .._ops import rae_type
            return RAET_get_token(rae_type(x)) == token
        elif isinstance(x, BlobPtr):
            if internals.BT(x) != internals.BT.ATTRIBUTE_ENTITY_NODE:
                return False
            x_aet = internals.AET(x)
        elif isinstance(x, ValueType) and type_name(x) == "AET":
            x_aet = RAET_get_token(x)
            if x_aet is None:
                return False
        elif isinstance(x, (AttributeEntityTypeToken, AET_QFloat, AET_QInt, AET_Enum)):
            x_aet = x
        elif isinstance(x, AttributeEntityRef):
            from .._ops import rae_type
            x_aet = rae_type(x)
        elif isinstance(x, AtomClass):
            from .._ops import rae_type
            x_aet = rae_type(x)
        else:
            return False

        if token == x_aet:
            return True

        # if isinstance(token, AttributeEntityTypeToken) and token.complex_value is not None:
        #     raise Exception(f"Checking isinstance on complex AETs (got {typ}) is not yet implemented. Coming soon!")
        # if isinstance(x_aet, AttributeEntityTypeToken) and x_aet.complex_value is not None:
        #     raise Exception(f"Checking isinstance on complex AETs (got {x_aet}) is not yet implemented. Coming soon!")

        if isinstance(token, (AET_QFloat, AET_QInt, AET_Enum)):
            if isinstance(x_aet, AttributeEntityTypeToken):
                if isinstance(token, AET_QFloat):
                    return internals.is_vrt_a_quantity_float(x_aet.rep_type)
                if isinstance(token, AET_QInt):
                    return internals.is_vrt_a_quantity_int(x_aet.rep_type)
                if isinstance(token, AET_QEnum):
                    return internals.is_vrt_a_enum(x_aet.rep_type)
        return False
def RT_is_a(x, typ):
    from . import DelegateRef, RelationRef, FlatRef, AtomClass
    token = RAET_get_token(typ)
    if token is None:
        # Note: RT check needs to happen first, as the others recurse into this
        if isinstance(x, ValueType):
            return type_name(x) == "RT"
        elif isinstance(x, DelegateRef):
            print_or_error(" ⚠ ️ Warning, `RT` was used as the type in an isinstance comparison for a `Relation`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., Relation) instead. If you want to only catch e.g. isinstance(RT.X, RT) then until the deprecation occurs, you should use isinstance(RT.X, ValueType & RT). ⚠ ️")
            if type(x.item) == internals.DelegateRelationTriple:
                return isinstance(x.item.rt, RelationTypeToken)
            return isinstance(x.item, RelationTypeToken)
        elif isinstance(x, BlobPtr):
            print_or_error(" ⚠ ️ Warning, `RT` was used as the type in an isinstance comparison for a `Relation`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., Relation) instead. If you want to only catch e.g. isinstance(RT.X, RT) then until the deprecation occurs, you should use isinstance(RT.X, ValueType & RT). ⚠ ️")
            return internals.BT(x) == internals.BT.RELATION_EDGE
        elif isinstance(x, RelationRef):
            print_or_error(" ⚠ ️ Warning, `RT` was used as the type in an isinstance comparison for a `Relation`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., Relation) instead. If you want to only catch e.g. isinstance(RT.X, RT) then until the deprecation occurs, you should use isinstance(RT.X, ValueType & RT). ⚠ ️")
            return True
        elif isinstance(x, AtomClass):
            print_or_error(" ⚠ ️ Warning, `RT` was used as the type in an isinstance comparison for a `Relation`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., Relation) instead. If you want to only catch e.g. isinstance(RT.X, RT) then until the deprecation occurs, you should use isinstance(RT.X, ValueType & RT). ⚠ ️")
            from .._ops import rae_type
            return isinstance(rae_type(x), RT)
        return False
    else:
        if isinstance(x, DelegateRef):
            if type(x.item) == internals.DelegateRelationTriple:
                return x.item.rt == token
            else:
                return x.item == token
        elif isinstance(x, FlatRef):
            from .._ops import rae_type
            return RAET_get_token(rae_type(x)) == token
        elif isinstance(x, BlobPtr):
            if internals.BT(x) != internals.BT.RELATION_EDGE:
                return False
            # TODO: EntityTypeToken
            if isinstance(token, ValueType):
                return isinstance(internals.RT(x), token)
            return internals.RT(x) == token
        elif isinstance(x, RelationRef):
            from .._ops import rae_type
            return RAET_get_token(rae_type(x)) == token
        elif isinstance(x, AtomClass):
            from .._ops import rae_type
            return RAET_get_token(rae_type(x)) == token
        else:
            return False
def BT_is_a(x, typ):
    token = RAET_get_token(typ)
    if token is None:
        # Note: BT check needs to happen first, as the others recurse into this
        if isinstance(x, ValueType):
            return type_name(x) == "BT"
        elif isinstance(x, BlobPtr):
            print_or_error(" ⚠ ️ Warning, `BT` was used as the type in an isinstance comparison for a `BlobPtr`. This will become False in the future, but currently returns True. Update your code to compare isinstance(..., BlobPtr) instead. If you want to only catch e.g. isinstance(BT.VALUE_NODE, BT) then until the deprecation occurs, you should use isinstance(BT.VALUE_NODE, ValueType & BT). ⚠ ️️")
            return True
        return False
    else:
        c_bt = token
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
def VRT_is_a(x, typ):
    from . import DelegateRef
    token = RAET_get_token(typ)
    if token is None:
        # Note: VRT check needs to happen first, as the others recurse into this
        if isinstance(x, ValueType):
            return type_name(x) == "VRT"
        elif isinstance(x, DelegateRef):
            print_or_error(" ⚠ ️ Warning, `VRT` was used as the type in an isinstance comparison for a `Delegate`. This will become False in the future, but currently returns True. This actually is nonsensical, update your code logic. ⚠ ️")
            return isinstance(x.item, ValueRepTypeToken)
        elif isinstance(x, BlobPtr):
            print_or_error(" ⚠ ️ Warning, `VRT` was used as the type in an isinstance comparison for a `Delegate`. This will become False in the future, but currently returns True. This actually is nonsensical, update your code logic. ⚠ ️")
            return internals.BT(x) in [internals.BT.ATTRIBUTE_ENTITY_NODE,
                                       internals.BT.VALUE_NODE]
        return False
    else:
        if isinstance(x, DelegateRef):
            x_vrt = x.item
        elif isinstance(x, BlobPtr):
            if internals.BT(x) not in [internals.BT.ATTRIBUTE_ENTITY_NODE,
                                       internals.BT.VALUE_NODE]:
                return False
            x_vrt = internals.VRT(x)
        elif isinstance(x, ValueType) and type_name(x) == "VRT":
            x_vrt = RAET_get_token(x)
            if x_vrt is None:
                return False
        elif isinstance(x, (ValueRepTypeToken, VRT_QFloat, VRT_QInt, VRT_Enum)):
            x_vrt = x
        else:
            return False

        if isinstance(token, ValueType):
            return isinstance(x_vrt, token)

        if token == x_vrt:
            return True

        if isinstance(token, (VRT_QFloat, VRT_QInt, VRT_Enum)):
            if isinstance(x_aet, AttributeEntityTypeToken):
                if isinstance(token, VRT_QFloat):
                    return internals.is_vrt_a_quantity_float(x_vrt)
                if isinstance(token, VRT_QInt):
                    return internals.is_vrt_a_quantity_int(x_vrt)
                if isinstance(token, VRT_QEnum):
                    return internals.is_vrt_a_enum(x_vrt)
        return False


def ET_ctor(self, *args, **kwargs):
    from . import EntityRef
    if RAET_get_token(self) is None:
        assert len(args) == 1
        assert len(kwargs) == 0
        input = args[0]
        if isinstance(input, ValueType) and isinstance(input, ET):
            return input
        if isinstance(input, EntityRef):
            return input.d["type"]
        from . import AtomClass
        if isinstance(input, AtomClass):
            from ..atom import _get_atom_type
            return ET(_get_atom_type(input))
        return ET[internals.ET(input)]
    else:
        names = RAET_get_names(self)
        names = names + args
        from . import AtomClass
        return AtomClass(RAET_without_names(self), *names, **kwargs)
    
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
             str_func=token_str)

AttributeEntityTypeToken = make_VT('AttributeEntityTypeToken', pytype=internals.AttributeEntityType)
def AET_ctor(self, x):
    if RAET_get_token(self) is None:
        input = x
        if isinstance(input, ValueType) and isinstance(input, AET):
            return input
        return AET[internals.AET(x)]

    else:
        names = RAET_get_names(self)
        names = (x, *names)
        from . import AtomClass
        return AtomClass(RAET_without_names(self), *names)

def AET_ctor(self, *args, **kwargs):
    if RAET_get_token(self) is None or isinstance(RAET_get_token(self), AET_QFloat | AET_QInt | AET_Enum):
        assert len(args) == 1
        assert len(kwargs) == 0
        input = args[0]
        if isinstance(input, ValueType) and isinstance(input, AET):
            return input
        if RAET_get_token(self) is None:
            from . import AtomClass
            if isinstance(input, AtomClass):
                from ..atom import _get_atom_type
                return AET(_get_atom_type(input))
            return AET[internals.AET(input)]
        else:
            return AET[RAET_get_token(self)(input)]
    else:
        names = RAET_get_names(self)
        names = names + args
        from . import AtomClass
        return AtomClass(RAET_without_names(self), *names, **kwargs)

AET = make_VT('AET',
              constructor_func=AET_ctor,
              pass_self=True,
              attr_funcs=wrap_attr_readonly_token(internals.AET),
              is_a_func=AET_is_a,
              is_subtype_func=token_subtype,
              str_func=token_str)

RelationTypeToken = make_VT('RelationTypeToken', pytype=internals.RelationType)

def RT_ctor(self, *args, **kwargs):
    if RAET_get_token(self) is None:
        assert len(args) == 1
        assert len(kwargs) == 0
        input = args[0]
        if isinstance(input, ValueType) and isinstance(input, RT):
            return input
        from . import AtomClass
        if isinstance(input, AtomClass):
            from ..atom import _get_atom_type
            return RT(_get_atom_type(input))
        return RT[internals.RT(input)]
    else:
        names = RAET_get_names(self)
        names = names + args
        from . import AtomClass
        return AtomClass(RAET_without_names(self), *names, **kwargs)

RT = make_VT('RT',
             constructor_func=RT_ctor,
             pass_self=True,
             attr_funcs=wrap_attr_readonly_token(internals.RT),
             is_a_func=RT_is_a,
             is_subtype_func=token_subtype,
             str_func=token_str)


BlobTypeToken = make_VT('BlobTypeToken', pytype=internals.BlobType)
BT = make_VT('BT',
             constructor_func=lambda x: BT[internals.BT(x)],
             attr_funcs=wrap_attr_readonly_token(internals.BT),
             is_a_func=BT_is_a,
             is_subtype_func=token_subtype,
             str_func=token_str)

# BT         = ValueType_(type_name='BT',   constructor_func=pyzef.internals.BT, attr_funcs=wrap_attr_readonly(internals.BT, None), pytype=internals.BlobType)

ValueRepTypeToken = make_VT('ValueRepTypeToken', pytype=internals.ValueRepType)
def VRT_ctor(self, x):
    if type(x) == str:
        return getattr(self, x)
    token = RAET_get_token(self)
    if token is not None:
        return NotImplemented
    return VRT[internals.VRT(x)]
VRT = make_VT('VRT',
              constructor_func=VRT_ctor,
              pass_self=True,
              attr_funcs=wrap_attr_readonly_token(internals.VRT),
              is_a_func=VRT_is_a,
              is_subtype_func=token_subtype,
              str_func=token_str)

RAET = insert_VT("RAET", ET | RT | AET)
