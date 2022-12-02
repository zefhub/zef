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

from .. import report_import
report_import("zef.core.serialization")

__all__ = [
    "serialize",
    "deserialize",
]

from ._core import *
from .internals import BaseUID, EternalUID, ZefRefUID, Val_
from .VT import *
from .VT import ValueType_
from ._ops import *
from .op_structs import ZefOp_, CollectingOp, SubscribingOp, ForEachingOp, LazyValue, Awaitable, is_python_scalar_type
from .abstract_raes import EntityRef_, RelationRef_, AttributeEntityRef_
from ._error import Error_
from ._image import Image_
from .fx.fx_types import FXElement, Effect
from .flat_graph import FlatGraph_, FlatRef_, FlatRefs_
from ..pyzef import internals as pyinternals
from .symbolic_expression import SymbolicExpression_
from .user_value_type import UserValueInstance_

##############################
# * Definition
#----------------------------

def get_serializable_types():
    return list(serialization_mapping.keys())

# This will get filled in soon
serialization_mapping = {}
deserialization_mapping = {}

def is_serialization(v):
    return isinstance(v, dict) and v.get("_zeftype", None) == "serialization"

def serialize(v):
    """
    Given a dictionary, list, and any Zeftype this function converts all of the
    ZefTypes into dictionaries that describe the ZefType and allows it to be
    serialized using JSON dumps.

    This function wraps the result of serialize_internal with a serialization
    header. The output of this function will always be a dictionary, even if
    serialize_internal does no work.
    """

    return {"_zeftype": "serialization",
            "version": 1,
            "data": serialize_internal(v)}

def serialize_internal(v):
    """
    This function is the recursive core of the serialization, which does not
    wrap the objects with any header.
    """

    # Serializes a list and recursively calls itself if one of the list elements is of type List
    if type(v) == bytes:
        # Our target is for a JSON-compatible format, so need to convert this to
        # a string smoehow. We choose base64 encoding.
        import base64
        return {
            "_zeftype": "bytes",
            "data": base64.b64encode(v).decode("utf-8")
        }
    if is_python_scalar_type(v):
        return v
    elif type(v) in serialization_mapping:
         return serialization_mapping[type(v)](v)
    raise Exception(f"Don't know how to serialize type {type(v)}")

def deserialize(v):
    """
    Given an output from a previous call to serialize, convert the contained
    Zeftypes, Dicts, Lists into their original forms. This function recursively
    calls itself and other internal functions to fully deserialize nested lists
    and dictionaries.
    """
    if not is_serialization(v):
        from .logger import log
        log.warn("Warning, deserializing an object without a serialization header. This behaviour is deprecated.")
        return deserialize_internal(v)

    if v.get("version", None) != 1:
        raise Exception("Don't understand serialization version '{v.get('version', None)}'")

    return deserialize_internal(v["data"])

def deserialize_internal(v):
    """
    This function is the recursive core of the deserialization, where the
    objects are not wrapped in the serialization header.
    """
    from .logger import log
    if isinstance(v, dict) and "_zeftype" in v:
        v = deserialization_mapping[v["_zeftype"]](v)
    elif isinstance(v, dict):
        v = deserialize_dict(v)
    elif isinstance(v, list):
        v = deserialize_list(v)
    elif isinstance(v, tuple):
        log.warning("Should never get to the point of trying to deserialize a tuple anymore.")
        v = deserialize_tuple(v)

    return v


####################################
# * Implementations
#----------------------------------
def serialize_flatgraph_or_flatref(fg_or_fr) -> dict:
    if isinstance(fg_or_fr, FlatGraph_):
        return {
            "_zeftype": "FlatGraph",
            "key_dict": serialize_dict(fg_or_fr.key_dict),
            "blobs": serialize_tuple(fg_or_fr.blobs),
        }
    elif isinstance(fg_or_fr, FlatRef):
        return {
            "_zeftype": "FlatRef",
            "fg": serialize_flatgraph_or_flatref(fg_or_fr.fg),
            "idx": fg_or_fr.idx,
        }
    elif isinstance(fg_or_fr, FlatRefs):
        return {
            "_zeftype": "FlatRefs",
            "fg": serialize_flatgraph_or_flatref(fg_or_fr.fg),
            "idxs": serialize_list(fg_or_fr.idxs),
        }

def serialize_tuple(l: tuple) -> dict:
    return {
        "_zeftype": "tuple",
        "items": [serialize_internal(el) for el in l]
    }

def serialize_list(l: list) -> list:
    return [serialize_internal(el) for el in l]

def serialize_dict(json_d: dict) -> dict:
    # return {k: serialize(v) for k,v in json_d.items()}
    return {
        "_zeftype": "dict",
        "items": [[serialize_internal(k), serialize_internal(v)] for k,v in json_d.items()]
    }

def serialize_zeftypes(z) -> dict:
    if isinstance(z, ZefRef):
        return {
                "_zeftype"  : "ZefRef",
                "guid"      : str(base_uid(Graph(z))),
                "uid"       : str(base_uid(z)),
                "tx_uid"    : str(base_uid(z | frame | to_tx)),
            }

    elif isinstance(z, EZefRef):
        return {
                "_zeftype"  : "EZefRef",
                "guid"      : str(base_uid(Graph(z))),
                "uid"       : str(base_uid(z)),
            }

    # elif isinstance(z, ZefRefs) or isinstance(z, EZefRefs):
    #     bt_type = {ZefRefs: "ZefRefs", EZefRefs: "EZefRefs"}[type(z)]
    #     tx_uid = str(base_uid(z | frame | to_tx)) if bt_type == "ZefRefs" else None
    #     guid = str(base_uid(Graph(z | first))) if len(z) > 0 else None
    #     return {
    #         "_zeftype"  : bt_type,
    #         "tx_uid"    : tx_uid,
    #         "guid"      : guid, # could be None if ZefRefs or UZefrefs was empty
    #         "value" : [{"uid": str(base_uid(zr))} for zr in z]
    #             }

    elif isinstance(z, RelationTypeToken) or isinstance(z, EntityTypeToken) or isinstance(z, AttributeEntityTypeToken):
        bt_type = {internals.RelationType: "RTToken", internals.EntityType: "ETToken", internals.AttributeEntityType: "AETToken"}[type(z)]
        return {"_zeftype": bt_type, "value": token_name(z)}

    elif isinstance(z, Graph):
        return {"_zeftype": "Graph", "guid": str(uid(z))}
    elif isinstance(z, GraphRef):
        return {"_zeftype": "GraphRef", "guid": str(z.uid)}

    elif isinstance(z, Enum):
        return {"_zeftype": "Enum", "enum_type": z.enum_type, "enum_val": z.enum_value}

    elif isinstance(z, QuantityFloat) or isinstance(z, QuantityInt):
        q_type = {QuantityFloat: "QuantityFloat", QuantityInt: "QuantityInt"}[type(z)]
        return {"_zeftype": q_type, "value": z.value, "unit": serialize_internal(z.unit)}

    elif isinstance(z, Time):
        return {"_zeftype": "Time", "value": z.seconds_since_1970} 

    elif type(z) in [ZefOp_, CollectingOp, SubscribingOp, ForEachingOp]:
        if type(z) == ZefOp_:
            z_type = "ZefOp"
        else:
            z_type = type(z).__name__
        return serialize_zefops(z_type, z.el_ops)

    elif type(z) in [LazyValue, Awaitable]:
        if isinstance(z, LazyValue):
            additional_dict = {"initial_val": z.initial_val}
        else:
            additional_dict = {"pushable": z.pushable}
        z_type = {LazyValue: "LazyValue", Awaitable: "Awaitable"}[type(z)]
        inner_ztype = {ZefOp_: "ZefOp", CollectingOp: "CollectingOp", SubscribingOp: "SubscribingOp", ForEachingOp: "ForEachingOp"}[type(z.el_ops)]

        return {"_zeftype": z_type, "el_ops": serialize_zefops(inner_ztype, z.el_ops.el_ops), **additional_dict}

    elif isinstance(z, (BaseUID, EternalUID, ZefRefUID)):
        return {"_zeftype": "UID", "value": str(z)}

    elif isinstance(z, Image):
        import zstd, base64
        encoded_buffer = z.buffer
        encoded_buffer = base64.b64encode(zstd.decompress(encoded_buffer)).decode('utf8')
        return {"_zeftype": "Image", "format": z.format, "compression": z.compression, "buffer" : encoded_buffer}

    elif isinstance(z, (EntityRef, RelationRef, AttributeEntityRef)):
        abstract_type = {EntityRef_: "Entity", RelationRef_: "Relation", AttributeEntityRef_: "AttributeEntity"}[type(z)]
        return {"_zeftype": abstract_type, "d": serialize_internal(z.d)}

    elif isinstance(z, Error):
        return {"_zeftype": "Error", "type": z.name, "args": serialize_list(z.args)}

    elif isinstance(z, FXElement):
        return {"_zeftype": "FXElement", "elements": [e for e in z.d]}

    else:
        raise NotImplementedError(f"{z} (type {type(z)}) isn't part of the supported serializable zeftypes!")

def serialize_delegate(z) -> dict:
    if isinstance(z, Delegate):
        return {"_zeftype": "Delegate", "order": z.order, "item": serialize_internal(z.item)}
    elif isinstance(z, pyinternals.DelegateTX):
        return {"_zeftype": "DelegateTX"}
    elif isinstance(z, pyinternals.DelegateRoot):
        return {"_zeftype": "DelegateRoot"}
    elif isinstance(z, pyinternals.DelegateRelationTriple):
        return {"_zeftype": "DelegateRelationTriple", "rt": z.rt, "source": serialize_internal(z.source), "target": serialize_internal(z.target)}

    raise NotImplementedError(f"{z} not a delegate")


def serialize_zefops(k_type, ops):
    serialized_ops = []
    for op in ops:
        op_rt, op_subops = op
        assert type(op_rt) == internals.RelationType
        op_rt = serialize_internal(op_rt)

        serialized_subops = []
        if len(op_subops) > 0 and type(op_subops[0]) == internals.RelationType and op_subops[0] == RT.L:
            serialized_subops.append(serialize_zefops("ZefOp", (op_subops,)))
        else:
            for sub_op in op_subops:
                if type(sub_op) in list(serialization_mapping.keys()):
                    sub_op = serialize_internal(sub_op)
                serialized_subops.append(sub_op,)
        serialized_ops.append({"op": op_rt, "curried_ops": serialized_subops})

    return {"_zeftype": k_type, "el_ops": serialized_ops}

def serialize_valuetype(vt):
    # Super dodgy version just to get something off the ground for now
    return {
        "_zeftype": "ValueType",
        **{key: serialize_internal(val) for (key,val) in vt._d.items()},
    }


def serialize_symbolicexpression(se):
    return {
        "_zeftype": "SymbolicExpression",
        "name": se.name,
        "root_node": serialize_internal(se.root_node),
    }

def serialize_user_value_instance(uvi):
    return {
        "_zeftype": "UserValueInstance",
        "user_type_id": uvi._user_type_id,
        "value": serialize_internal(uvi._value),
    }

def serialize_val(val):
    return {
        "_zeftype": "Val",
        "arg": serialize_internal(val.arg),
        "iid": serialize_internal(val.iid),
    }


def deserialize_tuple(json_d: dict) -> tuple:
    return tuple(deserialize_internal(el) for el in json_d["items"])

def deserialize_list(l: list) -> list:
    return [deserialize_internal(el) for el in l]

def deserialize_dict(json_d):
    # return {k: deserialize(v) for k,v in json_d.items()}
    return {deserialize_internal(k): deserialize_internal(v) for k,v in json_d["items"]}

    
def deserialize_zeftypes(z) -> dict:
    if z['_zeftype'] == "ZefRef":
        g = Graph(z['guid'])
        # Note: can't use in_frame here, because if the z itself is a TX, this will not behave correctly.
        return ZefRef(g[z['uid']], g[z['tx_uid']])

    elif z['_zeftype'] == "EZefRef":
        g = Graph(z['guid'])
        return g[z['uid']] 

    # elif z['_zeftype'] == "ZefRefs" or z['_zeftype'] == "EZefRefs":
    #     g = Graph(z['guid'])
    #     if z['_zeftype'] == "ZefRefs": return ZefRefs([g[zr['uid']] | to_frame[g[z['tx_uid']]] for zr in z['value']])
    #     else: return EZefRefs([g[zr['uid']] for zr in z['value']])


    elif z['_zeftype'] in {"RTToken", "ETToken"}:
        bt_class = {"RTToken": internals.RT, "ETToken": internals.ET}[z['_zeftype']]
        base = bt_class(z['value'])
        return base

    elif z['_zeftype'] == "AETToken": 
        type_map = {
                "Int":              internals.AET.Int,
                "Float":            internals.AET.Float,
                "Bool":             internals.AET.Bool,
                "String":           internals.AET.String,
                "Enum":             internals.AET.Enum,
                "QuantityFloat":    internals.AET.QuantityFloat, 
                "QuantityInt":      internals.AET.QuantityInt, 
                "Time":             internals.AET.Time,
                "Serialized":       internals.AET.Serialized,
        }
        first_part,*rest = z['value'].split('.')
        out = type_map[first_part]
        for part in rest:
            out = getattr(out, part)
        return out

    elif z['_zeftype'] == "Graph":
        return Graph(z['guid'])
    elif z['_zeftype'] == "GraphRef":
        return GraphRef(base_uid(z['guid']))

    elif z['_zeftype'] == "Enum":
        return EN(z['enum_type'], z['enum_val'])

    elif z['_zeftype'] in {"QuantityFloat", "QuantityInt"}:
        quantity_type = {"QuantityFloat": QuantityFloat, "QuantityInt": QuantityInt}[z['_zeftype']]
        en = deserialize_internal(z['unit'])
        return quantity_type(z['value'], en)

    elif z['_zeftype'] == "Time":
        return Time(z['value']) 

    elif z['_zeftype'] in {"ZefOp", "CollectingOp", "SubscribingOp", "ForEachingOp"}:
        types = {"ZefOp": ZefOp,  "CollectingOp":CollectingOp,  "SubscribingOp":SubscribingOp, "ForEachingOp": ForEachingOp}
        if z['_zeftype'] != "ZefOp": return types[z['_zeftype']](ZefOp(deserialize_zefops(z['el_ops'])))
        return ZefOp(deserialize_zefops(z['el_ops']))

    elif z['_zeftype'] in {"LazyValue", "Awaitable"}:
        types = {"LazyValue": LazyValue,  "Awaitable":Awaitable}

        if z['_zeftype'] == "LazyValue":
            res = LazyValue(z['initial_val'])
        else:
            res = Awaitable(z['pushable'])

        res.el_ops = deserialize_internal(z['el_ops'])
        return res

    elif z['_zeftype'] == "UID":
        return uid(z['value'])

    elif z['_zeftype'] == "Image":
        import base64
        encoded_buffer = z['buffer']
        compressed_buffer = base64.b64decode(encoded_buffer)
        return Image(compressed_buffer, z['format'])

    elif z['_zeftype']  in {"Entity", "Relation", "AttributeEntity"}:
        abstract_type = {"Entity": EntityRef, "Relation": RelationRef, "AttributeEntity": AttributeEntityRef}[z['_zeftype']]
        d = deserialize_internal(z["d"])
        return abstract_type(d)

    elif z['_zeftype'] == "Error":
        return getattr(Error, z['type'])(*deserialize_list(z['args']))

    elif z['_zeftype'] == "Effect":
        return deserialize_dict(z['internal_dict'])

    elif z['_zeftype'] == "FXElement":
        return FXElement(tuple(z['elements']))

    elif z['_zeftype'] == "FlatGraph":
        new_fg = FlatGraph_()
        new_fg.key_dict = deserialize_dict(z['key_dict'])
        new_fg.blobs = deserialize_tuple(z['blobs'])
        return new_fg

    elif z['_zeftype'] == "FlatRef":
        fg = deserialize_internal(z['fg'])
        return FlatRef(fg, z['idx'])

    elif z['_zeftype'] == "FlatRefs":
        fg = deserialize_internal(z['fg'])
        return FlatRefs(fg, deserialize_list(z['idxs']))

    else:
        raise NotImplementedError(f"{z['_zeftype']} isn't part of the supported deserializable zeftypes!")

def deserialize_delegate(d) -> dict:
    if d['_zeftype'] == "Delegate":
        return Delegate(d["order"], deserialize_internal(d["item"]))
    elif d['_zeftype'] == "DelegateTX":
        return pyinternals.DelegateTX()
    elif d['_zeftype'] == "DelegateRoot":
        return pyinternals.DelegateRoot()
    elif d['_zeftype'] == "DelegateRelationTriple":
        return pyinternals.DelegateRelationTriple(d["rt"], deserialize_internal(d["source"]), deserialize_internal(d["target"]))

    raise NotImplementedError(f"{d} not a serialized delegate")


def deserialize_zefops(ops):
    deserialized_ops = ()
    for op in ops:
        op_rt, op_subops = op['op'], op['curried_ops']
        assert op_rt['_zeftype'] == "RTToken"
        op_rt = deserialize_internal(op_rt)

        deserialized_subops = ()
        for sub_op in op_subops:
            sub_op = deserialize_internal(sub_op)
            deserialized_subops = deserialized_subops + (sub_op,)
        deserialized_ops = (*deserialized_ops , (op_rt, (*deserialized_subops,)))

    return deserialized_ops

def deserialize_valuetype(d_in):
    # Super dodgy version just to get something off the ground for now

    d = {key: deserialize_internal(val) for key,val in d_in.items() if key != "_zeftype"}
    # Look for the same typename
    from . import VT
    for var in dir(VT):
        item = getattr(VT, var)
        if isinstance(item, ValueType_) and item._d["type_name"] == d["type_name"]:
            return item._replace(**d)
    raise Exception(f"Couldn't find a ValueType of type '{d['type_name']}'")

def deserialize_symbolicexpression(d):
    return SymbolicExpression_(
        name = d["name"],
        root_node= deserialize_internal(d["root_node"]),
    )

def deserialize_user_value_instance(d):
    return UserValueInstance_(
        d["user_type_id"],
        deserialize_internal(d["value"]),
    )

def deserialize_val(d):
    return Val(
        deserialize_internal(d["arg"]),
        deserialize_internal(d["iid"]),
    )

serialization_mapping[internals.ZefRef] = serialize_zeftypes
# serialization_mapping[ZefRefs] = serialize_zeftypes
serialization_mapping[internals.EZefRef] = serialize_zeftypes
# serialization_mapping[EZefRefs] = serialize_zeftypes
serialization_mapping[internals.RelationType] = serialize_zeftypes
serialization_mapping[internals.EntityType] = serialize_zeftypes
serialization_mapping[internals.AttributeEntityType] = serialize_zeftypes
serialization_mapping[internals.Graph] = serialize_zeftypes
serialization_mapping[internals.GraphRef] = serialize_zeftypes
serialization_mapping[internals.ZefEnumValue] = serialize_zeftypes
serialization_mapping[QuantityFloat] = serialize_zeftypes
serialization_mapping[QuantityInt] = serialize_zeftypes
serialization_mapping[Time] = serialize_zeftypes
serialization_mapping[ZefOp_] = serialize_zeftypes
serialization_mapping[CollectingOp] = serialize_zeftypes
serialization_mapping[SubscribingOp] = serialize_zeftypes
serialization_mapping[ForEachingOp] = serialize_zeftypes
serialization_mapping[LazyValue] = serialize_zeftypes
serialization_mapping[Awaitable] = serialize_zeftypes
serialization_mapping[internals.BaseUID] = serialize_zeftypes
serialization_mapping[internals.EternalUID] = serialize_zeftypes
serialization_mapping[internals.ZefRefUID] = serialize_zeftypes
serialization_mapping[EntityRef_] = serialize_zeftypes
serialization_mapping[RelationRef_] = serialize_zeftypes
serialization_mapping[AttributeEntityRef_] = serialize_zeftypes
serialization_mapping[Error_] = serialize_zeftypes
serialization_mapping[Image_] = serialize_zeftypes
serialization_mapping[FXElement] = serialize_zeftypes
serialization_mapping[Delegate] = serialize_delegate
serialization_mapping[FlatGraph_] = serialize_flatgraph_or_flatref
serialization_mapping[FlatRef_] = serialize_flatgraph_or_flatref
serialization_mapping[FlatRefs_] = serialize_flatgraph_or_flatref
serialization_mapping[pyinternals.DelegateTX] = serialize_delegate
serialization_mapping[pyinternals.DelegateRoot] = serialize_delegate
serialization_mapping[pyinternals.DelegateRelationTriple] = serialize_delegate
serialization_mapping[ValueType_] = serialize_valuetype
serialization_mapping[list] = serialize_list
serialization_mapping[tuple] = serialize_tuple
serialization_mapping[dict] = serialize_dict
serialization_mapping[SymbolicExpression_] = serialize_symbolicexpression
serialization_mapping[UserValueInstance_] = serialize_user_value_instance
serialization_mapping[Val_] = serialize_val

deserialization_mapping["dict"] = deserialize_dict
deserialization_mapping["tuple"] = deserialize_tuple
deserialization_mapping["ZefRef"] = deserialize_zeftypes
# deserialization_mapping["ZefRefs"] = deserialize_zeftypes
deserialization_mapping["EZefRef"] = deserialize_zeftypes
# deserialization_mapping["EZefRefs"] = deserialize_zeftypes
# Note: ET/RT/AET are ValueTypes now
# deserialization_mapping["RT"] = deserialize_zeftypes
# deserialization_mapping["ET"] = deserialize_zeftypes
# deserialization_mapping["AET"] = deserialize_zeftypes
deserialization_mapping["RTToken"] = deserialize_zeftypes
deserialization_mapping["ETToken"] = deserialize_zeftypes
deserialization_mapping["AETToken"] = deserialize_zeftypes
deserialization_mapping["Graph"] = deserialize_zeftypes
deserialization_mapping["GraphRef"] = deserialize_zeftypes
deserialization_mapping["Enum"] = deserialize_zeftypes
deserialization_mapping["QuantityFloat"] = deserialize_zeftypes
deserialization_mapping["QuantityInt"] = deserialize_zeftypes
deserialization_mapping["Time"] = deserialize_zeftypes
deserialization_mapping["ZefOp"] = deserialize_zeftypes
deserialization_mapping["CollectingOp"] = deserialize_zeftypes
deserialization_mapping["SubscribingOp"] = deserialize_zeftypes
deserialization_mapping["ForEachingOp"] = deserialize_zeftypes
deserialization_mapping["LazyValue"] = deserialize_zeftypes
deserialization_mapping["Awaitable"] = deserialize_zeftypes
deserialization_mapping["UID"] = deserialize_zeftypes
deserialization_mapping["Entity"] = deserialize_zeftypes
deserialization_mapping["Relation"] = deserialize_zeftypes
deserialization_mapping["AttributeEntity"] = deserialize_zeftypes
deserialization_mapping["Error"] = deserialize_zeftypes
deserialization_mapping["Image"] = deserialize_zeftypes
deserialization_mapping["Effect"] = deserialize_zeftypes
deserialization_mapping["FXElement"] = deserialize_zeftypes
deserialization_mapping["FlatGraph"] = deserialize_zeftypes
deserialization_mapping["FlatRef"] = deserialize_zeftypes
deserialization_mapping["FlatRefs"] = deserialize_zeftypes
deserialization_mapping["Delegate"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateTX"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateRoot"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateRelationTriple"] = deserialize_delegate
deserialization_mapping["ValueType"] = deserialize_valuetype
deserialization_mapping["SymbolicExpression"] = deserialize_symbolicexpression
deserialization_mapping["UserValueInstance"] = deserialize_user_value_instance
deserialization_mapping["Val"] = deserialize_val
