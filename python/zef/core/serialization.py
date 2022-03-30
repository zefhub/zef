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

__all__ = [
    "serialize",
    "deserialize",
]

from ._core import *
from .internals import BaseUID, EternalUID, ZefRefUID
from ._ops import *
from .op_structs import ZefOp, CollectingOp, SubscribingOp, ForEachingOp, LazyValue, Awaitable, is_python_scalar_type
from .abstract_raes import Entity, Relation, AtomicEntity
from .error import _ErrorType, Error
from .image import Image
from .fx.fx_types import _Effect_Class, FXElement, Effect
from .flat_graph import FlatGraph, FlatRef, FlatRefs
from ..pyzef import internals as pyinternals

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
    elif isinstance(v, list):
        return serialize_list(v)
    elif isinstance(v, tuple):
        return serialize_tuple(v)
    elif isinstance(v, dict):
        return serialize_dict(v)
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
    if isinstance(fg_or_fr, FlatGraph):
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

    elif isinstance(z, ZefRefs) or isinstance(z, EZefRefs):
        bt_type = {ZefRefs: "ZefRefs", EZefRefs: "EZefRefs"}[type(z)]
        tx_uid = str(base_uid(z | frame | to_tx)) if bt_type == "ZefRefs" else None
        guid = str(base_uid(Graph(z | first))) if len(z) > 0 else None
        return {
            "_zeftype"  : bt_type,
            "tx_uid"    : tx_uid,
            "guid"      : guid, # could be None if ZefRefs or UZefrefs was empty
            "value" : [{"uid": str(base_uid(zr))} for zr in z]
                }

    elif isinstance(z, RelationType) or isinstance(z, EntityType) or isinstance(z, AtomicEntityType):
        bt_type = {RelationType: "RT", EntityType: "ET", AtomicEntityType: "AET"}[type(z)]
        absorbed_args = LazyValue(z) | absorbed | collect
        absorbed_args = serialize_internal(absorbed_args)
        return {"_zeftype": bt_type, "value": str(z), "absorbed": absorbed_args}

    elif isinstance(z, Graph):
        return {"_zeftype": "Graph", "guid": str(uid(z))}

    elif isinstance(z, ZefEnumValue):
        return {"_zeftype": "Enum", "enum_type": z.enum_type, "enum_val": z.enum_value}

    elif isinstance(z, QuantityFloat) or isinstance(z, QuantityInt):
        q_type = {QuantityFloat: "QuantityFloat", QuantityInt: "QuantityInt"}[type(z)]
        return {"_zeftype": q_type, "value": z.value, "unit": serialize_internal(z.unit)}

    elif isinstance(z, Time):
        return {"_zeftype": "Time", "value": z.seconds_since_1970} 

    elif type(z) in [ZefOp, CollectingOp, SubscribingOp, ForEachingOp]:
        z_type = type(z).__name__
        return serialize_zefops(z_type, z.el_ops)

    elif type(z) in [LazyValue, Awaitable]:
        if isinstance(z, LazyValue):
            additional_dict = {"initial_val": z.initial_val}
        else:
            additional_dict = {"pushable": z.pushable}
        z_type = {LazyValue: "LazyValue", Awaitable: "Awaitable"}[type(z)]
        inner_ztype = {ZefOp: "ZefOp", CollectingOp: "CollectingOp", SubscribingOp: "SubscribingOp", ForEachingOp: "ForEachingOp"}[type(z.el_ops)]

        return {"_zeftype": z_type, "el_ops": serialize_zefops(inner_ztype, z.el_ops.el_ops), **additional_dict}

    elif type(z) in [BaseUID, EternalUID, ZefRefUID]:
        return {"_zeftype": "UID", "value": str(z)}

    elif isinstance(z, Image):
        import zstd
        encoded_buffer = z.buffer
        encoded_buffer = zstd.decompress(encoded_buffer).decode()
        return {"_zeftype": "Image", "format": z.format, "compression": z.compression, "buffer" : encoded_buffer}

    elif type(z) in [Entity, Relation, AtomicEntity]:
        abstract_type = {Entity: "Entity", Relation: "Relation", AtomicEntity: "AtomicEntity"}[type(z)]
        uid_or_uids = "uids" if abstract_type == "Relation" else "uid"
        type_or_types = [serialize_internal(rae) for rae in z.d['type']] if abstract_type == "Relation" else serialize_internal(z.d['type'])
        absorbed_args = z.d['absorbed']
        return {"_zeftype": abstract_type, "type": type_or_types, uid_or_uids: serialize_internal(z.d[uid_or_uids]), 'absorbed': serialize_internal(absorbed_args)}

    elif isinstance(z, _ErrorType):
        return {"_zeftype": "ErrorType", "type": z.name, "args": serialize_list(z.args)}

    elif isinstance(z, _Effect_Class):
        return {"_zeftype": "Effect", "internal_dict": serialize_dict(z.d)}
    
    elif isinstance(z, FXElement):
        return {"_zeftype": "FXElement", "elements": [e for e in z.d]}

    else:
        raise NotImplementedError(f"{z} isn't part of the supported deserializable zeftypes!")

def serialize_delegate(z) -> dict:
    if isinstance(z, Delegate):
        return {"_zeftype": "Delegate", "order": z.order, "item": serialize_delegate(z.item)}
    elif isinstance(z, pyinternals.DelegateEntity):
        return {"_zeftype": "DelegateEntity", "et": z.et}
    elif isinstance(z, pyinternals.DelegateAtomicEntity):
        return {"_zeftype": "DelegateAtomicEntity", "aet": z.aet}
    elif isinstance(z, pyinternals.DelegateRelationGroup):
        return {"_zeftype": "DelegateRelationGroup", "rt": z.rt}
    elif isinstance(z, pyinternals.DelegateTX):
        return {"_zeftype": "DelegateTX"}
    elif isinstance(z, pyinternals.DelegateRoot):
        return {"_zeftype": "DelegateRoot"}
    elif isinstance(z, pyinternals.DelegateRelationTriple):
        return {"_zeftype": "DelegateRelationTriple", "rt": z.rt, "source": serialize_delegate(z.source), "target": serialize_delegate(z.target)}

    raise NotImplementedError(f"{z} not a delegate")


def serialize_zefops(k_type, ops):
    serialized_ops = []
    for op in ops:
        op_rt, op_subops = op
        assert type(op_rt) == RelationType
        op_rt = serialize_internal(op_rt)

        serialized_subops = []
        if len(op_subops) > 0 and type(op_subops[0]) == RelationType and op_subops[0] == RT.L:
            serialized_subops.append(serialize_zefops("ZefOp", (op_subops,)))
        else:
            for sub_op in op_subops:
                if type(sub_op) in list(serialization_mapping.keys()):
                    sub_op = serialize_internal(sub_op)
                serialized_subops.append(sub_op,)
        serialized_ops.append({"op": op_rt, "curried_ops": serialized_subops})

    return {"_zeftype": k_type, "el_ops": serialized_ops}


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

    elif z['_zeftype'] == "ZefRefs" or z['_zeftype'] == "EZefRefs":
        g = Graph(z['guid'])
        if z['_zeftype'] == "ZefRefs": return ZefRefs([g[zr['uid']] | to_frame[g[z['tx_uid']]] for zr in z['value']])
        else: return EZefRefs([g[zr['uid']] for zr in z['value']])


    elif z['_zeftype'] in {"RT", "ET"}:
        bt_class = {"RT": RT, "ET": ET}[z['_zeftype']]
        absorbed_args = deserialize_internal(z['absorbed'])
        base = bt_class(z['value'])
        base._absorbed = absorbed_args
        return base

    elif z['_zeftype'] == "AET": 
        type_map = {
                "Int":              AET.Int,
                "Float":            AET.Float,
                "Bool":             AET.Bool,
                "String":           AET.String,
                "Enum":             AET.Enum,
                "QuantityFloat":    AET.QuantityFloat, 
                "QuantityInt":      AET.QuantityInt, 
                "Time":             AET.Time,
                "Serialized":       AET.Serialized,
        }
        absorbed_args = deserialize_internal(z['absorbed'])
        first_part,*rest = z['value'].split('.')
        out = type_map[first_part]
        for part in rest:
            out = getattr(out, part)
        if absorbed_args: out._absorbed = absorbed_args
        return out

    elif z['_zeftype'] == "Graph":
        return Graph(z['guid'])

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
        encoded_buffer = z['buffer']
        compressed_buffer = bytes(encoded_buffer,"UTF-8")
        return Image(compressed_buffer, z['format'])

    elif z['_zeftype']  in {"Entity", "Relation", "AtomicEntity"}:
        abstract_type = {"Entity": Entity, "Relation": Relation, "AtomicEntity": AtomicEntity}[z['_zeftype']]
        uid_or_uids = "uids" if z['_zeftype'] == "Relation" else "uid"
        uid_or_uids_value = deserialize_internal(z[uid_or_uids])
        type_or_types = tuple([deserialize_internal(rae) for rae in z['type']]) if z['_zeftype'] == "Relation" else deserialize_internal(z['type'])
        absorbed_args = deserialize_internal(z['absorbed'])
        return abstract_type({'type': type_or_types, uid_or_uids: uid_or_uids_value, 'absorbed': absorbed_args})

    elif z['_zeftype'] == "ErrorType":
        return Error.__getattribute__(z['type'])(*deserialize_list(z['args']))

    elif z['_zeftype'] == "Effect":
        return Effect(deserialize_dict(z['internal_dict']))

    elif z['_zeftype'] == "FXElement":
        return FXElement(tuple(z['elements']))

    elif z['_zeftype'] == "FlatGraph":
        new_fg = FlatGraph()
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
        return Delegate(d["order"], deserialize_delegate(d["item"]))
    elif d['_zeftype'] == "DelegateEntity":
        return pyinternals.DelegateEntity(d["et"])
    elif d['_zeftype'] == "DelegateAtomicEntity":
        return pyinternals.DelegateAtomicEntity(d["aet"])
    elif d['_zeftype'] == "DelegateRelationGroup":
        return pyinternals.DelegateRelationGroup(d["rt"])
    elif d['_zeftype'] == "DelegateTX":
        return pyinternals.DelegateTX()
    elif d['_zeftype'] == "DelegateRoot":
        return pyinternals.DelegateRoot()
    elif d['_zeftype'] == "DelegateRelationTriple":
        return pyinternals.DelegateRelationTriple(d["rt"], deserialize_delegate(d["source"]), deserialize_delegate(d["target"]))

    raise NotImplementedError(f"{d} not a serialized delegate")


def deserialize_zefops(ops):
    deserialized_ops = ()
    for op in ops:
        op_rt, op_subops = op['op'], op['curried_ops']
        assert op_rt['_zeftype'] == "RT"
        op_rt = deserialize_internal(op_rt)

        deserialized_subops = ()
        for sub_op in op_subops:
            sub_op = deserialize_internal(sub_op)
            deserialized_subops = deserialized_subops + (sub_op,)
        deserialized_ops = (*deserialized_ops , (op_rt, (*deserialized_subops,)))

    return deserialized_ops


serialization_mapping[ZefRef] = serialize_zeftypes
serialization_mapping[ZefRefs] = serialize_zeftypes
serialization_mapping[EZefRef] = serialize_zeftypes
serialization_mapping[EZefRefs] = serialize_zeftypes
serialization_mapping[RelationType] = serialize_zeftypes
serialization_mapping[EntityType] = serialize_zeftypes
serialization_mapping[AtomicEntityType] = serialize_zeftypes
serialization_mapping[Graph] = serialize_zeftypes
serialization_mapping[ZefEnumValue] = serialize_zeftypes
serialization_mapping[QuantityFloat] = serialize_zeftypes
serialization_mapping[QuantityInt] = serialize_zeftypes
serialization_mapping[Time] = serialize_zeftypes
serialization_mapping[ZefOp] = serialize_zeftypes
serialization_mapping[CollectingOp] = serialize_zeftypes
serialization_mapping[SubscribingOp] = serialize_zeftypes
serialization_mapping[ForEachingOp] = serialize_zeftypes
serialization_mapping[LazyValue] = serialize_zeftypes
serialization_mapping[Awaitable] = serialize_zeftypes
serialization_mapping[BaseUID] = serialize_zeftypes
serialization_mapping[EternalUID] = serialize_zeftypes
serialization_mapping[ZefRefUID] = serialize_zeftypes
serialization_mapping[Entity] = serialize_zeftypes
serialization_mapping[Relation] = serialize_zeftypes
serialization_mapping[AtomicEntity] = serialize_zeftypes
serialization_mapping[_ErrorType] = serialize_zeftypes
serialization_mapping[Image] = serialize_zeftypes
serialization_mapping[_Effect_Class] = serialize_zeftypes
serialization_mapping[FXElement] = serialize_zeftypes
serialization_mapping[Delegate] = serialize_delegate
serialization_mapping[FlatGraph] = serialize_flatgraph_or_flatref
serialization_mapping[FlatRef] = serialize_flatgraph_or_flatref
serialization_mapping[FlatRefs] = serialize_flatgraph_or_flatref
serialization_mapping[pyinternals.DelegateEntity] = serialize_delegate
serialization_mapping[pyinternals.DelegateAtomicEntity] = serialize_delegate
serialization_mapping[pyinternals.DelegateRelationGroup] = serialize_delegate
serialization_mapping[pyinternals.DelegateTX] = serialize_delegate
serialization_mapping[pyinternals.DelegateRoot] = serialize_delegate
serialization_mapping[pyinternals.DelegateRelationTriple] = serialize_delegate


deserialization_mapping["dict"] = deserialize_dict
deserialization_mapping["tuple"] = deserialize_tuple
deserialization_mapping["ZefRef"] = deserialize_zeftypes
deserialization_mapping["ZefRefs"] = deserialize_zeftypes
deserialization_mapping["EZefRef"] = deserialize_zeftypes
deserialization_mapping["EZefRefs"] = deserialize_zeftypes
deserialization_mapping["RT"] = deserialize_zeftypes
deserialization_mapping["ET"] = deserialize_zeftypes
deserialization_mapping["AET"] = deserialize_zeftypes
deserialization_mapping["Graph"] = deserialize_zeftypes
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
deserialization_mapping["AtomicEntity"] = deserialize_zeftypes
deserialization_mapping["ErrorType"] = deserialize_zeftypes
deserialization_mapping["Image"] = deserialize_zeftypes
deserialization_mapping["Effect"] = deserialize_zeftypes
deserialization_mapping["FXElement"] = deserialize_zeftypes
deserialization_mapping["FlatGraph"] = deserialize_zeftypes
deserialization_mapping["FlatRef"] = deserialize_zeftypes
deserialization_mapping["FlatRefs"] = deserialize_zeftypes
deserialization_mapping["Delegate"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateEntity"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateAtomicEntity"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateRelationGroup"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateTX"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateRoot"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateRelationTriple"] = deserialize_delegate
