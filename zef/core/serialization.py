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

from ..pyzef import internals as pyinternals

##############################
# * Definition
#----------------------------

def get_serializable_types():
    return list(serialization_mapping.keys())

# This will get filled in soon
serialization_mapping = {}
deserialization_mapping = {}

def serialize(v):
    # from ..zefops import uid, tx, to_frame, first
    """
    Given a dictionary, list, and any Zeftype this function converts all of the ZefTypes into
    dictionaries that describe the ZefType and allows it to be serialized using JSON dumps.
    This function recursively call iself and other internal functions 
    to fully explore nested lists and dictionaries.
    """

    # Serializes a list and recursively calls itself if one of the list elements is of type List
    if is_python_scalar_type(v):
        return v
    elif isinstance(v, list):
        return serialize_list(v)
    elif isinstance(v, dict):
        return serialize_dict(v)
    elif type(v) in serialization_mapping:
         return serialization_mapping[type(v)](v)
    raise Exception(f"Don't know how to serialize type {type(v)}")

def deserialize(v):
    # from ..zefops import to_frame
    """
    Given a dictionary that includes serialized Zeftypes, Dicts, Lists this function converts all of the serialized ZefTypes 
    into their original ZefType form.
    This function recursively call iself and other internal functions 
    to fully deserialize nested lists and dictionaries.
    """
    if isinstance(v, dict) and "_zeftype" in v:
        v = deserialization_mapping[v["_zeftype"]](v)
    # elif isinstance(v, dict):
    #     v = deserialize_dict(v)
    elif isinstance(v, list):
        v = deserialize_list(v)

    return v


####################################
# * Implementations
#----------------------------------

def serialize_list(l: list) -> list:
    return [serialize(el) for el in l]

def serialize_dict(json_d: dict) -> dict:
    # return {k: serialize(v) for k,v in json_d.items()}
    return {
        "_zeftype": "dict",
        "items": [(serialize(k), serialize(v)) for k,v in json_d.items()]
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
        return {"_zeftype": bt_type, "value": str(z)}

    elif isinstance(z, Graph):
        return {"_zeftype": "Graph", "guid": str(uid(z))}

    elif isinstance(z, ZefEnumValue):
        return {"_zeftype": "Enum", "enum_type": z.enum_type, "enum_val": z.enum_value}

    elif isinstance(z, QuantityFloat) or isinstance(z, QuantityInt):
        q_type = {QuantityFloat: "QuantityFloat", QuantityInt: "QuantityInt"}[type(z)]
        return {"_zeftype": q_type, "value": z.value, "unit": serialize_zeftypes(z.unit)}

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
        type_or_types = [serialize_zeftypes(rae) for rae in z.d['type']] if abstract_type == "Relation" else serialize_zeftypes(z.d['type'])
        return {"_zeftype": abstract_type, "type": type_or_types, uid_or_uids: serialize_zeftypes(z.d[uid_or_uids])}

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
        op_rt = serialize_zeftypes(op_rt)

        serialized_subops = []
        if len(op_subops) > 0 and type(op_subops[0]) == RelationType and op_subops[0] == RT.L:
            serialized_subops.append(serialize_zefops("ZefOp", (op_subops,)))
        else:
            for sub_op in op_subops:
                if type(sub_op) in list(serialization_mapping.keys()):
                    sub_op = serialize_zeftypes(sub_op)
                serialized_subops.append(sub_op,)
        serialized_ops.append({"op": op_rt, "curried_ops": serialized_subops})

    return {"_zeftype": k_type, "el_ops": serialized_ops}


def deserialize_list(l: list) -> list:
    return [deserialize(el) for el in l]

def deserialize_dict(json_d):
    # return {k: deserialize(v) for k,v in json_d.items()}
    return {deserialize(k): deserialize(v) for k,v in json_d["items"]}

    
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
        return bt_class(z['value'])

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
        }
        first_part,*rest = z['value'].split('.')
        out = type_map[first_part]
        for part in rest:
            out = getattr(out, part)
        return out
            

    elif z['_zeftype'] == "Graph":
        return Graph(z['guid'])

    elif z['_zeftype'] == "Enum":
        return EN(z['enum_type'], z['enum_val'])

    elif z['_zeftype'] in {"QuantityFloat", "QuantityInt"}:
        quantity_type = {"QuantityFloat": QuantityFloat, "QuantityInt": QuantityInt}[z['_zeftype']]
        en = deserialize_zeftypes(z['unit'])
        return quantity_type(z['value'], en)

    elif z['_zeftype'] == "Time":
        return Time(z['value']) 

    elif z['_zeftype'] == "GraphDelta":
        return deserialize_list(z['value'])

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

        res.el_ops = deserialize_zeftypes(z['el_ops'])
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
        uid_or_uids_value = tuple(z[uid_or_uids]) if z['_zeftype'] == "Relation" else z[uid_or_uids]
        type_or_types = tuple([deserialize_zeftypes(rae) for rae in z['type']]) if z['_zeftype'] == "Relation" else deserialize_zeftypes(z['type'])
        return abstract_type({'type': type_or_types, uid_or_uids: deserialize_zeftypes(uid_or_uids_value)})

    elif z['_zeftype'] == "ErrorType":
        return Error.__getattribute__(z['type'])(*deserialize_list(z['args']))

    elif z['_zeftype'] == "Effect":
        return Effect(deserialize_dict(z['internal_dict']))

    elif z['_zeftype'] == "FXElement":
        return FXElement(tuple(z['elements']))
        
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
        op_rt = deserialize_zeftypes(op_rt)

        deserialized_subops = ()
        for sub_op in op_subops:
            sub_op = deserialize_zeftypes(sub_op)
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
serialization_mapping[pyinternals.DelegateEntity] = serialize_delegate
serialization_mapping[pyinternals.DelegateAtomicEntity] = serialize_delegate
serialization_mapping[pyinternals.DelegateRelationGroup] = serialize_delegate
serialization_mapping[pyinternals.DelegateTX] = serialize_delegate
serialization_mapping[pyinternals.DelegateRoot] = serialize_delegate
serialization_mapping[pyinternals.DelegateRelationTriple] = serialize_delegate


deserialization_mapping["dict"] = deserialize_dict
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
deserialization_mapping["GraphDelta"] = deserialize_zeftypes
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
deserialization_mapping["Delegate"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateEntity"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateAtomicEntity"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateRelationGroup"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateTX"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateRoot"] = deserialize_delegate
deserialization_mapping["pyinternals.DelegateRelationTriple"] = deserialize_delegate