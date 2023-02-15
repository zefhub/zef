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

import builtins
from typing import Generator, Iterable, Iterator


from rx import operators as rxops
from ..VT.value_type import ValueType_

# This is the only submodule that is allowed to do this. It can assume that everything else has been made available so that it functions as a "user" of the core module.
from .. import *
from .._ops import *
from ..logger import log
from ...pyzef import zefops as pyzefops, main as pymain
from .. import internals
from typing import Generator, Iterable, Iterator
from ..atom import Atom_


#-----------------------------FlatGraph Implementations-----------------------------------
def fg_insert_imp(fg, new_el):
    from ..graph_additions.types import PrimitiveValue, PleaseAssign
    from ..graph_additions.common import map_scalar_to_aet
    from ...pyzef.internals import DelegateRelationTriple

    def without_names(raet):
        if isinstance(raet, ValueType):
            from ...core.VT.helpers import remove_names, absorbed
            abs = remove_names(absorbed(raet))
            return raet._replace(absorbed=abs)
        else:
            return raet

    def internal_name(rae):
        if isinstance(rae, RAERef):
            names = absorbed(rae)
        elif isinstance(rae, ValueType):
            from ...core.VT.helpers import names_of
            names = names_of(rae)
        elif isinstance(rae, DelegateRef):
            names = absorbed(rae)
        else:
            raise Exception(f"Need to implement code for type {rae}")
        return names[0] if names else None

    assert is_a(fg, FlatGraph)
    new_fg = FlatGraph()
    new_blobs, new_key_dict = [*fg.blobs], {**fg.key_dict}
    def idx_generator(n):
        def next_idx():
            nonlocal n
            n = n + 1
            return n
        return next_idx

    next_idx = idx_generator(length(fg.blobs) - 1)

    def inner_zefop_type(zefop, rt):
        return peel(zefop)[0][0] == rt

    def construct_abstract_rae_and_return_idx(rae_type, rae_uid):
        if isinstance(rae_type, RT):
            assert rae_uid in new_key_dict, "Can't construct an Abstract Relation!"
            return new_key_dict[rae_uid]
        else:
            rae_class = AttributeEntityRef if isinstance(rae_type, AET) else EntityRef
            return common_logic(rae_class({"type": rae_type, "uid": rae_uid}))

    def common_logic(new_el):
        nonlocal new_blobs, new_key_dict
        if is_a(new_el, PrimitiveValue):
            aet = map_scalar_to_aet(new_el)
            idx = next_idx()
            new_blobs.append((idx, aet, [], None, new_el))

        elif isinstance(new_el, AtomClass):
            from ..atom import _get_atom_type, get_most_authorative_id, _get_ref_pointer
            if is_a(new_el, Relation):
                # TODO Make this part work with unwrapping an Atom pointing to an RT
                # examples:
                # z = db | now | all[RT.Game] | last | collect
                # FlatGraph([Atom(RelationRef(z))])  
                # FlatGraph([Atom(z)])  
                
                # if get_ref_pointer(new_el):
                    # idx = _insert_single(RelationRef(get_ref_pointer(new_el)))
                # else:
                
                # raise NotImplementedError("An Atom of a Relation Type is not yet supported.")
                # Copying from the code below with RelationRef
                rt = rae_type(new_el)
                rt_uid = origin_uid(new_el)
                src = source(new_el)
                trgt = target(new_el)
                src_uid = origin_uid(src)
                trgt_uid = origin_uid(trgt)

                if isinstance(src, Relation) and src_uid not in new_key_dict: raise ValueError("Source of an abstract Relation can't be a Relation that wasn't inserted before!")
                if isinstance(trgt, Relation) and trgt_uid not in new_key_dict: raise ValueError("Target of an abstract Relation can't be a Relation that wasn't inserted before!")
                src_idx = construct_abstract_rae_and_return_idx(rae_type(src), src_uid)
                trgt_idx = construct_abstract_rae_and_return_idx(rae_type(trgt), trgt_uid)
                idx = next_idx()
                new_blobs.append((idx, rt, [], rt_uid, src_idx, trgt_idx))
                new_key_dict[rt_uid] = idx
                if idx not in new_blobs[src_idx][2]: new_blobs[src_idx][2].append(idx)
                if idx not in new_blobs[trgt_idx][2]: new_blobs[trgt_idx][2].append(-idx)
            # elif _get_ref_pointer(new_el):
            #     idx = common_logic(_get_ref_pointer(new_el))
            else:
                node_type, node_uid = rae_type(new_el), get_most_authorative_id(new_el)
                if node_uid not in new_key_dict:
                    idx = next_idx()
                    new_blobs.append((idx, node_type, [], node_uid))
                    new_key_dict[node_uid] = idx
                idx = new_key_dict[node_uid]

        elif is_a(new_el, BlobPtr):
            idx = common_logic(discard_frame(new_el))
            if isinstance(new_blobs[idx][1], AET) and isinstance(new_el, ZefRef):
                new_blobs[idx] = (*new_blobs[idx][:4], value(new_el))

        elif is_a(new_el, Delegate):
            if isinstance(new_el.item, DelegateRelationTriple):
                if new_el in new_key_dict:
                    idx = new_key_dict[new_el]
                else:
                    src, rt, trgt = source(new_el), new_el.item.rt, target(new_el)
                    src_idx = common_logic(src)
                    trgt_idx = common_logic(trgt)
                    idx = next_idx()

                    new_blobs.append((idx, new_el, [], None, src_idx, trgt_idx))
                    if idx not in new_blobs[src_idx][2]: new_blobs[src_idx][2].append(idx)
                    if idx not in new_blobs[trgt_idx][2]: new_blobs[trgt_idx][2].append(-idx)
                    new_key_dict[new_el] = idx
            else:
                internal_id = internal_name(new_el)
                new_el = without_names(new_el)
                if new_el in new_key_dict:
                    idx = new_key_dict[new_el]
                else:
                    idx = next_idx()
                    if internal_id: new_key_dict[internal_id] = idx
                    new_key_dict[new_el] = idx
                    new_blobs.append((idx, new_el, [], None))

        elif is_a(new_el, EntityRef):
            node_type, node_uid = new_el.d['type'], new_el.d['uid']
            if node_uid not in new_key_dict:
                idx = next_idx()
                new_blobs.append((idx, node_type, [], node_uid))
                new_key_dict[node_uid] = idx
            idx = new_key_dict[node_uid]

        elif is_a(new_el, AttributeEntityRef):
            node_type, node_uid = new_el.d['type'], new_el.d['uid']
            if node_uid not in new_key_dict:
                idx = next_idx()
                new_blobs.append((idx, node_type, [], node_uid, None))
                new_key_dict[node_uid] = idx
            idx = new_key_dict[node_uid]

        elif is_a(new_el, ValueType & ET):
            idx = next_idx()
            internal_id = internal_name(new_el)
            new_el = without_names(new_el)
            if internal_id: new_key_dict[internal_id] = idx
            new_blobs.append((idx, new_el, [], None))

        elif is_a(new_el, ValueType & AET):
            idx = next_idx()
            internal_id = internal_name(new_el)
            new_el = without_names(new_el)
            if internal_id: new_key_dict[internal_id] = idx
            new_blobs.append((idx, new_el, [], None, None))

        elif is_a(new_el, ZefOp) and inner_zefop_type(new_el, RT.Instantiated):
            raise ValueError("!!!!SHOULD NO LONGER ARRIVE HERE!!!!")
        
        elif is_a(new_el, ZefOp[terminate]):
            raise ValueError("!!!!SHOULD NO LONGER ARRIVE HERE!!!!")
            to_be_removed = LazyValue(new_el) | absorbed | attempt[first][None] | collect
            idx = None
            if to_be_removed:
                try:
                    new_fg = FlatGraph()
                    new_fg.key_dict = new_key_dict
                    new_fg.blobs = (*new_blobs,)
                    new_fg = fg_remove_imp(new_fg, to_be_removed)
                    new_blobs, new_key_dict = [*new_fg.blobs], {**new_fg.key_dict}
                except KeyError:
                    pass
                except Exception:
                    raise Exception(f"An exception happened while trying to perform {new_el} on {fg}")

        # TODO remove this once Z is fully deprecated
        elif is_a(new_el, ZefOp[Z]):
            key = peel(new_el)| first | second | first | collect
            if key not in new_key_dict and not isinstance(key, Int): raise KeyError(f"{key} doesn't exist in internally known ids!")
            idx = new_key_dict.get(key, key)

        elif is_a(new_el, NamedAny):
            key = absorbed(new_el) | first | collect
            if key not in new_key_dict and not isinstance(key, Int): raise KeyError(f"{key} doesn't exist in internally known ids!")
            idx = new_key_dict.get(key, key)

        # i.e: z4 | assign[42] ; AET.String | assign[42] ; AET.String['z1'] | assign[42] ; Any['n1'] | assign[42]
        elif is_a(new_el, PleaseAssign):
            new_el = collect(new_el)
            first_op = new_el.target
            aet_value = new_el.value.arg

            # if isinstance(first_op, ZefRef) or isinstance(first_op, EZefRef) or is_a(first_op, AttributeEntityRef):
            if isinstance(first_op, AttributeEntity):
                idx = common_logic(first_op)
                assert isinstance(new_blobs[idx][1], AET), f"This key must refer to an AET found {new_blobs[idx][1]}"
                new_blobs[idx] = (*new_blobs[idx][:4], aet_value)

            elif is_a(first_op, ValueType & AET):
                    internal_id = internal_name(first_op)
                    aet_maybe = without_names(first_op)
                    assert isinstance(aet_maybe, AET), f"{new_el} should be of type AET"
                    idx = next_idx()
                    new_blobs.append((idx, aet_maybe, [], None, aet_value))
                    if internal_id: new_key_dict[internal_id] = idx
            
            elif is_a(first_op, NamedAny):
                key = absorbed(first_op) | first | collect
                if key not in new_key_dict and not isinstance(key, Int): raise KeyError(f"{key} doesn't exist in internally known ids!")
                idx = new_key_dict.get(key, key)
                assert isinstance(new_blobs[idx][1], AET), f"This key must refer to an AET found {new_blobs[idx][1]}"
                new_blobs[idx] = (*new_blobs[idx][:4], aet_value)
                
            # TODO remove this once Z is fully deprecated
            elif isinstance(first_op, ZefOp):
                if inner_zefop_type(first_op, RT.Z):
                    key = peel(first_op)[0][1][0]
                    if key not in new_key_dict and not isinstance(key, Int): raise KeyError(f"{key} doesn't exist in internally known ids!")
                    idx = new_key_dict.get(key, key)
                    assert isinstance(new_blobs[idx][1], AET), f"This key must refer to an AET found {new_blobs[idx][1]}"
                    new_blobs[idx] = (*new_blobs[idx][:4], aet_value)
                else:
                    raise ValueError(f"Expected a Z['n1'] <= 42 got {new_el} instead!")
            else:
                raise ValueError(f"Expected a Any['x'] | assign[42] or AET.String | assign[42] got {new_el} instead!")

        elif isinstance(new_el, Val):
            new_el = new_el.arg
            hash_vn = value_hash(new_el)
            if hash_vn not in new_key_dict:
                idx = next_idx()
                new_key_dict[hash_vn] = idx
                new_blobs.append((idx, BT.VALUE_NODE, [], new_el))  # TODO Don't treat as str once added to Zef types
            idx = new_key_dict[hash_vn]

        elif isinstance(new_el, FlatRef):
            if new_el.fg == fg:
                idx = new_el.idx
            else:
                # If the flatgraphs are different then merge the FlatGraph in and return the
                # new index of the blob originally in the other FlatGraph
                idx = fr_merge_and_retrieve_idx(new_blobs, new_key_dict,next_idx, new_el)
        else:
            idx = None
        return idx
    
    def _insert_dict(new_el):
        try: 
            ent, sub_d = list(new_el.items()) | single | collect
            ent_idx = common_logic(ent)
            for k,v in sub_d.items():
                if isinstance(v, Dict):
                    target_idx =  _insert_dict(v)
                    _insert_single((Any[ent_idx], k, Any[target_idx]))
                else:
                    _insert_single((Any[ent_idx], k, v))
            return ent_idx
        except KeyError as e:
            raise ValueError(f"{e} \nMake sure that the dictionary {new_el} doesn't reference any internal elements that weren't inserted before i.e z[p1].\nIf you are using list syntax even if the dict comes later in the statements it will be inserted first.")
   

    def _insert_single(new_el):
        if is_a(new_el, EntityRef | AttributeEntityRef | ZefOp | PleaseAssign | BlobPtr | Val | Delegate | AtomClass | PrimitiveValue | (ValueType & ET | AET)):
            common_logic(new_el)
        elif is_a(new_el, tuple) and len(new_el) == 3:
            src, rt, trgt = new_el
            src_idx = common_logic(src)
            trgt_idx = common_logic(trgt)
            assert isinstance(src_idx, Int) and isinstance(trgt_idx, Int), "Couldn't find/create src or target!"
            assert issubclass(rt, RT) or isinstance(rt, ZefOp), "Tuples must have Relation as second item."
            idx = next_idx()

            # Case of RT.A['a']
            if isinstance(rt, RT):
                internal_id = internal_name(rt)
                rt = without_names(rt)
                if internal_id: new_key_dict[internal_id] = idx

            # Case of Any['a']
            elif is_a(rt, NamedAny): 
                raise ValueError(f"Cannot reference an internal element to be used as a Relation. {rt}")

            new_blobs.append((idx, rt, [], None, src_idx, trgt_idx))
            if idx not in new_blobs[src_idx][2]: new_blobs[src_idx][2].append(idx)
            if idx not in new_blobs[trgt_idx][2]: new_blobs[trgt_idx][2].append(-idx)
        elif is_a(new_el, RelationRef):
            rt = new_el.d['type']
            rt_uid = new_el.d["uid"]
            src = new_el.d["source"]
            trgt = new_el.d["target"]
            if not isinstance(src, RAERef) or not isinstance(trgt, RAERef):
                raise Exception(f"Source and target must themselves be RAERefs (got {src} and {trgt} from {new_el})")
            src_uid = origin_uid(src)
            trgt_uid = origin_uid(trgt)

            if isinstance(src, Relation) and src_uid not in new_key_dict: raise ValueError("Source of an abstract Relation can't be a Relation that wasn't inserted before!")
            if isinstance(trgt, Relation) and trgt_uid not in new_key_dict: raise ValueError("Target of an abstract Relation can't be a Relation that wasn't inserted before!")
            src_idx = construct_abstract_rae_and_return_idx(rae_type(src), src_uid)
            trgt_idx = construct_abstract_rae_and_return_idx(rae_type(trgt), trgt_uid)
            idx = next_idx()
            new_blobs.append((idx, rt, [], rt_uid, src_idx, trgt_idx))
            new_key_dict[rt_uid] = idx
            if idx not in new_blobs[src_idx][2]: new_blobs[src_idx][2].append(idx)
            if idx not in new_blobs[trgt_idx][2]: new_blobs[trgt_idx][2].append(-idx)
        elif is_a(new_el, Dict): 
                _insert_dict(new_el)
        else: 
            raise NotImplementedError(f"Insert not implemented for {type(new_el)}.\nnew_el={new_el}")
        
    if is_a(new_el, list): 
        def sorting_key(el):
            if is_a(el, Dict): return 1
            elif is_a(el, RelationRef): return 2
            elif is_a(el, tuple) and len(el) == 3: 
                is_z = lambda el: is_a(el, ZefOp) and inner_zefop_type(el, RT.Z)
                has_internal_id = lambda rt: is_a(rt, RT) and (internal_name(rt)) != None
                return 3 + sum([is_z(el) for el in el]) - has_internal_id(el[1])
            return 0
        new_el.sort(key=sorting_key)
        [_insert_single(el) for el in new_el]
    elif is_a(new_el, Dict):
        _insert_dict(new_el)
    else: 
        _insert_single(new_el)
        
    new_fg.key_dict = new_key_dict
    new_fg.blobs = (*new_blobs,)
    return new_fg

def fr_merge_and_retrieve_idx(blobs, k_dict, next_idx, fr):
    fr_idx = fr.idx
    fg2 = fr.fg

    idx_key_2 = {i:k for k,i in fg2.key_dict.items()}
    old_to_new = {}

    def retrieve_or_insert_blob(new_b):
        old_idx = new_b[0]
        key = idx_key_2.get(new_b[0], None)

        if old_idx in old_to_new:
            idx = old_to_new[old_idx]
            new_b = blobs[idx]
        elif (new_b[1] == BT.VALUE_NODE or is_a(new_b[3], UID)) and key in k_dict:
            new_b = blobs[k_dict[key]]
            idx = new_b[0]
        else:
            idx = next_idx()
            new_b = (idx, new_b[1], [], *new_b[3:])
            blobs.append(new_b)
        old_to_new[old_idx] = idx
        return new_b

    for b in fg2.blobs | filter[lambda b: isinstance(b[1], RT)] | sort[lambda b: -len(b[2])]:
        src_b, trgt_b = fg2.blobs[b[4]], fg2.blobs[b[5]]
        src_b  = retrieve_or_insert_blob(src_b)
        trgt_b = retrieve_or_insert_blob(trgt_b)
        
        idx = next_idx()
        rt_b = (idx, b[1], [], None, src_b[0], trgt_b[0])
        src_b[2].append(idx)
        trgt_b[2].append(-idx)
        blobs.append(rt_b)
        old_to_new[b[0]] = idx

            
    for b in fg2.blobs | filter[lambda b: not isinstance(b[1], RT)]:
        if b[0] not in old_to_new: retrieve_or_insert_blob(b)

    # If there is no overlab with the main k_dict, we insert
    # the old key with the new index
    for k,v in fg2.key_dict.items():
        if k not in k_dict: k_dict[k] = old_to_new[v]

    return old_to_new[fr_idx]

def fg_get_imp(fg, key):
    kdict = fg.key_dict
    if type(key) in {ZefRef, EZefRef} and origin_uid(key) in kdict: return FlatRef(fg, kdict[origin_uid(key)])
    elif type(key) in {EntityRef, AttributeEntityRef} and key.d['uid'] in kdict: return FlatRef(fg, kdict[key.d['uid']])
    elif isinstance(key, RelationRef) and key.d['uids'][1] in kdict:return FlatRef(fg, kdict[key.d['uids'][1]])
    elif isinstance(key, Val) and  value_hash(key.arg) in kdict: return FlatRef(fg, kdict[value_hash(key.arg)])
    elif key in kdict: return FlatRef(fg, kdict[key])
    else: raise KeyError(f"{key} isn't found in this FlatGraph!")


def fg_remove_imp(fg, key):
    error = KeyError(f"{key} isn't found in this FlatGraph!")
    kdict = fg.key_dict
    if type(key) in {ZefRef, EZefRef}:  
        if origin_uid(key) not in kdict: raise error
        idx = kdict[origin_uid(key)]
        key = origin_uid(key)
    elif type(key) in {EntityRef, AttributeEntityRef}: 
        if key.d['uid'] not in kdict: raise error
        idx = kdict[key.d['uid']]
        key = key.d['uid']
    elif isinstance(key, RelationRef):
        if key.d['uids'][1] not in kdict: raise error
        idx = kdict[key.d['uids'][1]]
        key = key.d['uids'][1]
    elif isinstance(key, Val):
        if value_hash(key.arg) not in kdict: raise error
        idx = kdict[value_hash(key.arg)]
        key = value_hash(key.arg)
    elif key in kdict:  
        idx = kdict[key]
    else: raise error

    idx_key = {idx:key for key,idx in kdict.items()}
    kdict   = {**fg.key_dict}
    blobs   = [*fg.blobs]

    def remove_blob(idx, key = None):
        blob  = blobs[idx]
        if blob:
            blob_type = blob[1]
            ins_outs  = blob[2]
            blobs[idx] = None
            if not key: 
                key = idx_key.get(idx, None)
                if key : del(kdict[key])
            else: del(kdict[key])
            if issubclass(blob_type, RT):
                src_idx, trgt_idx = blob[4:]
                if blobs[src_idx] and idx in blobs[src_idx][2]: blobs[src_idx][2].remove(idx)
                if blobs[trgt_idx] and -idx in blobs[trgt_idx][2]: blobs[trgt_idx][2].remove(-idx)
            ins_outs | map[abs] | for_each[remove_blob]
    remove_blob(idx, key)

    new_fg = FlatGraph()
    new_fg.blobs = blobs
    new_fg.key_dict = kdict
    return new_fg


def flatgraph_to_commands(fg):
    return_elements = set()
    idx_key = {idx:key for key,idx in fg.key_dict.items()}
    def dispatch_on_blob(b, for_rt=False):
        idx = b[0]
        if isinstance(b[1], ET):
            if idx in idx_key:
                key = idx_key[idx]
                if is_a(key, UID): return EntityRef({"type": b[1], "uid": key})
                else:
                    if for_rt: return Any[key]
                    return b[1][key]
            else:
                if b[1][idx] in return_elements: return Any[idx]
                return  b[1][idx] 
        elif isinstance(b[1], AET):
            if idx in idx_key:
                key = idx_key[idx]
                if is_a(key, UID):
                    if b[-1] != None: return AttributeEntityRef({"type": b[1], "uid": key})| assign[b[-1]]
                    else:     return AttributeEntityRef({"type": b[1], "uid": key})
                else:
                    if for_rt: return Any[key]
                    if b[-1] != None: return b[1][key] | assign[b[-1]]
                    else:     return b[1][key]
            else:
                if b[-1] != None: 
                    if (b[1][idx] | assign[b[-1]]) in return_elements: return Any[idx] | assign[b[-1]]
                    return b[1][idx] | assign[b[-1]]
                else:     
                    if (b[1][idx]) in return_elements: return Any[idx]
                    return b[1][idx]
        elif isinstance(b[1], RT):
            if idx in idx_key: 
                key = idx_key[idx]
                if for_rt: return Any[key]
                if is_a(key, UID):
                    return RelationRef({"type": b[1], "uid": key, "source": dispatch_on_blob(fg.blobs[b[4]]), "target": dispatch_on_blob(fg.blobs[b[5]]), "absorbed": ()})
            if for_rt: return Any[idx]
            src_blb  = dispatch_on_blob(fg.blobs[b[4]], True)
            trgt_blb = dispatch_on_blob(fg.blobs[b[5]], True)
            if b[0] in idx_key: base = b[1][idx_key[b[0]]]
            else: base = b[1][idx]
            return (src_blb, base, trgt_blb)

        elif b[1] == BT.VALUE_NODE:
            if for_rt:
                return Any[value_hash(b[-1])]
            else:
                from ..graph_additions.types import PrimitiveValue
                from ..graph_additions.types import map_scalar_to_aet
                if isinstance(b[-1], PrimitiveValue):
                    aet = map_scalar_to_aet(b[-1])
                    return aet[value_hash(b[-1])] | assign[b[-1]] 
                else:
                    return AET.Serialized[value_hash(b[-1])]| assign[to_json(b[-1])]
        elif isinstance(b[1], Delegate):
            return b[1]

        raise NotImplementedError(f"Unhandled type in dispatching {b}")

    for b in fg.blobs | filter[lambda b: b != None] | collect:
        el = dispatch_on_blob(b)
        if isinstance(el, LazyValue):
            el = collect(el)
        if el != None: return_elements.add(el)

    from ..graph_delta import construct_commands
    res =  construct_commands(list(return_elements))
    return res


# ------------------------------FlatRef----------------------------------
def fr_source_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    assert isinstance(blob[1], RT), f"| source is only allowed for FlatRef of type RelationType, but {type(blob[1])} was passed!"
    return FlatRef(fr.fg, blob[4])

def fr_target_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    assert isinstance(blob[1], RT), f"| target is only allowed for FlatRef of type RelationType, but {type(blob[1])} was passed!"
    return FlatRef(fr.fg, blob[5])

def fr_value_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    assert isinstance(blob[1], AET | SetOf(BT.VALUE_NODE)), "Can only ask for the value of an AET"
    return blob[-1]

def traverse_flatref_imp(fr, rt, direction, traverse_type):
    translation_dict = {
        "outout": "Out",
        "out"  : "out_rel",
        "inin": "In",
        "in": "in_rel",
    }
    assert isinstance(rt, RT), f"Passed Argument to traverse should be of type RelationType but got {rt}"
    blob = fr.fg.blobs[fr.idx]
    specific = blob[2] | filter[greater_than[0] if direction in {"out", "outout"} else less_than[0]] | collect
    specific_blobs = specific | map[lambda idx: fr.fg.blobs[abs(idx)]] | filter[lambda b: b[1] == rt] | collect
    if traverse_type == "single" and len(specific_blobs) != 1: return Error.ValueError(f"There isn't exactly one {translation_dict[direction]} RT.{rt} Relation. Did you mean {translation_dict[direction]}s[RT.{rt}]?")
    
    if direction == "inin": idx = 4
    elif direction == "outout": idx = 5
    else: idx = 0 # itself the relation

    if traverse_type == "single": return FlatRef(fr.fg, specific_blobs[0][idx])
    return FlatRefs(fr.fg, specific_blobs | map[lambda b: b[idx]] | collect)

def fr_outs_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    outs = blob[2] | filter[greater_than[-1]] | collect
    return FlatRefs(fr.fg, outs)

def fr_ins_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    ins = blob[2] | filter[less_than[0]] | collect
    return FlatRefs(fr.fg, ins)

def fr_ins_and_outs_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    return FlatRefs(fr.fg, blob[2])

def fg_all_imp(fg, selector=None):
    assert is_a(fg, FlatGraph)
    if selector:
        selected_blobs = fg.blobs | filter[lambda b: is_a(b[1], selector)] | collect
    else:
        selected_blobs = fg.blobs 
    return FlatRefs(fg, [b[0] for b in selected_blobs])


# ------------------------------Merging FlatGraphs----------------------------------
def fg_merge_imp(fg1, fg2 = None):
    if isinstance(fg1, list): return fg1[1:] | reduce[fg_merge_imp][fg1[0]] | collect

    def idx_generator(n):
        def next_idx():
            nonlocal n
            n = n + 1
            return n
        return next_idx

    blobs, k_dict = [*fg1.blobs], {**fg1.key_dict}
    next_idx = idx_generator(length(blobs) - 1)

    idx_key_2 = {i:k for k,i in fg2.key_dict.items()}
    old_to_new = {}

    def retrieve_or_insert_blob(new_b):
        old_idx = new_b[0]
        key = idx_key_2.get(new_b[0], None)

        if old_idx in old_to_new:
            idx = old_to_new[old_idx]
            new_b = blobs[idx]
        elif (new_b[1] == BT.VALUE_NODE or is_a(new_b[3], UID)) and key in k_dict:
            new_b = blobs[k_dict[key]]
            idx = new_b[0]
        else:
            idx = next_idx()
            if key: k_dict[key] = idx
            new_b = (idx, new_b[1], [], *new_b[3:])
            blobs.append(new_b)
        old_to_new[old_idx] = idx
        return new_b

    for b in fg2.blobs | filter[lambda b: isinstance(b[1], RT)] | sort[lambda b: -len(b[2])]:
        rt_key = idx_key_2.get(b[0], None)

        src_b, trgt_b = fg2.blobs[b[4]], fg2.blobs[b[5]]
        src_b  = retrieve_or_insert_blob(src_b)
        trgt_b = retrieve_or_insert_blob(trgt_b)
        
        idx = next_idx()
        rt_b = (idx, b[1], [], None, src_b[0], trgt_b[0])
        src_b[2].append(idx)
        trgt_b[2].append(-idx)
        if rt_key: k_dict[rt_key] = idx
        blobs.append(rt_b)
        old_to_new[b[0]] = idx

            
    for b in fg2.blobs | filter[lambda b: not isinstance(b[1], RT)]:
        if b[0] not in old_to_new: retrieve_or_insert_blob(b)

    new_fg = FlatGraph()
    new_fg.blobs = blobs
    new_fg.key_dict = k_dict
    return new_fg



# ------------------------------FlatGraph GraphWish Syntax----------------------------------
def fg_remove_imp2(kdict, blobs, idx, key):
    kdict   = {**kdict}
    blobs   = [*blobs]
    idx_key = {idx:key for key,idx in kdict.items()}

    def remove_blob(idx, key = None):
        blob  = blobs[idx]
        if blob:
            blob_type = blob[1]
            ins_outs  = blob[2]
            blobs[idx] = None
            if not key: 
                key = idx_key.get(idx, None)
                if key : del(kdict[key])
            else: del(kdict[key])
            if issubclass(blob_type, RT):
                src_idx, trgt_idx = blob[4:]
                if blobs[src_idx] and idx in blobs[src_idx][2]: blobs[src_idx][2].remove(idx)
                if blobs[trgt_idx] and -idx in blobs[trgt_idx][2]: blobs[trgt_idx][2].remove(-idx)
            ins_outs | map[abs] | for_each[remove_blob]
    remove_blob(idx, key)

    return blobs, kdict
def without_names(raet):
    if isinstance(raet, ValueType):
        from ...core.VT.helpers import remove_names, absorbed
        abs = remove_names(absorbed(raet))
        return raet._replace(absorbed=abs)
    else:
        return raet

def internal_name(rae):
    if isinstance(rae, RAERef):
        names = absorbed(rae)
    elif isinstance(rae, ValueType):
        from zef.core.VT.helpers import names_of
        names = names_of(rae)
    elif isinstance(rae, DelegateRef):
        names = absorbed(rae)
    else:
        raise Exception(f"Need to implement code for type {rae}")
    return names[0] if names else None

def inner_zefop_type(zefop, rt):
    return peel(zefop)[0][0] == rt

def idx_generator(n):
    def next_idx():
        nonlocal n
        n = n + 1
        return n
    return next_idx


def fg_transaction_implementation(cmds, fg):
    from ..graph_additions.types import PleaseInstantiate, PleaseAssign, WishIDInternal, Val, FlatRefUID, PleaseTerminate
    from ..graph_additions.common import maybe_unwrap_variable

    def _add_internal_id(internal_ids, idx):
        # TODO: What to do with multiple internal_ids?
        internal_name = internal_ids[0] if internal_ids else None
        if internal_name is not None:
            if is_a(internal_name, (WishIDInternal, FlatRefUID)): 
                _wish_ids[internal_name] = idx
            else:
                internal_name = maybe_unwrap_variable(internal_name)
                new_key_dict[internal_name] = idx
    
    def _extract_idx_from_id(id):
        if is_a(id, (WishIDInternal, FlatRefUID)): 
            assert id in _wish_ids, "Internal wish id not found in _wish_ids"
            return _wish_ids[id]
        else:
            id = maybe_unwrap_variable(id)
            assert id in new_key_dict, "Internal id not found in new_key_dict"
            return new_key_dict[id]

    assert is_a(fg, FlatGraph)
    new_fg = FlatGraph()
    new_blobs = [*fg.blobs]
    new_key_dict =  {**fg.key_dict}
    _wish_ids = {}
    next_idx = idx_generator(length(fg.blobs) - 1)

    
    def _insert_cmd(cmd):
        nonlocal new_blobs, new_key_dict

        if isinstance(cmd, PleaseInstantiate):
            atom = cmd['atom']
            _origin_uid = cmd._value.get('origin_uid', None)
            internal_ids = cmd._value.get('internal_ids', None)

            if is_a(atom, Delegate):
                internal_id = internal_name(atom)
                atom = without_names(atom)
                if atom in new_key_dict:
                    idx = new_key_dict[atom]
                else:
                    idx = next_idx()
                    if internal_id: new_key_dict[internal_id] = idx
                    new_key_dict[atom] = idx
                    new_blobs.append((idx, atom, [], None))

            elif is_a(atom, ValueType & ET):
                
                if _origin_uid:

                    if _origin_uid not in new_key_dict:
                        idx = next_idx()
                        new_blobs.append((idx, atom, [], _origin_uid))
                        new_key_dict[_origin_uid] = idx
                    else:
                        idx = new_key_dict[_origin_uid]

                else:
                    idx = next_idx()
                    new_blobs.append((idx, atom, [], None))
                    _add_internal_id(internal_ids, idx)
            
            elif is_a(atom, ValueType & AET):

                if _origin_uid:

                    if _origin_uid not in new_key_dict:
                        idx = next_idx()
                        new_blobs.append((idx, atom, [], _origin_uid, None))
                        new_key_dict[_origin_uid] = idx
                    else:
                        idx = new_key_dict[_origin_uid]

                else:
                    idx = next_idx()
                    new_blobs.append((idx, atom, [], None, None))
                    _add_internal_id(internal_ids, idx)
            
            elif is_a(atom, dict) and atom.get("rt", None):
                rt = atom["rt"]
                
                src_idx = _extract_idx_from_id(atom['source'])
                trgt_idx = _extract_idx_from_id(atom['target'])
                idx = next_idx()

                new_blobs.append((idx, rt, [], None, src_idx, trgt_idx))
                if idx not in new_blobs[src_idx][2]: new_blobs[src_idx][2].append(idx)
                if idx not in new_blobs[trgt_idx][2]: new_blobs[trgt_idx][2].append(-idx)
                _add_internal_id(internal_ids, idx)

            elif is_a(atom, Val):
                atom = atom.arg
                hash_vn = value_hash(atom)
                if hash_vn not in new_key_dict:
                    idx = next_idx()
                    new_key_dict[hash_vn] = idx
                    new_blobs.append((idx, BT.VALUE_NODE, [], atom))  
                idx = new_key_dict[hash_vn]

            else:
                raise NotImplementedError(f"Can't handle PleaseInstaniate for {atom}")

        elif isinstance(cmd, PleaseAssign):
            target_internal_id = cmd['target']
            _value = cmd['value']

            idx =  _extract_idx_from_id(target_internal_id)
            new_blobs[idx] = (*new_blobs[idx][:4], _value.arg) 

        elif isinstance(cmd, PleaseTerminate):
            key = cmd['target']
            idx =  _extract_idx_from_id(key)
            new_blobs, new_key_dict = fg_remove_imp2(new_key_dict, new_blobs, idx, key)

        else:
            raise NotImplementedError(f"Can't handle {cmd} for FlatGraph") 
        
    for cmd in cmds:  _insert_cmd(cmd)
        
    new_fg.key_dict = new_key_dict
    new_fg.blobs = (*new_blobs,)

    return new_fg, {}