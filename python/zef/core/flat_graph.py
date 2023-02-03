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
report_import("zef.core.flat_graph")

from operator import ne
from dataclasses import dataclass
from .VT import *
from .VT import make_VT


class FlatGraph_:
    """
    Internal data representation:
    1) self.blobs: a tuple of blobs
    2) self.key_dict: a dictionary mapping keys to indices in self.blobs

    Each element of self.blobs is a tuple of the form 
    (
        index: Int, 
        blob_type: e.g. ET.Foo / BT.VALUE_NODE / RT.Bar / AET.Int,
        edge_list: a list of blob indexes (integers). Positive for outgoing, negative for incoming
        origin_uid (optional)
    )
    """
    def __init__(self, *args):
        from ._ops import insert, collect
        if args == ():
            self.key_dict = {}
            self.blobs = ()
        elif len(args) == 1 and isinstance(args[0], list):
            new_fg = FlatGraph_()
            new_fg = new_fg | insert[args[0]] | collect
            self.key_dict = new_fg.key_dict
            self.blobs = new_fg.blobs
        elif len(args) == 1 and isinstance(args[0], FlatRef_):
            self.key_dict =  args[0].fg.key_dict
            self.blobs = args[0].fg.blobs
        else:
            raise NotImplementedError("FlatGraph with args")

    def __repr__(self):
        kdict = "\n".join([f"({k}=>{v})" for k,v in self.key_dict.items()])
        blobs = "\n".join([str(e) for e in self.blobs])
        return f'FlatGraph(\n{kdict}\n-------\n{blobs}\n)'
    
    def __or__(self, other):
        from .VT import LazyValue
        return LazyValue(self) | other

    def __ror__(self, other):
        from ._ops import insert
        return self | insert[other] 

    def __getitem__(self, key):
        from ._ops import get
        return get(self, key)

    def __contains__(self, key):
        if isinstance(key, Val):
            from ._ops import value_hash
            return value_hash(key.arg) in self.key_dict
        else:
            return key in self.key_dict

FlatGraph = make_VT("FlatGraph", pytype=FlatGraph_)


class FlatRef_:
    def __init__(self, fg, idx):
        self.fg = fg
        self.idx = idx
    def __repr__(self):
        return f'<FlatRef #{abs(self.idx)} {repr(self.fg.blobs[self.idx][1])}>'
    
    def __or__(self, other):
        from .VT import LazyValue
        return LazyValue(self) | other

    def __gt__(self, other):
        from .VT import LazyValue
        return LazyValue(self) > other

    def __lt__(self, other):
        from .VT import LazyValue
        return LazyValue(self) < other

    def __lshift__(self, other):
        from .VT import LazyValue
        return LazyValue(self) << other
    
    def __rshift__(self, other):
        from .VT import LazyValue
        return LazyValue(self) >> other

FlatRef = make_VT("FlatRef", pytype=FlatRef_)

def FlatRef_rae_type(fr):
    return fr.fg.blobs[fr.idx][1]
def FlatRef_value(fr):
    if FlatRef_rae_type(fr) != "BT.ValueNode":
        raise Exception("Not a value node")
    return fr.fg.blobs[fr.idx][1]

class FlatRefs_:
    def __init__(self, fg, idxs):
        self.fg = fg
        self.idxs = idxs
    def __repr__(self):
        newline = '\n'
        return f"""<FlatRefs len={len(self.idxs)}> [
{["    " + repr(FlatRef_(self.fg, i)) for i in self.idxs] | join[newline] | collect}
]"""
    
    def __or__(self, other):
        from .VT import LazyValue
        return LazyValue(self) | other

    def __iter__(self):
        return (FlatRef_(self.fg, i) for i in self.idxs)

    def __len__(self):
        return len(self.idxs)

    def __gt__(self, other):
        from .VT import LazyValue
        return LazyValue(self) > other

    def __lt__(self, other):
        from .VT import LazyValue
        return LazyValue(self) < other

    def __lshift__(self, other):
        from .VT import LazyValue
        return LazyValue(self) << other
    
    def __rshift__(self, other):
        from .VT import LazyValue
        return LazyValue(self) >> other

FlatRefs = make_VT("FlatRefs", pytype=FlatRefs_)



# Danny added to avoid implementation specific coding
def FlatRef_rae_type(fr):
    return fr.fg.blobs[fr.idx][1]

def FlatRef_maybe_uid(fr):
    return fr.fg.blobs[fr.idx][-1]






fg_registry = {}
def register_flatgraph(fg):
    from .VT.value_type import hash_frozen
    h = hash_frozen(fg)
    fg_registry[h] = fg
    return h

def lookup_flatgraph(h):
    return fg_registry.get(h, None)


from .VT import make_VT, insert_VT

def RelationFlatRef_is_a(x, typ):
    from ._ops import rae_type, is_a
    return isinstance(x, FlatRef & Is[rae_type | is_a[RT]])
RelationFlatRef = make_VT("RelationFlatRef", is_a_func=RelationFlatRef_is_a)
def EntityFlatRef_is_a(x, typ):
    from ._ops import rae_type, is_a
    return isinstance(x, FlatRef & Is[rae_type | is_a[ET]])
EntityFlatRef = make_VT("EntityFlatRef", is_a_func=EntityFlatRef_is_a)
def AttributeEntityFlatRef_is_a(x, typ):
    from ._ops import rae_type, is_a
    return isinstance(x, FlatRef & Is[rae_type | is_a[AET]])
AttributeEntityFlatRef = make_VT("AttributeEntityFlatRef", is_a_func=AttributeEntityFlatRef_is_a)
def TXNodeFlatRef_is_a(x, typ):
    from ._ops import abstract_type, equals
    return isinstance(x, FlatRef & Is[abstract_type | equals[BT.TX_EVENT_NODE]])
TXNodeFlatRef = make_VT("TXNodeFlatRef", is_a_func=TXNodeFlatRef_is_a)
def RootFlatRef_is_a(x, typ):
    from ._ops import abstract_type, equals
    return isinstance(x, FlatRef & Is[abstract_type | equals[BT.ROOT_NODE]])
RootFlatRef = make_VT("RootFlatRef", is_a_func=RootFlatRef_is_a)



RAEFlatRef = insert_VT('RAEFlatRef', EntityFlatRef | AttributeEntityFlatRef | RelationFlatRef)
