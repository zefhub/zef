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

from .VT import *
from .VT import insert_VT, make_VT

def Atom_is_global_identifier(input):
    if type(input) == str and input.startswith("㏈-"):
        return True
    if isinstance(input, EternalUID):
        return True
    # TODO: This will go away at some point
    if isinstance(input, ZefRefUID):
        return True
    if isinstance(input, DBStateRefUID):
        return True
    if isinstance(input, FlatRefUID):
        return True
    if isinstance(input, DelegateRef):
        return True
    return False


# This is like a GraphSlice but without an explicit ZefRef tx.
DBStateUID = UserValueType("DBStateUID",
                           Dict,
                           Pattern[{"tx_uid": BaseUID,
                                    "graph_uid": BaseUID}],
                           forced_uid="5f58a60ab5ae121b")

def make_dbstate_uid(gs):
    from ._ops import uid
    ctx = gs.tx
    tx_uid = uid(ctx).blob_uid
    graph_uid = uid(ctx).graph_uid
    return DBStateUID(tx_uid=tx_uid, graph_uid=graph_uid)

def find_dbstate(dbstate_uid: DBStateUID):
    # Tries to find the DBState for the given UID but returns None if we can't
    # without causing a graph load.
    gref = GraphRef(dbstate_uid.graph_uid)
    from .internals import get_loaded_graph
    g = get_loaded_graph(gref)
    if g is None:
        return None
    gs = GraphSlice(g[dbstate_uid.tx_uid])
    return gs

    
# A temporary internal structure to represent a global id with a reference frame
DBStateRefUID = UserValueType("DBStateRefUID",
                           Dict,
                           Pattern[{"global_uid": EternalUID | DelegateRef,
                                    "dbstate_uid": DBStateUID}],
                           forced_uid="e2481c246411fd50")

def make_dbstateref_uid(z):
    if isinstance(z, Atom_):
        z = get_ref_pointer(z)
    if not isinstance(z, ZefRef):
        raise Exception(f"Can't get DBStateRefUID from a {z}")

    from ._ops import origin_uid, frame, get_field, collect, uid

    global_uid = origin_uid(z)
    # Have to avoid calling to_tx as that would recurse into this function.
    dbstate_uid = make_dbstate_uid(z | frame | collect)

    return DBStateRefUID(
        global_uid=global_uid,
        dbstate_uid=dbstate_uid,
    )

FlatGraphPlaceholder = UserValueType("FlatGraphPlaceholder", Int, Any)
FlatRefUID = UserValueType("FlatRefUID",
                           Dict,
                           Pattern[{"idx": Int,
                                    "flatgraph": FlatGraphPlaceholder}])

def make_flatref_uid(fr):
    from .flat_graph import register_flatgraph
    h = register_flatgraph(fr.fg)
    placeholder = FlatGraphPlaceholder(h)
    return FlatRefUID(idx=fr.idx, flatgraph=placeholder)

# UIDString = String & Where[startswith["㏈-"]]
UIDString = String & Is[lambda x: x.startswith["㏈-"]]
AtomIdentity = (
    Pattern[{
        Optional["global_uid"]: EternalUID,
        Optional["frame_uid"]: DBStateUID,
        Optional["local_names"]: List[~UIDString],
        Optional["flatref_idx"]: Int,
    }]
    # & Cond[Contains["frame_uid"]][Contains["global_uid"]]
    & Cond[Is[lambda x: "frame_uid" in x]]
          [Is[lambda x: "global_uid" in x] | Is[lambda x: "flatref_idx" in x]]
)

# Yes, the last bit is just List[Any] but it makes more sense for me to write
# down the various parts.
PrettyAtomIdentity = AtomIdentity | List[UIDString | EternalUID | ~UIDString]



    

def interpret_atom_identity(inputs) -> AtomIdentity:
    # Danny has been writing this from a combination of memory and making up
    # things where needed. Here is a list of points that need addressing.
    # TODO:
    # - what should "global identifier" be called?
    # - EternalUID should be replaced with new global identifier binary object
    # - tx_uid and graph_uid should be replaced with their appropriate sizes.

    from ._ops import uid, filter, collect, single
    from .flat_graph import FlatGraph, FlatRef, FlatRef_maybe_uid

    # Given a tuple of inputs to an Atom, determine a standardised dictionary to represent this.

    # Only one item in the list is allowed to be an item with a global
    # identifier (for now?). So check this first.
    glob_identifiers = []
    others = set()
    for input in inputs:
        if Atom_is_global_identifier(input):
            glob_identifiers += [input]
        else:
            others.add(input)
    if len(glob_identifiers) >= 2:
        raise Exception("Can't interpret atom identity, too many global identifiers")
        
    desc = {}
    
    if len(glob_identifiers) == 1:
        input = glob_identifiers[0]
        if type(input) == str and input.startswith("㏈-"):
            parts = input[len("㏈-"):].split("-")

            def parse_global_uid(x):
                out = uid(x)
                assert isinstance(out, EternalUID)
                return out
            
            # Option 1: just a global identifier "㏈-49836587346876342856236478"
            if len(parts) == 1:
                # TODO: this type of ID will change
                desc = dict(global_uid=parse_global_uid(parts[0]))
            # Option 2: "㏈-49836587346876342856236478-xxx-yyy"
            elif len(parts) == 3:
                dbstate_uid = DBStateUID(
                    tx_uid=BaseUID(parts[1]),
                    graph_uid=BaseUID(parts[2])
                )
                desc = dict(
                    global_uid=parse_global_uid(parts[0]),
                    frame_uid=dbstate_uid,
                )
            # Option 3: "㏈-49836587346876342856236478-FG#xxx"
            elif len(parts) == 2:
                assert parts[1].startswith("FG#")
                flatgraph_uid = FlatGraphPlaceholder(int(parts[1][len("FG#"):]))
                desc = dict(
                    global_uid=parse_global_uid(parts[0]),
                    frame_uid=flatgraph_uid,
                )
            else:
                raise Exception(f"Not sure how to interpret this kind of identifier! {input}")
        elif isinstance(input, EternalUID):
            desc = dict(global_uid=input)
        elif isinstance(input, ZefRefUID):
            raise Exception("Shouldn't be getting a ZefRefUID anymore - we would need to load the graph to determine the global uid")
        elif isinstance(input, DBStateRefUID):
            desc = dict(
                global_uid=input.global_uid,
                frame_uid=input.dbstate_uid,
            )
        elif isinstance(input, FlatRefUID):
            # The global identifier must be given in the FlatRef if we hit this point
            desc = dict(
                flatref_idx=input.idx,
                frame_uid=input.flatgraph,
            )
        elif isinstance(input, DelegateRef):
            desc = dict(global_uid=input)
        else:
            raise Exception("Shouldn't get here")
    elif len(glob_identifiers) >= 2:
        raise Exception("Can't have more than one global identifier")
    else:
        pass

    # There could be a FlatGraph in the list when strictly_invertible is used in
    # pretty_atom_identity. This is to be interpreted as the reference frame
    fgs = others | filter[FlatGraphPlaceholder] | collect
    others = others | filter[~FlatGraphPlaceholder] | collect
    if len(fgs) > 0:
        if "frame_uid" in desc:
            raise Exception("Can't have a FlatGraph as a reference frame when the atom already has a reference graph")
        desc["frame_uid"] = single(fgs)


    # Same but with DBStateUID
    dbs = others | filter[DBStateUID] | collect
    others = others | filter[~DBStateUID] | collect
    if len(dbs) > 0:
        if "frame_uid" in desc:
            raise Exception("Can't have a DBState as a reference frame when the atom already has a reference graph")
        desc["frame_uid"] = single(dbs)


    if len(others) > 0:
        desc["local_names"] = others

    return desc

def pretty_atom_identity(desc: AtomIdentity):
    from .flat_graph import FlatGraph

    assert isinstance(desc, AtomIdentity)
    # The inverse of interpret_atom_identity
    names = []
    if "frame_uid" not in desc:
        if "global_uid" in desc:
            if isinstance(desc["global_uid"], DelegateRef):
                # We can't pretty print delegates so just output directly
                names += [desc["global_uid"]]
            else:
                names += ["㏈-" + str(desc["global_uid"])]

    elif isinstance(desc["frame_uid"], DBStateUID):
        if isinstance(desc["global_uid"], DelegateRef):
            # We can't pretty print delegates so just output directly
            names += [DBStateRefUID(global_uid=desc["global_uid"],
                                    dbstate_uid=desc["frame_uid"])]
        else:
            s = "㏈-" + str(desc["global_uid"])
            s += f"-{desc['frame_uid'].tx_uid}-{desc['frame_uid'].graph_uid}"
            names += [s]

    elif isinstance(desc["frame_uid"], FlatGraphPlaceholder):
        if "global_uid" in desc:
            s = "㏈-" + str(desc["global_uid"])
            s += f"-FG#{desc['frame_uid']._value}"
            names += [s]
        else:
            names += [FlatRefUID(global_uid=desc["flatref_idx"],
                                frame_uid=desc["frame_uid"])]
            
    else:
        raise NotImplementedError(f"Unknown type of frame: {desc['frame_uid']}")

    if "local_names" in desc:
        names += list(desc["local_names"])

    return tuple(names)
    

class Atom_:
    """
    AtomType: e.g. ET.Person, RT.Foo, AET[String]
    names: containing elements like e.g. '㏈-49836587346-57863478568734-876342856236478' / 'Fred'
    fields: Dict
    # not for now: optional_zefref_or_ezefref

    derived quantity reference_type: e.g. 'unidentified', 'local_name', 'red', 'db_ref', 'db_state_ref', 'graph_ref'
    """

    def __init__(self, *args, **fields):
        # Construct a blank slate and then call ourselves
        object.__setattr__(self, "atom_type", None)
        object.__setattr__(self, "atom_id", {})
        object.__setattr__(self, "fields", {})
        object.__setattr__(self, "ref_pointer", None)

        # Special case for an empty atom
        if len(args) == 0 and len(fields) == 0:
            return
        
        # Because we can't return a completely different value, we have to
        # assign everything to us.
        new_atom = self(*args, **fields)

        attrs = ["atom_type", "atom_id", "fields", "ref_pointer"]
        for attr in attrs:
            object.__setattr__(self, attr, object.__getattribute__(new_atom, attr))
    

    def __replace__(self, **kwargs):
        attrs = ["atom_type", "atom_id", "fields", "ref_pointer"]
        assert all(kwarg in attrs for kwarg in kwargs), "Trying to set an Attribute for Atom that isn't allowed."

        new_atom = Atom_()
        for attr in attrs:
            if attr in kwargs:
                object.__setattr__(new_atom, attr, kwargs[attr])
            else:
                object.__setattr__(new_atom, attr, object.__getattribute__(self, attr))

        return new_atom

    def __call__(self, *args, **kwargs):
        atom_type = object.__getattribute__(self, "atom_type")
        fields = dict(object.__getattribute__(self, "fields"))
        ref_pointer = object.__getattribute__(self, "ref_pointer")

        # We represent our internal id as a pretty identity so that we can
        # conform to whatever the user is speaking.
        atom_id = object.__getattribute__(self, "atom_id")
        names = pretty_atom_identity(atom_id)

        fields.update(kwargs)
        
        from ._ops import is_a, rae_type, source, target, origin_uid, abstract_type, discard_frame, value, uid
        from .VT import BlobPtr, RAET, EntityRef, RelationRef, AttributeEntityRef, FlatRef, Relation, AttributeEntity
        from .flat_graph import FlatRef, FlatRef_rae_type, FlatRef_maybe_uid

        for arg in args:
            if is_a(arg, AtomClass):
                # Take over everything about the other atom
                other_ref_pointer = object.__getattribute__(arg, "ref_pointer")
                other_fields = object.__getattribute__(arg, "fields")
                other_atom_type = object.__getattribute__(arg, "atom_type")
                other_atom_id = object.__getattribute__(arg, "atom_id")

                assert ref_pointer is None or other_ref_pointer is None
                if ref_pointer is None:
                    ref_pointer = other_ref_pointer

                fields.update(other_fields)

                assert atom_type is None or other_atom_type is None
                if atom_type is None:
                    atom_type = other_atom_type

                other_names = pretty_atom_identity(other_atom_id)
                names = names + other_names
            
            elif is_a(arg, BlobPtr):
                # This means we can extract the atom_type and uid from the Ref
                assert ref_pointer is None
                ref_pointer = arg
                ptr_atom_type = abstract_type(ref_pointer)
                assert atom_type is None or atom_type == ptr_atom_type
                atom_type = ptr_atom_type
                if isinstance(arg, ZefRef):
                    names = (make_dbstateref_uid(ref_pointer), *names)
                else:
                    names = (origin_uid(ref_pointer), *names)
        
            elif is_a(arg, FlatRef):
                ref_pointer = arg

                from .flat_graph import register_flatgraph
                fr_uid = make_flatref_uid(arg)
                tag = FlatRef_maybe_uid(arg)
                if isinstance(tag, EternalUID):
                    names = (tag, fr_uid.flatgraph, *names)
                else:
                    names = (fr_uid, *names)
                atom_type = FlatRef_rae_type(arg)

            elif is_a(arg, EntityRef | AttributeEntityRef | RelationRef):
                rae = arg
                assert atom_type is None
                atom_type = rae_type(rae)
                names = (origin_uid(rae), *names)

            elif is_a(arg, RAET):
                assert atom_type is None
                atom_type = arg
            else:
                names = names + (arg,)
        
        atom_id = interpret_atom_identity(names)

        return self.__replace__(atom_type=atom_type,
                                atom_id=atom_id,
                                fields=fields,
                                ref_pointer=ref_pointer)


    def __repr__(self):
        ref_pointer = _get_ref_pointer(self)
        atom_type = _get_atom_type(self)
        atom_id = pretty_atom_identity(_get_atom_id(self))
        fields = _get_fields(self)
        # items = [f'"{get_reference_type(self)}"'] + [f"{k}={v!r}" for k,v in fields.items()]
        items = [f"{k}={v!r}" for k,v in fields.items()]
        items = [repr(name) for name in atom_id] + list(items)
        if ref_pointer:
            # This is just for us while we are developing. Will be removed in the future.
            items += ["**{}"]
        return f'{atom_type}({f", ".join(items)})'

    def __setattr__(self, name, value):
        raise AttributeError("Atoms are immutable")
    def __delattr__(self, name):
        raise AttributeError("Atoms are immutable")

    def __getattribute__(self, name):
        if name.startswith("__"):
            return object.__getattribute__(self, name)
        # Need to convert KeyErrors to AttributeErrors for calling code to handle things like __x__ accesses
        try:
            return object.__getattribute__(self, "fields")[name]
        except KeyError:
            raise AttributeError(name)
    def __dir__(self):
        return dir(object.__getattribute__(self, "fields"))

    def __eq__(self, other):
        if type(other) != Atom_:
            return False
        return (_get_atom_type(self) == _get_atom_type(other)
                and _get_ref_pointer(self) == _get_ref_pointer(other)
                and _get_atom_id(self) == _get_atom_id(other)
                and _get_fields(self) == _get_fields(other))

    def __hash__(self):
        from .VT.value_type import hash_frozen
        return hash_frozen(("Atom_", _get_atom_type(self), _get_ref_pointer(self), _get_atom_id(self), _get_fields(self)))


# This means we are really specifically just the atom class, not a generic
# "atom".
AtomClass = make_VT('AtomClass', pytype=Atom_)

def _get_atom_type(atom: AtomClass):
    return object.__getattribute__(atom, "atom_type")
def _get_atom_id(atom: AtomClass):
    return object.__getattribute__(atom, "atom_id")
def _get_fields(atom: AtomClass):
    return object.__getattribute__(atom, "fields")
def _get_ref_pointer(atom: AtomClass):
    return object.__getattribute__(atom, "ref_pointer")

def get_most_authorative_id(atom: AtomClass):
    atom_id = _get_atom_id(atom)
    if "global_uid" in atom_id:
        return atom_id["global_uid"]
    if "flatref_idx" in atom_id:
        return FlatRefUID(idx=atom_id["flatref_idx"], flatgraph=atom_id["frame_uid"])
    if "local_names" in atom_id:
        from ._ops import sort, first, collect
        from .VT.value_type import hash_frozen
        return atom_id["local_names"] | sort[hash_frozen] | first | collect
    return None

def get_all_ids(atom: AtomClass):
    atom_id = _get_atom_id(atom)

    names = []
    if "global_uid" in atom_id:
        names += [atom_id["global_uid"]]
    if "local_names" in atom_id:
        names += atom_id["local_names"]

    return tuple(names)

def find_concrete_pointer(atom: AtomClass):
    # Find the ZefRef for an atom if we can. If we can't, return None
    z = _get_ref_pointer(atom)
    if z is not None:
        if isinstance(z, EZefRef):
            return None
        return z

    atom_id = _get_atom_id(atom)
    if "frame_uid" not in atom_id:
        return None
    frame_uid = atom_id.frame_uid
    if isinstance(frame_uid, DBStateUID):
        gs = find_dbstate(frame_uid)
        if gs is None:
            return None
        z = gs[atom_id.global_uid]
        return z
    elif isinstance(frame_uid, FlatGraphUID):
        fg = lookup_flatgraph(frame_uid.flatgraph)
        fr = fg[frame_uid.tag]
        return fr
    else:
        return None


def get_uid_type(uid_str: str) -> str:
    uid_chunk_size = 12
    uid_str = uid_str.strip("㏈-")

    if len(uid_str) == (uid_chunk_size*3 + 2)   and uid_str.count("-") == 2:      # 12 * 3 + (-,-) i.e ㏈-2ae69e6f28ab-2ae69e6f28ab-2ae69e6f28ab
        return 'db_state_ref'
    elif len(uid_str) == (uid_chunk_size*2 + 1) and uid_str.count("-") == 1:    # 12 * 2 + (-) i.e ㏈-2ae69e6f28ab-2ae69e6f28ab
        return 'db_ref'
    elif len(uid_str) == (uid_chunk_size*2):        # 12 * 2 i.e ㏈-2ae69e6f28ab2ae69e6f28ab
        return 'ref'
    else:
        raise ValueError(f"Invalid UID: {uid_str}")

def uid_str_to_uid(uid_str: str) -> UID:
    uid_str = uid_str[2:]
    if get_uid_type(uid_str) == "db_state_ref":
        return ZefRefUID(*uid_str.split('-'))
    elif get_uid_type(uid_str) == "db_ref":
        return EternalUID(*uid_str.split('-'))
    elif get_uid_type(uid_str) == "ref":
        return BaseUID(uid_str)
    else:
        raise Exception(f"Unknown type of uid: {uid_str}")

def get_reference_type(atom: AtomClass) -> str:
    atom_id = get_atom_id(atom)
    if "local_names" not in atom_id and "global_uid" not in atom_id:
        return 'unidentified'
    if "global_uid" not in atom_id:
        return "local_name"
    # return get_uid_type(uids[0])
    if "tx_uid" in atom_id:
        return "db_state_ref"
    else:
        return "db_ref"


def RelationAtom_is_a(x, typ):
    from ._ops import rae_type, is_a
    return isinstance(x, AtomClass & Is[rae_type | is_a[RT]])
RelationAtom = make_VT("RelationAtom", is_a_func=RelationAtom_is_a)
def EntityAtom_is_a(x, typ):
    from ._ops import rae_type, is_a
    return isinstance(x, AtomClass & Is[rae_type | is_a[ET]])
EntityAtom = make_VT("EntityAtom", is_a_func=EntityAtom_is_a)
def AttributeEntityAtom_is_a(x, typ):
    from ._ops import rae_type, is_a
    return isinstance(x, AtomClass & Is[rae_type | is_a[AET]])
AttributeEntityAtom = make_VT("AttributeEntityAtom", is_a_func=AttributeEntityAtom_is_a)
def TXNodeAtom_is_a(x, typ):
    from ._ops import abstract_type, equals
    return isinstance(x, AtomClass & Is[abstract_type | equals[BT.TX_EVENT_NODE]])
TXNodeAtom = make_VT("TXNodeAtom", is_a_func=TXNodeAtom_is_a)
def RootAtom_is_a(x, typ):
    from ._ops import abstract_type, equals
    return isinstance(x, AtomClass & Is[abstract_type | equals[BT.ROOT_NODE]])
RootAtom = make_VT("RootAtom", is_a_func=RootAtom_is_a)