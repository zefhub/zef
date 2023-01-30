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
    return False


# UIDString = String & Where[startswith["㏈-"]]
UIDString = String & Is[lambda x: x.startswith["㏈-"]]
AtomIdentity = (
    Pattern[{
        Optional["global_uid"]: EternalUID,
        Optional["tx_uid"]: BaseUID,
        Optional["graph_uid"]: BaseUID,
        Optional["local_names"]: List[~UIDString],
    }]
    # & Cond[Contains["tx_uid"] | Contains["graph_uid"]]
    #       [Contains["graph_uid"] & Contains["tx_uid"] & Contains["global_uid"]]
    & Cond[Is[lambda x: "tx_uid" in x or "graph_uid" in x]]
          [Is[lambda x: "graph_uid" in x and "tx_uid" in x and "global_uid" in x]]
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

    from ._ops import uid

    # Given a tuple of inputs to an Atom, determine a standardised dictionary to represent this.

    # Only one item in the list is allowed to be an item with a global
    # identifier (for now?). So check this first.
    glob_identifiers = []
    others = []
    for input in inputs:
        if Atom_is_global_identifier(input):
            glob_identifiers += [input]
        else:
            others += [input]
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
            elif len(parts) == 3:
                desc = dict(
                    global_uid=parse_global_uid(parts[0]),
                    tx_uid=BaseUID(parts[1]),
                    graph_uid=BaseUID(parts[2])
                )
            else:
                raise Exception(f"Not sure how to interpret this kind of identifier! {input}")
        elif isinstance(input, EternalUID):
            desc = dict(global_uid=input)
        elif isinstance(input, ZefRefUID):
            # TODO: This will change
            global_uid = EternalUID(input.blob_uid, input.graph_uid)
            tx_uid = input.tx_uid
            graph_uid = input.graph_uid
            desc = dict(
                global_uid=global_uid,
                tx_uid=tx_uid,
                graph_uid=graph_uid,
            )
        else:
            raise Exception("Shouldn't get here")

    if len(others) > 0:
        desc["local_names"] = others

    return desc

def pretty_atom_identity(desc: AtomIdentity):
    assert isinstance(desc, AtomIdentity)
    # The inverse of interpret_atom_identity
    names = []
    if "global_uid" in desc:
        s = "㏈-" + str(desc["global_uid"])
        if "tx_uid" in desc:
            s += f"-{desc['tx_uid']}-{desc['graph_uid']}"

        names += [s]
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

        new_atom = Atom()
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

        for arg in args:
            if is_a(arg, Atom):
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
                names = (uid(ref_pointer), *names)
        
            elif is_a(arg, FlatRef):
                raise NotImplementedError("TODO")
                # TODO
                ref_pointer = arg
                fr = arg
                atom_type = fr.fg.blobs[fr.idx][1]
                fr_uid =  fr.fg.blobs[fr.idx][-1]
                if fr_uid: names =  (str(fr_uid), *names)

            elif is_a(arg, EntityRef | AttributeEntityRef):
                raise NotImplementedError("TODO")
                rae = arg
                atom_type = rae_type(rae)
                names =  (origin_uid(rae), *names)

            elif is_a(arg, RelationRef):
                raise NotImplementedError("TODO")
                rae = arg
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
        ref_pointer = get_ref_pointer(self)
        atom_type = get_atom_type(self)
        atom_id = pretty_atom_identity(get_atom_id(self))
        fields = get_fields(self)
        # items = [f'"{get_reference_type(self)}"'] + [f"{k}={v!r}" for k,v in fields.items()]
        items = [f"{k}={v!r}" for k,v in fields.items()]
        items = [repr(name) for name in atom_id] + list(items)
        if ref_pointer:
            items += [f"*"]
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
        return (get_atom_type(self) == get_atom_type(other)
                and get_ref_pointer(self) == get_ref_pointer(other)
                and get_atom_id(self) == get_atom_id(other)
                and get_fields(self) == get_fields(other))

    def __hash__(self):
        from .VT.value_type import hash_frozen
        return hash_frozen(("Atom_", get_atom_type(self), get_ref_pointer(self), get_atom_id(self), get_fields(self)))


Atom = make_VT('Atom', pytype=Atom_)

def get_atom_type(atom: Atom):
    return object.__getattribute__(atom, "atom_type")
def get_atom_id(atom: Atom):
    return object.__getattribute__(atom, "atom_id")
def get_fields(atom: Atom):
    return object.__getattribute__(atom, "fields")
def get_ref_pointer(atom: Atom):
    return object.__getattribute__(atom, "ref_pointer")

def get_most_authorative_id(atom: Atom):
    atom_id = get_atom_id(atom)
    if "global_uid" in atom_id:
        return atom_id["global_uid"]
    if "local_names" in atom_id:
        return atom_id["local_names"][0]
    return None

def get_all_names(atom: Atom):
    atom_id = get_atom_id(atom)

    names = []
    if "global_uid" in atom_id:
        names += [atom_id["global_uid"]]
    if "local_names" in atom_id:
        names += atom_id["local_names"]

    return tuple(names)

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

def get_reference_type(atom: Atom) -> str:
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
    return isinstance(x, Atom & Is[rae_type | is_a[RT]])
RelationAtom = make_VT("RelationAtom", is_a_func=RelationAtom_is_a)
def EntityAtom_is_a(x, typ):
    from ._ops import rae_type, is_a
    return isinstance(x, Atom & Is[rae_type | is_a[ET]])
EntityAtom = make_VT("EntityAtom", is_a_func=EntityAtom_is_a)
def AttributeEntityAtom_is_a(x, typ):
    from ._ops import rae_type, is_a
    return isinstance(x, Atom & Is[rae_type | is_a[AET]])
AttributeEntityAtom = make_VT("AttributeEntityAtom", is_a_func=AttributeEntityAtom_is_a)
def TXNodeAtom_is_a(x, typ):
    from ._ops import abstract_type, equals
    return isinstance(x, Atom & Is[abstract_type | equals[BT.TX_EVENT_NODE]])
TXNodeAtom = make_VT("TXNodeAtom", is_a_func=TXNodeAtom_is_a)
def RootAtom_is_a(x, typ):
    from ._ops import abstract_type, equals
    return isinstance(x, Atom & Is[abstract_type | equals[BT.ROOT_NODE]])
RootAtom = make_VT("RootAtom", is_a_func=RootAtom_is_a)