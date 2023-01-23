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

class Atom_:
    """
    AtomType: e.g. ET.Person, RT.Foo, AET[String]
    names: containing elements like e.g. '㏈-49836587346-57863478568734-876342856236478' / 'Fred'
    fields: Dict
    # not for now: optional_zefref_or_ezefref

    derived quantity reference_type: e.g. 'unidentified', 'local_name', 'red', 'db_ref', 'db_state_ref', 'graph_ref'
    """

    def __init__(self, arg, *names, **fields):
        from ._ops import is_a, rae_type, source, target, origin_uid, rae_type, discard_frame, value
        from .VT import BlobPtr, RAET, EntityRef, RelationRef, AttributeEntityRef, FlatRef, Relation, AttributeEntity

        ref_pointer, rt_source, rt_target, ae_value = None, None, None, None
        if is_a(arg, BlobPtr):
            # This means we can extract the atom_type and uid from the Ref
            ref_pointer = arg
            atom_type = rae_type(ref_pointer)
            names =  (origin_uid(ref_pointer), *names)
            if is_a(arg, Relation):
                rt_source = discard_frame(source(arg))
                rt_target = discard_frame(target(arg))
            elif is_a(arg, AttributeEntity):
                ae_value = value(arg)
        
        elif is_a(arg, FlatRef):
            ref_pointer = arg
            fr = arg
            atom_type = fr.fg.blobs[fr.idx][1]
            fr_uid =  fr.fg.blobs[fr.idx][-1]
            if fr_uid: names =  (str(fr_uid), *names)

        elif is_a(arg, EntityRef | AttributeEntityRef):
            rae = arg
            atom_type = rae_type(rae)
            names =  (origin_uid(rae), *names)

        elif is_a(arg, RelationRef):
            rt_source = source(arg)
            rt_target = target(arg)
            rae = arg
            atom_type = rae_type(rae)
            names =  (origin_uid(rae), *names)

        elif is_a(arg, RAET):
            atom_type = arg
        elif is_a(arg, Nil):
            atom_type = None
        else:
            atom_type = None
            names = (arg,) + names

        object.__setattr__(self, "atom_type", atom_type)
        object.__setattr__(self, "names", names)
        object.__setattr__(self, "fields", fields)
        object.__setattr__(self, "ref_pointer", ref_pointer)
        object.__setattr__(self, "rt_source", rt_source)
        object.__setattr__(self, "rt_target", rt_target)
        object.__setattr__(self, "ae_value", ae_value)

    

    def __replace__(self, **kwargs):
        attrs = ["atom_type", "names", "fields", "ref_pointer", "rt_source", "rt_target", "ae_value"]
        assert all(kwarg in attrs for kwarg in kwargs), "Trying to set an Attribute for Atom that isn't allowed."

        new_atom = Atom(get_atom_type(self))
        for attr in attrs:
            if attr in kwargs:
                object.__setattr__(new_atom, attr, kwargs[attr])
            else:
                object.__setattr__(new_atom, attr, object.__getattribute__(self, attr))

        return new_atom

    def __call__(self, *args, **fields):
        # TODO Add checks on passed *args to ensure they are valid names or accepted values
        new_fields = dict(object.__getattribute__(self, "fields"))
        new_fields.update(fields)
        names = object.__getattribute__(self, "names")
        ref_pointer = object.__getattribute__(self, "ref_pointer")

        if ref_pointer:
            # In case ref_pointer is defined remove the first name "uid" as it gets added when we __init__ a new Object
            new_names = names[1:] + args
        else:
            new_names = names + args

        return Atom_(
            # If ref_pointer is defined then pass that instead of atom_type
            ref_pointer if ref_pointer else object.__getattribute__(self, "atom_type"),
            *new_names,
            **new_fields
        )


    def __repr__(self):
        ref_pointer = get_ref_pointer(self)
        atom_type = get_atom_type(self)
        names = get_names(self)
        fields = get_fields(self)
        rt_source = get_rt_source(self)
        rt_target = get_rt_target(self)
        items = [f'"{get_reference_type(self)}"'] + [f"{k}={v!r}" for k,v in fields.items()]
        items = [str(name) for name in names] + list(items)
        if ref_pointer:
            items += [f"Pointer: {ref_pointer}"]
        if rt_target:
            items += [f"Src: {rt_source}", f"Trgt: {rt_target}"]
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
                and get_names(self) == get_names(other)
                and get_fields(self) == get_fields(other))

    def __hash__(self):
        from .VT.value_type import hash_frozen
        return hash_frozen(("Atom_", get_atom_type(self), get_ref_pointer(self), get_names(self), get_fields(self)))


Atom = make_VT('Atom', pytype=Atom_)

def get_atom_type(atom: Atom):
    return object.__getattribute__(atom, "atom_type")
def get_names(atom: Atom):
    return object.__getattribute__(atom, "names")
def get_fields(atom: Atom):
    return object.__getattribute__(atom, "fields")
def get_ref_pointer(atom: Atom):
    return object.__getattribute__(atom, "ref_pointer")
def get_rt_source(atom: Atom):
    return object.__getattribute__(atom, "rt_source")
def get_rt_target(atom: Atom):
    return object.__getattribute__(atom, "rt_target")
def get_ae_value(atom: Atom):
    return object.__getattribute__(atom, "ae_value")

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
    names = get_names(atom)
    if len(names) == 0:
        return 'unidentified'
    def is_uid(x):
        return isinstance(x, str) and x.startswith('㏈-')
    uids = [name for name in names if is_uid(name)]
    if len(uids) >= 2:
        raise Exception("Atom containing more than 1 uid makes no sense currently")
    if len(uids) == 0:
        return "local_name"
    return get_uid_type(uids[0])


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