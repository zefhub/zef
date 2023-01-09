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

class Atom_:
    """
    AtomType: e.g. ET.Person, RT.Foo, AET[String]
    names: containing elements like e.g. '㏈-49836587346-57863478568734-876342856236478' / 'Fred'
    fields: Dict
    # not for now: optional_zefref_or_ezefref

    derived quantity reference_type: e.g. 'unidentified', 'local_name', 'red', 'db_ref', 'db_state_ref', 'graph_ref'
    """

    def __init__(self, atom_type, *names, **fields):
        from ._ops import is_a, rae_type, uid
        from .VT import ZefRef, EZefRef, Entity

        ref_pointer = None
        if is_a(atom_type, ZefRef) or is_a(atom_type, EZefRef):
            assert is_a(atom_type, Entity), "Atom was called with a ZefRef not of EntityType"
            # This means we can extract the atom_type and uid from the Ref
            ref_pointer = atom_type
            atom_type = rae_type(ref_pointer)
            names =  (str(uid(ref_pointer)), *names)

        object.__setattr__(self, "atom_type", atom_type)
        object.__setattr__(self, "names", names)
        object.__setattr__(self, "fields", fields)
        object.__setattr__(self, "ref_pointer", ref_pointer)

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
        items = [f'"{get_reference_type(self)}"'] + [f"{k}={v!r}" for k,v in fields.items()]
        items = names + tuple(items)
        ref_pointer_str = f" -> {ref_pointer}" if ref_pointer else ""
        return f'{atom_type}({f", ".join(items)}){ref_pointer_str}'

    def __setattr__(self, name, value):
        raise AttributeError("Atoms are immutable")
    def __delattr__(self, name):
        raise AttributeError("Atoms are immutable")

    def __getattribute__(self, name):
        # Need to convert KeyErrors to AttributeErrors for calling code to handle things like __x__ accesses
        try:
            return object.__getattribute__(self, "fields")[name]
        except KeyError:
            raise AttributeError(name)
    def __dir__(self):
        return dir(object.__getattribute__(self, "fields"))

from .VT import make_VT
Atom = make_VT('Atom', pytype=Atom_)

def get_atom_type(atom: Atom):
    return object.__getattribute__(atom, "atom_type")
def get_names(atom: Atom):
    return object.__getattribute__(atom, "names")
def get_fields(atom: Atom):
    return object.__getattribute__(atom, "fields")
def get_ref_pointer(atom: Atom):
    return object.__getattribute__(atom, "ref_pointer")

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


def get_reference_type(atom: Atom) -> str:
    names = get_names(atom)
    if len(names) == 0:
        return 'unidentified'
    def is_uid(x):
        return x.startswith('㏈-')
    uids = [name for name in names if is_uid(name)]
    if len(uids) >= 2:
        raise Exception("Atom containing more than 1 uid makes no sense currently")
    if len(uids) == 0:
        return "local_name"
    return get_uid_type(uids[0])