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
report_import("zef.core.atom")
from .VT import *



class Atom:
    """
    AtomType: e.g. ET.Person, RT.Foo, AET[String]
    name_or_uid: e.g. '㏈-49836587346-57863478568734-876342856236478' / 'Fred' / None
    reference_type: e.g. 'unidentified', 'local_name', 'red', 'db_ref', 'db_state_ref', 'graph_ref'
    fields: Dict | None
    # not for now: optional_zefref_or_ezefref
    """

    def __init__(self, atom_type, name_or_uid = None, **fields):
        self.atom_type = atom_type
        self.name_or_uid = name_or_uid
        self.reference_type = get_reference_type(name_or_uid)
        self.fields = fields


    def __call__(self, *args, **fields):
        new_fields = dict(self.fields)
        new_fields.update(fields)
        return Atom(self.atom_type, self.name_or_uid, **new_fields)


    def __repr__(self):
        items = [f"{k}={v!r}" for k,v in self.fields.items()]
        items = [(self.name_or_uid if self.name_or_uid else ""), f'"{self.reference_type}"', *items]
        return f'{self.atom_type}({f", ".join(items)})'
        


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


def get_reference_type(name_or_uid: str) -> str:
    if not name_or_uid:
        return 'unidentified'
    elif name_or_uid.startswith('㏈-'):
        return get_uid_type(name_or_uid)
    # elif is_a(name_or_uid, ZefRefUID):
    #     return 'db_state_ref'
    # elif is_a(name_or_uid, EternalUID):
    #     return 'db_ref'
    # elif is_a(name_or_uid, BaseUID):
    #     return 'ref'
    else:
        return 'local_name'