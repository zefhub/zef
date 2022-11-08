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
    "instantiated",
    "terminated",
    "assigned",
]

from .VT import *
from .VT import make_VT, insert_VT
from .VT.helpers import generic_subtype_get, generic_subtype_validate

# from .abstract_raes import Entity, AttributeEntity, Relation, TXNode, Root, make_custom_entity
from .abstract_raes import make_custom_entity
# TODO Move this somewhere sensible
def is_a_custom_entity(x, typ):
    return (isinstance(x, EntityRef) and x.d["type"] == ET.ZEF_CustomEntity)
CustomEntity = make_VT("CustomEntity", is_a_func=is_a_custom_entity)


instantiated     = make_custom_entity(name_to_display='instantiated', predetermined_uid='60252a53a03086b7')
terminated       = make_custom_entity(name_to_display='terminated', predetermined_uid='4f676154ffeb9dc8')
assigned         = make_custom_entity(name_to_display='assigned', predetermined_uid='c31287dab677f38c')

infinity           = make_custom_entity(name_to_display='infinity',    predetermined_uid='4906648460291096')
nil                = make_custom_entity(name_to_display='nil',         predetermined_uid='1654670075329719') #| register_call_handler[f1] | run[execute] | get['entity'] | collect  # TODO

def is_a_has_uid(x, typ):
    assert generic_subtype_validate(typ)
    from ._ops import uid
    return uid(x) == generic_subtype_get(typ)
# TODO: This is "early" because it is available before the uid zefop. Change this if possible.
def early_uid(x):
    return x.d["uid"]
HasUID = make_VT("HasUID", is_a_func=is_a_has_uid)
insert_VT("Instantiated", CustomEntity & HasUID[early_uid(instantiated)])
insert_VT("Terminated", CustomEntity & HasUID[early_uid(terminated)])
insert_VT("Assigned", CustomEntity & HasUID[early_uid(assigned)])