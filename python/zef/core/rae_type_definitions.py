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
report_import("zef.core.rae_type_definitions")

from .VT import *
from .VT import insert_VT, make_VT

Relation = insert_VT("Relation", RelationRef | RelationConcrete | RelationAtom | RelationFlatRef)
AttributeEntity = insert_VT("AttributeEntity", AttributeEntityRef | AttributeEntityConcrete | AttributeEntityAtom | AttributeEntityFlatRef)
Entity = insert_VT("Entity", EntityRef | EntityConcrete | EntityAtom | EntityFlatRef)
RAE = insert_VT('RAE', Entity | AttributeEntity | Relation)

TX = insert_VT("TX", TXNodeConcrete | TXNodeRef | TXNodeAtom | TXNodeFlatRef)
Root = insert_VT("Root", RootConcrete | RootRef | RootAtom | RootFlatRef)

def Atom_is_a(x, typ):
    return isinstance(x, RAE | TX | Root)
def Atom_ctor(self, *args, **kwargs):
    return AtomClass(*args, **kwargs)
    
Atom = make_VT("Atom",
               is_a_func=Atom_is_a,
               constructor_func=Atom_ctor,
               pass_self=True)