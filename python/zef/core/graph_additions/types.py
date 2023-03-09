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

from ... import report_import
report_import("zef.core.graph_additions.types")

__all__ = [
    "PleaseInstantiateEntity",
    "PleaseInstantiateRelation",
    "PleaseInstantiateAttributeEntity",
    "PleaseInstantiateValueNode",
    "PleaseInstantiateDelegate",
    "PleaseInstantiate",
    "PleaseTerminate",
    "PleaseAssign",
    "PleaseAssignJustValue",
    "PleaseAssignAlsoInstantiate",
    "PleaseTag",
    "PleaseTagJustTag",
    "PleaseTagAlsoInstantiate",
    "PleaseCommandLevel1",
    "Level1CommandInfo",

    "PleaseMustLive",
    "PleaseBeSource",
    "PleaseBeTarget",
    "PleaseAlias",

    "ResolvedVariableDict",

    "PleaseRun",
    "PleaseCommandLevel2",
    "Level2CommandInfo",
    "Level2AtomClass",

    "PureET",
    "PureRT",
    "PureAET",

    "RelationTriple",
    "PrimitiveValue",
    "WrappedValue",
    # "AETWithValue",
    "OldStyleDict",
    "OldStyleRelationTriple",

    "GraphWishValue",
    "GraphWishInputSimple",
    "GraphWishInput",
    "GraphWishReceipt",

    "WishID",
    "WishIDInternal",
    "TagIDInternal",
    "InternalIDs",
    "FlatRefUID",
    "Variable",
    "AllIDs",
    "UserWishID",
    "ExtraUserAllowedIDs",
    "NamedZ",
    "NamedAny",
]


# Implementation questions - should ValueTypes or UserValueTypes be preferred for these kinds of things?
# Pros:
# - ValueTypes don't include a tag, e.g. PleaseTerminate, so are easier to
#   create future versions that extend previous types. e.g. a new key in the
#   dictionary.
# - UserValueTypes do include a tag, so are easier to avoid accidental incorrect
#   use in older versions rather than erroring. Extra code is required for
#   converting between versions but it can be made explicit.
# Cons:
# - ValueTypes need custom code to describe their objects (i.e. being a
#   dictionary is actually several lines to describe). This could be alleviated
#   with factory code to generate ValueTypes.
# - See the opposites of the Pros.

from ..VT import *
from ..VT import make_VT
from .. import _ops
from ..atom import FlatRefUID, FlatGraphPlaceholder

def non_zefop_alias(vt, name):
    from ..VT import ValueType_
    vt2 = ValueType_(type_name=vt._d['type_name'], absorbed=vt._d['absorbed'])
    vt2._d['alias'] = name
    return vt2
_alias = non_zefop_alias

# IDs are allowed for many aspects of linking between parts of a wish. We want
# to allow variables, but need to include strings for backwards compoatibility
# for a version or two.
# WishID = String | Variable
Variable = _alias(SymbolicExpression & Is[_ops.get_field["root_node"] | _ops.equals[None]],
                 "Variable")
    
# Internal wish ids as used to reference items, but are themselves not returned in the receipt.
WishIDInternal = UserValueType("WishIDInternal", String, forced_uid="83192238824")

# This is only to allow tagging to share same names as other labels, but be
# distinguishable. Only for internal use in this module.
TagIDInternal = UserValueType("TagIDInternal", Any, forced_uid="54701468908")

InternalIDs = WishIDInternal | FlatRefUID | TagIDInternal

WishID = _alias(Variable | InternalIDs,
               "WishID")

# This is to allow us potentially switching up the intention of Val in the main
# user namespace
WrappedValue = Val

AllIDs = WishID | EternalUID | DelegateRef | WrappedValue

NamedAny = ValueType[Any] & Is[_ops.absorbed | _ops.length | _ops.greater_than[0]]
def delayed_check_namedz(x):
    if not isinstance(x, ZExpression):
        return False
    if _ops.rae_type(x.root_node) != ET.GetItem:
        return False
    if x.root_node.arg1 != ET.Z:
        return False
    return True
NamedZ = Is[delayed_check_namedz]

ExtraUserAllowedIDs = NamedZ | NamedAny

UserWishID = WishID | ExtraUserAllowedIDs

PrimitiveValue = _alias(PyInt | String | PyFloat | PyBool | Time | Enum | QuantityInt | QuantityFloat,
                       "PrimitiveValue")

# This is to work around deprecation issues
PureET = ValueType & ET
PureRT = ValueType & RT
PureAET = ValueType & AET


######################################
# * Level 1 commands
#------------------------------------
# Level 1 commands - precise commands that are direct actions on a graph slice.
# These can be generated from a set of levl 2 commands and a graph slice and
# should then be *guaranteeed* to be applicable, in constrast to level 2
# commands.

PleaseInstantiateEntity = PureET
PleaseInstantiateRelation = Pattern[{"rt": PureRT,
                                     "source": AllIDs,
                                     "target": AllIDs}]
PleaseInstantiateAttributeEntity = PureAET
PleaseInstantiateValueNode = WrappedValue
PleaseInstantiateDelegate = DelegateRef

PleaseInstantiateAtom = _alias(PleaseInstantiateEntity
                                           | PleaseInstantiateRelation
                                           | PleaseInstantiateAttributeEntity
                                           | PleaseInstantiateValueNode
                                           | PleaseInstantiateDelegate,
                                   "PleaseInstantiateAtom")
PleaseInstantiate = UserValueType("PleaseInstantiate",
                                  Dict,
                                  Pattern[{"atom": PleaseInstantiateAtom,
                                           Optional["origin_uid"]: EternalUID,
                                           Optional["internal_ids"]: List[WishID]}]
                                  # An exception to the rule, is that value nodes and delegates cannot have origin_uids
                                  & ~Pattern[{"atom": PleaseInstantiateDelegate | PleaseInstantiateValueNode,
                                              "origin_uid": Any}],
                                  forced_uid="79672068288",
                                  )

PleaseTerminate = UserValueType("PleaseTerminate",
                                Dict,
                                Pattern[{
                                    "target": AllIDs,
                                    # Note: including internal_ids here makes
                                    # some of the logic trickier. Going to
                                    # exclude this possibility for the moment,
                                    # but can introduce it later on if needed.
                                    #"internal_ids": Optional[List[WishID]]
                                }],
                                forced_uid="40694214316")

PleaseAssign = UserValueType("PleaseAssign",
                              Dict,
                              # Pattern[{"target": AllIDs | PureAET,
                             # We go to allowing anything in here, but this is only the user-facing front.
                             Pattern[{"target": Any,
                                      "value": WrappedValue,
                                      # This is for when an AE is merged across
                                      # but can be overridden by an explicit
                                      # assign.
                                      Optional["droppable"]: Bool}],
                             forced_uid="64659041312")

# Only the "JustValue" version is acceptable as a low level command
PleaseAssignJustValue = PleaseAssign & Is[_ops.get_field["target"] | _ops.is_a[AllIDs]]
PleaseAssignAlsoInstantiate = PleaseAssign & ~PleaseAssignJustValue

PleaseTag = UserValueType("PleaseTag",
                          Dict,
                          Pattern[{"target": Any,
                                   "tag": Any}],
                          forced_uid="60325761894")
# Only the "JustTag" version is acceptable as a low level command
PleaseTagJustTag = PleaseTag & Is[_ops.get_field["target"] | _ops.is_a[AllIDs]]
PleaseTagAlsoInstantiate = PleaseTag & ~PleaseTagJustTag

# Only for internal use to resolve potential invalid graph wishes.
PleaseMustLive = UserValueType("PleaseMustLive",
                               Dict,
                               Pattern[{"target": AllIDs}],
                               forced_uid="84949934812")
# Only for internal use to handle set_field etc
PleaseBeSource = UserValueType("PleaseBeSource",
                               Dict,
                               Pattern[{"target": AllIDs,
                                        "rel_ids": List[AllIDs],
                                        "exact": Bool,
                                        "rt": RT,
                                        }],
                               forced_uid="60712358786")
PleaseBeTarget = UserValueType("PleaseBeTarget",
                               Dict,
                               Pattern[{"target": AllIDs,
                                        "rel_ids": [AllIDs],
                                        "exact": Bool,
                                        "rt": RT,
                                        }],
                               forced_uid="97470448928")
PleaseAlias = UserValueType("PleaseAlias",
                            Dict,
                            Pattern[{"ids": List[AllIDs]}],
                            forced_uid="53003798204")

PleaseCommandLevel1 = _alias(PleaseInstantiate
                             | PleaseAssignJustValue
                             | PleaseTerminate
                             | PleaseTagJustTag
                             | PleaseMustLive
                             | PleaseBeSource
                             | PleaseBeTarget
                             | PleaseAlias,
                             "PleaseCommandLevel1")




# An import delay
def are_commands_ordered(info):
    from .command_ordering import are_commands_ordered
    return are_commands_ordered(info["cmds"], info["gs"])
OrderedCommands = Is[are_commands_ordered]
# When prepared, a list of commands must be in the context of a graph slice
ResolvedVariableDict = Dict[Variable][EternalUID | Variable]
Level1CommandInfo = UserValueType(
    "Level1CommandInfo",
    Dict,
    Pattern[{"gs": GraphSlice | FlatGraph,
             "cmds": List[PleaseCommandLevel1],
             # Resolved variables are those that are identified at translation
             # time, in contrast to instantiations that will happen at action
             # time.
             "resolved_variables": ResolvedVariableDict,
             }] & OrderedCommands)


##############################
# * Level 2 commands
#----------------------------
# Level 2 commands - declaration of what to be done, although intent could be
# for a particular graph slice, could be attempted to be applied on a later
# graph slice.

PleaseRun = UserValueType("PleaseRun",
                          Dict,
                          Pattern[{"action": LazyValue | ZefOp | ZExpression}],
                          forced_uid="37775167477")

# This is a restricted version of an atom, which does not allow too many different things in the fields.
def Level2AtomClass_is_a(x, typ):
    from ..atom import Atom_, _get_atom_id, _get_fields, _get_atom_type, _get_ref_pointer
    if type(x) != Atom_: return False

    # ref_pointer = _get_ref_pointer(x)
    # if ref_pointer is not None: return False

    atom_type = _get_atom_type(x)
    if not (isinstance(atom_type, RAET) or atom_type in [BT.TX_EVENT_NODE, BT.ROOT_NODE, Val, None]): return False

    atom_id = _get_atom_id(x)
    # if "flatref_idx" in atom_id: return False
    if "global_uid" in atom_id:
        if not isinstance(atom_id["global_uid"], EternalUID | DelegateRef | Val): return False
    if "frame_uid" in atom_id:
        # Urgh this is ugly. A flatref needs its flatgraph to be able to
        # uniquely identify itself. So we allow that, but not any other frame.
        if "flatref_idx" not in atom_id: return False

    fields = _get_fields(x)
    for name,(rel,val) in fields.items():
        if not isinstance(rel, Level2AtomClass | RT): return False
        if not isinstance(val, Set[GraphWishValue | Level2AtomClass | WishID]): return False

    return True
        

Level2AtomClass = make_VT("Level2AtomClass", is_a_func=Level2AtomClass_is_a)

PleaseCommandLevel2 = _alias(PleaseCommandLevel1 | PleaseRun | Level2AtomClass,
                                 "PleaseCommandLevel2")
Level2CommandInfo = UserValueType("Level2CommandInfo",
                                  Dict,
                                  Pattern[{"cmds": List[PleaseCommandLevel2],
                                           Optional["custom"]: Any}])

##############################
# * Main interface input
#----------------------------


# These are the external graph wish interface possibilities. This is the most
# flexible, which is converted to level 2 commands in order to transmit to the
# transactor.

# RelationTriple = _alias(Tuple[UserWishID | Atom | PrimitiveValue,
#                                   PureRT,
#                                   UserWishID | Atom | PrimitiveValue],
#                             "RelationTriple")
# TODO: A RelationTriple should recursively allow anything that is a GraphWishInput for its source and target
def is_relation_triple(x):
    if not isinstance(x, List):
        return False
    if not len(x) == 3:
        return False
    # I'm uncomfortable with EternalUID appearing in here - it is not enough
    # information on its own, and must be accompanied by an instantiate
    # elsewhere. But it does appear a lot as part of the iteration
    Item = GraphWishInput | UserWishID | EternalUID
    if not isinstance(x[0], Item):
        return False
    if not isinstance(x[1], PureRT | RelationAtom):
        return False
    if not isinstance(x[2], Item):
        return False
    return True
RelationTriple = _alias(Is[is_relation_triple], "RelationTriple")
def is_relation_triple_OS(x):
    if not isinstance(x, List):
        return False
    Item = GraphWishInput | UserWishID
    if len(x) == 3:
        return (isinstance(x[0], Item | List[Item])
                and isinstance(x[1], PureRT)
                and isinstance(x[2], Item | List[Item]))
    elif len(x) == 2:
        if isinstance(x[0], Item) and isinstance(x[1], List[Tuple[PureRT, Item]]):
            return True
        return False
    else:
        return False
OldStyleRelationTriple = _alias(Is[is_relation_triple_OS], "OldStyleRelationTriple")
GraphWishValue = _alias(PrimitiveValue | WrappedValue, "GraphWishValue")

# Backwards compatibility
# Any is used here instead of GraphWishInputSimple. It would be nice to have the
# recursive definition but that makes for ugly hacks to implement it.
OldStyleDict = Dict[UserWishID | PureET | RAE][Dict[PureRT | RelationAtom][Any]] & Is[_ops.length | _ops.equals[1]]

# "Simple" inputs are those that can be interpreted almost (excepting EUID references) directly into lvl1 commands
GraphWishInputSimple = _alias(
    LazyValue
    | SymbolicExpression
    | RelationTriple
    | OldStyleRelationTriple
    | GraphWishValue
    | PleaseAssign
    | PleaseTag
    | FlatGraph
    | PureET
    | PureAET
    | Atom
    | OldStyleDict,
    "GraphWishInputSimple"
)

GraphWishInput = _alias(PleaseCommandLevel2 | GraphWishInputSimple,
                        "GraphWishInput")

GraphWishReceipt = Dict[AllIDs][AtomRef]