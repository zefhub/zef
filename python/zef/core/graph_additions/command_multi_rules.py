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
report_import("zef.core.graph_additions.command_multi_rules")

from .common import *

AliasDict = Dict[AllIDs][AllIDs]

################################################################
# * Entrypoint dispatch functions
#--------------------------------------------------------------

def cull_unnecessary_cmds(cmds: List[PleaseCommandLevel1], gs: GraphSlice):
    # Remove cmds that should have no effect on the final graph. Becvause
    # culling could also remove ids, we should make sure that all aliasing of
    # names has been resolved before this point. Cmds that would create a link
    # between names (e.g. an instantiate that has an origin_uid and an internal
    # id) should raise an error at this point, as it indicates aliasing was not
    # properly resolved.
    #
    # I immediately ran into an issue with this for things like value nodes, or
    # delegates. So instead, the culling is allowed to produce new aliasing,
    # from IDs to ZefRefs.

    out_cmds = []
    aliases = {}

    out_cmds,aliases = (
        cmds
        | map[lambda cmd: (cmd,gs) | match_rules[[
            *cull_rules,
            (Any, not_implemented_error["Unknown cmd type for culling"]),
        ]] | collect]
        # Checking that each cmd produces only 0-1 cmds
        | map[Assert[first | length | less_than_or_equal[1]]]
        | reducemany[concat, merge_nodups]
        | collect)

    return out_cmds,aliases
        

def relabel_cmds(cmds: List[PleaseCommandLevel1], aliases: AliasDict) -> List[PleaseCommandLevel1]:
    out_cmds = (
        cmds
        | map[lambda cmd: (cmd,aliases) | match_rules[[
            *relabel_rules,
            (Any, not_implemented_error["Unknown cmd type for relabelling"]),
        ]] | collect]
        | collect)

    return out_cmds

def distinguish_cmd(cmd: PleaseCommandLevel1):
    # Returns the distinguishable representation of an object, without any other context known.
    #
    # Entity(ET.X, uid="abcd")[V.name] -> Entity(ET.X, uid="abcd")
    # ET.Machine[V.name] -> ET.Machine[InternalID(n+1)]
    # 
    # This is used to fill in the list of unique operations, which will be pointed at by other names later on.

    cmd_distinguish,names = cmd | match[[
        *distinguish_rules,
        (Any, not_implemented_error["Unknown cmd type"]),
    ]] | collect

    return cmd_distinguish,names

#########
# ** PleaseInstantiate

def cull_instantiate(cmd: PleaseInstantiate, gs):
    maybe_ezr = None
    if "origin_uid" in cmd:
        from ..graph_slice import get_instance_rae
        maybe_ezr = get_instance_rae(cmd.origin_uid, gs)
    elif isinstance(cmd.atom, PleaseInstantiateValueNode):
        maybe_ezr = Graph(gs).get_value_node(cmd.atom)
    elif isinstance(cmd.atom, PleaseInstantiateDelegate):
        maybe_ezr = delegate_of(cmd.atom, g, False)

    if maybe_ezr is not None:
        # We have an existing item, don't need to action this but do store the
        # found item for looking up wish ids.
        alias = {}
        if "internal_ids" in cmd:
            for wish_id in cmd.internal_ids:
                alias[wish_id] = maybe_ezr
            
        # We can discard this command.
        return [], alias

    return [cmd], {}

def relabel_instantiate(cmd: PleaseInstantiate, aliases: AliasDict) -> PleaseInstantiate:
    atom = cmd.atom
    if isinstance(atom, PleaseInstantiateRelation):
        atom = (atom
                | insert["source"][lookup_alias(atom["source"], aliases)]
                | insert["target"][lookup_alias(atom["target"], aliases)]
                | collect)

    d = dict(cmd._value)
    d = d | insert["atom"][atom] | collect
    if "internal_ids" in d:
        d = (d
             | insert["internal_ids"][cmd.internal_ids
                                      | map[lambda x: lookup_alias(x, aliases)]
                                      | collect]
             | collect)

    return PleaseInstantiate(d)


def distinguish_instantiate(cmd: PleaseInstantiate) -> Tuple[PleaseInstantiate, List[AllIDs]]:
    if "origin_uid" in cmd:
        names = [cmd.origin_uid] + cmd._value.get("internal_ids", [])
        new_cmd = cmd._get_type()(cmd._value | without[["internal_ids"]] | collect)
        return new_cmd,names
    elif "internal_ids" in cmd and len(cmd.internal_ids) >= 1:
        names = cmd.internal_ids
        # Just use the first name as the unique name
        new_cmd = cmd._get_type()(cmd._value | insert["internal_ids"][[names[0]]] | collect)
        return new_cmd,names
    else:
        new_name = global_gen_internal_id()
        new_cmd = cmd._get_type()(cmd._value | insert["internal_ids"][[new_name]] | collect)
        return new_cmd,[new_name]

#########
# ** PleaseAssign

def cull_assign(cmd: PleaseAssign, gs):
    if isinstance(cmd.target, AttributeEntityRef):
        if cmd.target in gs:
            z = gs | get[cmd.target] | collect
            if value(z) == cmd.value:
                # Only here are we sure we can skip this!
                return [], {origin_uid(cmd.target): z}

    return [cmd], {}

#########
# ** PleaseTerminate

def cull_terminate(cmd: PleaseTerminate, gs):
    # If not on the graph, don't need to terminate
    if isinstance(cmd.target, RAERef):
        if cmd.target not in gs:
            return [], {}
    return [cmd], {}

def relabel_terminate(cmd: PleaseTerminate, aliases: AliasDict) -> PleaseTerminate:
    new_target = lookup_alias(cmd.target, aliases)
    return PleaseTerminate(target=new_target)
    

#########
# ** PleaseAlias

def cull_alias(cmd: PleaseAlias, gs):
    if len(cmd.ids) >= 2:
        raise Exception(f"Got to the point of culling an alias which is still shared across two names. This shouldn't happend! {cmd}")
    return [], {}

def relabel_alias(cmd: PleaseAlias, aliases: AliasDict) -> PleaseAlias:
    ids = cmd.ids | map[lambda x: lookup_alias(x, aliases)] | distinct | collect
    return PleaseAlias(ids=ids)

def distinguish_alias(cmd):
    return cmd, cmd.ids

#########
# ** PleaseMustLive

def distinguish_must_live(cmd):
    return cmd, [cmd.target]

#########
# ** General rules

def cull_unconditionally(cmd, gs):
    return [], {}
    

def relabel_just_target(cmd, aliases: AliasDict):
    # Works on any please command that has a "target" field that needs reassigning and nothing else.
    d = dict(cmd._value)
    d = d | insert["target"][lookup_alias(d["target"], aliases)] | collect

    return cmd._get_type()(d)


def distinguish_assign(cmd: PleaseAssign) -> Tuple[PleaseAssign, List[AllIDs]]:
    if isinstance(cmd.target, AttributeEntityRef):
        name = origin_uid(cmd.target)
    else:
        name = cmd.target
    return cmd, [name]

def distinguish_has_no_id(cmd):
    return cmd, []

def distinguish_be_source_target(cmd):
    return cmd, [cmd.target]



######################################
# * Listing of rules
#------------------------------------

cull_rules = [
    (PleaseInstantiate, cull_instantiate),
    (PleaseAssign, cull_assign),
    (PleaseAlias, cull_alias),
    (PleaseTerminate, cull_terminate),
    # These don't have to be culled, could just be no-ops at the low level. 
    (PleaseBeSource | PleaseBeTarget, cull_unconditionally),
    (PleaseMustLive, cull_unconditionally),
]

relabel_rules = [
    (PleaseInstantiate, relabel_instantiate),
    (PleaseAssign, relabel_just_target),
    (PleaseBeSource | PleaseBeTarget, relabel_just_target),
    (PleaseMustLive, relabel_just_target),
    (PleaseAlias, relabel_alias),
    (PleaseTerminate, relabel_terminate),
]
        
    
distinguish_rules = [
    (PleaseInstantiate, distinguish_instantiate),
    (PleaseAssign, distinguish_assign),
    (PleaseBeSource | PleaseBeTarget, distinguish_be_source_target),
    (PleaseAlias, distinguish_alias),
    (PleaseMustLive, distinguish_must_live),
    (PleaseTerminate, distinguish_has_no_id),
]


##############################
# * Utils
#----------------------------



def id_preference_pair(x: AllIDs, y: AllIDs) -> AllIDs:
    # Take the dominant id from two given ids: EternalUID >> Variable >> WishIDInternal

    assert isinstance(x, AllIDs) and isinstance(y, AllIDs)
    if isinstance(x, EternalUID) or isinstance(y, EternalUID):
        if isinstance(x, EternalUID) and isinstance(y, EternalUID) and x != y:
            raise Exception("Two different EUIDs are trying to be merged!")
        if isinstance(x, EternalUID):
            return x
        return y
    elif isinstance(x, Variable) or isinstance(y, Variable):
        if isinstance(x, Variable):
            return x
        return y
    else:
        return x

def id_preference(l: List[AllIDs]) -> AllIDs:
    out = l | reduce[id_preference_pair] | collect
    return out
        
def lookup_alias(name: AllIDs, aliases: AliasDict) -> AllIDs:
    # We have to do this recursively, in case there's an alias chain
    while name in aliases:
        # Just for santiy check at the moment.
        assert id_preference_pair(aliases[name], name) == aliases[name]
        name = aliases[name]
    return name

def resolved_variables_from_aliases(aliases: AliasDict) -> ResolvedVariableDict:
    out = {}
    for k in aliases:
        if isinstance(k, Variable):
            out[k] = lookup_alias(k, aliases)

    assert isinstance(out, ResolvedVariableDict)
    return out

