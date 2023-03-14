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

from .common_opt import *

AliasDict = Dict[AllIDs][AllIDs]

################################################################
# * Entrypoint dispatch functions
#--------------------------------------------------------------

def cull_unnecessary_cmds(cmds: List[PleaseCommandLevel1], gs: GraphSlice):

    out_cmds = []
    aliases = {}

    for cmd in cmds:
        if is_a_PleaseInstantiate(cmd):
            out = cull_instantiate(cmd, gs)
        elif is_a_PleaseAssignJustValue(cmd):
            out = cull_assign(cmd, gs)
        elif is_a_PleaseAlias(cmd):
            out = cull_alias(cmd, gs)
        elif is_a_PleaseTerminate(cmd):
            out = cull_terminate(cmd, gs)
        # These don't have to be culled, could just be no-ops at the low level. 
        elif is_a_PleaseBeSource(cmd) or is_a_PleaseBeTarget(cmd):
            out = cull_unconditionally(cmd, gs)
        elif is_a_PleaseMustLive(cmd):
            out = cull_unconditionally(cmd, gs)
        elif is_a_PleaseTagJustTag(cmd):
            out = cull_tag(cmd, gs)
        else:
            not_implemented_error(cmd, "Unknown cmd type for culling")
        this_cmds,this_aliases = out
        assert len(this_cmds) <= 1
        out_cmds += this_cmds
        merge_nodups(aliases, this_aliases)

    return out_cmds,aliases
        

def relabel_cmds(cmds: List[PleaseCommandLevel1], aliases: AliasDict) -> List[PleaseCommandLevel1]:
    out_cmds = []
    for cmd in cmds:
        if is_a_PleaseInstantiate(cmd):
            out = relabel_instantiate(cmd, aliases)
        elif is_a_PleaseAssignJustValue(cmd):
            out = relabel_just_target(cmd, aliases)
        elif is_a_PleaseBeSource(cmd) or is_a_PleaseBeTarget(cmd):
            out = relabel_just_target(cmd, aliases)
        elif is_a_PleaseMustLive(cmd):
            out = relabel_just_target(cmd, aliases)
        elif is_a_PleaseAlias(cmd):
            out = relabel_alias(cmd, aliases)
        elif is_a_PleaseTerminate(cmd):
            out = relabel_terminate(cmd, aliases)
        elif is_a_PleaseTagJustTag(cmd):
            out = relabel_tag(cmd, aliases)
        else:
            not_implemented_error((cmd, aliases), "Unknown cmd type for relabelling")
        out_cmds += [out]
    return out_cmds


def distinguish_cmd(cmd: PleaseCommandLevel1):
    # Returns the distinguishable representation of an object, without any other context known.
    #
    # Entity(ET.X, uid="abcd")[V.name] -> Entity(ET.X, uid="abcd")
    # ET.Machine[V.name] -> ET.Machine[InternalID(n+1)]
    # 
    # This is used to fill in the list of unique operations, which will be pointed at by other names later on.

    if is_a_PleaseInstantiate(cmd):
        return distinguish_instantiate(cmd)
    elif is_a_PleaseAssignJustValue(cmd):
        return distinguish_assign(cmd)
    elif is_a_PleaseBeSource(cmd) or is_a_PleaseBeTarget(cmd):
        return distinguish_has_target(cmd)
    elif is_a_PleaseAlias(cmd):
        return distinguish_alias(cmd)
    elif is_a_PleaseMustLive(cmd):
        return distinguish_must_live(cmd)
    elif is_a_PleaseTerminate(cmd):
        return distinguish_has_target(cmd)
    elif is_a_PleaseTagJustTag(cmd):
        return distinguish_tag(cmd)
    else:
        not_implemented_error(cmd, "Unknown cmd type")

    return cmd_distinguish,names

#########
# ** PleaseInstantiate

def cull_instantiate(cmd: PleaseInstantiate, gs):
    maybe_ezr = None
    if "origin_uid" in cmd:
        maybe_ezr = find_rae_in_target(cmd.origin_uid, gs)
    elif isinstance(cmd.atom, PleaseInstantiateValueNode):
        maybe_ezr = find_value_node_in_target(cmd.atom, gs)
    elif isinstance(cmd.atom, PleaseInstantiateDelegate):
        maybe_ezr = to_delegate_gs_opt(cmd.atom, gs, False)

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
    atom = cmd._value["atom"]
    if is_a_PleaseInstantiateRelation(atom):
        atom = dict(atom)
        atom["source"] = lookup_alias(atom["source"], aliases)
        atom["target"] = lookup_alias(atom["target"], aliases)

    d = dict(cmd._value)
    d["atom"] = atom
    if "internal_ids" in d:
        d["internal_ids"] = [lookup_alias(x, aliases) for x in d["internal_ids"]]

    return UVT_ctor_opt(PleaseInstantiate, d)


def distinguish_instantiate(cmd: PleaseInstantiate) -> Tuple[PleaseInstantiate, List[AllIDs]]:
    atom = cmd._value["atom"]
    if is_a_DelegateRef(atom) or is_a_Val(atom):
        # Delegate is special - its own type is its id
        names = [atom] + cmd._value.get("internal_ids", [])
        if "internal_ids" in cmd._value:
            new_dict = dict(cmd._value)
            del new_dict["internal_ids"]
            new_cmd = UVT_ctor_opt(PleaseInstantiate, new_dict)
        else:
            new_cmd = cmd
        return new_cmd,names
    elif "origin_uid" in cmd._value:
        names = [cmd.origin_uid] + cmd._value.get("internal_ids", [])
        if "internal_ids" in cmd._value:
            new_dict = dict(cmd._value)
            del new_dict["internal_ids"]
            new_cmd = UVT_ctor_opt(PleaseInstantiate, new_dict)
        else:
            new_cmd = cmd
        return new_cmd,names
    elif "internal_ids" in cmd._value and len(cmd._value["internal_ids"]) >= 1:
        names = cmd._value["internal_ids"]
        # Just use the first name as the unique name
        new_dict = dict(cmd._value)
        new_dict["internal_ids"] = [names[0]]
        new_cmd = UVT_ctor_opt(PleaseInstantiate, new_dict)
        return new_cmd,names
    else:
        # TODO: Optimise
        new_name = global_gen_internal_id()
        new_cmd = cmd._get_type()(cmd._value | insert["internal_ids"][[new_name]] | collect)
        return new_cmd,[new_name]

#########
# ** PleaseAssign

def cull_assign(cmd: PleaseAssign, gs):
    if isinstance(cmd.target, EternalUID):
        if GraphSlice_contains(gs, cmd.target):
            z = GraphSlice_getitem(gs, cmd.target)
            if Val(pyzo.value(z)) == cmd.value:
                # Only here are we sure we can skip this!
                return [], {cmd.target: z}

    return [cmd], {}

def distinguish_assign(cmd: PleaseAssign) -> Tuple[PleaseAssign, List[AllIDs]]:
    name = cmd.target
    return cmd, [name]


#########
# ** PleaseTerminate

def cull_terminate(cmd: PleaseTerminate, gs):
    # If not on the graph, don't need to terminate
    if isinstance(cmd.target, EternalUID):
        if not GraphSlice_contains(gs, cmd.target):
            return [], {cmd.target: None}
    return [cmd], {}

def relabel_terminate(cmd: PleaseTerminate, aliases: AliasDict) -> PleaseTerminate:
    new_target = lookup_alias(cmd.target, aliases)
    return PleaseTerminate(target=new_target)
    
#########
# ** PleaseTag

def distinguish_tag(cmd):
    return cmd, [cmd.target, TagIDInternal(cmd.tag)]

def cull_tag(cmd: PleaseTag, gs):
    # If object is already tagged, no need to do it again
    if isinstance(cmd.target, EternalUID):
        if cmd.tag in gs:
            atom = gs[cmd.tag]
            if cmd.target == origin_uid(atom):
                # Note: we are not aliasing the target but the tag id, in
                # contrast to something like PleaseTerminate
                return [], {TagIDInternal(cmd.tag): None}
    return [cmd], {}

def relabel_tag(cmd: PleaseTag, aliases: AliasDict):
    new_target = lookup_alias(cmd.target, aliases)
    return PleaseTag(target=new_target, tag=cmd.tag)

#########
# ** PleaseAlias

def cull_alias(cmd: PleaseAlias, gs):
    if len(cmd.ids) >= 2:
        raise Exception(f"Got to the point of culling an alias which is still shared across two names. This shouldn't happend! {cmd}")
    return [], {}

def relabel_alias(cmd: PleaseAlias, aliases: AliasDict) -> PleaseAlias:
    ids = {lookup_alias(x, aliases) for x in cmd._value["ids"]}
    return UVT_ctor_opt(PleaseAlias, dict(ids=list(ids)))

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
    d["target"] = lookup_alias(d["target"], aliases)

    return UVT_ctor_opt(cmd._get_type(), d)

def distinguish_has_target(cmd):
    return cmd, [cmd.target]

##############################
# * Utils
#----------------------------


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

