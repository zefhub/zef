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
report_import("zef.core.graph_additions.wish_translation")

from .types import *
from .common import *



##########################################
# * Level 1 generation
#----------------------------------------

class CouldNotProcess: pass

WishMapping = Dict[WishID][EZefRef|WishID]
CmdDispatchContext = UserValueType("Level1CmdDispatchContext",
                                   Dict,
                                   Pattern[{"mapping": WishMapping, "gen_id_state": GenIDState}])

DispatchOutput = SetOf(CouldNotProcess) | Tuple[List[PleaseCommandLevel1], List[PleaseCommandLevel2], CmdDispatchContext]

# Rules should follow the signature below, but we can't specify that just yet.
# TranslationFunc = Func[PleaseCommandLevel2, CmdDispatchContext][DispatchOutput]
TranslationFunc = Any

# Really want to use Subtype[PleaseCommandLevel2] instead of ValueType
def generate_level1_commands(commands: List[PleaseCommandLevel2], gs: GraphSlice, rules: List[Tuple[ValueType,TranslationFunc]]) -> Level1CommandInfo:
    # For each command:
    #
    # - if PleaseInstantiate
    #   - see if already exists (given origin_uid)

    # - At end, consolidate such that commands which do the same thing, but might have a different WishID, are combined.

    # Note: we could save the level2 command that generated each level1 command
    # for tracability of errors back to their source.

    # - output_cmds is the order of the cmds to put into the output - these
    #   entries are unnamed
    # - cmd_naming is an efficient way to "translate" the unnamed commands into
    #   the same command with all ids applied.
    cmd_naming = {}
    output_cmds = []

    # If any commands are elided or otherwise referencing existing objects, we
    # can indicate them here. These replacements only occur for targets of
    # commands, not the identity of a command itself.
    #
    # If the mapping maps to a WishID, this indicates the target will be created
    # during the graph transaction. It guarantees that the item does not yet
    # exist on the graph.
    context = CmdDispatchContext({
        "mapping": {},
        "gen_id_state": generate_initial_state("lvl1"),
    })
    
    # We have to have multiple passes at the commands as each command can slowly
    # describe different wish ids.

    todo = list(commands)
    while len(todo) > 0:
        new_todo = []
        processed_one = False

        for cmd in todo:
            # new_lvl1_cmds,new_lvl2_cmds,mapping = cmd | match[
            result = (cmd, gs, context) | match_rules[[
                *rules,
                (Any, lambda x: not_implemented_error["Unknown cmd type"]), 
            ]] | collect

            if result == CouldNotProcess:
                new_todo.append(cmd)
            else:
                processed_one = True
                new_lvl1_cmds,new_lvl2_cmds,context = result
                for new_cmd in new_lvl1_cmds:
                    cmd_naming,output_cmds = add_command(new_cmd, cmd_naming, output_cmds)
                new_todo += new_lvl2_cmds

        if not processed_one:
            raise Exception("We were unable to process all lvl2 commands into lvl1 commands.")

        todo = new_todo

    # TODO: Go through and replace wish ids with euids where possible.
    # TODO: Then update cmd_naming so that these are consolidated into one.

    named_output_cmds = output_cmds | map[match[
        (PleaseCommandLevel1Distinguishable, lambda x: cmd_naming[x]),
        (Any, identity)
    ]] | collect

    return Level1CommandInfo({"gs": gs,
                                "cmds": named_output_cmds})
        

################################################################
# ** Dispatched instantiation cmds

# Note: these dispatch methods are meant to be immutable, but will actually
# mutate the state (context) for speed at the moment.

@func
def cmds_for_instantiate(cmd: PleaseInstantiate, gs: GraphSlice, context: CmdDispatchContext) -> DispatchOutput:
    # First look to see if we can discard this command because the item is
    # already on the graph.
    maybe_ezr = None
    if isinstance(cmd, PleaseInstantiateDistinguishable):
        if "origin_uid" in cmd:
            if cmd.origin_uid in gs:
                maybe_ezr = gs[cmd.origin_uid]
        elif isinstance(cmd.atom, PleaseInstantiateValueNode):
            maybe_ezr = find_value_node_in_target(cmd.atom, gs)
        elif isinstance(cmd.atom, PleaseInstantiateDelegate):
            maybe_ezr = delegate_of(cmd.atom, g, False)
        else:
            raise Exception("Shouldn't get here")

    if maybe_ezr is not None:
        # We have an existing item, don't need to action this but do store the
        # found item for looking up wish ids.
        if "internal_ids" in cmd:
            for wish_id in cmd.internal_ids:
                # TODO: immutable version
                context.mapping[wish_id] = maybe_ezr
        # We can discard this command.
        return [], [], context

    # If this was not already on the graph, then leave an indication that this
    # will be generated for anyone reference our wish id.
    if "internal_ids" in cmd:
        for wish_id in cmd.internal_ids:
            context.mapping[wish_id] = wish_id

    if isinstance(cmd.atom, PleaseInstantiateRelation):
        # We attempt to apply this straight away, but we can only do that if the
        # source/target is in the mapping. If they are direct atom refs, we must
        # emit merge cmds for those too.
        lvl2_cmds = []
        lvl1_cmds = []
        rt = cmd.atom["rt"]
        can_apply_now = True
        if isinstance(cmd.atom["source"], AtomRef):
            can_apply_now = False
            source_id,context.gen_id_state = gen_internal_id(context.gen_id_state)
            lvl2_cmds.append(please_instantiate_from_atom_ref(cmd.atom["source"], source_id))
        else:
            if cmd.atom["source"] not in context.mapping:
                can_apply_now = False
            source_id = cmd.atom["source"]
        if isinstance(cmd.atom["target"], AtomRef):
            can_apply_now = False
            target_id,context.gen_id_state = gen_internal_id(context.gen_id_state)
            lvl2_cmds.append(please_instantiate_from_atom_ref(cmd.atom["target"], target_id))
        else:
            if cmd.atom["target"] not in context.mapping:
                can_apply_now = False
            target_id = cmd.atom["target"]

        new_cmd = PleaseInstantiate({"atom": {"rt": rt,
                                              "source": source_id,
                                              "target": target_id}})
        if can_apply_now:
            lvl1_cmds.append(new_cmd)
        else:
            lvl2_cmds.append(new_cmd)

        return lvl1_cmds, lvl2_cmds, context
    else:
        return [cmd], [], context

@func
def cmds_for_terminate(cmd: PleaseTerminate, gs: GraphSlice, context: CmdDispatchContext) -> DispatchOutput:
    if isinstance(cmd.target, WishID):
        if cmd.target not in context["mapping"]:
            # We return this command to the pool of lvl 2 commands to translate.
            return CouldNotProcess
        target = context["mapping"][cmd.target]
    else:
        if cmd.target not in gs:
            target = None
        else:
            target = cmd.target

    # Look to see if we can discard this command because the item is
    # not in the graph slice.
    if target is None:
        # Note: if internal_ids is introduce to PleaseTerminate, then this logic will have to be updated.
        return [], [], context
    return [cmd], [], context

@func
def cmds_for_assign(cmd: PleaseAssign, gs: GraphSlice, context: CmdDispatchContext) -> DispatchOutput:
    if isinstance(cmd.target, WishID):
        if cmd.target not in context["mapping"]:
            # We return this command to the pool of lvl 2 commands to translate.
            return CouldNotProcess
        target = context["mapping"][cmd.target]
        cmd = cmd._get_type()(cmd._value | insert["target"][target] | collect)
    else:
        if cmd.target not in gs:
            raise Exception(f"PleaseAssign for a target ({cmd.target}) which is not in the graph.")

    return [cmd], [], context


default_translation_rules = [
    (PleaseInstantiate, cmds_for_instantiate),
    (PleaseTerminate, cmds_for_terminate),
    (PleaseAssign, cmds_for_assign),
]


##############################
# ** Level1 utils 

def is_command_distinguishable(cmd: PleaseCommandLevel1):
    # Distinguishable commands are those with uids attached, e.g. a ZefRef merged in.
    # Indistinguishable commands are others like ET.Machine, which will be assign different uids later.
    return 

def get_unnamed_command(cmd: PleaseCommandLevel1):
    # All but internal_id must be the same
    wo_id = cmd._value | without[["internal_ids"]] | collect
    return get_uvt_type(cmd)(wo_id)

def get_command_ids(cmd: PleaseCommandLevel1):
    return cmd._value | get["internal_ids"][[]] | collect

def add_ids(cmd: PleaseCommandLevel1, extra_names: List[WishID]):
    if len(extra_names) == 0:
        return cmd
    internal_ids = get_command_ids(cmd) + extra_names
    d = cmd._value | insert["internal_ids"][internal_ids] | collect
    return get_uvt_type(cmd)(d)

# We abstract away the adding of commands so we can consolidate the wish ids
# at the same time. This is in contrast to doing it all at once at the end
# using a group_by or equivalent, with the slight advantage that ordering is
# preserved more easily this way.
# def add_command(cmd: PleaseCommandLevel1, cmd_naming, output_cmds):
#     # TODO: THIS FUNCTION NEEDS TO RECONSIDER WHAT MIGHT BE THE SAME BY USING COLLAPSE VIA NAMES
#     #
#     # Basically we need a bipartite-graph-like structure to resolve unique
#     # objects from various names. As new commands are entered, a
#     # (internally-ided) list of actions is created and a mapping of names to
#     # that list is kept. As either a) multiple names are used to refer to two
#     # different objects via name aliasing, or b) a single name is used to refer
#     # to multiple objects, then these objects need to be reconciled into one
#     # object.
#     #
#     # From another point of view, we deliberate make all objects
#     # distinguishable, in order to track them with user-provided names. A
#     # concrete example would be that Entity(uid="abcd") is already
#     # distinguishable, but ET.Machine would internally be given an id
#     # ET.Machine[InternalID("x1")] which makes it distinguishable.
#     #
#     # However, the recombination rules are different. Two internal ids can be
#     # merged together if it turns out they are meant to be the same. An "allowed
#     # collapse". But two EUIDs cannot be collapsed together. Maybe this should
#     # be called an "external constraint". But it is really a policy we are
#     # choosing, to allow cases like: ET.X[1], ET.X[2], ET.X[1][2] instead of
#     # throwing an error.

#     # Being careful to allow for a immutable+optimise switch here. But
#     # mutating for speed for now.
#     if not isinstance(cmd, PleaseCommandLevel1Distinguishable):
#         # Indistinguishable commands should just be entered as is.
#         output_cmds.append(cmd)
#         return cmd_naming, output_cmds
#     # Distinguishable commands should be combined.
#     cmd_wo_id = get_unnamed_command(cmd)
#     if cmd_wo_id in cmd_naming:
#         # TODO: immutable version, replace with "update" on the whole dictionary.
#         cmd_naming[cmd_wo_id] = add_ids(cmd_naming[cmd_wo_id], get_command_ids(cmd))
#     else:
#         output_cmds.append(cmd_wo_id)
#         cmd_naming[cmd_wo_id] = cmd

#     return cmd_naming, output_cmds

def please_instantiate_from_atom_ref(atom_ref: AtomRef, maybe_iid=None):

    d = {"atom": raet,
         "origin_uid": origin_uid(atom_ref)}
    if maybe_iid is not None:
        d["internal_ids"] = [maybe_iid]

    return PleaseInstantiate(d)
                              



def distinguish_cmd(cmd):
    # Returns the distinguishable representation of an object, without any other context known.
    #
    # Entity(ET.X, uid="abcd")[V.name] -> Entity(ET.X, uid="abcd")
    # ET.Machine[V.name] -> ET.Machine[InternalID(n+1)]
    # 
    # This is used to fill in the list of unique operations, which will be pointed at by other names later on.

    cmd_distinguish,names = cmd | match[
        distinguish_rules,
        (Any, not_implemented_error["Unknown cmd type"]),
    ] | collect

    return cmd_distinguish,names

def recombination(cmd1, cmd2):
    # When two objects have been identified as being the same, this is the procedure (or problem-aware point) for combining the two objects together.
    #
    # (ET.Machine[InternalID(1)], ET.Machine[InternalID(2)]) -> ET.Machine[InternalID(n+1)]
    # (Entity(ET.Machine, uid="abcd"), ET.Machine[InternalID(1)]) -> Entity(ET.Machine, uid="abcd")
    # (ET.X[InternalID(1)], ET.Y[InternalID(2)]) -> ERROR

    new_cmd = cmd | match[
        recombination_rules,
        (Any, not_implemented_error["Unknown cmd pair type"]),
    ] | collect

    return new_cmd
    

def add_command(cmd: PleaseCommandLevel1,
                name_mapping: Dict[WishID][Set[PleaseCommandLevel1Distinguishable]],
                cmd_names: Dict[PleaseCommandLevel1Distinguishable][Set[WishID]]):
    # Fully mutating style, but function signature at least allows for potential
    # of immutable implementation.

    # name_mapping - maps of names to cmds (cmds identified as values).
    #              - this uses a set of cmds, even though the final result from
    #                this function should only ever be sets of 1 cmd, i.e.
    #                unique. This is just for the intermediate values.
                
    # cmd_names - functions as both list of cmds and sets of names belonging to cmds.
    
    cmd_distinguish,names = distinguish_cmd(cmd)

    cmd_set.add(cmd_distinguish)

    names_needing_reconcilliation = []

    for name in names:
        name_mapping[name].add(cmd_distingiush)
        if len(name_mapping[name]) >= 2:
            names_needing_reconcilliation += [name]


    # Now we build up the connected subgraph of aliased name/cmd combinations.
    #
    # We could do this at the end of all commands instead. The advantage here is
    # that we have a very easy way to find the subgraph. Whereas doing it at the
    # end would require proper connected_components analysis.

    if len(names_needing_reconcilliation) != 0:
        from functools import reduce
        from operator import or_

        all_sets = [name_mapping[name] for name in names_needing_reconcilliation]
        all_cmds = reduce(_or, all_sets)

        resolve_connected_group(all_cmds)

    return name_mapping, cmd_names


def resolve_connected_group(group_cmds, name_mapping, cmd_names):
    all_names = reduce(_or, [cmd_names[cmd] for cmd in all_cmds])

    new_cmd = reduce(recombination, all_cmds)

    for cmd in all_cmds:
        del cmd_names[cmd]

    cmd_names[new_cmd] = all_names
    for name in all_names:
        name_mapping[name] = {new_cmd}