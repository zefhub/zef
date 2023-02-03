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


SetFieldAction = LazyValue[Any][ZefOp[set_field]]
# TODO: More stuff here
SetFieldActionValid = LazyValue[AtomClass | AllIDs][ZefOp[set_field]]

##########################################
# * Level 1 generation
#----------------------------------------

class CouldNotProcess: pass

WishMapping = Dict[WishID][EZefRef|WishID]
# CmdDispatchContext = UserValueType("Level1CmdDispatchContext",
#                                    Dict,
#                                    # Pattern[{"mapping": WishMapping, "gen_id_state": GenIDState}]
#                                    Pattern[{"gen_id_state": GenIDState}]
#                                    )
CmdDispatchContext = Pattern[{"gen_id_state": GenIDState}]

DispatchOutput = SetOf(CouldNotProcess) | Tuple[List[PleaseCommandLevel1], List[PleaseCommandLevel2], CmdDispatchContext]

# Rules should follow the signature below, but we can't specify that just yet.
# TranslationFunc = Func[PleaseCommandLevel2, CmdDispatchContext][DispatchOutput]
TranslationFunc = Any


from .command_preparation import dispatch_preparation
from .command_multi_rules import relabel_cmds, cull_unnecessary_cmds, distinguish_cmd, resolved_variables_from_aliases
from .command_recombination import recombination
from .command_ordering import order_level1_commands




# Really want to use Subtype[PleaseCommandLevel2] instead of ValueType
def generate_level1_commands(commands: List[PleaseCommandLevel2], gs: GraphSlice) -> Level1CommandInfo:
    # Blind generation, using no culling at this stage, merely resolving PleaseRun commands

    # context = CmdDispatchContext({
    #     "gen_id_state": generate_initial_state("lvl1"),
    # })
    context = {"gen_id_state": generate_initial_state("lvl1")}
    
    # We have to have multiple passes at the commands as each command can slowly
    # describe different wish ids.

    # Resolve the arbitrary please run commands and object notation
    todo = list(commands)
    prepared_cmds = []
    
    # print("*** Start preparation loop")
    while len(todo) > 0:
        cmd = todo.pop(0)
       
        ready_cmds, new_todo, context = dispatch_preparation(cmd,gs,context)
        # print()
        # print("===")
        # print(ready_cmds)
        # print(new_todo)
        # print()

        prepared_cmds += ready_cmds
        todo += new_todo

    # print()
    # print("AFTER PREPARATION RUN")
    # for cmd in prepared_cmds:
    #     print(cmd)
    # print()

    # Now we know all after_please_run commands are level1 commands, but we need
    # to include references where there are relations being affected, as these
    # rely on their source/target remaining alive. This is also where we would
    # include dependencies on other nodes potentially terminated via hostage
    # flags.
    out_cmds = []

    def add_must_live_cmds(src_or_trg):
        if isinstance(src_or_trg, WishID):
            id = src_or_trg
        elif isinstance(src_or_trg, Delegate):
            return
        elif isinstance(src_or_trg, WrappedValue):
            return
        elif isinstance(src_or_trg, Relation):
            add_must_live_cmds(source(src_or_trg))
            add_must_live_cmds(target(src_or_trg))
            id = origin_uid(src_or_trg)
        elif isinstance(src_or_trg, AtomClass):
            id = origin_uid(src_or_trg)
        elif isinstance(src_or_trg, EternalUID):
            id = src_or_trg
            z = find_rae_in_target(id, gs)
            if isinstance(z, Relation):
                add_must_live_cmds(source(z))
                add_must_live_cmds(target(z))
        elif isinstance(src_or_trg, BlobPtr):
            id = origin_uid(src_or_trg)
            if isinstance(src_or_trg, Relation):
                add_must_live_cmds(source(src_or_trg))
                add_must_live_cmds(target(src_or_trg))
        else:
            raise Exception(f"Shouldn't get here: {src_or_trg}")
        nonlocal out_cmds
        out_cmds += [PleaseMustLive({"target": id})]
            
    for cmd in prepared_cmds:
        if isinstance(cmd, PleaseInstantiate) and isinstance(cmd.atom, PleaseInstantiateRelation):
            add_must_live_cmds(cmd.atom["source"])
            add_must_live_cmds(cmd.atom["target"])
        elif isinstance(cmd, PleaseTag):
            if isinstance(cmd.target, RelationRef):
                add_must_live_cmds(source(cmd.target))
                add_must_live_cmds(target(cmd.target)) 
        elif isinstance(cmd, PleaseInstantiate | PleaseAssign | PleaseAlias | PleaseTerminate | PleaseMustLive | PleaseBeSource | PleaseBeTarget):
            pass
        # elif hostage detection things...
        else:
            raise Exception(f"Shouldn't get here in realising implicit constraints: {cmd}")

        out_cmds += [cmd]

    # print()
    # print("OUT CMDS from implicit constraints")
    # for cmd in out_cmds:
    #     print(cmd)
    # print()

    # Now we know all out_cmds are level1 commands, including all possible
    # dependents (e.g. relation source/target references). But we don't know if
    # these are valid or can be simplified.

    resolved_variables = {}
    cur_cmds = out_cmds
    while True:
        new_cmds,simplify_aliases,did_something = validate_and_simplify_lvl1_cmds(cur_cmds)
        if not did_something:
            break
        cur_cmds = relabel_cmds(new_cmds, simplify_aliases)
        resolved_variables = resolved_variables | merge[resolved_variables_from_aliases(simplify_aliases)] | collect
        # print()
        # print("NAMED OUTPUT CMDS")
        # for cmd in cur_cmds:
        #     print(cmd)
        # print()

    named_output_cmds = cur_cmds


    culled_cmds,cull_aliases = cull_unnecessary_cmds(named_output_cmds, gs)
    # TODO: Apply aliases
    culled_cmds = relabel_cmds(culled_cmds, cull_aliases)
    resolved_variables = resolved_variables | merge[resolved_variables_from_aliases(cull_aliases)] | collect

    # print()
    # print("CULLED CMDS")
    # for cmd in culled_cmds:
    #     print(cmd)
    # print("---")

    ordered_cmds = order_level1_commands(culled_cmds, gs)

    # print()
    # print("ORDERED CMDS")
    # for cmd in ordered_cmds:
    #     print(cmd)
    # print("---")

    # print()
    # print("RESOLVED VARIABLES:")
    # for a,b in resolved_variables.items():
    #     print(a,b)
    # print("---")

    return Level1CommandInfo(gs=gs,
                             cmds=ordered_cmds,
                             resolved_variables=resolved_variables)


def validate_and_simplify_lvl1_cmds(cmds_in):
    # We need to reduce the ambiguity on naming of commands, so we build up a
    # bipartite graph of which names/cmds refer to each other.
    #
    # Note that relations only need to refer to their own relation, not their
    # source/target. Any validation tracking has been taken care of with the
    # PleaseMustLive on the source/targets already and any relabelling will
    # happen after we first handle the simplification process.

    # WAIT! What happens if two relations are being merged in and they have
    # different source/targets. We can reduce the distinguishable part to the
    # uid/internal id only, where internal id wins (just as for entities). But
    # in recombination, checks will be made on the source/target and an
    # invalidation error will be throw at that point.

    did_something = False

    from collections import defaultdict
    name_mapping = defaultdict(lambda: set())
    cmd_mapping = defaultdict(lambda: set())

    for cmd in cmds_in:
        cmd_distinguish,names = distinguish_cmd(cmd)
        assert len(names) > 0, f"Command without any names would otherwise be silently ignored! {cmd}"
        if cmd != cmd_distinguish:
            did_something = True
        for name in names:
            name_mapping[name].add(cmd_distinguish)
        cmd_mapping[cmd_distinguish].update(names)
            

    # print()
    # print("NAME MAPPING")
    # for name,cmds in name_mapping.items():
    #     print(name)
    #     for cmd in cmds:
    #         print("  -", cmd)
    # print("---")

    # Find any connected components to simplify multiple names.
    relabelled_names = {}
    new_name_mapping = {}

    todo_names = set(name_mapping.keys())
    components = []
    while len(todo_names) > 0:
        this_todo_names = {todo_names.pop()}
        comp_names = set()
        while len(this_todo_names) > 0:
            name = this_todo_names.pop()
            comp_names.add(name)

            hop2_names = {x for cmd in name_mapping[name] for x in cmd_mapping[cmd]}
            assert len(hop2_names) > 0
            hop2_names = hop2_names & todo_names
            this_todo_names |= hop2_names
            todo_names ^= hop2_names

        if len(comp_names) >= 2:
            components += [comp_names]
        else:
            name = comp_names.pop()
            new_name_mapping[name] = name_mapping[name]

    # Only components (i.e. name sets of >= 2 names) need further processing
    for comp_names in components:
        did_something = True
        chosen_name = id_preference(comp_names)
        for name in comp_names:
            # print("CHOSEN NAME COMPARE:", name, chosen_name, bool(name == chosen_name), bool(name != chosen_name))
            if name != chosen_name:
                relabelled_names[name] = chosen_name

        all_cmds = {cmd for name in comp_names for cmd in name_mapping[name]}
        new_name_mapping[chosen_name] = all_cmds

    # Now for each name, validate and recombine the commands into the smallest set.
    #
    # This means validating against every pair. And reducing every pair.
    #
    # Can reduce with every pair and expect either 1 or 0 outputs. 1 output
    # means it is a reduction of the pair and an output of None means that
    # the pair were distinct from one another. Any errors will just be
    # raised as exceptions.

    # print()
    # print("NEW NAME MAPPING")
    # for name,cmds in new_name_mapping.items():
    #     print(name)
    #     for cmd in cmds:
    #         print("  -", cmd)
    # print("---")

    # This might not be the best way to do it, but it will work and I can
    # think about how else to approach this.
    all_cmds = []

    for name in new_name_mapping:
        todo_cmds = set(new_name_mapping[name])
        done_cmds = []
        while len(todo_cmds) > 0:
            new_cmd = todo_cmds.pop()
            for i in range(len(done_cmds)):
                keep,new_cmds,later_cmds = recombination(done_cmds[i], new_cmd)
                if len(new_cmds) > 0:
                    did_something = True
                    todo_cmds |= set(new_cmds)
                if len(later_cmds) > 0:
                    did_something = True
                    # These will be handled in the next iteration
                    all_cmds += later_cmds
                if not keep:
                    did_something = True
                    done_cmds.pop(i)
                    break
            else:
                done_cmds += [new_cmd]
        all_cmds += done_cmds

    # print()
    # print("ALL CMDS")
    # for cmd in all_cmds:
    #     print(cmd)
    # print("---")


    # print()
    # print("NAMES")
    # print("=====")
    # print(relabelled_names)
    # print()

    # It's possible that we did something inadventently by the implicit unique in set.
    if len(all_cmds) != len(cmds_in):
        did_something = True

    return all_cmds, relabelled_names, did_something