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
report_import("zef.core.graph_additions.command_recombination")

from .common import *


def recombination(cmd1, cmd2):
    # When two objects have been identified as being the same, this is the procedure (or problem-aware point) for combining the two objects together.
    #
    # (ET.Machine[InternalID(1)], ET.Machine[InternalID(2)]) -> ET.Machine[InternalID(n+1)]
    # (Entity(ET.Machine, uid="abcd"), ET.Machine[InternalID(1)]) -> Entity(ET.Machine, uid="abcd")
    # (ET.X[InternalID(1)], ET.Y[InternalID(2)]) -> ERROR

    # print("Doing recomb for", cmd1, cmd2)
    return (cmd1, cmd2) | match[[
        *recombination_rules,
        (Any, not_implemented_error["Unknown cmd pair type"]),
    ]] | collect
    
def recombination_unchanged(_):
    return True,[],[]

def recombination_invalid(arg):
    x,y = arg
    raise Exception(f"Can't have two commands for the same name which contradict one another: {x} and {y}")

def recombination_checks(cmds):
    cmd1,cmd2 = cmds
    
    def get_target(x):
        if isinstance(x, PleaseInstantiate):
            return x._value.get("origin_uid", None)
        else:
            if isinstance(x.target, AtomRef):
                return x.target
            return None
    target1 = get_target(cmd1)
    target2 = get_target(cmd2)

    if target1 is not None and target2 is not None:
        if target1 != target2:
            raise Exception("Two commands refer to different origin uids but have the same name")
        target = target1
    elif target1 is None:
        target = target2
    else:
        target = target1

    if cmd1._get_type() == cmd2._get_type():
        if isinstance(cmd1, PleaseInstantiate):
            if target is not None:
                # At least one of the commands has an origin uid - we take this version
                if "origin_uid" in cmd1:
                    new_cmd = cmd1
                else:
                    new_cmd = cmd2
            else:
                # Neither command has an origin uid - so just take the first one (will be a given name)
                new_cmd = cmd1

            new_cmds = [new_cmd]
            later_cmds = []

            if isinstance(cmd1.atom, PleaseInstantiateRelation):
                assert isinstance(cmd2.atom, PleaseInstantiateRelation)
                assert cmd1.atom["rt"] == cmd2.atom["rt"]
                if cmd1.atom["source"] != cmd2.atom["source"]:
                    later_cmds += [PleaseAlias(ids=[force_as_id(cmd1.atom["source"]), force_as_id(cmd2.atom["source"])])]
                if cmd1.atom["target"] != cmd2.atom["target"]:
                    later_cmds += [PleaseAlias(ids=[force_as_id(cmd1.atom["target"]), force_as_id(cmd2.atom["target"])])]
            else:
                assert cmd1.atom == cmd2.atom, f"Atoms are different between two instantiations: {cmd1}, {cmd2}"

            return False, new_cmds, later_cmds
        elif isinstance(cmd1, PleaseAssign):
            if cmd1.value != cmd2.value:
                raise Exception("Two assigns have different values")
            # Any of the ids will do - they will be relabelled anyway.
            _,names = distinguish_assign(cmd1)
            return False, [PleaseAssign(target=names[0], value=cmd1.value)], []
        else:
            raise NotImplementedError(f"TODO: {cmd1._get_type()}")
    else:
        # Two different commands, just leave as is
        return True, [], []

def recombination_be_source_target(cmds):
    cmd1,cmd2 = cmds

    if not cmd1.combine and not cmd2.combine:
        return True, [], []

    if cmd1.rt == cmd2.rt:
        if cmd1.rel_id == cmd2.rel_id:
            # This is basically the same thing, so just simplify
            return False, [cmd1], []

        cmd_alias = PleaseAlias(ids=[cmd1.rel_id, cmd2.rel_id])
        return False, [cmd1], [cmd_alias]

    return True, [], []

def recombination_alias(cmds):
    cmd1,cmd2 = cmds

    return False, [PleaseAlias(ids=distinct(cmd1.ids + cmd2.ids))], []
            

recombination_rules = [
    (Tuple[PleaseBeSource, PleaseBeSource], recombination_be_source_target),
    (Tuple[PleaseBeTarget, PleaseBeTarget], recombination_be_source_target),
    # (Tuple[PleaseBeSource, ~PleaseBeSource], recombination_pass_through),
    # (Tuple[PleaseBeTarget, ~PleaseBeTarget], recombination_pass_through),
    (Tuple[PleaseAlias, PleaseAlias], recombination_alias),
    (Tuple[PleaseAlias, ~PleaseAlias], recombination_unchanged),
    (Tuple[~PleaseAlias, PleaseAlias], recombination_unchanged),
    (Tuple[PleaseMustLive, PleaseMustLive], first | wrap_list),
    (Tuple[~PleaseTerminate, ~PleaseTerminate], recombination_checks),
    (Tuple[~PleaseTerminate, PleaseTerminate], recombination_invalid),
    (Tuple[PleaseTerminate, ~PleaseTerminate], recombination_invalid),
]

