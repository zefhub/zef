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

from .common_opt import *


def recombination(cmd1, cmd2):
    # When two objects have been identified as being the same, this is the procedure (or problem-aware point) for combining the two objects together.
    #
    # (ET.Machine[InternalID(1)], ET.Machine[InternalID(2)]) -> ET.Machine[InternalID(n+1)]
    # (Entity(ET.Machine, uid="abcd"), ET.Machine[InternalID(1)]) -> Entity(ET.Machine, uid="abcd")
    # (ET.X[InternalID(1)], ET.Y[InternalID(2)]) -> ERROR

    # print("Doing recomb for", cmd1, cmd2)
    if ((is_a_PleaseBeSource(cmd1) and is_a_PleaseBeSource(cmd2)) or
        (is_a_PleaseBeTarget(cmd1) and is_a_PleaseBeTarget(cmd2))):
        return recombination_be_source_target(cmd1, cmd2)
    elif is_a_PleaseAlias(cmd1) and is_a_PleaseAlias(cmd2):
        return recombination_alias(cmd1, cmd2)
    elif is_a_PleaseAlias(cmd1) and not is_a_PleaseAlias(cmd2):
        return recombination_unchanged(cmd1, cmd2)
    elif not is_a_PleaseAlias(cmd1) and is_a_PleaseAlias(cmd2):
        return recombination_unchanged(cmd1, cmd2)
    elif is_a_PleaseMustLive(cmd1) and is_a_PleaseMustLive(cmd2):
        return (False, [cmd1], [])
    elif not is_a_PleaseTerminate(cmd1) and not is_a_PleaseTerminate(cmd2):
        return recombination_checks(cmd1, cmd2)
    elif not is_a_PleaseTerminate(cmd1) and is_a_PleaseTerminate(cmd2):
        return recombination_invalid(cmd1, cmd2)
    elif is_a_PleaseTerminate(cmd1) and not is_a_PleaseTerminate(cmd2):
        return recombination_invalid(cmd1, cmd2)
    else:
        not_implemented_error((cmd1, cmd2), "Unknown cmd pair type")
    
def recombination_unchanged(cmd1, cmd2):
    return True,[],[]

def recombination_invalid(x, y):
    raise Exception(f"Can't have two commands for the same name which contradict one another: {x} and {y}")

def recombination_checks(cmd1, cmd2):
    def get_target(x):
        if is_a_PleaseInstantiate(x):
            return x._value.get("origin_uid", None)
        else:
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

    if cmd1._user_type_id == cmd2._user_type_id:
        if is_a_PleaseInstantiate(cmd1):
            if target is not None:
                # At least one of the commands has an origin uid - we take this version
                if "origin_uid" in cmd1._value:
                    new_cmd = cmd1
                else:
                    new_cmd = cmd2
            else:
                # Neither command has an origin uid - so just take the first one (will be a given name)
                new_cmd = cmd1

            new_cmds = [new_cmd]
            later_cmds = []

            if is_a_PleaseInstantiateRelation(cmd1._value["atom"]):
                assert is_a_PleaseInstantiateRelation(cmd2._value["atom"])
                assert cmd1._value["atom"]["rt"] == cmd2._value["atom"]["rt"]
                if cmd1._value["atom"]["source"] != cmd2._value["atom"]["source"]:
                    later_cmds += [
                        UVT_ctor_opt(PleaseAlias,
                                     dict(ids=[
                                         force_as_id(cmd1._value["atom"]["source"]),
                                         force_as_id(cmd2._value["atom"]["source"])
                                     ]))
                    ]
                if cmd1._value["atom"]["target"] != cmd2._value["atom"]["target"]:
                    later_cmds += [
                        UVT_ctor_opt(PleaseAlias,
                                     dict(ids=[
                                         force_as_id(cmd1._value["atom"]["target"]),
                                         force_as_id(cmd2._value["atom"]["target"])
                                     ]))
                    ]
            else:
                assert cmd1._value["atom"] == cmd2._value["atom"], f"Atoms are different between two instantiations: {cmd1}, {cmd2}"

            return False, new_cmds, later_cmds
        elif is_a_PleaseAssign(cmd1):
            cmd1_droppable = cmd1._value.get("droppable", False)
            cmd2_droppable = cmd2._value.get("droppable", False)
            if cmd1_droppable and not cmd2_droppable:
                return False, [cmd2], []
            elif cmd2_droppable and not cmd1_droppable:
                return False, [cmd1], []
            else:
                if cmd1._value["value"] != cmd2._value["value"]:
                    raise Exception(f"Two assigns have different values: {cmd1} {cmd2}")
                # Any of the ids will do - they will be relabelled anyway.
                from .command_multi_rules_opt import distinguish_assign
                _,names = distinguish_assign(cmd1)
                new_dict = dict(cmd1._value)
                new_dict["target"][names[0]]
                return False, [UVT_ctor_opt(PleaseAssign, new_dict)], []
        else:
            raise NotImplementedError(f"TODO: {cmd1._get_type()}")
    else:
        # Two different commands, just leave as is
        return True, [], []

def recombination_be_source_target(cmd1, cmd2):
    if not cmd1.exact and not cmd2.exact:
        return True, [], []

    # Since either of these are exact, we must be able to combine these together
    # to be the same. Ideally we would be able to account for multiple BeSource
    # or BeTargets at once, but since we're doing this one at a time, we just
    # have to make sure:
    # a) Any non-exact is a subset of any exact
    # b) Two exacts are exactly the same
    #
    # However, note that a subset is tricky - which of the rels is matched up to
    # other rels? So will fail in that case.
    #
    # We'll try and match with sets but this is going to be impossible locally,
    # it requires global information. For now, we'll put them in as if they are
    # ordered lists which will work if the user passes in identical sets twice.
    if cmd1._value["rt"] == cmd2._value["rt"]:
        if len(set(cmd1._value["rel_ids"]) - set(cmd2._value["rel_ids"])) == 0:
            # cmd2 is a subset
            return False, [cmd1], []
        if len(set(cmd2._value["rel_ids"]) - set(cmd1._value["rel_ids"])) == 0:
            # cmd1 is a subset
            return False, [cmd2], []

        if len(cmd1._value["rel_ids"]) != len(cmd2._value["rel_ids"]):
            raise Exception("Can't match up ids blindly")

        # Going to match up ids blindy and hope for the best
        # Note: using lists to preserve order
        alias_cmds = []
        cmd1_diff = [id for id in cmd1._value["rel_ids"] if id not in cmd2._value["rel_ids"]]
        cmd2_diff = [id for id in cmd2._value["rel_ids"] if id not in cmd1._value["rel_ids"]]
        for id1,id2 in zip(cmd1_diff, cmd2_diff):
            alias_cmds += [UVT_ctor_opt(PleaseAlias, dict(ids=[id1,id2]))]
        return False, [cmd1], alias_cmds

    return True, [], []

def recombination_alias(cmd1, cmd2):
    return False, [PleaseAlias(ids=distinct(cmd1.ids + cmd2.ids))], []
            

