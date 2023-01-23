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
report_import("zef.core.graph_additions.command_ordering")

from .common import *

def order_level1_commands(commands: List[PleaseCommandLevel1], gs: GraphSlice):
    # Form dependency graph using WishIDs and Refs
    #
    # Take partial ordering from graph, using order of original commands to decide ties.

    def simple_key(cmd):
        if isinstance(cmd, PleaseInstantiate):
            if isinstance(cmd.atom, PleaseInstantiateEntity | PleaseInstantiateAttributeEntity):
                return 0
            elif isinstance(cmd.atom, PleaseInstantiateRelation):
                return 1
        elif isinstance(cmd, PleaseAssign):
            return 10
        return 999

    commands = commands | sort[simple_key] | collect


    # TODO: Build a dependency graph
    if True:
        new_commands = []

        from collections import defaultdict
        cmd_mapping = {}
        ready_cmds = []
        dependencies = {}
        dependents = defaultdict(lambda: [])

        # Build graph
        for cmd in commands:
            name,deps = (cmd,gs) | match_rules[[
                *command_dep_rules,
                (Any, not_implemented_error["Command unknown for dependency ordering"]),
            ]] | collect

            # Special case for no id - can't have any dependents
            if name is None:
                assert len(deps) == 0
                new_commands += [cmd]
                continue

            cmd_mapping[name] = cmd

            if len(deps) == 0:
                ready_cmds += [name]
            else:
                dependencies[name] = deps
                for dep in deps:
                    dependents[dep] += [name]

        # Work through commands that have their deps done
        while len(ready_cmds) > 0:
            name = ready_cmds.pop(0)
            new_commands += [cmd_mapping[name]]

            # Free up other commands that dependended on this one
            for other in dependents[name]:
                dependencies[other].remove(name)
                if len(dependencies[other]) == 0:
                    ready_cmds += [other]

        # We only went through all "ready" commands. There might have been some
        # that never became ready in an invalid wish case.
        if len(new_commands) != len(commands):
            reverse_mapping = {b:a for a,b in cmd_mapping.items()}
            print()
            print("======")
            for cmd in commands:
                if cmd in new_commands:
                    continue
                name = reverse_mapping[cmd]
                print("----")
                print(name)
                print(cmd)
                print("--")
                for dep in dependencies[name]:
                    print(dep)
                
            print()
            raise Exception("Unable to order all commands")

        commands = new_commands
            
    assert are_commands_ordered(commands, gs)
    # TODO: Temporary for testing
    return commands

def are_commands_ordered(commands: List[PleaseCommandLevel1], gs: GraphSlice):
    # Instead of generating a list, this should really just check that the
    # dependencies satisfy their ordering requirements.

    # Build a dependency graph
    cmd_mapping = {}
    dependencies = {}

    for ind,cmd in enumerate(commands):
        name,deps = (cmd,gs) | match_rules[[
            *command_dep_rules,
            (Any, not_implemented_error["Command unknown for dependency ordering"]),
        ]] | collect

        # Special case for no id - can't have any dependents
        if name is None:
            continue

        cmd_mapping[name] = ind

        if len(deps) > 0:
            dependencies[name] = deps

    # Check the order in the graph
    for name,deps in dependencies.items():
        for dep in deps:
            if cmd_mapping[dep] > cmd_mapping[name]:
                return False

    return True




def deps_instantiate(cmd, gs):
    if isinstance(cmd.atom, PleaseInstantiateRelation):
        deps = []
        if not isinstance(cmd.atom["source"], EternalUID | DelegateRef | WrappedValue) or cmd.atom["source"] not in gs:
            deps += [("Instantiate", cmd.atom['source'])]
        if not isinstance(cmd.atom["target"], EternalUID | DelegateRef | WrappedValue) or cmd.atom["target"] not in gs:
            deps += [("Instantiate", cmd.atom['target'])]
    else:
        deps = []
        
    if isinstance(cmd.atom, PleaseInstantiateDelegate | PleaseInstantiateValueNode):
        name = ("Instantiate", cmd.atom)
    elif "origin_uid" in cmd:
        name = ("Instantiate", cmd.origin_uid)
    elif "internal_ids" in cmd:
        name = ("Instantiate", single(cmd.internal_ids))
    else:
        name = None
    return name,deps

def deps_terminate(cmd, gs):
    name = ("Terminate", cmd.target)
    deps = []
    return name, deps

def deps_assign(cmd, gs):
    name = ("Assign", cmd.target)
    deps = []
    if not isinstance(cmd.target, EternalUID) or cmd.target not in gs:
        deps += [("Instantiate", cmd.target)]
    return name,deps

command_dep_rules = [
    (PleaseInstantiate, deps_instantiate),
    (PleaseTerminate, deps_terminate),
    (PleaseAssign, deps_assign),
]