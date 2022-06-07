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


# These functions are here for __main__.py and whoever wants to write their own
# main equivalent.



def create_schema_graph(schema_string, hooks_string=None):
    g_schema = Graph()
    if hooks_string is not None:
        # Prep the schema graph with the hooks, so that the schema nodes can point directly at these
        prepare_hooks(g_schema, args.hooks_file)

    schema_gql = args.schema_file | read_file | run | get["content"] | collect
    parsed_dict = parse_partial_graphql(schema_gql)
    commands = json_to_minimal_nodes(parsed_dict, g_schema)
    r = commands | transact[g_schema] | run
    root = r["root"]

    return root


def prepare_hooks(graph, hooks_file):
    with open(hooks_file) as f:
        code = compile(f.read(), hooks_file, "exec")
        globs = {"g": graph}
        locs = {}
        exec(code, globs, locs)

    # We could try and be fancy here with autodetection of names - but for the initial test let's just make the user do everything, including tagging nodes.