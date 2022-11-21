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


from ... import *
from ...ops import *

from ...core.logger import log

from .schema_file_parser import parse_partial_graphql, json_to_minimal_nodes

def create_schema_graph(schema_string, hooks_string=None):
    """Create a Zef representation of the GraphQL schema given in `schema_string`
along with the zef functions given in hooks_string on a new blank graph `g`.

    The graph `g` is marked keep alive. If the server is manually restarted in
    the same python session multiple times, then this should be removed by the
    calling function.
    """

    g_schema = Graph()
    set_keep_alive(g_schema, True)

    if hooks_string is not None:
        # Prep the schema graph with the hooks, so that the schema nodes can point directly at these
        prepare_hooks(g_schema, hooks_string)

    parsed_dict = parse_partial_graphql(schema_string)
    commands = json_to_minimal_nodes(parsed_dict, g_schema)
    r = commands | transact[g_schema] | run
    root = r["root"]

    return root


def prepare_hooks(graph, hooks_string):
    with Transaction(graph):
        # Note that we save the string to a temporary file, because @func requires
        # getsource which cannot identify the source from inside an arbitrary
        # string.
        import tempfile, os
        fd,path = tempfile.mkstemp(prefix="SimpleGQL_hooks_", suffix=".py")
        try:
            with open(fd, "w") as file:
                file.write(hooks_string)
            code = compile(hooks_string, path, "exec")
            globs = {"g": graph}
            exec(code, globs, globs)
        finally:
            os.unlink(path)

        # We could try and be fancy here with autodetection of names - but for the initial test let's just make the user do everything, including tagging nodes.

        # Going to try autodetecting any zef functions on the graph
        funcs = graph | now | all[ET.ZEF_Function] | collect

        nodup_and_dup_funcs = (funcs
                    | group_by[F.OriginalName]
                    | group_by[second
                                | length
                                | greater_than_or_equal[2]]
                            [[False,True]]
                    | func[dict]
                    | collect)

        nodups = nodup_and_dup_funcs[False] | map[apply[first, second | single]] | collect
        dups = nodup_and_dup_funcs[True] | map[first] | collect

        if len(dups) > 0:
            log.error("There are zef functions that have duplicate names on the schema graph. I can't autotag these functions and will ignore them.", dups=dups)

        for name,z in nodups:
            z | tag[name] | graph | run