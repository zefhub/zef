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

from ... import *
from ...ops import *
from .main import create_schema_graph
from .server2 import start_server

from ...core.logger import log

import os
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--schema-file", type=str, metavar="schema_file", default=os.environ.get("SIMPLEGQL_SCHEMA_FILE", None),
                    help="The graphql schema file.")
parser.add_argument("--data-tag", type=str, metavar="data_tag", default=os.environ.get("SIMPLEGQL_DATA_TAG", None),
                    help="The tag of the Zef graph with the data (not the schema).")
parser.add_argument("--scratch", action="store_true", default=(False if "SIMPLEGQL_SCRATCH" in os.environ else None),
                    help="Use a temporary disposable local graph instead of persisting it on ZefHub.")
parser.add_argument("--port", type=int, default=int(os.environ.get("SIMPLEGQL_PORT", "443")))
parser.add_argument("--bind", type=str, default=os.environ.get("SIMPLEGQL_BIND", "0.0.0.0"),
                    help="IP address to bind to.")
parser.add_argument("--no-host-role", action="store_false", dest="host_role", default=(False if "SIMPLEGQL_NO_HOST_ROLE" in os.environ else True),
                    help="Whether the server should acquire host role on the data graph.")
parser.add_argument("--create", action="store_true", default=(True if "SIMPLEGQL_CREATE" in os.environ else None),
                    help="If the data graph does not exist, then create a new blank graph.")
parser.add_argument("--hooks", type=str, dest="hooks_file", default=os.environ.get("SIMPLEGQL_HOOKS_FILE", None),
                    help="If present, a python file containing the hooks to make available for the schema file")
parser.add_argument("--init-hook", type=str, dest="init_hook", default=os.environ.get("SIMPLEGQL_INIT_HOOK", None),
                    help="If present, a function in the hooks file to run on initialisation of the data graph")
parser.add_argument("--debug-level", type=int, dest="debug_level", default=os.environ.get("SIMPLEGQL_DEBUG_LEVEL", "0"),
                    help="The amount of debug messages to output")
args = parser.parse_args()

if args.data_tag is not None and args.scratch:
    print("Can't specify both a data tag and scratch")
    raise SystemExit(4)
if args.data_tag is None and not args.scratch:
    print("Need one of data-tag or scratch specified.")
    raise SystemExit(4)


schema_gql = args.schema_file | read_file | run | get["content"] | collect
if args.hooks_file is not None:
    hooks_string = args.hooks_file | read_file | run | get["content"] | collect
    log.info(f"Going to read hooks from '{args.hooks_file}'")
else:
    hooks_string = None

root = create_schema_graph(schema_gql, hooks_string)
log.info(f"Created schema graph from '{args.schema_file}'")

if args.data_tag is not None:
    guid = lookup_uid(args.data_tag)
    if guid is None:
        if not args.create:
            print("Graph is not known to us, and create is False. Exiting.")
            raise SystemExit(2)
        g_data = Graph(True)
        try:
            g_data | tag[args.data_tag] | run
        except:
            print("Unable to create and tag graph with '{args.data_tag}'. Maybe this tag is already taken by another user?")
            raise SystemExit(2)
        log.info("Created data graph with tag", tag=args.data_tag)
    else:
        g_data = Graph(args.data_tag)
        log.info("Loaded existing data graph")
else:
    assert args.scratch
    g_data = Graph()
    log.info("Created blank scratch data graph")

if args.host_role and not args.scratch:
    try:
        g_data | take_transactor_role | run
    except:
        print("""
Unable to obtain host role for data graph. Either stop other processes from having host role on this graph (`g | release_transactor_role | run` in that process) or run this server with `--no-host-role`).
        
Note that running without the host role is currently dangerous as mutations do not currently verify pre-conditions.
""") 
        raise SystemExit(3)
    log.info("Obtained host role on data graph")

if args.init_hook is not None:
    g_schema = Graph(root)
    if args.init_hook not in now(g_schema):
        log.error("Could not find init hook on schema graph", init_hook=args.init_hook)
        raise SystemExit(4)
    hook_func = g_schema | now | get[args.init_hook] | collect
    hook_func(g_data)
    log.info(f"Ran init hook: {args.init_hook}")
    

server_uuid = start_server(root, g_data,
                           port=args.port,
                           bind_address=args.bind,
                           debug_level=args.debug_level)

import time
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    {
        "type": FX.HTTP.StopServer,
        "server_uuid": server_uuid
    } | run