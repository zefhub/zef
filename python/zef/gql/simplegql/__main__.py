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
from .server import start_server

from zef.core.logger import log

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("schema_file", type=str, metavar="SCHEMA-FILE",
                    help="The graphql schema file.")
parser.add_argument("data_tag", type=str, metavar="DATA-TAG",
                    help="The tag of the Zef graph with the data (not the schema).")
parser.add_argument("--port", type=int, default=443)
parser.add_argument("--bind", type=str, default="0.0.0.0",
                    help="IP address to bind to.")
parser.add_argument("--no-host-role", action="store_false", dest="host_role",
                    help="Whether the server should acquire host role on the data graph.")
parser.add_argument("--create", action="store_true",
                    help="If the data graph does not exist, then create a new blank graph.")
args = parser.parse_args()

from .schema_file_parser import parse_partial_graphql, json_to_minimal_nodes

g_schema = Graph()
schema_gql = args.schema_file | read_file | run | get["content"] | collect
parsed_dict = parse_partial_graphql(schema_gql)
commands = json_to_minimal_nodes(parsed_dict)
r = commands | transact[g_schema] | run
root = r["root"]
log.info(f"Created schema graph from '{args.schema_file}'")

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

if args.host_role:
    make_primary(g_data)
    log.info("Obtained host role on data graph")

server_uuid = start_server(root, g_data, args.port, args.bind)

import time
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    Effect({
        "type": FX.HTTP.StopServer,
        "server_uuid": server_uuid
    }) | run