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


#=============================================================================== GraphQL handlers =========================================================================================================
from .fx_types import Effect, FX
from .._ops import *
from .http import send_response, permit_cors, middleware, middleware_worker, fallback_not_found
from ariadne import graphql_sync
import os
import json

from ..logger import log


def graphql_start_server_handler(eff: Effect):
    """Example
    {
        "type": FX.GraphQL.StartServer,
        "port": 5001, (default=5000) 
        "schema_root": z_schema,
        "path": "/gql", (default="/gql")
        "playground_path": "/", (default=None)
        "open_browser": False (default=False)
    } | run
    """


    from ...gql.generate_gql_api import make_api
    schema = make_api(eff['schema_root'])
    port = eff.get("port", 5000)

    gql_path = eff.get("path", "/gql")
    playground_path = eff.get("playground_path", None)
    open_browser = eff.get("open_browser", False)

    if not gql_path.startswith("/"):
        raise Exception("GQL path must start with /")
    if playground_path is not None and not playground_path.startswith("/"):
        raise Exception("Playground path must start with /")

    def resolve_gql_query(req, port=port, playground_path=playground_path, gql_path=gql_path):
        import copy
        req = copy.deepcopy(req)
        if playground_path is not None and req["path"] == playground_path:
            from ... import gql
            import os
            path = os.path.join(*gql.__path__, "playground.html")
            with open(path) as file:
                body = file.read()
            body = body.replace("ZEF_PORT", str(port))
            body = body.replace("ZEF_GQL_PATH", gql_path)
            req["response_body"] = body
        elif req["path"] == gql_path:
            success, result = graphql_sync(schema, json.loads(req["request_body"]))
            req["response_body"] = json.dumps(result)
        return req


    logging = eff.get("logging", False)
    http_r = Effect({
        **eff,     # Verify that this is ok after change: the dict was mutated previously (using pop). There may be more key-value pairs in there now.
        'type': FX.HTTP.StartServer,
        'port': port,
        # 'pipe_into': map[resolve_gql_query] | subscribe[run]
        # This doesn't work with rxops atm... need to do it all in a custom map[] function
        # 'pipe_into': middleware[[permit_cors, resolve_gql_query, send_response]]
        'pipe_into': map[middleware_worker[permit_cors, resolve_gql_query, fallback_not_found, send_response]] | subscribe[run],
        'logging': logging,
    }) | run

    log.info(f"Started GQL server at http://localhost:{port}{gql_path}")
    if playground_path is not None:
        playground_url = f"http://localhost:{port}{playground_path}"
        log.info(f"Playground available at {playground_url}")
        if open_browser:
            import webbrowser
            webbrowser.open(playground_url)

    return http_r
    
    
def graphql_stop_server_handler(eff: dict):    
    return run({**eff, 'type': FX.HTTP.StopServer})

    
def graphql_start_playground_handler(eff: dict):
    """Example
    Effect({
        "type": FX.GraphQL.StartPlayground,
        "schema_root": z_schema,
        "port": 5001, (default=5000)
        "path": "/gql", (default="/gql")
        "playground_path": "/", (default="/")
        "open_browser": True (default=True)
    }) | run
    """   
    schema = eff['schema_root']
    port = eff.get("port", 5000)

    gql_path = eff.get("path", "/gql")
    playground_path = eff.get("playground_path", "/") # It is required!
    open_browser = eff.get("open_browser", True)

    if not gql_path.startswith("/"):
        raise Exception("GQL path must start with /")
    if not playground_path.startswith("/"):
        raise Exception("Playground path must start with /")

    http_r = {
        "type": FX.GraphQL.StartServer,
        "port": port,
        "schema_root": schema,
        "path": gql_path,
        "playground_path": playground_path,
        "open_browser": open_browser,
    } | run
    
    return http_r
    
    
def graphql_stop_playground_handler(eff: dict):    
    return run({**eff, 'type': FX.HTTP.StopServer})
    
