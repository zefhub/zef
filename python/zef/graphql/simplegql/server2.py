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

from ...core.fx.http import send_response, permit_cors, middleware, middleware_worker, fallback_not_found, route

from .generate_api2 import generate_resolvers_fcts
from ariadne import graphql_sync

from functools import partial as P
import json

from zef.core.logger import log

def resolve_token(context, request):
    root = context["z_gql_root"]

    header = root | F.AuthHeader | collect
    aud = root | F.AuthAudience | collect
    namespace = root | Outs[RT.AuthNamespace] | single_or[None] | value_or[None] | collect
    algo = root | F.AuthAlgo | collect

    import jwt
    import base64
    headers_as_lower = {k.lower(): v for k,v in request["request_headers"].items()}
    if header.lower() not in headers_as_lower:
        return None
    token_header = headers_as_lower[header.lower()].strip()
    if token_header == '':
        return None

    parts = token_header.split()
    if len(parts) != 2 or parts[0] != "Bearer":
        # raise Exception(f"x-auth-header is of the wrong format ({token_header})")
        if context["debug_level"] >= 0:
            log.error("x-auth-header is of the wrong format", token_header=token_header)
        raise Exception("Invalid auth header")
    token = parts[1]

    if algo == "RS256":
        signing_key = context["jwk_client"].get_signing_key_from_jwt(token)
        auth_result = jwt.decode(token,
                                 signing_key.key,
                                 algorithms=[algo],
                                 audience=aud)
    elif algo == "HS256":
        psk = root | F.AuthPresharedKey | collect
        auth_result = jwt.decode(token, psk, algorithms=[algo],
                                 audience=aud)
    else:
        raise Exception("Shouldn't get here")

    if isinstance(auth_result["aud"], list):
        if aud not in auth_result["aud"]:
            raise Exception("Invalid token for wrong audience")
    elif auth_result["aud"] != aud:
        raise Exception("Invalid token for wrong audience")

    if namespace is None:
        return auth_result
    else:
        return auth_result[namespace]
            
def query(request, context):
    root = context["z_gql_root"]
    if root | has_out[RT.AuthHeader] | collect:
        try:
            auth_context = resolve_token(context, request)
            if auth_context is None and not (root | Outs[RT.AuthPublic] | single_or[None] | value_or[True] | collect):
                raise Exception("No auth and public is False.")
        except Exception as exc:
            if context["debug_level"] >= 0:
                log.error("Auth failed", exc_info=exc)
            return {
                "response_body": "Auth failed",
                "response_status": 400,
                **request
            }

        if context["debug_level"] >= 4:
            log.debug("DEBUG 4: auth_context", auth_context=auth_context)
    else:
        auth_context = None

    if request["method"] == "GET" or request["request_body"].strip() == "":
        # This is to give some kind of response instead of a failure. Helps with
        # health checks.
        return {
            "response_body": "Server waiting for GraphQL requests",
            **request,
        }

    q = json.loads(request["request_body"])

    if context["debug_level"] >= 3:
        log.debug("DEBUG 3: incoming query", q=q)

    # We pass in the graph as a fixed slice, so that the queries can be done
    # consistently.
    start = now()
    if type(q) == list:
        queries = q
    else:
        queries = [q]

    success = True
    out_data = []
    for query in queries:
        this_success,this_data = graphql_sync(
            context["ari_schema"],
            query,
            context_value={"gs": now(context["g_data"]),
                        "auth": auth_context,
                        "debug_level": context["debug_level"],
                        "read_only": context["read_only"]},
        )
        if not this_success:
            if context["debug_level"] >= 0:
                log.error("Failure in GQL query.", data=this_data, q=query, auth_context=auth_context)
            success = False

        out_data += [this_data]


    if type(q) != list:
        out_data = out_data[0]

    if context["debug_level"] >= 1:
        log.debug("Total query time", dt=now()-start)

    response = json.dumps(out_data)
    if context["debug_level"] >= 3:
        log.debug("DEBUG 3: response", response=response)

    return {
        "response_body": response,
        **request
    }

def start_server(z_gql_root,
                 g_data,
                 *,
                 port=443,
                 bind_address="0.0.0.0",
                 logging=True,
                 debug_level=0,
                 read_only=False,
                 ):

    gql_dict = generate_resolvers_fcts(z_gql_root)
    from .. import make_graphql_api
    ari_schema = make_graphql_api(gql_dict)

    from logging import getLogger
    if debug_level < 4:
        logger = getLogger("ariadne").disabled = True
        log.debug("Disabled ariadne logging")
    
    context = {
        "z_gql_root": z_gql_root,
        "g_data": g_data,
        "ari_schema": ari_schema,
        "debug_level": debug_level,
        "read_only": read_only,
    }

    if z_gql_root | has_out[RT.AuthJWKURL] | collect:
        url = z_gql_root | F.AuthJWKURL | collect

        from jwt import PyJWKClient
        context["jwk_client"] = PyJWKClient(url)

    additional_routes = create_additional_routes(z_gql_root | Outs[RT.Route] | collect, context)
    main_routes = z_gql_root | Outs[RT.GraphQLRoute] | map[value] | collect
    if len(main_routes) == 0:
        main_routes = ["/gql"]

    send_json = (insert_in[["response_headers","content-type"]]["application/json"]
                 | send_response)
    http_r = {
        'type': FX.HTTP.StartServer,
        'port': port,
        'pipe_into': (map[middleware_worker[[
            permit_cors,
            route["/"][insert["response_body"]["Healthy"] | send_response],
            *[route[path][func[P(query, context=context)] | send_json] for path in main_routes],
            *additional_routes,
            fallback_not_found,
            send_response
        ]]] | subscribe[run]),
        'logging': logging,
        'bind_address': bind_address,
    } | run
    if is_a(http_r, Error):
        raise Exception("Error in creating server") from http_r.args[0]

    return http_r["server_uuid"]


def create_additional_route(z_route, context):
    s_route = z_route | F.Route | collect
    hook = func[z_route | F.Hook | collect]

    curried_hook = P(hook, context=context)

    return route[s_route][func[curried_hook] | send_response]

def create_additional_routes(z_routes, context):
    return z_routes | map[P(create_additional_route, context=context)] | collect


        