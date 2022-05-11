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

from .generate_api import generate_resolvers_fcts
from ariadne import make_executable_schema, graphql_sync

from functools import partial as P
import json

from zef.core.logger import log

def resolve_token(context, request):
    root = context["z_gql_root"]

    header = root >> RT.AuthHeader | value | collect
    aud = root >> RT.AuthAudience | value | collect
    namespace = root >> O[RT.AuthNamespace] | value_or[None] | collect
    algo = root >> RT.AuthAlgo | value | collect

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
        psk = root >> RT.AuthPresharedKey | value | collect
        auth_result = jwt.decode(token, psk, algorithms=[algo],
                                 audience=aud)
    else:
        raise Exception("Shouldn't get here")

    if auth_result["aud"] != aud:
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
            if auth_context is None and not (root >> O[RT.AuthPublic] | value_or[True] | collect):
                raise Exception("No auth and public is False.")
        except Exception as exc:
            log.error("Auth failed", exc_info=exc)
            return {
                "response_body": "Auth failed",
                "response_status": 400,
                **request
            }
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

    success,data = graphql_sync(
        context["ari_schema"],
        q,
        context_value={"g": context["g_data"],
                       "auth": auth_context},
    )
    if not success or "errors" in data:
        log.error("Failure in GQL query.", data=data, q=q, auth_context=auth_context)
    return {
        "response_body": json.dumps(data),
        **request
    }

def start_server(z_gql_root,
                 g_data,
                 port=443,
                 bind_address="0.0.0.0",
                 ):

    schema,objects = generate_resolvers_fcts(z_gql_root)
    ari_schema = make_executable_schema(schema, objects)

    import logging
    logger = logging.getLogger("ariadne").disabled = True
    log.debug("Disabled ariadne logging")
    
    context = {
        "z_gql_root": z_gql_root,
        "g_data": g_data,
        "ari_schema": ari_schema,
    }

    if z_gql_root | has_out[RT.AuthJWKURL] | collect:
        url = z_gql_root >> RT.AuthJWKURL | value | collect

        from jwt import PyJWKClient
        context["jwk_client"] = PyJWKClient(url)


    http_r = Effect({
        'type': FX.HTTP.StartServer,
        'port': port,
        'pipe_into': (map[middleware_worker[permit_cors,
                                           route["/gql"][P(query, context=context)],
                                           fallback_not_found,
                                            send_response]]
                      | subscribe[run]),
        'logging': True,
        'bind_address': bind_address,
    }) | run
    if is_a(http_r, Error):
        raise Exception("Error in creating server") from http_r.args[0]

    return http_r["server_uuid"]
