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

def resolve_token(request, header, aud, namespace):
    from authlib.jose import jwt
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
        return {
            **request,
            "response_body": "Invalid auth header",
            "response_status": 400
        }
    token = parts[1]

    auth_result = jwt.decode(token, context["jwk"])
    auth_result.validate()

    if auth_result["aud"] != aud:
        raise Exception("Invalid token for wrong audience")

    if namespace is None:
        return auth_result
    else:
        return auth_result[namespace]
            
def query(request, context):
    if context["z_gql_root"] | has_out[RT.AuthHeader] | collect:
        header = context["z_gql_root"] >> RT.AuthHeader | value | collect
        aud = context["z_gql_root"] >> RT.AuthAudience | value | collect
        namespace = context["z_gql_root"] >> O[RT.AuthNamespace] | value_or[None] | collect

        auth_context = resolve_token(request, header, aud, namespace)
    else:
        auth_context = None

    q = json.loads(request["request_body"])

    success,data = graphql_sync(
        context["ari_schema"],
        q,
        context_value={"g": context["g_data"],
                       "auth": auth_context},
    )
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

    if z_gql_root | has_out[RT.AuthHeader] | collect:
        url = z_gql_root >> RT.AuthJWKURL | value | collect
        # import urllib
        # with urllib.request.urlopen(url) as response:
        #     raw = response.read()
        #     # max_age = parse_max_age(response.headers)
        #     # authority_refresh.on_next(max_age)
        # import json
        # app.jwk = json.loads(raw)

        import requests
        r = requests.get(url)
        jwk = r.json()
    else:
        jwk = None


    context = {
        "z_gql_root": z_gql_root,
        "g_data": g_data,
        "ari_schema": ari_schema,
        "jwk": jwk,
    }

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
