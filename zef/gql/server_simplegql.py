from ..core import *
from ..ops import *

from ..core.fx.http import send_response, permit_cors, middleware, middleware_worker, fallback_not_found, route

from .generate_simplegql_api import generate_resolvers_fcts
from ariadne import make_executable_schema, graphql_sync

from functools import partial as P
import json

from zef.core.logger import log

def query(request, context):
    if context["z_gql_root"] | has_out[RT.AuthHeader] | collect:
        header = context["z_gql_root"] >> RT.AuthHeader | value | collect
        jwkurl = context["z_gql_root"] >> RT.AuthJWKURL | value | collect
        aud = context["z_gql_root"] >> RT.AuthAudience | value | collect
        namespace = context["z_gql_root"] >> O[RT.AuthNamespace] | value_or[None] | collect
        from authlib.jose import jwt
        import base64
        # token_header = request["request_headers"][header.lower]
        headers_as_lower = {k.lower(): v for k,v in request["request_headers"].items()}
        token_header = headers_as_lower[header.lower()]
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
            auth_context = auth_result
        else:
            auth_context = auth_result[namespace]
            
    else:
        auth_result = None
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



if __name__ == "__main__":
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
    gd = json_to_minimal_nodes(parsed_dict)
    r = gd | g_schema | run
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