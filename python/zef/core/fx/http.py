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


#=============================================================================== HTTP Server handlers =========================================================================================================
from .fx_types import Effect, FX, _Effect_Class
from uuid import uuid4
from .._ops import *
from ..error import Error

from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import threading
from concurrent.futures import Future, TimeoutError

from ..logger import log

_effects_processes = {}

class OurHTTPServer(ThreadingHTTPServer):
    def __init__(self, *args, do_logging, **kwds):
        super().__init__(*args, **kwds)
        self.do_logging = do_logging


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        return self.handle_generic("GET")
    def do_POST(self):
        return self.handle_generic("POST")
    def do_OPTIONS(self):
        return self.handle_generic("OPTIONS")

    def handle_generic(self, method):
        # We could override handle_one_request instead of overriding each of the
        # get_x functions, but there is error handling in handle_one_request and
        # it could change over the versions of http.server.

        try:
            future = Future()
            length = int(self.headers.get("content-length", "0"))

            body = self.rfile.read(length)

            request_id = str(uuid4())
            self.server.zef["open_requests"][request_id] = future

            splits = self.path.split("?", maxsplit=1)
            path = splits[0]
            params = splits[1] if len(splits) == 2 else ""
 
            d = {
                "request_id": request_id,
                "server_uuid": self.server.zef["server_uuid"],
                "method": method,
                "path":   path,
                "params":   params,
                "request_body":   body,
                "request_headers": dict(self.headers),      # use a plain data type
            }
            d | push[self.server.zef["stream"]] | run
            try:
                result = future.result(timeout=15.0)
            except TimeoutError:
                log.error("Timed out handling REST request")
                self.reply_with_error("Internal timeout")
                return
            finally:
                # TODO: Cleanup from open_requests here
                pass

            status,headers,msg = result
            if status != 200:
                msg = msg.decode('utf-8')
                self.send_response(status, msg)
            else:
                self.send_response(status)
            for key,val in headers.items():
                self.send_header(key, val)
            self.end_headers()
            if status == 200:
                self.wfile.write(msg)
        except BrokenPipeError:
            log.error("Connection aborted unexpectedly")
            pass
        except Exception as exc:
            # TODO: Place this error into a stream available to the user
            log.error("Some problem in handling HTTP request", exc_info=exc)
            self.reply_with_error("")

    def reply_with_error(self, err_msg):
        self.send_error(500, err_msg)

    def log_message(self, *args, **kwds):
        if self.server.do_logging:
            super().log_message(*args, **kwds)



def http_start_server_handler(eff: Effect):
    """[summary]

    Args:
        eff (Effect): {
            "type": FX.HTTP.StartServer,
            "port": 5000, (default = 5000)
            "bind_address": "0.0.0.0", (default = "localhost")
            "pipe_into": map[..] | filter[..] | subscribe[run],
    or
            "pipe_into": map[middleware_worker[[permit_cors, custom_handle, fallback_not_found, send_response]] | subscribe[run],
            "logging": True or "Graph" or "Stream"
        }
    """
    # print(f"http_start_server called for effect: {eff}")

    server_uuid = None
    pushable_stream = None
    server = None
    thread = None

    try:
        open_requests = {}
        pushable_stream = Effect({'type': FX.Stream.CreatePushableStream}) | run
        server_uuid = str(uid(pushable_stream.stream_ezefref))
        if eff.d['pipe_into'] is not None: pushable_stream | eff.d['pipe_into']
        port = eff.d.get('port', 5000)
        bind_address = eff.d.get('bind_address', "localhost")
        do_logging = eff.d.get('logging', True)

        zef_locals = {"open_requests": open_requests,
                    "stream" : pushable_stream,
                    "server_uuid": server_uuid,
                    "port": port}

        server = OurHTTPServer((bind_address, port), Handler, do_logging=do_logging)
        zef_locals["server"] = server
        server.zef = zef_locals


        thread = threading.Thread(target=server.serve_forever, daemon=True)
        zef_locals["thread"] = thread

        _effects_processes[server_uuid] = zef_locals

        thread.start()

        log.debug(f"http_server started", uuid=server_uuid, port=port, bind_address=bind_address)
        return zef_locals
    except Exception as exc:
        # Cleanup
        # TODO:

        return Error(exc)
        


def http_stop_server_handler(eff: Effect):
    d = _effects_processes[eff.d["server_uuid"]]
    d["server"].shutdown()
    d["server"].server_close()
    d["thread"].join()
    log.debug("http server stopped", uuid=eff.d["server_uuid"])
    return {}
    

def http_send_request_handler(eff: Effect):
    """[summary]

    Args:
        eff (Effect): {
                "type":   FX.HTTP.Request, # Required
                "url":    "url",           # Required
                "method": "GET",           # Optional
                "data":   {},              # Optional
                "params": {},              # Optional
        }
    """
    # TODO needs more testing and improvements
    import requests
    request_url = eff.d['url']
    request_method = eff.d.get('method', 'GET')
    request_data = eff.d.get('data', {})
    request_params = eff.d.get('params', {})

    method_dict =  {'GET': requests.get, 'POST': requests.post, 'PUT': requests.put, 'DELETE': requests.delete, 'HEAD': requests.head}
    request_obj = method_dict[request_method]
    response = request_obj(url = request_url, data = request_data, params = request_params)

    return {"response_text": response.text, "response_status": response.status_code}
    
def http_send_response_handler(eff: Effect):
    d = eff.d
    if "server_uuid" not in d:
        # TODO: register this on ProcessLogger
        return Error(f"An FX.HTTP.SendResponse event must contain a 'server_uuid' field. This was not the case for eff={eff}")

    d = _effects_processes[eff.d["server_uuid"]]
    future = d["open_requests"][eff.d["request_id"]]

    msg = eff.d.get("response", "")
    assert msg is None or isinstance(msg, str) or isinstance(msg, bytes)
    if isinstance(msg, str):
        msg = msg.encode("utf-8")

    status = eff.d.get("status", 200)
    headers = eff.d.get("headers", {})
    import copy
    headers = copy.deepcopy(headers)

    header_names = set(k.lower() for k in headers)
    if msg is not None and status == 200:
        if "content-length" not in header_names:
            headers["Content-Length"] = str(len(msg))
        if "content-type" not in header_names:
            headers["Content-Type"] = "text/html; charset=UTF-8"

    response = (status,headers,msg)
    # log.debug("Trying to attach response to future", response=response)
    future.set_result(response)
    return {}
    
    


########################################
# * Useful middleware
#--------------------------------------

from ..zef_functions import func

# TODO: Improve this! But maybe redesign the style of the app anyway.
@func
def route(req, path, handler):
    if req["path"] == path:
        try:
            return handler(req)
        except Exception as exc:
            log.error(f"There was an exception in the handler for path {path}", exc_info=exc)
            return {
                **req,
                "response_status": 500,
            }
            
    return req

@func
def permit_cors(req):
    needed_headers = {"Access-Control-Allow-Origin": "*"}

    if req["method"] == "OPTIONS":
        # TODO: more fancy stuff here
        req = dict(**req, response_headers=needed_headers)
        req["response_headers"]["Access-Control-Allow-Methods"] = "*"
        req["response_headers"]["Access-Control-Allow-Headers"] = "*"
        req["response_headers"]["Access-Control-Max-Age"] = "86400"
        return send_response(req)

    import copy
    req = copy.deepcopy(req)

    req.setdefault("response_headers", {})
    req["response_headers"].update(needed_headers)

    return req

@func
def send_response(req):
    eff_d = {
        "type": FX.HTTP.SendResponse,
        "server_uuid": req["server_uuid"],
        "request_id": req["request_id"],
    }
    if "response_body" in req:
        eff_d["response"] = req["response_body"]
    if "response_status" in req:
        eff_d["status"] = req["response_status"]
    if "response_headers" in req:
        eff_d["headers"] = req["response_headers"]

    return Effect(eff_d)

@func
def fallback_not_found(req):
    if "response_body" not in req and "response_status" not in req:
        import copy
        req = copy.deepcopy(req)
        req["response_status"] = 404
    return req


@func
def middleware_worker(req, mw):
    """Apply the functions in `mw` in order. If, at any point, None or an Effect is
returned, then immediately return the None or Effect. Otherwise, continue
applying the functions.
    
Intended to be used in the following manner:
source | map[middleware_worker[seq_of_funcs]] | run
    """

    cur = req
    for func in mw:
        cur = func(cur)

        if cur is None:
            return None
        if isinstance(cur, dict):
            continue
        # Why the hell is an Effect actually _Effect_Class??
        # if isinstance(cur, Effect):
        if isinstance(cur, _Effect_Class):
            return cur

        raise Exception(f"Unusual type in middleware_runner: {type(func)}")
        
    # Will only get here if the final middleware returns a dictionary
    return cur

# This doesn't work unfortunately - need to hook into the rx idea or something,
# not sure how this should look in the end.
@func
def middleware(req, mw):
    # return req | map[middleware_worker[mw]] | collect
    return req | map[middleware_worker[mw]]
    
