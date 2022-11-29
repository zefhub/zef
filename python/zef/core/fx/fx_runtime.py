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
from .. import *
from .._ops import *


def create_eff_to_process_graph_wish(eff: Dict) -> 'FX':
    from .. import internals
    g_process = internals.get_local_process_graph()
    pushable_stream = {'type': FX.Stream.CreatePushableStream} | run

    fields_d = {
        "port" : eff.get('port', 5000),
        "bind_address" : eff.get('bind_address', "0.0.0.0"),                      
        "type" : Val(eff['type']),   
        # TODO fix pipeinto being unserializeable if python objects are found
        # "pipe_into" : Val(eff.get('pipe_into', None)),       
        "PushableStream": pushable_stream.stream_ezefref,
        # "url" : 'ulfsproject.zefhub.io',    
    }

    transact_wish = ET.ZefFXService(**fields_d) | g_process 
    return transact_wish


def create_http_server(eff: Dict, server_zr: ZefRef) -> Dict:
    from ...core.fx.http import OurHTTPServer, Handler, _effects_processes
    from ...core.logger import log
    from ...core.op_structs import Awaitable
    import threading

    # create pushable stream and zef local variables
    server_uuid = None
    pushable_stream = None
    server = None
    thread = None

    try:
        open_requests = {}

        # Retrieve the pushable stream from the graph
        pushable_stream = Awaitable(to_ezefref(server_zr | Out[RT.PushableStream] | collect), True)
        server_uuid = str(uid(server_zr))

        # TODO Get this information from the graph
        if eff.get('pipe_into', None) is not None:  pushable_stream | eff['pipe_into']

        port = eff.get('port', 5000)
        bind_address = eff.get('bind_address', "localhost")
        do_logging = eff.get('logging', True)

        # Create the context for this server
        zef_locals = {
            "open_requests": open_requests,
            "stream" : pushable_stream,
            "server_uuid": server_uuid,
            "port": port
        }
        # Instantiate HTTP server
        server = OurHTTPServer((bind_address, port), Handler, do_logging=do_logging)

        # Set Zef local variables
        zef_locals["server"] = server
        server.zef = zef_locals


        # Start HTTP server in a thread
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        zef_locals["thread"] = thread

        _effects_processes[server_uuid] = zef_locals

        thread.start()

        log.debug(f"http_server started", uuid=server_uuid, port=port, bind_address=bind_address)
        return {"server_uuid": server_uuid}
    except Exception as exc:
        raise RuntimeError(f'Error starting HTTP server: in FX.HTTP.StartServer handler: {exc}')


fx_dispatch_dict = {
    FX.HTTP.StartServer: {"create_http_server": create_http_server, "create_eff_to_process_graph_wish": create_eff_to_process_graph_wish},
}

def fx_runtime(eff: Dict):
    from ...core.fx import _effect_handlers
    if not isinstance(eff, Dict): raise TypeError(f"run(x) called with invalid type for x: {type(eff)}. You can only run a wish, which are represented as dictionaries.")
    
    if eff['type'] in fx_dispatch_dict:

        if eff['type'] == FX.HTTP.StartServer:
            http_server_d = fx_dispatch_dict[FX.HTTP.StartServer]
            create_eff_to_process_graph_wish = http_server_d['create_eff_to_process_graph_wish']
            create_http_server = http_server_d['create_http_server']

            # Stage 1
            transact_wish = create_eff_to_process_graph_wish(eff)
            server_zr = _effect_handlers[FX.Graph.Transact.d](transact_wish)

            # Stage 2
            result_d = create_http_server(eff, server_zr)
            return result_d
    
    
    handler = _effect_handlers[eff['type'].d]
    return handler(eff)