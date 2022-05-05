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

from icecream import ic
import asyncio
from functools import partial as P

from zefdb import *
from zefdb.zefops import *
from zefdb import zefui

import socketio
sio = socketio.AsyncServer(logger=True, engineio_logger=True, cors_allowed_origins="*")

import sanic
sanic_app = sanic.Sanic("zefui")
sio.attach(sanic_app)


import logging
logging.getLogger('sanic_cors').level = logging.DEBUG

def start_server():
    asyncio.get_event_loop().set_debug(True)
    sanic_app.run(port=3001)



from zefdb.zefui.server_temp import *


####

make_app_funcs = {}

template_graph = generate_template_graph_programmatically()


####

@sio.event
def connect(sid, environ, auth):
    print("connecting ", sid)

    # TODO: If this is a server triggered from the REPL, send the current state immediately

@sio.event
async def page_request(sid, data):
    # TODO: If this is a server triggered from the REPL, ignore this request.

    desired_app = data["pageId"]
    make_app = make_app_funcs[desired_app]

    # Would hopefully have the ability to add stuff into data later and pass this in.
    root_component = make_app()

    await sio.emit("page_config", {
        "layout": ["asdf"],
        "layoutElements": {},
        "components": {},
        "state": {},
    })

    make_component(root_component, sid)

    outgoing_stream,incoming_stream = create_zefui_streams(Graph(root_component) | uid)

    async def send_updates(updates):
        print("In send updates")
        print(updates)

        # TODO: After the frontend accepts lists can change this
        # await sio.emit("state_update", updates)
        for update in updates:
            await sio.emit("update", update)
    asyncio.create_task(sync_map_rx_stream(send_updates, outgoing_stream))
    # outgoing_stream.subscribe(on_next=lambda x: print("Got an update", x))

    # Just hacking for now
    await asyncio.sleep(1)
    await send_updates([{#"layout": ["asdf"],
                         "layoutElements": {"asdf":
                                            {"<>": "div",
                                             "children":
                                             {"__set": [f"COMPONENT({root_component|uid})"]
                                              }
                                              }}}])

    await asyncio.sleep(1)
    root_component | attach[RT.Placeholder, "asdf"]
    await asyncio.sleep(3)
    terminate(root_component >> RT.Value)
    await asyncio.sleep(3)



    await sio.save_session(sid, {"root_component": root_component,
                                 "zefui_output_stream": outgoing_stream,
                                 "zefui_input_stream": incoming_stream})


#######

@sio.event
async def value_change(sid, data):
    session = await sio.get_session(sid)
    root_component = session["root_component"]

    # TODO: This is a just a quick test version. In the future, dispatch to
    # appropriate component logic.
    z = g | instances[now][ET.Input] | only
    ae = z >> RT.Value
    ae <= "triggered"
    print("value_change")


#######

def make_app_basic():
    ui_model = Graph()
    I = lambda *args: instantiate(*args, ui_model)

    z_root = I(ET.Input) | attach[RT.Value, "asdf"]
    
    global stored_ui_model
    stored_ui_model = ui_model
    
    return z_root|now
make_app_funcs["master-schedule"] = make_app_basic


if __name__ == "__main__":
    start_server()

