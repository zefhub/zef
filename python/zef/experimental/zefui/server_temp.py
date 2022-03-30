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

from zefdb import *
from zefdb.zefops import *
from zefdb import zefui

from rx.subject import ReplaySubject, Subject

# TODO: This will go into the graph crawler I'm guessing...
component_lookups = {}

def make_component(z_root, sid):
    z_template = component_lookups[ET(z_root)]

    # This is weird naming for the purpose here, will likely change in the future.
    zefui.start_backend_session(z_root, z_template, sid)


#######


def generate_template_graph_programmatically():
    g = Graph()
    with Transaction(g):
        I = lambda *args: instantiate(*args, g)

        input_component = I(ET.Input) | attach[[
            (RT.Value, "RAW(empty input)"),
            (RT.ReadOnly, False),
            (RT.Placeholder, "RAW(Enter text)")
        ]]

        update_component = I(ET.SendMessage) | attach[RT.MessageTemplate, """
output = {"components":{z_component_root|uid: {"component": "input"}}}
"""]

        update_value = I(ET.SendMessage) | attach[RT.MessageTemplate, """
maybe_ae = z_component_root >> O[RT.Value]
if maybe_ae is None:
    # This will be replaced by using the default value in the template graph.
    output = {"components":{z_component_root|uid: {"value": f"RAW(empty input)"}}}
else:
    output = {"components":{z_component_root|uid: {"value": f"STATE({maybe_ae|uid})"}}}
"""]

        update_onchange = I(ET.SendMessage) | attach[RT.MessageTemplate, """
value_present = z_component_root | has_out[RT.Value]
readonly = z_component_root >> O[RT.ReadOnly]
readonly = None if readonly is None else readonly|value
readonly = False if readonly is None else readonly

if value_present and not readonly:
    output = {"components":{z_component_root|uid: {
        "onChange": f"OPTIMISTIC({z_component_root>>RT.Value|uid})",
        "class": "",
}}}
else:
    output = {"components":{z_component_root|uid: {
        "onChange": None,
        "class": "readonly"
}}}
"""]

        update_placeholder  = I(ET.SendMessage) | attach[RT.MessageTemplate,  """
maybe_ae = z_component_root >> O[RT.Placeholder]
if maybe_ae is None:
    output = {"components":{z_component_root|uid: {"placeholder": f"RAW(Enter text)"}}}
else:
    output = {"components":{z_component_root|uid: {"placeholder": f"STATE({maybe_ae|uid})"}}}
"""]

        input_component | attach[[
            (RT.ZefUI_OnInstantiation, update_component),
        ]]

        (input_component > RT.Value) | attach[[
            (RT.ZefUI_OnInstantiation, update_value),
            (RT.ZefUI_OnInstantiation, update_onchange),
            (RT.ZefUI_OnTermination, update_value),
            (RT.ZefUI_OnTermination, update_onchange),
        ]]

        (input_component > RT.ReadOnly) | attach[[
            (RT.ZefUI_OnInstantiation, update_onchange),
            (RT.ZefUI_OnTermination, update_onchange),
        ]]
        (input_component >> RT.ReadOnly) | attach[[
            (RT.ZefUI_OnValueAssignment, update_onchange),
        ]]

        (input_component > RT.Placeholder) | attach[[
            (RT.ZefUI_OnInstantiation, update_placeholder),
            (RT.ZefUI_OnTermination, update_placeholder),
        ]]

    component_lookups[ET.Input] = input_component|now
    global stored_template_g
    stored_template_g = g




# To be redone in zefui

def create_zefui_streams(session_id=None):    
    # if no session_id is provided: check if there is only one session
    if session_id is None:        
        d = zefui._state['sessions']
        if len(d) != 1:
            raise RuntimeError(f"hook_up_streams_listeners called with no session id specified, but there are {len(d)} zefui sessions. It is not clear which one to choose.")
        session_id = next(iter(d.keys()))        
    
    ui_model = zefui._state['sessions'][session_id]['ui_model']
        
    # TODO: Instead of a replay subject, need to have a way to get the full current state.
    outgoing_stream = ReplaySubject()
    push_batched_msgs_from_tx_to_stream = zefui.make_push_batched_msgs_from_tx_to_stream(outgoing_stream)
    # this function is triggered after all individual subscriptions fire, once upon each tx closing on the ui_model
    ui_model | subscribe[push_batched_msgs_from_tx_to_stream]      
    # After realizing this too late: we also need to trigger this fct once here, since no tx is closing when setting all up
    # the arg passed to a general graph subscription is the root node in the respective time slice
    push_batched_msgs_from_tx_to_stream(ui_model[42]|now)
    # -----------------              hook up part to act on the messages coming in from the FE through the WS             ------------------
    # execute the zefui user-registered fcts or transform the messages and push them into the stream
    
    incoming_stream = Subject()
    
    def handle_msg_from_fe(msg: dict):
        """check what kind of message this is: look in the local dict whether 
        there are callbacks / cb streams registered for this kind of msg."""
        print(f"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ handle_msg_from_fe: new msg received from FE: {msg}")
        # TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    incoming_stream.pipe(
        #op.map(lambda msg_str: json.loads(msg_str))        
        ).subscribe(handle_msg_from_fe)

    return outgoing_stream, incoming_stream







import rx.operators as rxop
from rx.scheduler.eventloop import AsyncIOScheduler
from rx.core import notification as rxnotify
import asyncio

"""Mimic subscribe of an rx stream, but where the subscription function is an
asyncio coroutine AND these coroutines are guaranteed to be executed in order of
the stream.

Note: if order of execution is not required, then something like:
stream.pipe(rxop.map(lambda x: rxop.from_future(asyncio.create_task(func(x)))),
            rxop.merge_all()
           ).subscribe()
could be used instead.

Or
stream.subscribe(lambda x: asyncio.create_task(func(x)))
"""
async def sync_map_rx_stream(func, stream):
    queue = asyncio.Queue()

    disposable = stream.pipe(
        rxop.materialize()
    ).subscribe(
        on_next=lambda i: queue.put_nowait(i),
        scheduler=AsyncIOScheduler(loop=asyncio.get_event_loop())
    )

    while True:
        i = await queue.get()
        if isinstance(i, rxnotify.OnNext):
            await func(i.value)
            queue.task_done()
        else:
            disposable.dispose()
            if isinstance(i, rxnotify.OnError):
                raise(Exception(i.value))
            break
