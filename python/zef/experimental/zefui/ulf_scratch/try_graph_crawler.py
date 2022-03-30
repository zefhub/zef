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


#%%
from zefdb import zefui
from zefdb import *
from zefdb.zefops import *

from rx.subject import Subject
from icecream import ic

def show(z_app_root, z_template=None, ws_port:int=9016, launch_browser:bool=False, force_new_session=False, this_process:bool=True):
    """function called by user to actually start the zefui backend 
    on the current system and possibly launch the browser etc."""
    if this_process is False:
        raise NotImplementedError()
    
    zefui.start_backend_session(z_app_root, z_template)
    start_connection_listener(ws_port)
    
    # TODO: if force_new_session is False: check whether there is already a 
    # session running for the combination of (z_app_root, z_template). If
    # yes, then reuse this session (open question: does it make sense to 
    # clear the state here? The app should already reflect the latest 
    # ui_model state?
    
    



# ******************************************************************** mock IO system, e.g. SocketIO *********************************************************************



def start_connection_listener(ws_port:int):
    """should act idempotently: if there is already a connection listener on that port: don't 
    start another one. From the incoming connection the listener should get the session uid
    and connect to that session. 
    For now: if no session uid is sent along: check if the process it is running in has exactly 
    one zefui backend session running."""
    
    print(f"start_zefui_connection_listener...   ws_port={ws_port}")


    #TODO: set up socketIO lib here, listen on port and pass in handler function that verifies
    # incoming session uid from the WS connection etc. Then call hook_up_to_zefui_crawler !!!!!!!!!!!!

    # for now launch this immediately
    mock_new_ws_connected_handler()
    



def mock_new_ws_connected_handler(): 
    """This function is triggered when a new WS comes in and all is authenticated etc. """
    print("mock_new_ws_connected_handler called...")
    from_zefui_rx_stream = Subject()
    from_fe_rx_stream = Subject()    
    # TODO: hook up bidirectional communication between ws and rx stream messages!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    session_id = None   # get this from ws init message if present
    # just do a print output for now
    from_zefui_rx_stream.subscribe(lambda msg: print(f"\n--- SocketIO mock would process and send out: {msg}\n"))    
    hook_up_streams_listeners(from_zefui_rx_stream, from_fe_rx_stream, session_id)
    



def hook_up_streams_listeners(from_zefui_rx_stream, from_fe_rx_stream, session_id=None):    
    """given the rx streams and the session uid: find the respective session in the zefui._state
    with the graph, message queue in the zefui dict and set up listeners in both directions."""
    ic(f"hook_up_streams_listeners called... for {session_id}")
    
    
    # if no session_id is provided: check if there is only one session
    if session_id is None:        
        d = zefui._state['sessions']
        if len(d) != 1:
            raise RuntimeError(f"hook_up_streams_listeners called with no session id specified, but there are {len(d)} zefui sessions. It is not clear which one to choose.")
        session_id = next(iter(d.keys()))        
    
    ui_model = zefui._state['sessions'][session_id]['ui_model']
        
    push_batched_msgs_from_tx_to_stream = zefui.make_push_batched_msgs_from_tx_to_stream(from_zefui_rx_stream)
    # this function is triggered after all individual subscriptions fire, once upon each tx closing on the ui_model
    ui_model | subscribe[push_batched_msgs_from_tx_to_stream]      
    # After realizing this too late: we also need to trigger this fct once here, since no tx is closing when setting all up
    # the arg passed to a general graph subscription is the root node in the respective time slice
    push_batched_msgs_from_tx_to_stream(ui_model[42]|now)
    # -----------------              hook up part to act on the messages coming in from the FE through the WS             ------------------
    # execute the zefui user-registered fcts or transform the messages and push them into the stream
    
    def handle_msg_from_fe(msg: dict):
        """check what kind of message this is: look in the local dict whether 
        there are callbacks / cb streams registered for this kind of msg."""
        print(f"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ handle_msg_from_fe: new msg received from FE: {msg}")
        # TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    from_fe_rx_stream.pipe(
        #op.map(lambda msg_str: json.loads(msg_str))        
        ).subscribe(handle_msg_from_fe)






# %%


zefui._state = {}       # manual clear for now to only have one session


def create_template_graph():
    g = Graph()
    i = lambda x: instantiate(x, g)
    
    with Transaction(g):
        z_something = i(ET.Something)
        z_dropdown = i(ET.Dropdown)
        (z_dropdown 
        | attach[RT.Disabled, False]
        | attach[RT.ZefUI_Required, False]
        | attach[RT.Description, "default description"]   # assigned values on the template ARE the fdefault values
        | attach[RT.Options, i(ET.List) 
                | attach[RT.Selected, z_something]
                | attach[RT.ListElement, z_something]
                ]
        )
        ((z_dropdown > RT.Description) 
         | attach[RT.ZefUI_OnInstantation, i(ET.SendMessage) | attach[RT.MessageTemplate, """{
   "add_connection": {
       "comment": "dropdown description update",
       Z | source | uid: {"value": Z | target | uid}
    }
}"""]]  | attach[RT.ZefUI_OnTermination, i(ET.SendMessage) | attach[RT.MessageTemplate, """{
   "remove_connection": {
       "comment": "removed!!!",
       Z | source | uid: {"value": Z | target | uid}
    }
}"""]]
        )
                
    return g, z_dropdown



def create_instance_graph():
    g = Graph()
    i = lambda x: instantiate(x, g)
    
    with Transaction(g):
        z_something = i(ET.Something)
        z_dropdown = i(ET.Dropdown)
        (z_dropdown 
        #| attach[RT.Disabled, False]
        | attach[RT.Description, "abc"]   # assigned values on the template ARE the default values
        | attach[RT.Options, i(ET.List) 
                #| attach[RT.Selected, z_something]
                | attach[RT.ListElement, z_something]
                | attach[RT.ListElement, z_something]
                ]
        )
    return g, z_dropdown



"""app-specific initialization: create ui_model, merge in fkg 
or do whatever else needs to be done to prepare this"""

g_tpl, z_tpl_dropdown = create_template_graph()
g_ui_model, z_dropdown = create_instance_graph()
show(z_dropdown, z_tpl_dropdown)    #launch!



#%%




terminate(now(z_dropdown) > RT.Description)


# %%
with Transaction(g_ui_model):
    z_dropdown | attach[RT.Description, 'sdafdsf']
    z_dropdown | attach[RT.Disabled, True]

# %%
now(z_dropdown) >> RT.Description <= 'sdf'
[]

# %%

z = z_dropdown


py_str = """
{
    Z | uid : Z >> RT.Description | uid
}"""

print(eval(compile(py_str, '<string>', 'eval'),  {'Z': z, 'uid': uid, 'RT': RT}))


# %%

from zefdb.tools import pipeable



@pipeable
def get_name(z):
    return z >> RT.Name | value

g = Graph()
d = instantiate(ET.Dog, g) | attach[RT.Name, 'Rufus'] | attach[RT.Age, 5]
print(now(d) | get_name())










# %%

from zefdb.tools import ops
z_tpl_dropdown | outs | ops.map(fct = lambda x: RT(x)) | ops.filter(predicate=is_part_of_data)





z_tpt = z_tmplt_dropdown
z_inst = z_dropdown


# %%
import os, psutil
process = psutil.Process(os.getpid())
print(process.memory_info().rss) 
# %%

def make_f(p):
    def f(x):
        print('abcahfjksdasfjhsdjhf k sjfhjk asdkf')
        k = x*x*x*x
        return x*x + p
    return f


# %%
fs5 = [make_f(x) for x in range(100000)]
# %%
for x,y in [(3,4),(9,8), (2,3)]: 
    print(x, y)


# %%
y=20
(2000*12)*((1.08)**y )
# %%



(g 
 | add_spec(for_all(ET.Dog), must_exist(Z >> RT.Name)) 
 | add_spec(for_all(ET.Dog), Z >> RT.Age | value >=0)
 | add_spec(for_all(ET.Dog), Z >> RT.Color | value != 'white')
 )


# %%%%%%%%%%%%%%%%%%%

{
    ('rufus', ET.Dog),
    (K('rufus'), ET.Dog),
    (K('name_rel'), K('rufus'), RT.Name, 'Rufus'),
    
    
    (K('rufus'), RT.Weight, 14),           # we can omit naming the relation
    (ET.Cat, ),                         # or the RAE. But without an id, we can't refer to it anymore
}



@zefop
def own_zefop(zs, param1):
    return zs[-param1]




