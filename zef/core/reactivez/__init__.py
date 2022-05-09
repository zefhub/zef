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


"""
stores all of the ReactiveZ state, broken down by Actor/Runtime/Thread:

_rz_state = {
   thread_id1: runtime_state1,
   thread_id2: runtime_state2,
   ...
}

How is this obtained in Python?
'thread_id = threading.get_ident()'

What is the form of "runtime_state1"?

runtime_state1 = {
    'process_graph': ...,
	'push_fct': ...,
	'op_states': {...},
	'effect_states': {effect_ezr: ...},	
	???
}

"""

_rz_state = {}

def get_runtime_state():
    """
    Impure: depends on the thread this is being executed from.
    If not runtime state exists, None is returned.
    """    
    import threading    
    return _rz_state.get(threading.get_ident(), None)

from .zefop_graph import set_up_zefop_graph

from .various import completion_event
from .start_runtime import start_zef_runtime
