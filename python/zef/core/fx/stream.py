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

from .fx_types import Effect
from .._core import EZefRef, ET
from ..op_structs import Awaitable





def stream_create_pushable_stream_handler(eff: Effect):
    """ 
    On which process graph? The one targeted with "this_eff | run[target_runtime]".
    This is executed on the thread where it should be created.
    """
    from .. import internals
    from .._ops import get, run, collect, execute
    from zef import GraphDelta
    from zef.core.reactivez import get_runtime_state
    

    pg = get_runtime_state()['process_graph']
    s = GraphDelta([
        ET.ZEF_PushableStream['s']
    ]) | pg | run[execute] | get['s'] | collect          # But: we want to return a stream, not a raw ZefRef
    return {'stream': Awaitable(s)}             # the response of an effect handler is always a dictionary




def stream_push_handler(eff: Effect):   # -> Union[Nil, Error]
    assert 'stream' in eff.d
    s = eff.d['stream']
    z_stream = s.stream_ezefref
    assert isinstance(s, Awaitable)
    streams = _state['streams']
    if not z_stream in streams:
        raise RuntimeError(f'in push effect handler: associated stream not found in fx._state["streams"]')
    
    # TODO: check that type of item agrees if stream is typed!!!!!!!!!!!!!!!!!!
    streams[z_stream].on_next(eff.d['item'])
    return None


def stream_complete_handler(eff: Effect):
    raise NotImplementedError()
