from .fx_types import Effect
from .._core import EZefRef, ET
from .state import _state
from ..op_structs import Awaitable
from .._ops import *

def stream_create_pushable_stream_handler(eff: Effect):
    """ 
    Create an ET.PushableStream on the local process graph.
    Store the actual Awaitable in fx._state['streams']
    """
    from rx.subject import Subject
    from .. import internals

    g_process = internals.get_local_process_graph()
    # z_stream (an ET.PushableStream) is just a proxy for the actual stream.
    # Both on push and subscribing / applying further transformations
    z_stream = to_ezefref(ET.PushableStream | g_process | run)
    _state['streams'][z_stream] = Subject()
    # return { 'stream': my_stream }    # too verbose?
    return Awaitable(z_stream, True)


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

