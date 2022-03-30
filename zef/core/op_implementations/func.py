from .._ops import *
from .._core import *

##############################
# * Call
#----------------------------

# Call implements the laziness part of the op chain. It's use on the user-facing
# side is discouraged, although zef functions will be converted to call ops.

def call_implementation(input, f, *args):
    from typing import Generator, Iterable, Iterator
    if isinstance(input, Generator) or isinstance(input, Iterator):
        input = [i for i in input]
    return f(input, *args)

def call_type_info(op, curr_type):
    # TODO
    return VT.Any

##############################
# * Unpack
#----------------------------

def unpack_implementation(inputs, f):
    if isinstance(inputs, tuple):
        return f(*inputs)
    elif isinstance(inputs, dict):
        # if this has wrapped a call[...] then we need to unpack the function
        # inside of it, as call itself does not handle kwargs

        # Why is this nesting so confusing???
        while isinstance(f, ZefOp):
            assert len(f.el_ops) == 1 and f.el_ops[0][0] == RT.Call, "Can only support a single call[] zefop when using dictionary unpack"
            assert len(f.el_ops[0][1]) == 1, "Can't support curried arguments yet, e.g. unpack[func[something][1][2]], when using dicitonary unpack"
            f = f.el_ops[0][1][0]
        return f(**inputs)
    else:
        raise Exception("unpack requires either a tuple or a dictionary as input")

def unpack_type_info(op, curr_type):
    # TODO
    return VT.Any


