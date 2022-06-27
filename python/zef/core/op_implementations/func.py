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

from .._ops import *
from .._core import *

##############################
# * Unpack
#----------------------------

def unpack_implementation(inputs, f):
    """
    used to wrap an zefop or a function. If the function
    takes multiple arguments, it is converted into a function
    that takes one tuple or list as a single argument.
    This list is unpacked into the function arguments.

    ---- Signature ----
    (Tuple[T1, T2, ...], Func[T1, T2, ...][T] ) -> T

    ---- Examples ----
    >>> (42, 10) | unpack[subtract]      # => 32


    ---- Tags ----
    used for: control flow
    related zefop: reverse_args
    """
    from typing import Generator
    if isinstance(inputs, tuple) or isinstance(inputs, list):
        return f(*inputs)
    if isinstance(inputs, Generator):
        return f(*tuple(inputs))

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


