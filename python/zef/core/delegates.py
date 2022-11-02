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

from .VT import *
from .VT import make_VT
from . import internals
from ..pyzef import zefops as pyzefops

def to_delegate_imp(first_arg, *curried_args):
    # TODO: Break this up and document

    if isinstance(first_arg, DelegateRef):
        if len(curried_args) == 0:
            return first_arg
        if len(curried_args) == 1:
            return internals.delegate_to_ezr(first_arg, curried_args[0], False, 0)
        if len(curried_args) == 2:
            return internals.delegate_to_ezr(first_arg, curried_args[0], curried_args[1], 0)
        raise Exception("Too many args for to_delegate with a Delegate")

    if isinstance(first_arg, ZefRef) or isinstance(first_arg, EZefRef):
        assert len(curried_args) == 0
        return internals.ezr_to_delegate(first_arg)

    raise Exception(f"Unknown type for to_delegate: {type(first_arg)}. Maybe you meant to write delegate_of?")

# This is for internal use only - it tries to convert a tuple or single RAET
# into a delegate of order zero.
def attempt_to_delegate(args):
    if isinstance(args, tuple):
        assert len(args) == 3
        args = tuple(internals.get_token(x) if isinstance(x, ValueType) else x for x in args)
        return DelegateRef(DelegateRef(args[0]), args[1], DelegateRef(args[2]))
    else:
        args = internals.get_token(args) if isinstance(args, ValueType) else args
        return DelegateRef(args)

def delegate_of_imp(x, arg1=None, arg2=None):
    # TODO: Move implementation
    from ._ops import in_frame, frame, collect

    # TODO: Break this up and document
    if isinstance(x, EZefRef) or isinstance(x, ZefRef):
        assert arg2 is None
        if arg1 is None:
            create = False
        else:
            create = arg1
        assert isinstance(create, Bool)

        d = pyzefops.delegate_of(to_delegate_imp(x))
        z = to_delegate_imp(d, Graph(x), create)
        if z is None:
            return None
        if isinstance(x, ZefRef):
            z = z | in_frame[frame(x)] | collect
        return z

    if isinstance(x, DelegateRef):
        if arg1 is None:
            create = False
            g = None
        else:
            g = arg1
            if arg2 is None:
                create = False
            else:
                create = arg2

        d = pyzefops.delegate_of(x)
        if g is None:
            return d
        else:
            return to_delegate_imp(d, g, create)

    # Fallback
    return delegate_of_imp(attempt_to_delegate(x), arg1, arg2)



# TODO: Need a is_a for this too - or rather make this the main workhorse
DelegateRef = make_VT("DelegateRef", pytype=internals.Delegate)


def delegate_is_a(x, typ):
    if isinstance(x, BlobPtr):
        if not internals.is_delegate(x):
            return False
        del_ref = to_delegate_imp(x)
    elif isinstance(x, DelegateRef):
        del_ref = x
    else:
        return False

    # Get to here, x is at least some kind of delegate
    if len(typ._d["absorbed"]) == 0:
        return True

    raise NotImplementedError("TODO")
    # Could probably just do a isinstance on the absorbed part, but not if
    # there's more interesting types going in there.

Delegate = make_VT("Delegate", is_a_func=delegate_is_a)

def instancecheck_Delegate(self, other):
    raise NotImplementedError("TODO")
    