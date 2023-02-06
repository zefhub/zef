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
    from .flat_graph import FlatGraph, FlatRef
    from .VT import GraphSlice
    # TODO: Break this up and document

    if isinstance(first_arg, DelegateRef):
        if len(curried_args) == 0:
            return first_arg
        if isinstance(curried_args[0], Graph | FlatGraph):
            glike = curried_args[0]
        else:
            assert isinstance(curried_args[0], GraphSlice), f"{curried_args[0]} is not a GraphSlice"
            glike = Graph(curried_args[0])

        if isinstance(glike, Graph):
            if len(curried_args) == 1:
                d_ezr = internals.delegate_to_ezr(first_arg, glike, False, 0)
            elif len(curried_args) == 2:
                d_ezr = internals.delegate_to_ezr(first_arg, glike, curried_args[1], 0)
            else:
                raise Exception("Too many args for to_delegate with a Delegate")
        elif isinstance(glike, FlatGraph):
            if len(curried_args) == 1:
                pass
            elif len(curried_args) == 2:
                if curried_args[1]:
                    raise Exception("Can't create a delegate automatically on a FlatGraph")
            else:
                raise Exception("Too many args for to_delegate with a Delegate")
            if first_arg in glike:
                d_ezr = glike[first_arg]
            else:
                d_ezr = None
        else:
            raise Exception("Shouldn't get here")

        if d_ezr is None:
            return None
        elif isinstance(curried_args[0], GraphSlice):
            from ._ops import to_frame, exists_at
            if not exists_at(d_ezr, curried_args[0]):
                raise Exception("Delegate does not exist at given graph slice.")
            return to_frame(d_ezr, curried_args[0])
        else:
            return d_ezr

    from .op_implementations.implementation_typing_functions import check_Atom_with_ref
    from .atom import _get_ref_pointer, AtomClass
    if check_Atom_with_ref(first_arg):
        # TODO: Lots of fixups needed - reconsider the whole thing carefully
        out = to_delegate_imp(_get_ref_pointer(first_arg), *curried_args)
        if isinstance(out, BlobPtr | FlatRef):
            return AtomClass(out)
        return out
    if isinstance(first_arg, BlobPtr):
        assert len(curried_args) == 0
        return internals.ezr_to_delegate(first_arg)
    if isinstance(first_arg, FlatRef):
        raise NotImplementedError("TODO")

    raise Exception(f"Unknown type for to_delegate: {type(first_arg)}. Maybe you meant to write delegate_of?")

# This is for internal use only - it tries to convert a tuple or single RAET
# into a delegate of order zero.
def attempt_to_delegate(args):
    if isinstance(args, tuple):
        assert len(args) == 3
        args = tuple(internals.get_c_token(x) if isinstance(x, ValueType) else x for x in args)
        return DelegateRef(DelegateRef(args[0]), args[1], DelegateRef(args[2]))
    else:
        args = internals.get_c_token(args) if isinstance(args, ValueType) else args
        return DelegateRef(args)

def delegate_of_imp(x, arg1=None, arg2=None):
    # TODO: Move implementation
    from ._ops import to_frame, frame, collect

    from .op_implementations.implementation_typing_functions import check_Atom_with_ref
    from .atom import _get_ref_pointer, AtomClass
    if check_Atom_with_ref(x):
        # TODO: Lots of fixups needed - reconsider the whole thing carefully
        out = delegate_of_imp(_get_ref_pointer(x), arg1, arg2)
        if isinstance(out, BlobPtr):
            return AtomClass(out)
        return out

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
            z = z | to_frame[frame(x)] | collect
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


def DelegateRef_ctor(*args):
    from .VT.helpers import type_name
    from .VT.rae_types import RAET_get_token
    if len(args) < 1 or len(args) > 3:
        raise Exception("DelegateRef constructor requires 1, 2 or 3 arguments")
    def to_c_token_maybe(x):
        if isinstance(x, ValueType) and type_name(x) in ["ET", "RT", "VRT"]:
            return RAET_get_token(x)
        return x

    args = [to_c_token_maybe(x) for x in args]
    return internals.Delegate(*args)

# TODO: Need a is_a for this too - or rather make this the main workhorse
DelegateRef = make_VT("DelegateRef", pytype=internals.Delegate,
                      constructor_func=DelegateRef_ctor)


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
    