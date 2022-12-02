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

from .. import report_import
report_import("zef.core.op_structs")

""" 
Until we have flat graphs, we represent zefops as tuples of tuples.
The outer dimensio represents the data flow direction.



General Structure:

**************************** LazyValues ****************************
- what we actually mean: an iterable
- pull based, potentially lazy
- value based semantics
- can be sent over the wire
- representation: (initial_val, ZefOp)
- storage in evaluated form simply means that ZefOp = None
- only compute values when 
    a) piped through 'collect'      
    b) the user casts to some data type, e.g. int(my_lazy_value)  will trigger compute
    
    
**************************** Awaitables ****************************
- what we mean: an observable
- push based
- does not hold on to / cache results (do we want to enable that functionality?)
- Is it lazy? 
    a) yes, in the sense that cmpute is only performed if someone is listening
    b) but it then performs compute as soon as a new value comes in
    c) compared to iterables where there is internal freedom to transform 
       the data stage-wise or element-wise



***** should zefops be able to deal with switching between types within one operator chain? *****
unsure about all possible combinations one may want. Let's aggregate examples

(
my_lazy_value 
| filter[...] 
| map[...] 
| first         # let us assume this returns a Awaitable()
) | map[...] | subscribe[...]

Claim: 
We would ever only do anything if there is an evaluating keyword at the very end:
subscribe / for_each / collect.
Therefore we don't ever have to try to infer intermediate types. Just keep a look out 
for evaluating keywords.






********************************* Evaluating Operators *********************************
Side effects: 
subscribe / for_each

Pure value: 
collect
Further chaining possible.


********************************* Expect Operator *********************************
Introduce the 'expect' operator?
In C++ one may consider making it templated.
my_zefop | ensureInt] | ...
Will throw if during evaluation a value of a different type passes through.
Can be used as an important tool by the runtime operator introspector, e.g.
if no types can be inferred due to missing type annotations in functions.

Op
RT
LazyValue
Awaitable
CollectingOp 
ForEachingOp 
SubscribingOp


LazyValue | Op -> LazyValue
Awaitable | Op -> Awaitable
Op | Op -> Op

LazyValue | CollectingOp -> LazyValue                           # The lazy_value is the general evaluated 'value' type
LazyValue | ForEachingOp -> Disposable
LazyValue | SubscribingOp -> DisposableProcessHandle        # the op should convert to an observable at some point

Awaitable | CollectingOp -> ???                            # we could allow Awaitable | CollectingOp -> Buffer
Awaitable | ForEachingOp -> ???
Awaitable | SubscribingOp -> RunningProcessHandle

Op | CollectingOp  -> CollectingOp
Op | ForEachingOp  -> ForEachingOp
Op | SubscribingOp -> SubscribingOp

CollectingOp | Op -> Op                                 # the 'collect' can be inserted as an elementary op
SubscribingOp | Op -> ???
ForEachingOp | Op -> ???

ZefRef.AET | Op -> Awaitable                               # should the Op be required to start with 'to_Awaitable()'?
RXObservable | Op -> Awaitable
ZefRef.AET | SubscribingOp -> RunningProcessHandle
RXObservable | SubscribingOp -> RunningProcessHandle


# we don't need a separate Collect, ForEach, Subscribe: these are just minimal cases of CollectingOp, ...


#************************************ Imperative Shell Interface for Awaitables: how to manually push data in ******************************************
Use the same Awaitable object type, but init with 
s1 = Awaitable(pushable=True)
s1.on_next('test event')
???????????? how to deal with values pushed in before any observer is subscribed ??????????????????



****************************** iterating over vs collecting an lazy_value **********************************
"for el in my_lazy_value"        # we don't need to evaluate all. Calculate lazily. Return wrapped itertools for now
my_lazy_value | collect          # we are free to also choose the same lazy eval up to completion. Or do 'breadth first' internally.


my_op1 = (
 ensureAwaitable[Dict]]
 | map[my_f]
 | merge
 | group_by[get_value['type']]
 | merge 
)

my_subscribing_op = my_op1 | subscribe[side_eff_fct]





*********************************** How do we want to declare new zefops? ********************************

map = ZefOp(
    RT.Map,                                     # data type to represent this op
    (),                                         # curried in params for the raw op
    get_item_handler = map_get_item_handler,    # custom behavior to deal with this_op[x]
    repr = map_repr,                            # how shoud this be displayed?
    apply_to_lazy_value_fct = ...
    apply_to_Awaitable()_fct = ...
    # verification fcts?
)




**** Composite ZefOps ****

my_op = map[f1] | filter[f2]

"""

# TODO fix repr for L op
# TODO make repr for ZefOp better
import warnings
from inspect import isfunction, getfullargspec
from types import LambdaType
from typing import Generator, Iterable, Iterator
from ._core import *
from ._error import  ExceptionWrapper, EvalEngineCoreError, add_error_context, convert_python_exception, make_custom_error, custom_error_handling_activated
from .VT import *
from .VT import make_VT
from .VT.helpers import generic_subtype_validate, generic_subtype_get
from . import internals, VT
from .internals import BaseUID, EternalUID, ZefRefUID
from ..pyzef import zefops as pyzefops
from .generators import ZefGenerator_

# this is used to circumvent the python '<' and '>' operator resolution rules
_terrible_global_state = {}
_call_0_args_translation = {
    internals.RT.Now : pyzefops.now,
}
_call_n_args_translation = {}
_sources = {}

def unpack_ops(rt, ops):
    if len(ops) > 1:
        return ((rt, ops[0]),  *ops[1:])
    else:
        return ((rt, *ops),)

def unpack_tmpzefop(rt, ops):
    return ((rt, (ops[0][1])), *ops[1:])

def is_tmpzefop(arg):
    return arg.el_ops[0][0] == internals.RT.TmpZefOp

def is_valid_LorO_op(other) -> bool:
    return (
        isinstance(other, ZefOp) 
        and len(other.el_ops)==1 
        and other.el_ops[0][0] in [internals.RT.L, internals.RT.O]
        )

# TODO Merge all ValueTypes checks together into a single function
def is_supported_stream(o):
    from rx.subject import Subject
    from rx.core import Observable
    if type(o) in {Subject, Observable}: return True
    return False

def is_python_scalar_type(o):
    return type(o) in {str, bytes, int, bool, float, type(None)}

def is_supported_value(o):
    from . import  GraphSlice
    from types import GeneratorType
    from . import Image
    from ..pyzef.main import Keyword
    from types import ModuleType
    from .op_implementations.implementation_typing_functions import ZefGenerator
    from .VT import Entity, Relation, AttributeEntity
    if is_python_scalar_type(o): return True
    if isinstance(o, (set, range, ZefGenerator, GeneratorType, list, tuple, dict, ValueType, GraphSlice, Time, Image, Bytes, Error, Keyword, ModuleType, UserValueInstance, Delegate, Val, EntityValueInstance, RAE)): return True
    if isinstance(o, (BaseUID, EternalUID, ZefRefUID, Enum, ZefRef, EZefRef, Graph, ET, RT, AET, GraphSlice)): return True
    return False

def is_supported_zef_value(o):
    if isinstance(o, (BaseUID, EternalUID, ZefRefUID, Enum, ZefRef, EZefRef, Graph, ET, RT, AET, GraphSlice)): return True
    return False

def is_supported_on_subscription(o, op):
    return op.el_ops[0][0] == internals.RT.On and (is_supported_zef_value(o) or isinstance(o, tuple))

def op_chain_pretty_print(el_ops):
    if not isinstance(el_ops, list) and not isinstance(el_ops, tuple):
        return repr(el_ops)

    from ._ops import map, collect, to_snake_case
    def param_to_str(pp):
        return f"[{repr(pp)}]"

    def el_op_to_str(p):
        import types
        if p[0] == internals.RT.Function:
            inner_f = p[1][0][1]
            if isinstance(inner_f, types.FunctionType):
                name = inner_f.__name__
                return name + ''.join([param_to_str(pp) for pp in p[1][1:]])
        # if p[0] == RT.OutOutOld:
        #     return f"\n>> todo!!!!"            
        return to_snake_case(p[0].name) + ''.join([param_to_str(pp) for pp in p[1]])
    return ' | '.join(el_op_to_str(x) for x in el_ops)

#   _                          ___                  ___                    _                                _           _    _               
#  | |      __ _  ____ _   _  / _ \  _ __   ___    |_ _| _ __ ___   _ __  | |  ___  _ __ ___    ___  _ __  | |_   __ _ | |_ (_)  ___   _ __  
#  | |     / _` ||_  /| | | || | | || '_ \ / __|    | | | '_ ` _ \ | '_ \ | | / _ \| '_ ` _ \  / _ \| '_ \ | __| / _` || __|| | / _ \ | '_ \ 
#  | |___ | (_| | / / | |_| || |_| || |_) |\__ \    | | | | | | | || |_) || ||  __/| | | | | ||  __/| | | || |_ | (_| || |_ | || (_) || | | |
#  |_____| \__,_|/___| \__, | \___/ | .__/ |___/   |___||_| |_| |_|| .__/ |_| \___||_| |_| |_| \___||_| |_| \__| \__,_| \__||_| \___/ |_| |_|
#                      |___/        |_|                            |_|                                                                       

# # For base python error handling
# import sys
# def zef_error_hook(typ, value, tb, *, prior_hook):
#     if typ in [EvalEngineCoreError, _ErrorType]:
#         try:
#             print(zef_error_as_str(value))
#             print_tb_up_to_zef(tb)
#         except Exception as exc:
#             print("Error in exception handler")
#             import traceback
#             traceback.print_tb(exc.__traceback__)
#     else:
#         return prior_hook(typ, value, tb)
# _old_excepthook = sys.excepthook
# sys.excepthook = lambda *args, prior_hook=_old_excepthook: zef_error_hook(*args, prior_hook=prior_hook)

class Evaluating:
    def __repr__(self):
        return "evaluating"
evaluating = Evaluating()

class ZefOp_:    
    def __init__(self, el_ops: tuple):
        if isinstance(el_ops, tuple):
            self.el_ops = el_ops
        else:
            self.el_ops = (el_ops, )
    
    def __len__(self):
         return len(self.el_ops)
        
    def __repr__(self):
        return op_chain_pretty_print(self.el_ops)
    
    def __le__(self, other):
        # we want to allow for "AET.Float['some_name'] <= 42.1"
        # We convert the first expression to a "instantiated[AET.Float]['some_name']"
        from ._ops import assign
        return LazyValue(self) | assign[other]
    
    def __ror__(self, other):
        from ._ops import is_a
        # if is_type(other) and (is_strict_subtype(other, RelationType) or is_strict_subtype(other, BlobType)):
        #     return ZefOp(((RT.TmpZefOp, (other,)), )) | self
        if False:
            pass
        elif isinstance(other, CollectingOp) or isinstance(other, ForEachingOp)  or isinstance(other, SubscribingOp):
            return other.__ror__(self)
        elif is_supported_on_subscription(other, self):
            from .op_implementations.implementation_typing_functions import on_implementation
            return on_implementation(other, *self.el_ops[0][1])
        elif is_supported_stream(other):
            from .fx import FX
            from ._ops import run
            stream =  {'type': FX.Stream.CreatePushableStream} | run
            return stream | self
        elif is_supported_value(other) or is_supported_zef_value(other):
            return LazyValue(other) | self
        # This is just for a bit of help for users to understand what's going on
        elif isinstance(other, Iterable):
            raise TypeError(f"An arbitrary iterable (we saw {type(other)}) will not be automatically converted to a LazyValue when being piped into a ZefOp. This is because it cannot be determined if it is a stateful iterator (e.g. a generator or a file object) or a stateless iterable (e.g. a list or a range() object).\n\nIf you know your iterable is immutable, then wrap the iterable in `LazyValue`, that is use `LazyValue(itr) | zefop` instead of `itr | zefop`.\n\nIf your iterable is an iterator that mutates on iteration you should either\n\ta) first wrap it in a caching mechanism (TODO make this available in zef: not available in any external library that I can see) or\n\tb) make its content available as a Stream, e.g. TODO.")

        return NotImplemented
        

    def __or__(self, other):
        if isinstance(other, ZefOp):
            res = ZefOp( (*self.el_ops, *other.el_ops) )
            return res
        return NotImplemented
        

    def __lshift__(self, other):
        # if isinstance(other, TraversableABC):
        #     return ZefOp( (*self.el_ops, (internals.RT.InInOld, (other, ))) )
        if is_valid_LorO_op(other):
            return ZefOp( (*self.el_ops, *unpack_ops(internals.RT.InInOld, other.el_ops)))
        if is_tmpzefop(other):
            return ZefOp( (*self.el_ops, *unpack_tmpzefop(internals.RT.InInOld, other.el_ops))) 
        raise TypeError(f'Unexpected type in "ZefOp << ..." expression. Only an RT or L[RT] makes sense here. It was called with {other} of type {type(other)}' )
        

    def __rshift__(self, other):
        # if isinstance(other, TraversableABC):
        #     return ZefOp( (*self.el_ops, (internals.RT.OutOutOld, (other, ))) )
        if is_valid_LorO_op(other):
            return ZefOp( (*self.el_ops, *unpack_ops(internals.RT.OutOutOld, other.el_ops)))
        if is_tmpzefop(other):
            return ZefOp( (*self.el_ops, *unpack_tmpzefop(internals.RT.OutOutOld, other.el_ops)))
        raise TypeError(f'Unexpected type in "ZefOp >> ..." expression. Only an RT or L[RT] makes sense here. It was called with {other} of type {type(other)}' )
    
    def __rrshift__(self, other):
        if is_supported_zef_value(other):
            return LazyValue(other) >> self
        raise TypeError(f'Unexpected type in "ZefType >> ZefOp" expression. It was called with {other} of type {type(other)}' )

    def __rlshift__(self, other):
        if is_supported_zef_value(other):
            return LazyValue(other) << self
        raise TypeError(f'Unexpected type in "ZefType << ZefOp" expression. It was called with {other} of type {type(other)}' )
    
    def __eq__(self, other):
        if isinstance(other, ZefOp):
            return self.el_ops == other.el_ops
        return NotImplemented

    def __hash__(self):
        return hash(self.el_ops)

    def lt_gt_behavior(self, other, rt):
        # other could be one of 3 cases
        # 1) It is another ZefOp
        # 2) It is another ZefOp that is of instance TmpZefOp
        # 3) It is of type RelationType
        import inspect
        frame = inspect.currentframe().f_back.f_back
        line_no = frame.f_lineno        #identify reuse within expression from line_no: even when splitting lines for one expression, the first line is used throughout
        other_id = None
        # Start by checking if self has been previously evaluated
        if (line_no, id(self.el_ops)) in _terrible_global_state:
            cached = _terrible_global_state[(line_no, id(self.el_ops))]
        elif (line_no, id(self)) in _terrible_global_state: # If it is an RT
            cached = _terrible_global_state[(line_no, id(self))]
        else:
            cached = self.el_ops

        if isinstance(cached, LazyValue):
            base = (*cached.el_ops.el_ops,)
        else:
            base = (*(cached),)

        # if isinstance(other, TraversableABC):
        #     res = ZefOp( (*base, (rt, (other, ))) )
        #     other_id = id(other)
        # elif isinstance(other, ForEachingOp) or isinstance(other, SubscribingOp) or isinstance(other, CollectingOp):
        if isinstance(other, ForEachingOp) or isinstance(other, SubscribingOp) or isinstance(other, CollectingOp):
            if len(other.el_ops) > 0 and is_tmpzefop(other):
                res = type(other)(ZefOp((*base, *unpack_tmpzefop(rt, other.el_ops))))
            else:
                res = type(other)(ZefOp((*base, *unpack_ops(rt, other.el_ops))))
            if type(other) in {SubscribingOp, ForEachingOp}: 
                res.func = other.func
                other.func = None

        elif is_tmpzefop(other):
            res = ZefOp( (*base, *unpack_tmpzefop(rt, other.el_ops)))
        else:
            res = ZefOp( (*base, *unpack_ops(rt, other.el_ops)))
        
        if isinstance(cached, LazyValue):
            new_lv = LazyValue(cached.initial_val)
            new_lv.el_ops = res
            res = new_lv
            if should_trigger_eval(res): return evaluate_lazy_value(res)
        
        other_id = other_id or id(other.el_ops) # Before checking the types above we can't assume other is a ZefOp
        _terrible_global_state[(line_no, other_id)] = res.el_ops
        return res

    def __gt__(self, other):
        if is_supported_zef_value(other): return LazyValue(other) < self
        return self.lt_gt_behavior(other, internals.RT.OutOld)

    def __lt__(self, other):
        if is_supported_zef_value(other): return LazyValue(other) > self
        return self.lt_gt_behavior(other, internals.RT.InOld)
        
    def __getitem__(self, x):
        if not len(self.el_ops)==1: 
            raise Exception(f'You can only curry arguments / parameters into an elementary zefop, i.e. one that is not chained. It was attempted to curry [{self.el_ops}] into {self}')

        return ZefOp(((self.el_ops[0][0], (*self.el_ops[0][1], x)), ))

    def __iter__(self):
        return (ZefOp((op,)) for op in self.el_ops)


    def __call__(self, *args, **kwargs):

        if len(kwargs) > 0 and self.el_ops[0][0] == internals.RT.Function:
            from .op_implementations.dispatch_dictionary import _op_to_functions
            if len(args) > 1: extra = args[1:]
            else: extra = []
            return _op_to_functions[self.el_ops[0][0]][0](args[0], *self.el_ops[0][1], *extra, **kwargs)

        # now()
        if len(self.el_ops) == 1 and len(args) == 0:
            if self.el_ops[0][0] in  _call_0_args_translation: 
                return _call_0_args_translation[self.el_ops[0][0]]()
            raise NotImplementedError(f"A ZefOp with 0 args was called but a 0-arg dispatch doesn't exist! ({self.el_ops[0][0]!r})")
        
        # a = map[...] | last ; a() 
        if len(self.el_ops) > 1 and len(args) == 0:
            raise NotImplementedError("A ZefOp with multiple internal ops and 0 args was called!")      
            
        # run(42 | f1 | print)
        if len(self.el_ops) == 1 and is_evaluating_run(self):
            res = args[0] | self
            return res
        # x = map[...] | last ; x([1,2,3])
        # x = [1,2,3] | map[...] ; last(x)
        if len(args) == 1: 
            collect_op = CollectingOp(self) 
        else:
            curried_op = self
            for arg in args[1:]:
                curried_op = curried_op[arg]
            collect_op = CollectingOp(curried_op)
        
        lzy_val = LazyValue(args[0]) if not isinstance(args[0], LazyValue) else args[0]
        from ._error import EvalEngineCoreError
        try:
            res = lzy_val | collect_op
        except Exception as e:
            if not custom_error_handling_activated():
                raise
            if isinstance(e, ExceptionWrapper):
                tb = e.__traceback__
                from ._error import process_python_tb
                frames = process_python_tb(tb)
                err = add_error_context(e.wrapped, {"frames": frames})
                raise ExceptionWrapper(err) from None
            else:
                raise e
        # Raise if this didn't evaluate!
        if isinstance(res, CollectingOp):
            raise Exception(f"ZefOp call didn't evaluate! {res}")
        return res

def ZefOp_subtype_check(other, self):
    if not isinstance(other, ZefOp):
        return False

    if len(self.el_ops) != 1:
        raise Exception("Not allowed to use ZefOp as a type directly, if it is a chain. Use a Pattern instead (implementation coming soon)")
    if len(other.el_ops) != 1:
        return False
    if len(self.el_ops[0][1]) > 0:
        raise Exception("Not allowed to use ZefOp as a type directly, if it has any curried arguments. Use a Pattern instead (implementation coming soon)")
    if other.el_ops[0][0] != self.el_ops[0][0]: return False        
    # This more complicated logic should fall under "Pattern" now. For example, what should:
    # isinstance(some_op[3], some_op[Int])
    # return? Is Int a type here, or a value?
    # # compare the elements curried into the operator. Recursively use this subtyping function
    # if len(self.el_ops[0][1]) > len(other.el_ops[0][1]): return False
    # for el_other, el_self in zip(other.el_ops[0][1], self.el_ops[0][1]):
    #     if not isinstance(el_other, el_self): return False        
    # return True
    return True
        
def zefop_is_a(x, typ):
    # Note: this is only hit for the Zefop value type, not for actual zefops or zefop chains
    # TODO: Proper testing with patterns. For now, just passing to the ZefOp_
    # class to handle basic "zefop type" checks.
    from .VT.helpers import remove_names, absorbed
    items = remove_names(absorbed(typ))
    if len(items) >= 2:
        raise Exception(f"ZefOp type cannot have more than 1 item absorbed, found {items}")
    elif len(items) == 0:
        subtype = None
    else:
        subtype = items[0]
    if subtype is None:
        return isinstance(x, ZefOp_)
    return ZefOp_subtype_check(x, subtype)
ZefOp = make_VT("ZefOp",
                pytype=ZefOp_,
                is_a_func=zefop_is_a)

class CollectingOp:
    def __init__(self, other: ZefOp):
        self.el_ops = other.el_ops

    def __repr__(self):
        return f"CollectingOp({op_chain_pretty_print(self.el_ops)})"
        
    def __ror__(self, other):
        if isinstance(other, ZefOp): 
            return CollectingOp(ZefOp((*other.el_ops, *self.el_ops)))
        if isinstance(other, LazyValue):
            other.el_ops = CollectingOp(ZefOp((*other.el_ops, *self.el_ops)))
            return evaluate_lazy_value(other)
        # if isinstance(other, TraversableABC):
        #     return CollectingOp(ZefOp(((internals.RT.TmpZefOp, (other,)), *self.el_ops)))
        if is_supported_value(other) or is_supported_zef_value(other):
            return LazyValue(other) | self
        raise TypeError(f'We should not have landed here. Value passed {other} of type {type(other)}.')

    def __or__(self, other):
        base = (*self.el_ops, (internals.RT.Collect, ()))
        if isinstance(other, ForEachingOp) or isinstance(other, SubscribingOp):
            res = type(other)(ZefOp((*base, *other.el_ops)))
            res.func = other.func
            other.func = None
            return res
        elif isinstance(other, ZefOp):
            return ZefOp((*base, *other.el_ops))

        return NotImplemented

    def lt_gt_behavior(self, other, rt):
        import inspect
        frame = inspect.currentframe().f_back.f_back
        line_no = frame.f_lineno    

        # We found a cached CollectOp so we will use it
        if (line_no, id(self)) in _terrible_global_state:
            ops = _terrible_global_state[(line_no, id(self))].el_ops
        else:
            ops = self.el_ops
        base = ((internals.RT.Collect, ()),) if ops == () else (*ops, (internals.RT.Collect, ()))
        
        # if isinstance(other, TraversableABC):
        #     res = ZefOp((*base, (rt, (other,))))
        # elif isinstance(other, ForEachingOp) or isinstance(other, SubscribingOp):
        if isinstance(other, ForEachingOp) or isinstance(other, SubscribingOp):
            if len(other.el_ops) > 0 and other.el_ops[0][0] == internals.RT.TmpZefOp:
                res = type(other)(ZefOp((*base, (rt, (other.el_ops[0][1])), *other.el_ops[1:])))
            else:
                res = type(other)(ZefOp((*base, *unpack_ops(rt, other.el_ops))))
            res.func = other.func
            other.func = None
        elif isinstance(other,ZefOp) and is_tmpzefop(other):
            res = ZefOp( (*base, *unpack_tmpzefop(rt, other.el_ops)))
        else:
            res = ZefOp((*base, *unpack_ops(rt, other.el_ops)))

        _terrible_global_state[(line_no, id(other))] = res.el_ops
        return res

    def __gt__(self, other):
        if is_supported_zef_value(other): return LazyValue(other) < self
        return self.lt_gt_behavior(other, internals.RT.OutOld)

    def __lt__(self, other):
        if is_supported_zef_value(other): return LazyValue(other) > self
        return self.lt_gt_behavior(other, internals.RT.InOld)

    def lshift_rshift_behavior(self, other, rt):
        base = ((internals.RT.Collect, ()),) if self.el_ops == () else (*self.el_ops, (internals.RT.Collect, ()))
        # if isinstance(other, TraversableABC):
        #     res = ZefOp((*base, (rt, (other,))))
        # elif isinstance(other,ZefOp) and other.el_ops[0][0] == internals.RT.TmpZefOp:
        if isinstance(other,ZefOp) and other.el_ops[0][0] == internals.RT.TmpZefOp:
            res = ZefOp((*base, *unpack_tmpzefop(rt, other.el_ops)))
        else:
            res = ZefOp((*base, *unpack_ops(rt, other.el_ops)))
        return res

    def __lshift__(self, other):
        return self.lshift_rshift_behavior(other, internals.RT.InInOld)

    def __rshift__(self, other):
        return self.lshift_rshift_behavior(other, internals.RT.OutOutOld)

    def __rrshift__(self, other):
        if is_supported_zef_value(other):
            return LazyValue(other) >> self
        raise TypeError(f'Unexpected type in "ZefType >> CollectingOp" expression. It was called with {other} of type {type(other)}' )

    def __rlshift__(self, other):
        if is_supported_zef_value(other):
            return LazyValue(other) << self
        raise TypeError(f'Unexpected type in "ZefType << CollectingOp" expression. It was called with {other} of type {type(other)}' )
        
    def __eq__(self, other):
        if not hasattr(other, "el_ops"):
            return NotImplemented
        return self.el_ops == other.el_ops

    def __call__(self, *args):
        # collect()
        if len(args) == 0:
            raise Exception(f"Cannot call collect() without any args!")
            
        # collect([1,2,3] | map[...] | last)
        if len(args) == 1:
            return args[0] | self

    def __hash__(self):
        return hash(self.el_ops)
        


class ConcreteAwaitable:
    def __init__(self, concrete_awaitable, concrete_type, chain):
        self.concrete_awaitable = concrete_awaitable
        self.concrete_type = concrete_type
        self.chain = chain + [self.concrete_type] 

    def __repr__(self):
        return f"{self.concrete_awaitable} -> {self.chain}"

class Awaitable:
    
    def __init__(self, stream_ezefref, pushable=False, unwrapping = False):
        self.stream_ezefref = stream_ezefref
        self.pushable = pushable
        self.unwrapping = unwrapping
        self.el_ops = ZefOp(())

    def __repr__(self):
        return f"Awaitable({self.stream_ezefref} \n| {self.el_ops})"

    def __or__(self, other):
        if isinstance(other, ZefOp) and is_evaluating_run(other):
            return self.evaluation(other, "Run")

        if isinstance(other, ZefOp):
            new_awaitable = Awaitable(self.stream_ezefref, self.pushable, self.unwrapping)
            if len(self.el_ops) > 0:
                new_awaitable.el_ops = ZefOp((*self.el_ops.el_ops, *other.el_ops,)) 
            else:
                new_awaitable.el_ops = other
            return new_awaitable

        if isinstance(other, SubscribingOp):
            return self.evaluation(other, "Subscribe")

        if isinstance(other, CollectingOp):
            raise NotImplementedError(f"Awaitable | not implemented with collect")

        raise NotImplementedError(f"Awaitable | not implemented with {type(other)}")
        

    def lt_gt_lshift_rshift_behavior(self, other, rt):
        import inspect
        frame = inspect.currentframe().f_back.f_back
        line_no = frame.f_lineno   
        if len(self.el_ops) > 0:
            if rt == internals.RT.OutOld:
                res = self.el_ops > other
            elif rt == internals.RT.InOld:
                res = self.el_ops < other
            elif rt == internals.RT.InInOld:
                res = self.el_ops << other
            elif rt == internals.RT.OutOutOld:
                res = self.el_ops >> other
        else:
            if isinstance(other,ZefOp) and is_tmpzefop(other):
                res = ZefOp((*unpack_tmpzefop(rt, other.el_ops),))
            elif isinstance(other, CollectingOp) or isinstance(other, SubscribingOp):
                if is_tmpzefop(other):
                    res = type(other)(ZefOp((*unpack_tmpzefop(rt, other.el_ops), )))
                else:
                    res = type(other)(ZefOp((*unpack_ops(rt, other.el_ops),))) 

                if isinstance(other, SubscribingOp): res.func = other.func

            # elif isinstance(other, TraversableABC):
            #     res = ZefOp(((rt, (other,)), ))
            else:
                res = ZefOp((*unpack_ops(rt, other.el_ops),))
        
        new_awaitable = Awaitable(self.stream_ezefref, self.pushable, self.unwrapping)
        new_awaitable.el_ops = res

        if rt == internals.RT.OutOld or rt == internals.RT.InOld:  _terrible_global_state[(line_no, id(other))] = new_awaitable
        return new_awaitable

    def __gt__(self, other):
        return self.lt_gt_lshift_rshift_behavior(other, internals.RT.OutOld)

    def __lt__(self, other):
        return self.lt_gt_lshift_rshift_behavior(other, internals.RT.InOld)

    def __lshift__(self, other):
        return self.lt_gt_lshift_rshift_behavior(other, internals.RT.InInOld) 

    def __rshift__(self, other):
        return self.lt_gt_lshift_rshift_behavior(other, internals.RT.OutOutOld) 

    def evaluation(self, other, kind):
        import rx
        from rx import operators as rxops
        from rx.subject import Subject
        from rx.core import Observable
        from .op_implementations.dispatch_dictionary import _op_to_functions
        from .fx import _state
        from ._ops import absorbed, only

        curr_type =  VT.Awaitable[VT.List[VT.String]]
        source = _state['streams'][self.stream_ezefref]  
        
        concrete_awaitable = ConcreteAwaitable(source, curr_type, [])
        ops = (*self.el_ops.el_ops, *other.el_ops) if kind == "Subscribe" else self.el_ops.el_ops

        if isinstance(source, Subject) or isinstance(source, Observable):
            observable_chain = source
            for op in ops if kind == "Subscribe" else ops[:-1]:
                observable_chain = _op_to_functions[op[0]][0](observable_chain,  *op[1])

                abs = generic_subtype_get(curr_type)
                curr_type = VT.Awaitable[_op_to_functions[op[0]][1](op,  abs)]
                concrete_awaitable = ConcreteAwaitable(observable_chain, curr_type, concrete_awaitable.chain)

            if kind == "Subscribe":
                from .logger import log
                observable_chain.pipe(
                    rxops.do_action(on_next=other.func),
                    rxops.do_action(on_error=lambda e: log.error(f"Error caught in rx stream", exc_info=e)),
                    rxops.retry(),
                ).subscribe()
            else:
                observable_chain = observable_chain.pipe(rxops.to_list())
                def type_assertion_wrapper(el):
                    abs = generic_subtype_get(curr_type)
                    if isinstance(el, list) and abs != "List":
                        return ops[-1][1][0](only(el))
                    return ops[-1][1][0](el)
                observable_chain.subscribe(type_assertion_wrapper)

            abs = generic_subtype_get(curr_type)
            concrete_awaitable = ConcreteAwaitable(observable_chain, abs, concrete_awaitable.chain)
            return observable_chain 
        
        raise NotImplementedError(f"Awaitable evalation not implemented with {type(source)}")

    def on_next(self, x):
        raise NotImplementedError()
        
    def on_complete(self):
        raise NotImplementedError()

class ForEachingOp:
    def __init__(self, other):
        self.el_ops = other.el_ops
        self.func = None

    def __repr__(self):
        return f"ForEachingOp(func={self.func}, {op_chain_pretty_print(self.el_ops)})"
        
    def __ror__(self, other):
        if isinstance(other, ZefOp):
            new_zo = ZefOp( (*other.el_ops, *self.el_ops) )
            res = ForEachingOp(new_zo)
            res.func = self.func
            return res
        # if isinstance(other, TraversableABC):
        #     res = ForEachingOp(ZefOp(((RT.TmpZefOp, (other,)), )))
        #     res.func = self.func
        #     return res
        if is_supported_value(other) or is_supported_zef_value(other):
            return LazyValue(other) | self
        raise TypeError(f'We should not have landed here. Value passed {other} of type {type(other)}.')
    
    def __or__(self, other):
        if isinstance(other, ZefOp):
            res = ForEachingOp(other)
            res.func = self.func
            return res
        if isinstance(other, LazyValue):            
            raise NotImplementedError()
        return NotImplemented

    def __gt__(self, other):
        if is_supported_zef_value(other): return LazyValue(other) < self
        
    def __lt__(self, other):
        if is_supported_zef_value(other): return LazyValue(other) > self

    def __getitem__(self, func):
        assert func is not None, "for_each requires a function"
        new_for_each = ForEachingOp(self)
        new_for_each.func = func
        return new_for_each


class SubscribingOp:
    def __init__(self, op: ZefOp):
        self.el_ops = op.el_ops     # a tuple: each element representing an elementary operator
        self.func = None

    def __repr__(self):
        return f"SubscribingOp(func={self.func}, {op_chain_pretty_print(self.el_ops)})"

    def __ror__(self, other):
        if isinstance(other, ZefOp):
            res = SubscribingOp(other)
            res.func = self.func
            return res

        # if isinstance(other, TraversableABC):
        #     res = SubscribingOp(ZefOp(((RT.TmpZefOp, (other,)), )))
        #     res.func = self.func
        #     return res
            
        if isinstance(other, ZefRef):
            # check that this is an AET and allow subscribing natively
            raise NotImplemented
            
        raise TypeError(f'We should not have landed here. Value passed {other} of type {type(other)}.')
            
    def __or__(self, other):
        if isinstance(other, ZefOp):
            res =  SubscribingOp(other)
            res.func = self.func
            return res
        if isinstance(other, LazyValue):            
            raise NotImplementedError()
        return NotImplemented

    def __getitem__(self, func):
        new_op = SubscribingOp(ZefOp(self.el_ops))
        new_op.func =  func
        return new_op


class LazyValue:
    def __init__(self, arg):
        # if type(arg) == LazyValue:
        #     # Create a copy
        #     self.initial_val = arg.initial_val
        #     self.el_ops = arg.el_ops
        # else:
            # Allow LazyValue to encapsulate a LazyValue, if something breaks uncomment above part
            self.initial_val = arg
            self.el_ops = ()
    
    def __repr__(self):
        if type(self.el_ops) in {CollectingOp, ForEachingOp} or len(self.el_ops) > 0:
            return f"LazyValue({self.initial_val} | {self.el_ops})"
        return f"LazyValue({self.initial_val})"

    def __or__(self, other):
        if not isinstance(other, OpLike):
            return NotImplemented

        if len(self.el_ops) > 0:
            res = self.el_ops | other
        else:
            res = other
        
        res_lazyval = LazyValue(self.initial_val)
        res_lazyval.el_ops = res

        from ._error import EvalEngineCoreError
        try:
            if should_trigger_eval(res_lazyval): return evaluate_lazy_value(res_lazyval)
        except Exception as e:
            if not custom_error_handling_activated():
                raise
            if isinstance(e, ExceptionWrapper):
                tb = e.__traceback__
                from ._error import process_python_tb
                frames = process_python_tb(tb)
                err = add_error_context(e.wrapped, {"frames": frames})
                if getattr(e, "keep_traceback", None):
                    raise ExceptionWrapper(err) from e
                raise ExceptionWrapper(err) from None
            else:
                raise e
        return res_lazyval

    def lt_gt_lshift_rshift_behavior(self, other, rt):
        import inspect
        frame = inspect.currentframe().f_back.f_back   
        other_id = id(other)

        if len(self.el_ops) > 0:
            if rt == internals.RT.OutOld:
                res = self.el_ops > other
            elif rt == internals.RT.InOld:
                res = self.el_ops < other
            elif rt == internals.RT.InInOld:
                res = self.el_ops << other
            elif rt == internals.RT.OutOutOld:
                res = self.el_ops >> other
        else:
            if isinstance(other,ZefOp) and is_tmpzefop(other):
                other_id = id(other.el_ops[0][1][0])
                res = ZefOp((*unpack_tmpzefop(rt, other.el_ops),))
            elif isinstance(other, ForEachingOp) or isinstance(other, CollectingOp):
                if is_tmpzefop(other):
                    res = type(other)(ZefOp((*unpack_tmpzefop(rt, other.el_ops), )))
                else:
                    res = type(other)(ZefOp((*unpack_ops(rt, other.el_ops),))) 
                if type(other) in { ForEachingOp}: 
                    res.func = other.func
                    other.func = None
            # elif isinstance(other, TraversableABC):
            #     res = ZefOp(((rt, (other,)), ))
            else:
                res = ZefOp((*unpack_ops(rt, other.el_ops),))
        
        res_lazyval = LazyValue(self.initial_val)
        res_lazyval.el_ops = res
        if rt == internals.RT.OutOld or rt == internals.RT.InOld:
            # We are caching with as many line_nos as possible due to the fact that we don't how deep the call stack is to arrive to this point.
            while frame:_terrible_global_state[(frame.f_lineno, other_id)] = res_lazyval;frame = frame.f_back
        if should_trigger_eval(res_lazyval): return evaluate_lazy_value(res_lazyval)
        return res_lazyval

    def __gt__(self, other):
        return self.lt_gt_lshift_rshift_behavior(other, internals.RT.OutOld)

    def __lt__(self, other):
        return self.lt_gt_lshift_rshift_behavior(other, internals.RT.InOld)

    def __lshift__(self, other):
        return self.lt_gt_lshift_rshift_behavior(other, internals.RT.InInOld) 

    def __rshift__(self, other):
        return self.lt_gt_lshift_rshift_behavior(other, internals.RT.OutOutOld) 

    def __le__(self, value):
        from ._ops import assign
        return self | assign[value]

    def __iter__(self):
        return iter(self.evaluate(unpack_generator = False))

    def __call__(self, *args):
        # x = [1,2,3] | map[...] | last ; x()
        if len(args) == 0:
            return self | CollectingOp(ZefOp(((internals.RT.Collect, ()),)))
        
        # x = [1,2,3] | map[...] | last ; x([1,2,3])
        if len(args) > 0:
            raise Exception(f"Cannot call a LazyValue with args passed!")

    # For avoiding common mistakes
    def __eq__(self, other):
        if isinstance(other, LazyValue):
            return self.initial_val == other.initial_val and self.el_ops == other.el_ops

        import logging
        logging.warning("A LazyValue has been compared with == or !=. This is likely a mistake, and requires an additional '| collect'")
        import traceback
        traceback.print_stack()
        return NotImplemented

    def __hash__(self):
        # return hash((self.initial_val, self.el_ops))
        from .VT.value_type import hash_frozen
        return hash_frozen((self.initial_val, self.el_ops))


    def __bool__(self):
        if getattr(self, "_allow_bool", False):
            return True
        raise Exception("Shouldn't cast LazyValue to bool (this may change in the future to automatic evaluation)")

    def evaluate(self, unpack_generator = True):
        from .op_implementations.dispatch_dictionary import _op_to_functions
        from .op_implementations.implementation_typing_functions import ZefGenerator
        from ..core._error import Error_

        curr_op = None
        curr_value = self.initial_val

        try:

            for op_i,op in enumerate(self.el_ops.el_ops): 
                
                curr_op = op
                cur_context = {
                    "chain": self,
                    "op_i": op_i,
                    "input": curr_value,
                    "op": curr_op,
                }

                if op[0] == internals.RT.Collect: continue

                if op[0] == internals.RT.Run:
                    # If this raises an error then it will be handled as EvalEngineCoreError
                    # so that we can keep the traceback of the original nested error
                    # TODO:
                    # For FX and impure function errors, work on either displaying the frames or the traceback object.
                    if len(op[1]) > 1: # i.e run[impure_func]
                        try:
                            curr_value = op[1][1](curr_value)
                        except Exception as e:
                            message = f"Failed while trying to run this impure function {op[1][1]}: \n{e.args}"
                            err =  Error.Panic()
                            err = make_custom_error(e, err, message, cur_context)
                            err.keep_traceback = True
                            err.__traceback__ = e.__traceback__
                            raise err from e
                    elif isinstance(curr_value, dict): 
                        try:
                            curr_value = _op_to_functions[op[0]][0](curr_value)
                        except Exception as e:
                            message = f"Failed while trying to run the following FX: {str(curr_value)[:50]}...\n{e}"
                            raise make_custom_error(e, Error.Panic(), message, cur_context) from None
                    else:
                        message = f"only effects or nullary functions can be passed to 'run' to be executed in the imperative shell. Received {curr_value}"
                        raise make_custom_error(NotImplementedError(), Error.NotImplementedError(), message, cur_context) from None
                    break

                try:
                    to_call_func = _op_to_functions[op[0]][0]
                except KeyError as e:
                    raise make_custom_error(e, Error.KeyError(), f"Cannot find {e} inside dispatch dictionary",cur_context) from None
                except Exception as e:
                    raise make_custom_error(e, Error.Panic(), f"Error happened trying to access dispatch function for {op[0]}",cur_context) from None


                got_error = None
                try:
                    new_value = to_call_func(curr_value,  *op[1])
                except Exception as e:
                    if not custom_error_handling_activated():
                        raise

                    if isinstance(e, EvalEngineCoreError):
                        # This is definitely a panic - but we want to attach the
                        # current evaluation information along with this.
                        # Probably want to add in python traceback here
                        e = EvalEngineCoreError(e)
                        got_error = add_error_context(e, cur_context)
                    elif isinstance(e, ExceptionWrapper):
                        # Continue the panic, attaching more tb info
                        tb = e.__traceback__
                        from ._error import process_python_tb
                        frames = process_python_tb(tb)
                        got_error = add_error_context(e.wrapped, {"frames": frames})
                        got_error = add_error_context(got_error, type_checking_context(op, to_call_func, curr_value))
                
                    elif isinstance(e, Error_):
                        got_error = e
                        got_error = add_error_context(got_error, type_checking_context(op, to_call_func, curr_value))
                        # print("2")
                
                    else:
                        # print("3")
                        py_e,frames = convert_python_exception(e)
                        got_error = Error.Panic()
                        got_error.nested = py_e
                        got_error = add_error_context(got_error, {"frames": frames})
                        got_error = add_error_context(got_error, type_checking_context(op, to_call_func, curr_value))
                else:
                    if isinstance(new_value, Error_):
                        # Here we have a choice - depends on what the caller expects, an Error or an exception
                        # Could also pass this down the line
                        # Need to distinguish between a caller wanting an error or wanting an exception

                        # Build details here
                        # if user_wants_exception:
                        # TODO Can we add context about frame here?
                        if True:
                            new_value.nested = {"type": new_value.name, "args": new_value.args}
                            got_error = new_value
                            got_error = add_error_context(got_error, type_checking_context(op, to_call_func, curr_value))
                        else:
                            pass
                    elif isinstance(new_value, ZefGenerator_):
                        new_value = new_value.add_context(cur_context)
                    

                if got_error is not None:
                    if not custom_error_handling_activated():
                        raise got_error
                    else:
                        raise add_error_context(got_error, cur_context) from None

                if isinstance(new_value, (Generator, Iterator)):
                    print("Operator produced a raw generator or iterator")
                    print(type(new_value))
                    print(op)

                curr_value = new_value

            if unpack_generator:
                if isinstance(curr_value, Iterator) or isinstance(curr_value, Generator):
                    # This branch should be eliminated if possible
                    print("NEED TO GET RID OF THIS")
                    print("With type:", type(curr_value))
                    print("NEED TO GET RID OF THIS")

                    return_list = []

                    it = iter(curr_value)
                    i = 0
                    while True:
                        cur_context = {
                            "chain": self,
                            "state": "collecting",
                            "val_i": i,
                        }
                        try:
                            val = next(it)
                        except StopIteration:
                            break
                        except Exception as e:
                            if not custom_error_handling_activated():
                                raise
                                
                            if isinstance(e, EvalEngineCoreError):
                                raise add_error_context(e, cur_context)
                            elif isinstance(e, ExceptionWrapper):
                                raise add_error_context(e.wrapped, cur_context) from None
                            elif isinstance(e, Error_):
                                raise add_error_context(e, cur_context) from None
                            else:
                                py_e,frames = convert_python_exception(e)
                                e = Error.Panic()
                                e.nested = py_e
                                e = add_error_context(e, {"frames": frames})
                                e = add_error_context(e, cur_context)
                                raise e from None

                        if isinstance(val, Error_):
                            if not custom_error_handling_activated():
                                raise val
                            else:
                                raise add_error_context(val, cur_context) from None

                        return_list.append(val)
                    return return_list
                elif isinstance(curr_value, ZefGenerator_):
                    # ZefGenerator handles its own context and error raising
                    # TODO: We could bring the error handling into here?
                    return [i for i in curr_value]

            return curr_value

        except Exception as exc:
            if not custom_error_handling_activated():
                raise
            if isinstance(exc, EvalEngineCoreError):
                # print("7")
                raise
        
            elif isinstance(exc, ExceptionWrapper):
                # print("8")
                raise exc from None

            elif isinstance(exc, Error_):
                # print("9")
                if getattr(exc, "keep_traceback", None):
                    wrapper = ExceptionWrapper(exc)
                    wrapper.keep_traceback = True
                    wrapper.__traceback__ = exc.__traceback__
                    raise wrapper
                raise ExceptionWrapper(exc) from None

            else:
                # print("10")
                e = EvalEngineCoreError(exc)
                e = add_error_context(e, {
                    "chain": self,
                    "op_i": 0,
                    "input": curr_value,
                    "op": curr_op,}
                )
                e = add_error_context(e, {"frames": e.frames,} )
                raise e 

# Monkey patching for some handy warnings
from .VT.value_type import ValueType_
old_ValueType_instancecheck = ValueType_.__instancecheck__
def warning_ValueType_instancecheck(self, instance):
    if type(instance) == LazyValue and self._d["type_name"] != "LazyValue":
        import traceback
        traceback.print_stack()
        raise Exception("Checking whether a LazyValue is a particular ValueType directly with isinstance will always fail. Use is_a instead, which will handle LazyValues.")
    return old_ValueType_instancecheck(self, instance)
ValueType_.__instancecheck__ = warning_ValueType_instancecheck

# Perform type checking when an error occurs
def type_checking_context(op, function, inp):
    from .op_implementations.implementation_typing_functions import is_a_implementation
    try:
        if op[0] == internals.RT.Function:
            function = op[1][0][1]
            import types
            assert type(function) == types.FunctionType, "Failed function check"
        
        import inspect
        full_arg_spec = inspect.getfullargspec(function)
        args, annotations = full_arg_spec.args, full_arg_spec.annotations
        assert len(annotations) > 0, "Missing Annotations"
        arg_type = annotations.get(args[0], None)
        assert arg_type is not None, "Failed retrieving the annotation for input arg"
        return {"type_check": {"expected": {"input":inp, "arg": args[0], "type": arg_type}, "result": is_a_implementation(inp, arg_type), "function": function}}

    except Exception as exc:
        return {"type_check": None}


def find_type_of_current_value(curr_value):
    if isinstance(curr_value, Iterator) or isinstance(curr_value, Generator): 
        try:
            first_val = next(curr_value)
            def output_generator():
                yield first_val
                yield from curr_value
            return type_spec(first_val), output_generator()
        except Exception:
            warnings.warn("Cannot infer data type from an empty generator. Using Any instead!")
            return VT.Any, curr_value 
    else: 
        return type_spec(curr_value), curr_value                                                                                     
OpLike = (ZefOp, CollectingOp, ForEachingOp, SubscribingOp)


#   _____                _                _    _                   _____                _              
#  | ____|__   __  __ _ | | _   _   __ _ | |_ (_)  ___   _ __     | ____| _ __    __ _ (_) _ __    ___ 
#  |  _|  \ \ / / / _` || || | | | / _` || __|| | / _ \ | '_ \    |  _|  | '_ \  / _` || || '_ \  / _ \
#  | |___  \ V / | (_| || || |_| || (_| || |_ | || (_) || | | |   | |___ | | | || (_| || || | | ||  __/
#  |_____|  \_/   \__,_||_| \__,_| \__,_| \__||_| \___/ |_| |_|   |_____||_| |_| \__, ||_||_| |_| \___|
#                                                                                |___/                 

def is_evaluating_run(op):
    if isinstance(op, ZefOp) and op.el_ops[-1][0] == internals.RT.Run and op.el_ops[-1][1][0] == evaluating:
        return True
    return False

def should_trigger_eval(value_maybe):
    if isinstance(value_maybe, LazyValue):
        if is_evaluating_run(value_maybe.el_ops): return True
        if isinstance(value_maybe.el_ops, CollectingOp) or isinstance(value_maybe.el_ops, ForEachingOp):
            return True
        else:
            for op in value_maybe.el_ops.el_ops: # These are the Ops of the nested ZefOp
                if op[0] in {internals.RT.Collect}: return True 
    
    elif isinstance(value_maybe, Awaitable):
        raise NotImplementedError("Evaluating isn't implemented yet for Awaitables!")
    
    return False

# ----LazyValue evaluation-----
def evaluate_lazy_value_with_curried_op(lazyval: LazyValue) -> LazyValue:
    # Testcase: LazyValue([1,2,3]) | map[mapper] | collect | map[mapper] 
    if isinstance(lazyval.el_ops, ForEachingOp):
        if lazyval.el_ops.func is None: raise Exception("The curried ForeachingOp doesn't have a defined function.")
        res = [lazyval.el_ops.func(x) for x in lazyval]
    elif isinstance(lazyval.el_ops, CollectingOp):
        res = lazyval.evaluate()
    elif is_evaluating_run(lazyval.el_ops):
        res = lazyval.evaluate()
    else:
        raise NotImplementedError('No supported type for trigger_eval!')
    return res

def evaluate_lazy_value_with_zefop(lazyval: LazyValue) -> LazyValue:
    # Testcase: LazyValue([1,2,3]) | map[mapper] | collect >> filter[mapper]
    ops = lazyval.el_ops.el_ops
    for idx, op in enumerate(ops):
        if op[0] in {internals.RT.Collect}: break
    tmp_ops = ops[idx + 1:] if idx + 1 < len(ops) else ()
    if ops[idx][0] == internals.RT.Collect:
        tmp_op = CollectingOp(ZefOp(ops[:idx]))
    else:
        raise NotImplementedError

    new_inner = LazyValue(lazyval)
    new_inner.el_ops = tmp_op
    new_lazyval = LazyValue(evaluate_lazy_value_with_curried_op(new_inner))
    new_lazyval.el_ops = ZefOp(tmp_ops)
    return new_lazyval

def evaluate_lazy_value(lazyval: LazyValue) -> LazyValue:
    if type(lazyval.el_ops) in {CollectingOp, ForEachingOp} or is_evaluating_run(lazyval.el_ops):
        return evaluate_lazy_value_with_curried_op(lazyval)
    elif isinstance(lazyval.el_ops, ZefOp):
        return evaluate_lazy_value_with_zefop(lazyval)
    
    raise NotImplementedError(f"evaluate_lazy_value not implemented with {type(lazyval.el_ops)}")
        
    

# ---- Awaitable evaluation -----
def evaluate_Awaitable(Awaitable: Awaitable) -> Awaitable:
    raise NotImplementedError("evaluate_Awaitable")


#   _____  _             _     _____                         ___          __        
#  |  ___|(_) _ __    __| |   |_   _| _   _  _ __    ___    |_ _| _ __   / _|  ___  
#  | |_   | || '_ \  / _` |     | |  | | | || '_ \  / _ \    | | | '_ \ | |_  / _ \ 
#  |  _|  | || | | || (_| |     | |  | |_| || |_) ||  __/    | | | | | ||  _|| (_) |
#  |_|    |_||_| |_| \__,_|     |_|   \__, || .__/  \___|   |___||_| |_||_|   \___/ 
#                                     |___/ |_|                                     

def get_compiled_zeffunction(fct):
    from .. import zef_functions
    try:   
        fct = zef_functions._local_compiled_zef_functions[zef_functions.time_resolved_hashable(fct)]
    except KeyError:
        fct = zef_functions.compile_zef_function(fct)
    return fct

def type_spec_iterable(obj, vt_type):
    # TODO: this causes slowness with large lists i.e list(range(10**1000))
    try:
        tps = set(type_spec(e) for e in obj)
        if len(tps) == 1:
            return vt_type[next(iter(tps))]
        else:
            return vt_type[VT.Any]
    except:
        return vt_type[VT.Any]
        
def type_spec_dict(obj):
    tps = set((type_spec(k), type_spec(v)) for (k,v) in obj.items())
    keytypes = set(k for (k, v) in tps)
    valtypes =  set(v for (k, v) in tps)
    kt = next(iter(keytypes)) if len(keytypes) == 1 else VT.Any
    vt = next(iter(valtypes)) if len(valtypes) == 1 else VT.Any
    res = VT.Dict[kt, vt]
    return res

def type_spec_tuple(obj):
    new_tup = VT.Tuple
    if len(obj) > 0:
        return new_tup[type_spec(obj[0])]
    else:
        return new_tup[VT.Any]

def type_spec(obj, no_type_casting = False):
    from .VT import ValueType_
    from . import GraphSlice
    from rx.subject import Subject
    from rx.core import Observable
    if isinstance(obj, ValueType_):               return obj
    if isinstance(obj, type) or no_type_casting: t = obj
    else:                                        t = type(obj)
    res = {
        int:                        VT.Int,
        str:                        VT.String,
        bool:                       VT.Bool,
        float:                      VT.Float,
        type(None):                 VT.Nil,
        list:                       lambda o: type_spec_iterable(o, VT.List),
        set:                        lambda o: type_spec_iterable(o,  VT.Set),
        dict:                       type_spec_dict,
        tuple:                      type_spec_tuple,
        ZefRef:                     VT.ZefRef,
        EZefRef:                    VT.EZefRef,
        Graph:                      VT.Graph,
        Time:                       VT.Time,  
        Subject:                    VT.Awaitable,
        Observable:                 VT.Awaitable,
        GraphSlice:                 VT.GraphSlice,
    }.get(t, lambda o: ValueType_(type(o).__name__, 0))
    try:
        return res if str(res) in dir(VT) else res(obj)
    except Exception as e:
        raise Exception(f"An error happened in type_spec for arg {obj}") from e


def create_type_info(lazyval) -> list:
    from .op_implementations.dispatch_dictionary import _op_to_functions
    curr_type = type_spec(lazyval.initial_val)
    type_transformation = [curr_type]
    for op in lazyval.el_ops.el_ops:
        if op[0] in {internals.RT.Collect , internals.RT.Run}:
            pass
        elif op[0] in _op_to_functions:
            curr_type = _op_to_functions[op[0]][1](op, curr_type)
        else:
            raise NotImplementedError(f"{op[0]} isn't handled in create_type_info")
        
        type_transformation.append(curr_type)
    return type_transformation
