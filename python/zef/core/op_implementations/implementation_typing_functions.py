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

import builtins
from typing import Generator, Iterable, Iterator


from rx import operators as rxops
from ..VT.value_type import ValueType_, is_a_

# This is the only submodule that is allowed to do this. It can assume that everything else has been made available so that it functions as a "user" of the core module.
from .. import *
from ..op_structs import _call_0_args_translation, type_spec
from .._ops import *
from ..abstract_raes import abstract_rae_from_rae_type_and_uid
from .flatgraph_implementations import *
from ..logger import log
from .._error import Error_, Error
from .._error import EvalEngineCoreError, ExceptionWrapper, add_error_context, prepend_error_contexts, convert_python_exception, process_python_tb, custom_error_handling_activated

from ...pyzef import zefops as pyzefops, main as pymain
from ..internals import BaseUID, EternalUID, ZefRefUID, to_uid, ZefEnumStruct, ZefEnumStructPartial
from .. import internals
import itertools
from typing import Generator, Iterable, Iterator

zef_types = [VT.Graph, VT.ZefRef, VT.EZefRef]
ref_types = [VT.ZefRef, VT.EZefRef]


#--utils---
def curry_args_in_zefop(zefop, first_arg, nargs = ()):
    for arg in nargs: zefop = zefop[arg]
    return zefop(first_arg) if callable(zefop) else zefop[first_arg]

def parse_input_type(input_type: ValueType_) -> str:
    # TODO instead of returning a string may be return also types i.e VT.Zef or VT.Tools
    if input_type is None: return None
    if input_type == VT.Nil: return None
    if input_type == VT.Awaitable: return "awaitable"
    if input_type in  zef_types: return "zef"
    return "tools"
#--utils---

def is_RT_triple(x):
    if not isinstance(x, tuple):
        return False
    if not is_a(x[1], RT):
        return False
    return True


def wrap_error_raising(e, maybe_context=None):
    if type(e) == EvalEngineCoreError:
        if maybe_context is not None:
            e = prepend_error_contexts(e, maybe_context)
        raise e
    elif type(e) == ExceptionWrapper:
        # Continue the panic, attaching more tb info
        tb = e.__traceback__
        frames = process_python_tb(tb)
        e = e.wrapped
        e = add_error_context(e, {"frames": frames})
        if maybe_context is not None:
            e = prepend_error_contexts(e, maybe_context)
        raise e from None
    elif type(e) == Error_:
        if maybe_context is not None:
            e = prepend_error_contexts(e, maybe_context)
        raise e from None
    else:
        py_e,frames = convert_python_exception(e)
        e = Error.UnexpectedError()
        e.nested = py_e
        e = add_error_context(e, {"frames": frames})
        if maybe_context is not None:
            e = prepend_error_contexts(e, maybe_context)
        raise e from None

def call_wrap_errors_as_unexpected(func, *args, maybe_context=None, **kwargs):
    if not custom_error_handling_activated():
        return func(*args, **kwargs)
        
    from ..op_structs import EvalEngineCoreError
    try:
        return func(*args, **kwargs)
    # Below here is sort of a repeat of what's in op_structs.py, but not quite
    except EvalEngineCoreError as e:
        wrap_error_raising(e, maybe_context)
    except ExceptionWrapper as e:
        wrap_error_raising(e, maybe_context)
    except Error_ as e:
        wrap_error_raising(e, maybe_context)
    except Exception as e:
        # We are overwriting this frame information to add the func name
        # because without doing it shows call_wrap_errors_as_unexpected as the func_name
        # however we are unable to figure out the line_no information
        import inspect
        py_e,frames = convert_python_exception(e)
        e = Error.UnexpectedError()
        e.nested = py_e
        frames[0]['func_name'] = func.__name__
        frames[0]['filename'] =  inspect.getfile(func)
        frames[0]['lineno'] =  ""
        inspect.getlineno
        e = add_error_context(e, {"frames": frames})
        wrap_error_raising(e, maybe_context)

@func 
def verify_zef_list(z_list: ZefRef):
    """
    1) z_list must be of type ET.ZEF_List
    2) all RAEs attached to z_list via an RT.ZEF_ListElement must form a linear linked list
    3) all elements connect via RT.ZEF_NextElement must be to edges coming off that ZefList
    """
    assert z_list | is_a[ET.ZEF_List] | collect
    return True






####################################################
# * TEMPLATE FOR DOCSTRINGS
#--------------------------------------------------
#
    """ 
    <General description>
 
    ---- Examples ----
    >>> <example one-liner 1>       # => <result of the one-liner (a value)>
    >>> <example one-liner 2>       # <comment on the behaviour (not a value)>
 
    >>> # <description of following example>
    >>> <example one-liner with multiline result>
    <result line 1>
    <result line 2>
 
    >>> # <description of following example>
    >>> <multiline example line 1>    
    ... <multiline example line 2>
    ... <multiline example line 3>  
    <result of multiline example>
 
    ---- Signature ----
    <method 1: input types as tuple> -> <output type>
    <method 2: input types as tuple> -> <output type>
 
    ---- Related ----
    - <keyword 1>
    - <keyword 2>
    """
_dummy = None
#
# Notes:
#  - Always include a newline after the opening """.
#
#  - Always put the closing """ on a separate line.
#
#  - General description can be as long as needed and include multiple paragraphs.
#
#  - Include newlines between examples to allow automatic parsing.
#
#  - The examples should not include an explicit "collect", they instead
#  - document what the output value would be at this point in the op chain.
#
#  - If a zefop can be called with a single argument, then its input types in the
#  - signature should be written as just that type, e.g. "T".
#
#  - If a zefop can be called without arguments, then its input types in the
#  - signature should be written as "()".
#
#  - The related section is for related types/concepts which can be grouped in
#    automatic documentation.



#---------------------------- function ------------------------------------

def function_imp(x0, func_repr, *args, **kwargs):
    """
    func_repr is of form (int, any)
    where the first integer encodes how the function is
    represented in the ZefOp:
    -------- representation types -------
    0) Abstract Entity
    1) captured python lambda or local function
    """
    repr_indx, fct = func_repr
    if repr_indx == 0:
        from zef.core.zef_functions import abstract_entity_call
        return abstract_entity_call(fct, x0, *args, **kwargs)
    if repr_indx == 1:
        # return fct(x0, *args, **kwargs)
        return call_wrap_errors_as_unexpected(fct, x0, *args, **kwargs)
    else:
        raise NotImplementedError('Zef Lambda expressions is not implemented yet.')



def function_tp(op):
    return VT.Any


#---------------------------- on ------------------------------------
def on_implementation(g, op):
    """
    Create an event stream based on the event type declared 
    in the parameters. This function itself is impure! It sets 
    up a concrete stream and a callback listening for events 
    on a graph.

    The returned Streams do not contain dicts, but the events
    expressed as ZefOps. Why? The Zef query syntax is built
    upon predicate logic and the events in the stream are in 
    this very language and represent positive clauses, i.e. 
    state facts about what happened. For example, 
    "instantiated[z1]" is an assertion / account of the fact
    that the entity referenced by z1 occurred. The reference
    frame of z1 contains the time at which the logical thread
    of that frame learned about that fact.

    This function is pure, it returns an abstract stream. The
    program state is only changed and all is hooked up on the
    dataflow graph once an impure functions subscribes at 
    the very end.

    ---- Examples ----
    >>> g | on[assigned[AET.String]]                    # assigned[z3]['hello!']      c.f. with action: assign[z3]['hello!']
    >>> g | on[terminated[z2]]                          # terminated[z2], followed by completion_event
    >>> g | on[instantiated[ET.Foo]]                    # instantiated[z5]
     
    # listening for new relations
    # old syntax:  zz | subscribe[on_instantiation[outgoing][RT.Foo]][my_callback]       old syntax
    >>> g | on[ instantiated[(zz, RT.Foo, Z)] ]         # Z matches on anything, i.e. takes on role of "_"
    
    >>> One can also add more precise requirements
    >>> g | on[ instantiated[(zz, RT.Foo, ET.Bar)] ]    # an element "instantiated[z_rel]" is pushed into the stream. Instances are represented by a single ZefRef, types by a triple for relations
    >>> g | on[ terminated[(zz, RT.Foo, Z)] ]
    
    >>> g | on[terminated[z_rel]]

    ---- Signature ----
    (Graph, ZefOp[ValueAssigned]) -> Stream[ZefOp[ValueAssigned[ZefRef][Any]]]
    (Graph, ZefOp[Instantiated]) -> Stream[ZefOp[Instantiated[ZefRef]]]
    (Graph, ZefOp[Terminated]) -> Stream[ZefOp[Terminated[ZefRef]]]
    
    ...


    """
    assert isinstance(op, ValueType)
    assert len(absorbed(op)) == 1
    from ...pyzef import zefops as internal
    from ..fx import FX, Effect

    stream =  FX.Stream.CreatePushableStream() | run
    sub_decl = internal.subscribe[internal.keep_alive[True]]
    
    if isinstance(g, Graph):
        op_kind = without_absorbed(op)
        op_args = absorbed(op)      
        if op_kind in {VT.Instantiated, VT.Terminated}:
            selected_types = {VT.Terminated: (internal.on_termination, terminated), VT.Instantiated: (internal.on_instantiation, instantiated)}[op_kind]
            
            if not isinstance(op_args[0], tuple):
                rae_or_zr = op_args[0]
                # Type 1: a specific entity i.e on[terminated[zr]]  !! Cannot be on[instantiated[zr]] because doesnt logically make sense !!
                if isinstance(rae_or_zr, ZefRef): 
                    assert op_kind == VT.Terminated, "Cannot listen for a specfic ZefRef to be instantiated! Doesn't make sense."
                    def filter_func(root_node): root_node | frame | to_tx | events[VT.Terminated] | filter[lambda x: to_ezefref(absorbed(x)[0]) == to_ezefref(rae_or_zr)] |  for_each[lambda x: LazyValue(x) | push[stream] | run ] 
                    sub_decl = sub_decl[filter_func]
                    sub = g | sub_decl                
                # Type 2: any RAE  i.e on[terminated[ET.Dog]] or on[instantiated[RT.owns]] 
                elif type(rae_or_zr) in {AttributeEntityType, EntityType, RelationType}: 
                    def filter_func(root_node): root_node | frame | to_tx | events[op_kind] | filter[lambda x: rae_type(absorbed(x)[0]) == rae_or_zr] |  for_each[lambda x: LazyValue(x) | push[stream] | run ] 
                    sub_decl = sub_decl[filter_func]
                    sub = g | sub_decl
                else:
                    raise Exception(f"Unhandled type in on[{op_kind}] where the input was {rae_or_zr}")

            # Type 3: An instantiated or terminated relation outgoing or incoming from a specific zr 
            elif len(op_args[0]) == 3: 
                src, rt, trgt = op_args[0] 
                # sub_decl = sub_decl[selected_types[0][internal.outgoing][rt]][lambda x: LazyValue(selected_types[1][x]) | push[stream] | run] # TODO: is it always outgoing?
                # sub = src | sub_decl         
                def triple_filter(z):    
                    def src_or_trgt_filter(rae, rae_filter):
                        if isinstance(rae_filter, ZefRef):
                            return to_ezefref(rae) == to_ezefref(rae_filter)
                        elif isinstance(rae_filter, ValueType):
                            return is_a(rae, rae_filter)
                        else:
                            raise ValueError(f"Expected source or target filters to be ZefRef, RaeType, or ValueType but got {type(rae_filter)} instead.")

                    # First check on the RT itself
                    if rae_type(z) != rt: return False
                    # Checks on both source and target
                    return src_or_trgt_filter(source(z), src) and src_or_trgt_filter(target(z), trgt)

                def filter_func(root_node): root_node | frame | to_tx | events[op_kind] | filter[lambda x: triple_filter(absorbed(x)[0])] | for_each[lambda x: run(LazyValue(x) | push[stream]) ]  
                sub_decl = sub_decl[filter_func]
                sub = g | sub_decl              
            else:
                raise Exception(f"Unhandled type in on[{selected_types[1]}] where the curried args were {op_args}")
        elif op_kind == Assigned:
            assert len(op_args) == 1
            aet_or_zr = op_args[0]
            # Type 1: a specific zefref to an AET i.e on[assigned[zr_to_aet]]
            if isinstance(aet_or_zr, BlobPtr): 
                def filter_func(root_node): root_node | frame | to_tx | events[Assigned] | filter[lambda x: to_ezefref(absorbed(x)[0]) == to_ezefref(aet_or_zr)] |  for_each[lambda x: run(LazyValue(x) | push[stream]) ]  
                sub_decl = sub_decl[filter_func]
                sub = g | sub_decl
            # Type 2: any AET.* i.e on[assigned[AET.String]]
            elif isinstance(aet_or_zr, AET): 
                def filter_func(root_node): root_node | frame | to_tx | events[Assigned] | filter[lambda x: rae_type(absorbed(x)[0]) == aet_or_zr] |  for_each[lambda x: run(LazyValue(x) | push[stream]) ]  
                sub_decl = sub_decl[filter_func]
                sub = g | sub_decl        
        else:
            raise Exception(f"on subscription only allow Terminated/Instantiated/ValueAssigned subscriptions. {op_kind} was passed!")        
        return stream
    raise TypeError(f"The first argument passed should be a Graph. A {type(g)} was passed instead.")


#---------------------------------------- transpose -----------------------------------------------

def transpose_imp(iterable):
    """
    This operator is essentially the operation of transposition on a matrix
    that is infinite along one direction (vertical (m) for the 
    input matrix in normal A_(m,n) nomenclature).
    It is different from "interleave" in that it produces a List of Lists,
    whereas "interleave" flattens it out into one List.

    Note that transposing is its own inverse: transposing twice leads to the
    original List, if the operation succeeds.

    ---- Examples ----
    >>> [ [2,3,4], [5,6,7] ] | transpose             # => [ [2, 5], [3,6], [4,7] ]
     
    >>> # terminates upon the shortest one:
    >>> [ range(2, infinity), [5,6], [15,16,17] ] | transpose    # => [[2, 5, 15], [3, 6, 16]]

    ---- Tags ----
    - used for: list manipulation
    - used for: linear algebra
    - same naming as: C++ Ranges V3
    """
    def transpose_generator():
        its = [iter(el) for el in iterable]
        while True:
            try:
                yield [next(it) for it in its]
            except StopIteration:
                # if any one of the iterators completes, this completes
                return
    return ZefGenerator(transpose_generator)


def transpose_tp(op, curr_type):
    return VT.List


#---------------------------------------- match -----------------------------------------------
def match_imp(item, patterns):
    """
    Given an item and a list of Tuples[predicate, output]. The item is checked
    sequentially on each predicate until one matches. If non-matches an exception is raised.

    ---- Examples ----
    >>> -9 | match[
    ...     ({24, 42}, lambda x: f'a very special temperature'),
    ...     (Is[less_than[-10]], lambda x: f'it is a freezing {x} degrees'),
    ...     (Is[less_than[10]], lambda x: f'somewhat cold: {x} degrees'),
    ...     (Is[greater_than_or_equal[20]], lambda x: f'warm: {x}'),
    ...     (Any, lambda x: f'something else {x}'),
    ... ] | collect                             
    'somewhat cold: -9 degrees'

    ---- Arguments ----
    item: the incoming value
    patterns: (T, T) -> Bool

    ---- Signature ----
    (T, (T->Any)) -> T

    ---- Tags ----
    - used for: control flow
    - used for: logic
    - used for: function application
    """
    for tp, f_to_apply in patterns:
        try:
            if( (item in tp) 
                if isinstance(tp, set)    # if a set is passed in: check membership directly
                else is_a(item, tp)
                ): return call_wrap_errors_as_unexpected(f_to_apply, item)
        except Error_ as e:
            e = add_error_context(e, {"metadata": {"match_case": tp, "func": f_to_apply, "input": item}})
            raise e from None
    raise Error.MatchError("No case matched", item)

def match_tp(op, curr_type):
    return VT.Any


#---------------------------------------- match_on -----------------------------------------------
def match_on_imp(item, f_preprocess, patterns: List):
    """
    Given an item, a preprocessing function and a list of Tuples[predicate, output]. 
    The pre_func(item) is checked sequentially on each input type until one matches. 
    If non-matches an exception is raised.

    ---- Examples ----
    >>> 10 | match_on[mod3, mod5][
    ...    ({0}, {0}):  lambda _: 'FizzBuzz',
    ...    ({0}, Any):  lambda _: 'Fizz',
    ...    (Any, {0}):  lambda _: 'Buzz',
    ...    (Any, Any):  lambda x: str(x)
    ... ] | collect
    'Buzz'

    ---- Arguments ----
    item: the incoming value
    patterns: (T, T) -> Bool

    ---- Signature ----
    (T, (T->Any)) -> T

    ---- Tags ----
    - used for: control flow
    - related ZefOp: match
    """
    if type(f_preprocess) in {list, tuple}:
        ff = tuple([f(item) for f in f_preprocess])
        
        for tp, f_to_apply in patterns:
            try:
                if( (ff in tp) 
                    if isinstance(tp, set)    # if a set is passed in: check membership directly
                    else is_a(ff, Pattern[list(tp)])
                    ): return call_wrap_errors_as_unexpected(f_to_apply, item)
            except Error_ as e:
                e = add_error_context(e, {"metadata": {"match_case": tp, "func": f_to_apply, "input": item}})
                raise e from None
        raise Error.MatchError("No case matched", item)

    
    else:
        ff = f_preprocess(item)
    
        for tp, f_to_apply in patterns:
            try:
                if( (ff in tp) 
                    if isinstance(tp, set)    # if a set is passed in: check membership directly
                    else is_a(ff, tp)
                    ): return call_wrap_errors_as_unexpected(f_to_apply, item)
            except Error_ as e:
                e = add_error_context(e, {"metadata": {"match_case": tp, "func": f_to_apply, "input": item}})
                raise e from None
        raise Error.MatchError("No case matched", item)





#---------------------------------------- peel -----------------------------------------------
def peel_imp(el, *args):
    # LazyValue must come first to handle that case and bypass warnings.
    if isinstance(el, LazyValue):
        return (el.initial_val, el.el_ops)
    elif isinstance(el, ValueType):
        return el.nested()
    elif isinstance(el, dict):
        print('deprecation warning: `peel` called on a dictionary. This probably was an Effect before and is no longer needed.')
        return el
    elif isinstance(el, ZefOp):
        # TODO !!!!!!!!!!!!! Change this to always return elementary ZefOps and to use list(el) to get current behavior
        if len(el.el_ops) > 1:
            return [ZefOp((op,)) for op in el.el_ops]
        return el.el_ops
    else:
        raise NotImplementedError(f"Tried to peel an unsupported type {type(el)}")


def peel_tp(op, curr_type):
    if curr_type == VT.Effect:
        return VT.Dict[VT.String, VT.Any]
    elif curr_type in {VT.ZefOp, VT.LazyValue}:
        return VT.Tuple
    else:
        return absorbed(curr_type)[0]


#---------------------------------------- zip -----------------------------------------------
def zip_imp(x, second=None, *args):
    """
    can be used with allputs piped in via one tuple, 
    or other Lists to zip withbeing curried in.
    
    ---- Examples ----
    >>> (('a', 'b', 'c', 'd'), range(10)) | zip                  # => [('a', 0), ('b', 1), ('c', 2), ('d', 3)]
    >>> range(10) | zip['a', 'b', 'c', 'd']                      # => [(0, 'a'), (1, 'b'), (2, 'c'), (3, 'd')]
    >>> range(10) | zip['a', 'b', 'c', 'd'][True,False,True]     # => [(0, 'a', True), (1, 'b', False), (2, 'c', True)]
    >>> (('a', 'b', 'c', 'd'), range(10)) | zip | zip            # => [('a', 'b', 'c', 'd'), (0, 1, 2, 3)]
    """    
    # wrap the zip object as a generator to prevent type proliferation in engine.
    if second is None:
        def wrapper1():
            yield from builtins.zip(*x)
        return ZefGenerator(wrapper1)
        
    def wrapper2():
        yield from builtins.zip( *(x, second, *args) )    
    return ZefGenerator(wrapper2)



def zip_tp(iterable_tp, other_iterable_tp):
    return VT.List




#---------------------------------------- concat -----------------------------------------------
def concat_implementation(v, first_curried_list_maybe=None, *args):
    """
    Concatenate a list of lists (or streams).
    Can also be used to specify other lists to be concatenated as
    additional args curried in.

    ---- Examples ----
    >>> # A) all passed in via a List of Lists (no args curried into concat op)
    >>> [
    ...     [1,2,3],                                    
    ...     ['a', 'b', 'c', 'd'],
    ... ] | concat                                      
    [1, 2, 3, 'a', 'b', 'c', 'd']
     
    >>> # B) One list piped in, other lists to concatenated curried into op
    >>> [1,2,3] | concat[ (42,43) ][('a','b','c')]      # =>  [1, 2, 3, 42, 43, 'a', 'b', 'c']

    ---- Signature ----
    (List[T1], List[T2], ...)        -> List[T1 | T2 | ...]
    (Stream[T1], Stream[T2], ...)    -> Stream[T1 | T2 | ...]
    (String, String, ...)            -> String

    ---- Tags ----
    - operates on: List
    - operates on: Stream
    - operates on: String
    - related zefop: interleave
    - related zefop: interleave_longest
    - related zefop: merge
    - related zefop: append
    - related zefop: prepend
    - used for: list manipulation
    - used for: stream manipulation
    - used for: string manipulation
    """
    if (isinstance(v, list) or isinstance(v, tuple)) and len(v)>0 and isinstance(v[0], str):
        if not all((isinstance(el, str) for el in v)):
            raise TypeError(f'A list starting with a string was passed to concat, but not all other elements were strings. {v}')
        return v | join[''] | collect
    elif isinstance(v, str):
        if first_curried_list_maybe is None:
            return v
        else:
            return [v, first_curried_list_maybe, *args] | join[''] | collect
    else:
        if first_curried_list_maybe is None:
            def wrapper():
                it = iter(v)
                try:
                    while True:
                        yield from next(it)
                except StopIteration:
                    return
            
            return ZefGenerator(wrapper)
        else:
            def wrapper():
                yield from (el for sublist in (v, first_curried_list_maybe, *args)  for el in sublist)
            return ZefGenerator(wrapper)
            




def concat_type_info(op, curr_type):
    if curr_type in ref_types:
        curr_type = downing_d[curr_type]
    else:
        try:
            curr_type = absorbed(curr_type)[0]
        except AttributeError as e:
            raise Exception(f"An operator that downs the degree of a Nestable object was called on a Degree-0 Object {curr_type}: {e}")
    return curr_type



#---------------------------------------- prepend -----------------------------------------------
def prepend_imp(v, item, *additional_items):
    """ 
    Prepend an element to a list / observable.    
    Special overload for strings as well.

    ---- Examples ----
    >>> ['b', 'c'] | prepend['a']           # => ['a', 'b', 'c']
    >>> 'morning' | prepend['good ']        # => 'good morning'

    ---- Signature ----
    (List[T1], T2)    -> List[T1 | T2]
    (Stream[T1], T2)  -> Stream[T1 | T2]
    (String, String)  -> String

    ---- Tags ----
    - operates on: List
    - operates on: Stream
    - operates on: String
    - related zefop: append
    - related zefop: insert_at
    - used for: list manipulation
    - used for: string manipulation
    """
    from typing import Generator, Iterable, Iterator
    if isinstance(v, list):
        return [item, *v]
    elif isinstance(v, ZefRef) and is_a[ET.ZEF_List](v):
        """
        args must be existing RAEs on the graph
        """
        z_list = now(v)
        if not verify_zef_list(z_list): return Error('Invalid Zef List')
        elements_to_prepend = [item, *additional_items]
        all_zef = elements_to_prepend | map[lambda v: isinstance(v, ZefRef) or isinstance(v, EZefRef)] | all | collect
        if not all_zef: return Error("Append only takes ZefRef or EZefRef for Zef List")
        is_any_terminated = elements_to_prepend | map[events[VT.Terminated]] | filter[None] | length | greater_than[0] | collect 
        if is_any_terminated: return Error("Cannot append a terminated ZefRef")

        
        g = Graph(z_list)
        rels = z_list | out_rels[RT.ZEF_ListElement] | collect
        rels1 = (elements_to_prepend 
                | enumerate 
                | map[lambda p: (z_list, RT.ZEF_ListElement[str(p[0])], p[1])] 
                | collect
                )

        new_rels = rels1 | map[second | peel | first | second | second | inject[Any] ] | collect
        next_rels = new_rels | sliding[2] | attempt[map[lambda p: (p[0], RT.ZEF_NextElement, p[1])]][[]] | collect

        # Do we need a single connecting RT.ZEF_NextElement between the last element of the existing list and the first new element?
        
        # if there are elements in the list, use the last one. Otherwise return an empty list.
        first_existing_el_rel = (
            rels 
            | filter[lambda r: r | in_rels[RT.ZEF_NextElement] | length | equals[0] | collect] 
            | attempt[
                single
                | func[lambda x: [(new_rels[-1], RT.ZEF_NextElement, x)]]
                ][[]] 
            | collect)

        actions = ( 
                rels1,                              # the RT.ZEF_ListElement between the ET.ZEF_List and the RAEs
                next_rels,                          # the RT.ZEF_NextElement between each pair of new RT.ZEF_ListElement 
                first_existing_el_rel,              # list with single connecting last existing rel to newly created one or Empty
            ) | concat | collect
        return  actions     
    elif isinstance(v, tuple):
        return (item, *v)
    elif isinstance(v, str):
        return item + v
    elif isinstance(v, Generator) or isinstance(v, Iterator) or isinstance(v, ZefGenerator):
        def generator_wrapper():
            yield item
            yield from v
        return ZefGenerator(generator_wrapper)
    else:
        raise TypeError(f'prepend not implemented for type {type(v)}')


def prepend_tp(op, curr_type):
    return curr_type


#---------------------------------------- append -----------------------------------------------
def append_imp(v, item, *additional_items):
    """ 
    Append an element to a list / observable.    
    Special overload for strings as well.

    ---- Examples ----
    >>> ['b', 'c'] | append['d']           # => ['b', 'c', 'd']
    >>> 'good' | append[' evening']        # => 'good evening'

    ---- Signature ----
    (List[T1], T2)    -> List[T1 | T2]
    (Stream[T1], T2)  -> Stream[T1 | T2]
    (String, String)  -> String

    ---- Tags ----
    - operates on: List
    - operates on: Stream
    - operates on: String
    - related zefop: prepend
    - related zefop: insert_at
    - used for: list manipulation
    - used for: string manipulation
    """
    from typing import Generator, Iterable, Iterator
    if isinstance(v, list):
        return [*v, item]    
    elif isinstance(v, tuple):
        return (*v, item)
    elif isinstance(v, ZefRef) and is_a[ET.ZEF_List](v):
        """
        args must be existing RAEs on the graph
        """
        z_list = now(v)
        if not verify_zef_list(z_list): return Error('Invalid Zef List')

        elements_to_append = [item, *additional_items]
        all_zef = elements_to_append | map[lambda v: isinstance(v, ZefRef) or isinstance(v, EZefRef)] | all | collect
        if not all_zef: return Error("Append only takes ZefRef or EZefRef for Zef List")
        is_any_terminated = elements_to_append | map[events[VT.Terminated]] | filter[None] | length | greater_than[0] | collect 
        if is_any_terminated: return Error("Cannot append a terminated ZefRef")

        
        g = Graph(z_list)
        rels = z_list | out_rels[RT.ZEF_ListElement] | collect
        rels1 = (elements_to_append 
                | enumerate 
                | map[lambda p: (z_list, RT.ZEF_ListElement[str(p[0])], p[1])] 
                | collect
                )

        new_rels = rels1 | map[second | peel | first | second | second | inject[Any] ] | collect
        next_rels = new_rels | sliding[2] | attempt[map[lambda p: (p[0], RT.ZEF_NextElement, p[1])]][[]] | collect

        # Do we need a single connecting RT.ZEF_NextElement between the last element of the existing list and the first new element?
        
        # if there are elements in the list, use the last one. Otherwise return an empty list.
        last_existing_ZEF_ListElement_rel = (
            rels 
            | filter[lambda r: r | out_rels[RT.ZEF_NextElement] | length | equals[0] | collect] 
            | attempt[
                single
                | func[lambda x: [(x, RT.ZEF_NextElement, Any['0'])]]
                ][[]] 
            | collect)

        actions = ( 
                rels1,                              # the RT.ZEF_ListElement between the ET.ZEF_List and the RAEs
                next_rels,                          # the RT.ZEF_NextElement between each pair of new RT.ZEF_ListElement 
                last_existing_ZEF_ListElement_rel,  # list with single connecting rel to previous list or empty
            ) | concat | collect
        return  actions     
 

    elif isinstance(v, str):
        return v + item
    elif isinstance(v, Generator) or isinstance(v, Iterator) or isinstance(v, ZefGenerator):
        def generator_wrapper():
            yield from v
            yield item
        return ZefGenerator(generator_wrapper)
    else:
        raise TypeError(f'append not implemented for type {type(v)}')


def append_tp(op, curr_type):
    return curr_type



#---------------------------------------- get_in --------------------------------------------------
def get_in_imp(d: dict, path, default_val=VT.Error):
    """
    Enable one-shot access to elements in nested dictionary
    by specifying path as a tuple of keys.
    Based on Clojure's 'get-in'.

    ---- Examples ----
    >>> {'a': 1, 'b': {'c': 1}} | get_in[('b', 'c')]   # => 1
    >>> {'a': 1, 'b': {'c': 1}} | get_in[('b', 'wrong_key')][42]   # => 42

    ---- Tags ----
    - related zefop: get
    - related zefop: remove_in
    - related zefop: update_in
    - related zefop: insert_in
    - related zefop: get_in
    - operates on: Dict
    """
    assert isinstance(path, list) or isinstance(path, tuple)

    from ..pure_utils import get_in_pure
    return get_in_pure(d, path, default_val)

    
def get_in_tp(d_tp, path_tp, default_val_tp):
    return VT.Dict




#---------------------------------------- insert_in -----------------------------------------------
def insert_in_imp(d: dict, path, value):
    """
    Create a new dicitonary from an old one by inserting one value
    for a given nested path.

    ---- Examples ----
    >>> {'a': 1, 'b': {'c': 1}} | insert_in[('b', 'd')][42]   # => {'a': 1, 'b': {'c': 1, 'd': 42}} 
  
    ---- Tags ----
    - related zefop: insert
    - related zefop: get_in
    - related zefop: remove_in
    - related zefop: update_in
    - operates on: Dict
    """
    assert isinstance(path, list) or isinstance(path, tuple)
    res = {**d}    
    # purely side effectful
    def insert(dd, path):        
        if len(path) == 1:
            dd[path[0]] = value
        else:            
            dd[path[0]] = {} if path[0] not in dd else {**dd[path[0]]}
            insert(dd[path[0]], path[1:])            
    insert(res, path)        
    return res


def insert_in_tp(d_tp, path_tp, value_tp):
    return VT.Dict




#---------------------------------------- remove_in -----------------------------------------------
def remove_in_imp(d: dict, path):
    """
    Given a dictionary and a tuple describing a path into the dictionary,
    this returns a new dictionary with the pointed to value removed.
    The original dictionary is not mutated.

    ---- Examples ----
    >>> {'a': 1, 'b': {'c': 1}} | remove_in[('b', 'c')]   # => {'a': 1, 'b': {}}

    ---- Signature ----
    (Dict[T1][T2], List) -> Dict[T1][T2]

    ---- Tags ----
    - related zefop: remove
    - related zefop: remove_in
    - related zefop: remove_at
    - related zefop: update_in
    - related zefop: insert_in
    - related zefop: get_in
    - operates on: Dict
    - used for: control flow
    - used for: function application
    """
    assert isinstance(path, list) or isinstance(path, tuple)
    is_last_step = len(path)==1
    def change(k, v):
        return v if k!=path[0] else remove_in(v, path[1:])    
    return {k: change(k, v) for (k, v) in d.items() if not (is_last_step and k==path[0])}


def remove_in_tp(d_tp, path_tp):
    return VT.Dict




#---------------------------------------- update_in -----------------------------------------------
def update_in_imp(d: dict, path, func_to_update):
    """
    Given a dictionary, a tuple describing a path into
    the dictionary and an update function, 
    this returns a new dictionary with the element point to
    replaced with the value obtained by applying the update 
    function to the previous element.
    The original dictionary is not mutated.

    ---- Examples ----
    >>> {'a': 1, 'b': {'c': 1}} | update_in[('b', 'c')][add[1]]   # => {'a': 1, 'b': {'c': 2}} 

    ---- Signature ----
    (Dict, List, T1->T2) -> Dict

    ---- Tags ----
    - related zefop: update
    - related zefop: update_at
    - related zefop: insert_in
    - related zefop: remove_in
    - related zefop: get_in
    - operates on: Dict
    - used for: control flow
    - used for: function application
    """
    assert isinstance(path, list) or isinstance(path, tuple)
    assert callable(func_to_update)
    is_last_step = len(path)==1
    def change(k, v):
        if is_last_step: return func_to_update(v) if k==path[0] else v
        else: return update_in(v, path[1:], func_to_update) if k==path[0] else v
    return {k: change(k, v) for (k, v) in d.items()}



def update_in_tp(d_tp, path_tp, func_to_update_tp):
    return VT.Dict



#---------------------------------------- update_at -----------------------------------------------
def update_at_imp(v: list, n, f):
    """
    Apply a specified function "f" to a specified position of 
    a list using the existing element "el" as function input.
    A new list is returned which differs only in that element
    which is replaced with "f(el)".
    When called with negative index n, this is interpreted as
    reverse indexing with -1 referring to the last element.

    ---- Examples ----
    >>> [5,6,7] | update_at[2][add[10]]   # => [5,6,17]

    ---- Signature ----
    (List[T1], T1->T2) -> List[T1|T2]

    ---- Tags ----
    - related zefop: update
    - related zefop: update_in
    - related zefop: remove_at
    - related zefop: replace_at
    - operates on: List
    - used for: control flow
    - used for: function application
    """
    assert isinstance(n, int)
    assert callable(f)

    def wrapper():
        it = iter(v)
        if n>=0:
            try:
                for _ in range(n):
                    yield next(it)
                yield f(next(it))
                yield from it
            except StopIteration:
                return        
        else:
            # we want to enable this lazily, but only know which
            # element to apply the function to once the iterable completes.
            # This means we need to cache n values at any point in time.
            # Do this mutatingly here to optimize the tight loop.
            cached = []
            m = -n          # working with a positive m is easier
            offset = 0
            try:
                for _ in range(m):
                    cached.append(next(it))                
                while True:
                    cand = cached[offset]
                    cached[offset] = next(it)
                    yield cand
                    offset = 0 if offset==m-1 else offset + 1
            except StopIteration:
                if m<=len(cached):
                    yield f(cached[offset])
                else:
                    yield cached[offset]
                yield from cached[offset+1:]
                yield from cached[:offset]
                return

    return ZefGenerator(wrapper)



def update_at_tp(op, curr_type):
    return curr_type

    

#---------------------------------------- insert_at -----------------------------------------------

def insert_at_imp(v, n, new_value):
    """
    Insert a specified value at a specified position of 
    a list. A new list is returned, the input list is not mutated.
    When called with negative index n, this is interpreted as
    reverse indexing with -1 referring to the last element.

    ---- Examples ----
    >>> [0,1,2] | insert_at[1]['hello']   # => [0,'hello',1,2]

    ---- Signature ----
    (List[T1], T1->T2) -> List[T1|T2]
    (List[T1], T2) -> List[T1|T2]

    ---- Tags ----
    - related zefop: insert
    - related zefop: insert_in
    - related zefop: update_at
    - related zefop: remove_at
    - related zefop: replace_at
    - operates on: List
    - operates on: Stream
    - operates on: String
    - used for: list manipulation
    - used for: stream manipulation
    - used for: string manipulation
    """

    if isinstance(v, str):
        assert isinstance(new_value, str)
        if n>=0:
            if n > len(v):
                return v
            else:
                return f"{v[:n]}{new_value}{v[n:]}"
        else:
            l = len(v)
            m = -n
            if m > l+1:
                return v
            else:
                p = l-m+1
                return f"{v[:p]}{new_value}{v[p:]}"
     
    def wrapper():
        it = iter(v)
        if n>=0:
            try:
                for _ in range(n):
                    yield next(it)
                yield new_value
                yield from it
            except StopIteration:
                return        
        else:
            # we want to enable this lazily=, but only know which
            # element to apply the function to once the iterable completes.
            # This means we need to cache n values at any point in time.
            # Do this mutatingly here to optimize the tight loop.
            
            # treat this case separately. Indexing becomes annoying.
            if n == -1:
                yield from v
                yield new_value
                return
            
            cached = []
            m = -n-1          # working with a positive m is easier
            offset = 0            
            try:
                for _ in range(m):
                    cached.append(next(it))                
                while True:
                    cand = cached[offset]
                    cached[offset] = next(it)
                    yield cand
                    offset = 0 if offset==m-1 else offset + 1
            except StopIteration:
                if m<=len(cached):
                    yield new_value
                yield from cached[offset:]
                yield from cached[:offset]
                return

    return ZefGenerator(wrapper)    




#---------------------------------------- update -----------------------------------------------
def update_imp(d: dict, k, fn):
    """
    Change the value for a given key / index by applying a
    user provided function to the old value at that location.

    Note: the equivalent ZefOp to use on Lists is "update_at"
    (to be consistent with "remove_at" for Lists and 
    disambiguate acting on the value)

    ---- Examples ----
    >>> {'a': 5, 'b': 6} | update['a'][add[10]]   # => {'a': 15, 'b': 6}

    ---- Tags ----
    - related zefop: update_at
    - related zefop: update_in
    - related zefop: get
    - related zefop: insert
    - operates on: Dict
    - used for: control flow
    - used for: function application
    """
    if not isinstance(d, Dict): raise TypeError('"update" only acts on dictionaries. Use "update_at" for Lists or "update_in" for nested access.')
    r = {**d}
    r[k] = fn(d[k])
    return r



#---------------------------------------- remove_at -----------------------------------------------
def remove_at_imp(v, *nn):
    """
    Using "remove" based on indexes would be confusing,
    as Python's list remove, searches for the first 
    occurrence of that value and removes it.

    ---- Examples ----
    >>> ['a', 'b', 'c'] | remove_at[1]       # => ['a', 'c']
    >>> ['a', 'b', 'c'] | remove_at[1][0]    # => ['c']

    ---- Signature ----
    List[T] & Length[Z] -> List[T] & Length[Z-1]

    ---- Tags ----
    - related zefop: interleave_longest
    - related zefop: concat
    - related zefop: merge
    - operates on: List
    """
    if isinstance(v, Dict): raise TypeError('"remove_at" only acts on iterables. Use "remove" for dictionaries. For nested access, use "remove_in".')
    return (el for m, el in enumerate(v) if m not in nn)




#---------------------------------------- interleave -----------------------------------------------

def interleave_imp(v, first_curried_list_maybe=None, *args):
    """
    Interleaves elements of an arbitrary number M of lists.
    Length of output is determined by the length of the 
    shortest input list N_shortest: M*N_shortest.

    Note: "interleave" is equivelent to "transpose | concat"
    
    ---- Examples ----
    >>> # Either called with a list of of lists (or equivalent for streams)
    >>> [
    ...    [1,2,3],
    ...    ['a', 'b', 'c', 'd']        
    ... ] | interleave          
    [1, 'a', 2, 'b', 3, 'c']
    >>> 
    >>> # or with other lists to interleave with being curried in
    >>> [1,2,3] | interleave[42 | repeat]       # => [1, 42, 2, 42, 3, 42]
    >>> [1,2,3] | interleave[42 | repeat][('a','b')]      # => [1, 42, 'a', 2, 42, 'b']

    ---- Signature ----
    List[List[T]] -> List[T]

    ---- Tags ----
    - related zefop: interleave_longest
    - related zefop: concat
    - related zefop: merge
    - related zefop: transpose
    - operates on: List
    - same naming as: C++ Ranges V3
    """
    import more_itertools as mi    
    if first_curried_list_maybe is None:
        return mi.interleave(*v)
    else:
        return mi.interleave(*[v, first_curried_list_maybe, *args])


def interleave_tp(v_tp):        #TODO
    return VT.List


#---------------------------------------- interleave_longest -----------------------------------------------
def interleave_longest_imp(v, first_curried_list_maybe=None, *args):
    """
    Interleaves elements of an arbitrary number M of lists.
    Length of output is determined by the length of the 
    longest input list
    
    ---- Examples ----
    >>> # Either called with a list of of lists (or equivalent for streams)
    >>> [
    >>>     [1,2,3],
    >>>     ['a', 'b', 'c', 'd']        
    >>> ] | interleave_longest          # =>  [1, 'a', 2, 'b', 3, 'c', 'd']
    >>> 
    >>> # or with other lists to interleave with being curried in
    >>> [1,2,3] | interleave_longest[[42]*5]        # => [1, 42, 2, 42, 3, 42, 42, 42]
    >>> [1,2,3] | interleave_longest[[42]*5][('a','b')]      # => [1, 42, 'a', 2, 42, 'b', 3, 42, 42, 42]
    
    ---- Signature ----
    List[List[T]] -> List[T]

    ---- Tags ----
    - related zefop: interleave_longest
    - related zefop: concat
    - related zefop: merge
    - operates on: List
    """
    import more_itertools as mi    
    if first_curried_list_maybe is None:
        return mi.interleave_longest(*v)
    else:
        return mi.interleave_longest(*[v, first_curried_list_maybe, *args])


def interleave_longest_tp(v_tp):        #TODO
    return VT.List





#---------------------------------------- stride -----------------------------------------------
def stride_imp(v, step: int):
    """
    Return a new list where only every nth 
    (specified by the stride step) is sampled.

    ---- Examples ----
    >>> Range(8) | stride[3]     #  [0, 3, 6]
    
    ---- Signature ----
    (List[T], Int) -> List[T]

    ---- Tags ----
    operates on: List
    operates on: String
    operates on: Stream
    used for: list manipulation
    used for: string manipulation
    used for: stream manipulation
    related zefop: chunk
    related zefop: sliding
    related zefop: slice
    """
    # shield the yield for when we extend this.  
    def wrapper():
        it = iter(v)  
        while True:
            try:
                yield next(it)
                for _ in range(step-1):
                    next(it)                
            except StopIteration:            
                return
    return ZefGenerator(wrapper)


def stride_tp(v_tp, step_tp):
    return VT.List

#---------------------------------------- chunk -----------------------------------------------
def chunk_imp(iterable, chunk_size: int):
    """
    This one is tricky. We'll be opinionated and eagerly evaluate the 
    elements in each chunk, as the chunks are being asked for. The 
    chunks are returned as a generator.

    If the input is an iterable (could be infinite generator or not), 
    this returns a generator of generators. An operator downstream could 
    choose to evaluate the terms of a later iterator before those of an 
    earlier one emitted here. If we can't access via ...[], we have to
    iteratively step through.

    Be opinionated here: instead of going full in with checking whether 
    the iterable has random access ...[n] semantics (C++ has the more 
    detailed model built in for this), we'll perform caching.    

    Perform caching and store the evaluated values in case these are 
    to be accessed by the earlier iterable. Keep them up to the point
    until the earlier iterable has consumed them, then discard them.

    ---- Examples ----
    >>> range(8) | chunk[3]          # => [[0, 1, 2], [3, 4, 5], [6, 7]]
    >>> 'abcdefgh' | chunk_imp[3]    # => ['abc', 'def', 'gh']
    
    ---- Signature ----
    (List[T], Int)   -> List[List[T]]
    (Stream[T], Int) -> Stream[Stream[T]]
    (String, Int)   -> List[String]

    ---- Tags ----
    operates on: List
    operates on: String
    operates on: Stream
    used for: list manipulation
    used for: string manipulation
    used for: stream manipulation
    related zefop: stride
    related zefop: sliding
    related zefop: slice
    """
    if isinstance(iterable, String):
        def wrapper():
            c = 0        
            while c*chunk_size < len(iterable):
                yield iterable[c*chunk_size:(c+1)*chunk_size]
                c+=1
            return
        return ZefGenerator(wrapper)

    def wrapper():
        it = iter(iterable)
        while True:
            try:
                # evaluate the entire chunk. Otherwise we have to share 
                # state between the generators that are returned            
                # this_chunk = [next(it) for _ in range(chunk_size)]
                # we need to do this imperatively to treat the very last
                # chunk correctly: the iterable may raise a StopIteration,
                # but we want to have the list up to that point.
                # A list comprehension doesn't do this easily.
                this_chunk = []
                for _ in range(chunk_size):
                    this_chunk.append(next(it))
                yield this_chunk
            except StopIteration:            
                if this_chunk != []:
                    yield this_chunk
                return

    return ZefGenerator(wrapper)


def chunk_tp(v_tp, step_tp):
    return VT.List


#---------------------------------------- insert -----------------------------------------------
def sliding_imp(iterable, window_size: int, stride_step: int=1):
    """ 
    Given a list, return a list of internal lists of length "window_size".
    "stride_step" may be specified (optional) and determines how
    many elements are taken forward in each step.
    Default stride_step if not specified otherwise: 1.

    implementation follows Scala's, see https://stackoverflow.com/questions/32874419/scala-slidingn-n-vs-groupedn

    ---- Examples ----
    >>> range(5) | sliding[3]       # [(0, 1, 2), (1, 2, 3), (2, 3, 4)]
    >>> range(5) | sliding[3][2]    # [(0, 1, 2), (2, 3, 4)]
    
    ---- Signature ----
    (List[T], Int, Int) -> List[List[T]]

    ---- Tags ----
    - related zefop: stride
    - related zefop: chunk
    - related zefop: slice
    - operates on: List
    """
    def wrapper():
        it = iter(iterable)
        try:
            w = []
            for c in range(window_size):
                w.append(next(it))
            yield tuple(w)
        except StopIteration:
            if len(w) < window_size:
                return
            yield tuple(w)
            return    
        while True:
            try:
                new_vals = []
                for c in range(stride_step):
                    new_vals.append(next(it))
                w = (*w[stride_step:], *new_vals[-window_size:])
                yield w
            
            except StopIteration:
                if new_vals == []: return
                yield (*w[stride_step:], *new_vals)
                return      #TODO!!!!!!!!!!!!!
    
    return ZefGenerator(wrapper)

        

def sliding_tp(v_tp, step_tp):
    return VT.List






#---------------------------------------- insert -----------------------------------------------
def insert_imp(d: dict, key, val=None):
    """
    Takes a dictionary / flatgraph together with something to insert
    and returns a new dictionary / flatgraph with that inserted.
    The input values are not mutated or harmed during this operation.
    

    ---- Examples ----
    >>> {'a': 1} | insert['b'][2]   ->  {'a': 1, 'b': 2} 
    >>> FlatGraph() | insert[ET.God, RT.Favorite, Val(1/137)] | insert[ET.BigBang]
    
    ---- Signature ----
    (Dict[T1][T2], T1, T2) -> Dict[T1][T2]

    ---- Tags ----
    - level: easy
    - used for: control flow
    - operates on: Dict
    - operates on: FlatGraph
    - related zefop: insert_in
    - related zefop: remove
    - related zefop: update
    """
    if is_a(d, FlatGraph):
        fg = d
        new_el = key
        return fg_insert_imp(fg, new_el)
    elif isinstance(d, Dict):
        return {**d, key: val}
    else:
        return Error(f'"insert" zefop called with unhandled type d={d} key={key} val={val}')


def insert_tp(input_arg0_tp, input_arg1_tp):
    return VT.Dict



#---------------------------------------- reverse_args -----------------------------------------------
def reverse_args_imp(flow_arg, op, *args):
    """    
    Useful to transform one zefop into a new operator where
    the only difference is that the arguments are reversed.
    This applies when we want an operator where the dataflow 
    is determined by a different argument than the usual one.

    Suppose we want to have the op 'insert_into', which is a
    slight variation of "insert":
    ('my_key': 42) | insert_into[{'a':1}]

    With this operator, we can construct "insert_into" on the fly:
    >>> insert_into = reverse_args[insert]
    
    ---- Examples ----
    >>> ('my_key': 42) | reverse_args[insert][{'a':1}]      # {'my_key': 42, 'a': 1}
    >>> 'x' |  reverse_args[get][{'a':1, 'x': 42}]          # 42

    ---- Signature ----
    (T1, ZefOp[T..., T1][T2], T...) -> T2

    ---- Tags ----
    - level: advanced
    - used for: control flow
    - operates on: ZefOp
    - operates on: Function
    - related zefop: func
    - related zefop: apply_functions
    """
    return op(*(*args[::-1], flow_arg))



#---------------------------------------- insert_into -----------------------------------------------

def insert_into_imp(key_val_pair, x):
    """
    For Dicts:
    Given a dictionary as a curried in argument and the key 
    value pair as the data flow argument, return a new dictionary
    with the inserted value.

    For Lists:
    the dataflow argument is a tuple (pos, val), where
    val will be inserted into the given list at position pos.    
    For Lists this operator is equivalent to insert_at,
    but with the argument oder flipped.

    This function could also be used on Lists. 
    
    ---- Examples ----
    >>> (2, 'a') | insert_into[ range(4) ]    # [0,1,'a',2,3]

    ---- Signature ----
    ((T1, T2), Dict[T3][T4]) -> Dict[T1|T3][T2|T4]

    ---- Tags ----
    - level: advanced
    - operates on: Dict
    - operates on: List
    - related zefop: insert
    - related zefop: insert_at
    - related zefop: merge
    - related zefop: apply_functions

    # TODO: make this work: (10, 'a') | insert_into[range(10) | map[add[100]]] | take[5] | c

    """
    from typing import Generator
    if not isinstance(key_val_pair, (list, tuple)):
        return Error(f'in "insert_into": key_val_pair must be a list or tuple. It was type(x)={type(x)}     x={x}')
    
    k, v = key_val_pair
    if isinstance(x, Dict):
        return {**x, k:v}
    if type(x) in {list, tuple, range, Generator, ZefGenerator}:        
        assert isinstance(k, Int)
        # So much laziness!
        def wrapper():
            it = iter(x)
            try:
                for c in range(k):
                    yield next(it)
                yield v
                yield from it
            except StopIteration:
                return
        return ZefGenerator(wrapper)





#---------------------------------------- remove -----------------------------------------------
def remove_imp(d: dict, key_to_remove: tuple):
    """
    Given a key and a dictionary, returns a new dictionary 
    with the key value pair removed.

    This operator is NOT overloaded for Lists, since this
    could be confused with the mutating Python method on
    lists: my_list.remove(2) removes the first occurrence
    of the VALUE "2" and not at the location.
    Use "remove_at" for lists.

    ---- Examples ----
    >>> {'a': 1, 'b', 2} | remove['a']   ->  {'b': 2}
    
    ---- Tags ----
    - operates on: Dict
    - related zefop: remove_at
    - related zefop: remove_in
    - related zefop: insert
    - related zefop: get
    """
    if is_a(d, FlatGraph):
        fg = d
        return fg_remove_imp(fg, key_to_remove)
    return {p[0]: p[1] for p in d.items() if p[0] != key_to_remove}


def remove_tp(input_arg0_tp, input_arg1_tp):
    return VT.Dict




#---------------------------------------- get -----------------------------------------------
def get_imp(d, key, default=Error('Key not found in "get"')):
    """
    Typically used for key lookups, equivalent to '[]' operator.
    External function equivalent to Python's '__get__' method.
    Default values can be provided

    ---- Examples ----
    >>> {'a': 42, 'b': 'hello'} | get['a']                    # => 42
    >>> {'a': 42, 'b': 'hello'} | get['a']                    # => 42

    ----- Signature ----
    (Dict[T1, T2], T1) -> T2
    (Graph, T1) -> Any
    (FlatGraph, T1) -> Any
    
    ---- Tags ----
    - operates on: Dict
    - operates on: Graph
    - operates on: FlatGraph
    - related zefop: get_in
    - related zefop: get_field
    - related zefop: insert
    - related zefop: remove
    """
    from typing import Generator
    if is_a(d, FlatGraph):
        return fg_get_imp(d, key)
    elif isinstance(d, Dict):
        return d.get(key, default)
    elif isinstance(d, list) or isinstance(d, tuple) or isinstance(d, Generator) or isinstance(d, ZefGenerator):
        return Error(f"get called on a list. Use 'nth' to get an element at a specified index.")
    elif isinstance(d, Graph) or isinstance(d, GraphSlice):
        try:
            return d[key]
        except KeyError:
            return default
    else:
        return Error(f"get called with unsupported type {type(d)}.")

def get_tp(d_tp, key_tp):
    return VT.Any

#---------------------------------------- get_field -----------------------------------------------
def get_field_imp(obj, field):
    """
    Specific to python. Get the attribute of an object, equivalent to
    getattr(obj, field).

    ---- Examples ----
    # Note: the following example is much better expressed as ET("Machine")
    >>> ET | get_field["Machine"]                # => ET.Machine

    # Note: the nodes of a NetworkX graph can be accessed via, e.g. list(nxg.nodes)
    >>> nxg | get_field["nodes"] | filter[...]

    ----- Signature ----
    (T, String) -> T2
    
    ---- Tags ----
    - related zefop: get
    - operates on: Python objects
    """
    return getattr(obj, field)

def get_field_tp(d_tp, key_tp):
    return VT.Any


#---------------------------------------- enumerate -----------------------------------------------
def enumerate_imp(v):
    """
    Given an iterable, returns an iterable of pairs where 
    the first is an incrementing integer starting at zero.

    ---- Examples ----
    >>> ['a', 'b', 'c'] | enumerate     # [(1, 'a'), (2, 'b'), (3, 'c')]
 
    ---- Signature ----
    List[T1] -> List[Tuple[Int, T1]]
    Stream[T1] -> Stream[Tuple[Int, T1]]
    String -> List[Tuple[Int, String]]

    ---- Tags ----
    - used for: list manipulation
    - used for: string manipulation
    - used for: lazy transformation
    - operates on: List
    """
    import builtins
    def wrapper():
        return (x for x in builtins.enumerate(v))
    return ZefGenerator(wrapper)


def enumerate_tp(input_arg0_tp):
    return VT.List



#---------------------------------------- items -----------------------------------------------
def items_imp(d):
    """
    Return the key-value pairs of a dictionary as a tuple.

    ---- Examples ----
    >>> {'a': 100, 42: 'die antwoord', 'c': True} | values           # ( ('a', 100), (42, 'die antwoord'), ('c', True) )
 
    ---- Signature ----
    Dict[T1][T2] -> List[Tuple[T1, T2]]

    ---- Tags ----
    - used for: dict manipulation
    - used for: list manipulation
    - operates on: Dict
    """
    return tuple(d.items())


def items_tp(input_arg0_tp):
    return VT.List



#---------------------------------------- values -----------------------------------------------
def values_imp(d):
    """
    Return the values of a dictionary

    ---- Examples ----
    >>> {'a': 100, 42: 'die antwoord', 'c': True} | values           # (100, 'die antwoord', True)
 
    ---- Signature ----
    Dict[T1][T2] -> List[T2]

    ---- Tags ----
    - used for: dict manipulation
    - operates on: Dict
    """
    return tuple(d.values())


def values_tp(input_arg0_tp):
    return VT.List



#---------------------------------------- keys -----------------------------------------------
def keys_imp(d):
    """
    Return the keys of a dictionary

    ---- Examples ----
    >>> {'a': 100, 42: 'die antwoord', 'c': True} | keys           # ('a', 42, 'c')
 
    ---- Signature ----
    Dict[T1][T2] -> List[T1]

    ---- Tags ----
    - used for: dict manipulation
    - operates on: Dict
    """
    return tuple(d.keys())


def keys_tp(input_arg0_tp):
    return VT.List



#---------------------------------------- reverse -----------------------------------------------
def reverse_imp(v):
    """
    Reverse a list or equivalent structure.

    ---- Examples ----
    >>> [2,3,4] | reverse           # [4,3,2]
    >>> 'straw' | reverse           # 'warts'
 
    ---- Signature ----
    List[T1] -> List[T1]
    Stream[T1] -> Stream[T1]
    String -> String

    ---- Tags ----
    - used for: list manipulation
    - used for: stream manipulation
    - operates on: List
    """
    from typing import Generator
    if isinstance(v, (Generator, ZefGenerator)): return (tuple(v))[::-1]
    if isinstance(v, String): return v[::-1]
    if isinstance(v, list): return v[::-1]
    if isinstance(v, tuple): return v[::-1]
    # return reversed(v)
    return list(v)[::-1]
    

def reverse_tp(op, curr_type):
    return curr_type



#---------------------------------------- cycle -----------------------------------------------
def cycle_imp(iterable, n=None):
    """
    Given a list, this wll produce another list loft the same structure
    that cycles thorugh all elements of the original list n times.
    
    ---- Examples ----
    >>> [2,3] | cycle[3]  # => [2,3,2,3,2,3]
    >>> [2,3] | cycle     # => [2,3,2,3,2,3,...    # never terminates

    ---- Signature ----
    (List[T], Int) -> List[T]

    ---- Tags ----
    - used for: list manipulation
    - used for: stream manipulation
    - operates on: List
    """
    def wrapper():
        cached = []
        if n==0:
            return

        for x in iterable:
            cached.append(x)
            yield x

        rerun = 1
        while True:
            rerun += 1
            if n is not None and rerun > n:
                return
            for x in cached:
                yield x
    return ZefGenerator(wrapper)


def cycle_tp(iterable_tp, n_tp):
    return iterable_tp



#---------------------------------------- repeat -----------------------------------------------
def repeat_imp(x, n=None):
    """
    Repeat an element a given number of times 
    None (default) means infinite).

    ---- Examples ----
    >>> 42 | repeat[3]    # => [42,42,42]
    >>> (1,'a') | repeat  # => [(1,'a'), (1,'a'), ...]    # infinite sequence

    ---- Tags ----
    - related zefop: cycle
    - operates on: Any
    """
    def wrapper():
        try:
            if n is None:
                while True:
                    yield x
            else:
                for _ in range(n):
                    yield x
        except StopIteration:
            return

    return ZefGenerator(wrapper)


def repeat_tp(iterable_tp, n_tp):
    return VT.List



#---------------------------------------- contains -----------------------------------------------
def contains_imp(x, el):
    """
    Check whether an element is in a given
    set or iterable. Operator for of Python's
    `in`.

    ---- Examples ----
    >>> {42, 43} | contains[42]         # => 42
    >>> g | contains[my_entity_ref]     # => 

    ---- Tags ----
    - related zefop: contained_in
    - operates on: List
    - operates on: Set
    - operates on: Dict
    - operates on: Graph
    - operates on: FlatGraph
    """
    return el in x    
                        

def contains_tp(x_tp, el_tp):
    return VT.Bool



#---------------------------------------- contained_in -----------------------------------------------
def contained_in_imp(x, el):
    if isinstance(x, (Generator, ZefGenerator)) or isinstance(x, Iterator): x = [i for i in x]
    return x in el


def contained_in_tp(x_tp, el_tp):
    return VT.Bool



#---------------------------------------- all -----------------------------------------------
def all_imp(*args):
    """
    The all op has two different behaviours:

    A) The first is to "find all of" for a graph-like or graph-slice-like
    object. Of a GraphSlice/FlatGraph, this will return a ZefRef list of every RAE, and of a
    Graph this will return a EZefRef list of every blob.

    An optional argument can be a type that provides a filter on the kind of
    items returned. It should always be true that: `g | all[Type]` is equivalent
    to `g | all | filter[is_a[Type]]` however providing the Type to `all` can be
    much more efficient.
    
    B) The second behaviour is to test the truth of every element in a list. It
    is similar to the builtin `all` function of python. If the list is empty,
    returns True.
 
    ---- Examples ----
    >>> g | now | all[ET]       # all entities in the latest timeslice of the graph

    >>> g | all[TX]             # all transaction blobs in the graph

    >>> [True,True] | all   # => True
    >>> [False,True] | all  # => False
    >>> [] | all            # => True

    Test whether all ZefRefs have an RT.Name relation.
    >>> zrs | map[has_out[RT.Name] | all
 
    ---- Signature ----
    List[T] -> Bool
    GraphSliceLike -> List[ZefRef]
    GraphLike -> List[EZefRef]
    (GraphSliceLike, Type) -> List[ZefRef]
    (GraphLike, Type) -> List[EZefRef]
 
    ---- Tags ----
    - used for: predicate
    """

    import builtins
    from typing import Generator, Iterator   
    if is_a(args[0], FlatGraph):
        return fg_all_imp(*args)
    if isinstance(args[0], ZefRef) and is_a[ET.ZEF_List](args[0]):
        z_list = args[0]
        rels = z_list | out_rels[RT.ZEF_ListElement] | collect
        first_el = rels | attempt[filter[lambda r: r | in_rels[RT.ZEF_NextElement] | length | equals[0] | collect] | single | collect][None] | collect
        return (x for x in first_el | iterate[attempt[lambda r: r | Out[RT.ZEF_NextElement] | collect][None]] | take_while[Not[equals[None]]] | map[target])

    if isinstance(args[0], GraphSlice):
        # TODO: We should probalby make slice | all return the delegates too to
        # be in line with g | all. Then the current behaviour would become slice
        # | all[RAE]
        gs = args[0]
        if len(args) == 1:
            return gs.tx | pyzefops.instances
        if len(args) >= 3:
            raise Exception(f"all can only take a maximum of 2 arguments, got {len(args)} instead")

        fil = args[1]
        # These options have C++ backing so try them first
        # The specific all[ET.x/AET.x] options (not all[RT.x] though)
        if isinstance(fil, ET) or isinstance(fil, AET):
            after_filter = None
            from ..VT.rae_types import RAET_get_token
            token = RAET_get_token(fil)
            if token is None:
                if isinstance(fil, ET):
                    c_fil = None
                    after_filter = Entity
                else:
                    c_fil = None
                    after_filter = AttributeEntity
            else:
                if isinstance(token, (EntityTypeToken, AttributeEntityTypeToken)):
                    c_fil = token
                else:
                    # This must be a more general filter, so we should apply it afterwards
                    after_filter = token
                    c_fil = None
            
            if c_fil is None:
                initial = gs.tx | pyzefops.instances
            else:
                initial = gs.tx | pyzefops.instances[c_fil]
            if after_filter is not None:
                return ZefGenerator(lambda: iter(initial | filter[after_filter]))
            else:
                return initial
        
        # TODO: Probably rewrite this to take advantage of the above c-level calls
        if  isinstance(fil, ValueType) and fil != RAE and fil._d['type_name'] in {"Union", "Intersection"}:
            representation_types = absorbed(fil) | filter[lambda x: isinstance(x, (ET, AET))] | func[set] | collect
            value_types = set(absorbed(fil)) - representation_types
            if len(value_types) > 0: 
                # Wrap the remaining ValueTypes after removing representation_types in the original ValueType
                value_types = {"Union": Union, "Intersection": Intersection}[fil._d['type_name']][tuple(value_types)]      

            if fil._d['type_name'] == "Union":
                sets_union = list(set.union(*[set((gs.tx | pyzefops.instances[t])) for t in representation_types]))
                if not value_types: return sets_union
                return list(set.union(set(filter(gs.tx | pyzefops.instances, lambda x: is_a(x, value_types))), sets_union))
            elif fil._d['type_name'] == "Intersection":
                if len(representation_types) > 1: return []
                if len(representation_types) == 1: initial = gs.tx | pyzefops.instances[representation_types.pop()]
                else:  initial = gs.tx | pyzefops.instances
                if not value_types: return initial
                return filter(initial, lambda x: is_a(x, value_types))

        # The remaining options will just use the generic filter and is_a
        return filter(gs.tx | pyzefops.instances, lambda x: is_a(x, fil))
        
    if isinstance(args[0], Graph):
        g = args[0]
        if len(args) == 1:
            # return g | pyzefops.instances_eternal
            return blobs(g)           # show all low level nodes and edges, not only RAEs. We can still add ability  g | all[RAE] later

        if len(args) >= 3:
            raise Exception(f"all can only take a maximum of 2 arguments, got {len(args)} instead")

        fil = args[1]
        if fil == TX:
            return pyzefops.tx(g)

        # These options have C++ backing so try them first
        # The specific all[ET.x/AET.x] options (not all[RT.x] though)
        # Not using as this is not correct in filtering out the delegates
        # if isinstance(fil, EntityType) or isinstance(fil, AttributeEntityType):
        #     return g | pyzefops.instances_eternal[fil]

        # The remaining options will just use the generic filter and is_a
        return filter(blobs(g), lambda x: is_a(x, fil))

    if isinstance(args[0], ZefRef):
        assert len(args) == 1
        z, = args
        assert internals.is_delegate(z)
        return z | pyzefops.instances

    import types
    if isinstance(args[0], types.ModuleType):
        assert len(args) == 2
        assert isinstance(args[1], ValueType)
        list_of_module_values = [args[0].__dict__[x] for x in dir(args[0]) if not x.startswith("_") and not isinstance(getattr(args[0], x), types.ModuleType)]
        return list_of_module_values | filter[args[1]] | collect
    
    # once we're here, we interpret it like the Python "all"
    v = args[0]
    assert len(args) == 1
    assert isinstance(v, list) or isinstance(v, tuple) or isinstance(v, (Generator, ZefGenerator)) or isinstance(v, Iterator)
    # TODO: extend to awaitables
    return builtins.all(v)
            
def all_tp(v_tp):
    return VT.Any


         
#---------------------------------------- any -----------------------------------------------
def any_imp(v):
    """ 
    Given a list of booleans: check whether any of them are true.
    Equivalent to the logical 'or' in propositional logic.
    Also equivalent to Python's builtin 'any', but pipeable and
    applicable to Streams.
    
    An empty list will return False.

    ---- Examples ----
    >>> [False, True, False] | any                 # => True

    >>> [False, False, False] | any                # => False

    >>> [] | any                                   # => False
    """
    import builtins
    return builtins.any(v)
            
def any_tp(v_tp):
    return VT.Bool




#---------------------------------------- join -----------------------------------------------
def join_imp(list_of_strings: VT.List[VT.String], x=''):
    """ 
    Join a list of strings with a binding character.

    ---- Examples ----
    >>> ['foo', 'bar'] | join['-']              # => 'foo-bar'

    ---- Signature ----    
    (List[String], String) -> String

    ---- Tags ----
    - related zefop: concat
    - related zefop: insert_at
    - operates on: String
    """
    return x.join(list_of_strings)


def join_tp(v, x):
    return VT.String



#---------------------------------------- trim_left -----------------------------------------------
def trim_left_imp(v, el_to_trim):
    """
    Removes all contiguous occurrences of a specified 
    element / character from the left side of a list / string.

    ---- Examples ----
    >>> '..hello..' | trim_left['.']            # => 'hello..'

    ---- Signature ----
    (List[T], T) -> List[T]
    (String, String) -> String

    ---- Tags ----
    - related zefop: trim
    - related zefop: trim_right
    - related zefop: split
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    """
    if isinstance(v, String):
        if isinstance(el_to_trim, String):
            return v.lstrip(el_to_trim)
        elif isinstance(el_to_trim, Set):
            vv = v
            for el in el_to_trim:
                vv = vv | chunk[len(el)] | trim_left[{el}] | join[''] | collect
            return vv
        else:
            raise ValueError("Triming a str only takes a string or a set as argument.")
    
    predicate = make_predicate(el_to_trim)
    def wrapper():
        it = iter(v)
        try:
            next_el = next(it)
            while predicate(next_el):
                next_el = next(it)
            yield next_el
            while True:
                yield next(it)        
        except StopIteration:
            return
    return ZefGenerator(wrapper)


def trim_left_tp(v_tp, el_to_trim_tp):
    return v_tp
        
    
    
#---------------------------------------- trim_right -----------------------------------------------
def trim_right_imp(v, el_to_trim):
    """
    Removes all contiguous occurrences of a specified 
    element / character from the right side of a list / string.

    ---- Examples ----
    >>> '..hello..' | trim_right['.']            # => '..hello'

    ---- Signature ----
    (List[T], T) -> List[T]
    (String, String) -> String

    ---- Tags ----
    - related zefop: trim
    - related zefop: trim_left
    - related zefop: split
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    """
    import itertools 
    if isinstance(v, String):
        if isinstance(el_to_trim, String):
            return v.rstrip(el_to_trim)
        elif isinstance(el_to_trim, Set):
            vv = v
            for el in el_to_trim:
                vv = vv | chunk[len(el)] | trim_right[{el}] | join[''] | collect
            return vv
        else:
            raise ValueError("Triming a str only takes a string or a set as argument.")
    # we need to know all elements before deciding what is at the end
    vv = tuple(v)
    vv_rev = vv[::-1]
    predicate = make_predicate(el_to_trim)
    ind = len(list(itertools.takewhile(predicate, vv_rev)))
    return vv if ind==0 else vv[:-ind]


def trim_right_tp(v_tp, el_to_trim_tp):
    return v_tp
        
        

#---------------------------------------- trim -----------------------------------------------
def trim_imp(v, el_to_trim):
    """
    Removes all contiguous occurrences of a specified 
    element / character from both sides of a list / string.

    ---- Examples ----
    >>> '..hello..' | trim['.']            # => 'hello'

    ---- Signature ----
    (List[T], T) -> List[T]
    (String, String) -> String

    ---- Tags ----
    - related zefop: trim_left
    - related zefop: trim_right
    - related zefop: split
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    """
    import itertools 
    if isinstance(v, String):
        if isinstance(el_to_trim, String):
            return v.strip(el_to_trim)
        elif isinstance(el_to_trim, Set):
            vv = v
            for el in el_to_trim:
                # TODO: Need to add padding after chunking to remove correctly from the right
                vv = vv | chunk[len(el)] | trim[{el}] | join[''] | collect
            return vv
        else:
            raise ValueError("Triming a str only takes a string or a set as argument.")
                

    # we need to know all elements before deciding what is at the end
    vv = tuple(v)
    vv_rev = vv[::-1]
    predicate = make_predicate(el_to_trim)
    ind_left = len(list(itertools.takewhile(predicate, vv)))
    ind_right = len(list(itertools.takewhile(predicate, vv_rev)))    
    return vv[ind_left:] if ind_right==0 else vv[ind_left:-ind_right]


def trim_tp(v_tp, el_to_trim_tp):
    return v_tp
        

#---------------------------------------- tap -----------------------------------------------
def tap_imp(x, fct):
    """
    An operator mostly used for debugging and testing
    purposes: allows inserting an impure function into
    a pipeline.
    In contrast to apply/map it is not the output of
    provided function which is forwarded, but the original 
    input. It is therefore ever only of any use to
    insert an impure function into tap, since the return 
    value is discarded.

    ---- Examples ----
    >>> 41 | add[1] | tap[print] | multiply[2] | collect    # prints 42 and the expression returns 84

    ---- Tags ----
    - used for: debugging
    - related zefop: apply
    """
    # As we can receive a generator, we need to collect it up first before continuing
    if isinstance(x, (Generator, ZefGenerator)):
        x = list(x)
    fct(x)
    return x
        

def tap_tp(v_tp, f):
    return v_tp


#---------------------------------------- push -----------------------------------------------
def push_imp(item: VT.Any, stream: VT.Stream):  # -> Union[Effect, Error]
    """ 
    A pure operator that constructs an effect (a value)
    to push an element into a pushable stream.

    ---- Examples ----
    >>> {'msg': 'hello'} | push[my_pushable_stream]                     # returns an effect
    >>> my_msg_stream | subscribe[push[my_pushable_stream]]             # messages are transformed into Effects and executed upon arrival
    >>>
    >>> # upcoming functionality
    >>> my_effect | push[z_process]         # wraps an effect as a task effect that will be routed and potentially executed on the specified process
    >>> my_effect | push[zefhub]

    ---- Signature ----
    (T, Stream) -> Effect
    """
    if isinstance(stream, Awaitable) or isinstance(stream, ZefRef) or isinstance(stream, EZefRef):
        return {
            'type': FX.Stream.Push,
            'stream': stream,
            'item': item,
        }
    raise RuntimeError('push must be called with a Stream pushable stream represented by a ZefRef[ET[PushableStream]] curried in.')
    
    
def push_tp(op, curr_type):
    return VT.Effect




#---------------------------------------- cartesian_product -----------------------------------------------
def cartesian_product_imp(x, second=None, *args):
    """ 
    Note that Python's itertools calls this "product only", but 
    but that term is too overloaded, e.g. the 'multiply' operator.
    
    ---- Examples ----
    >>> [1,2,3] | cartesian_product[('a', 'b')]                   # => [ (1, 'a'), (2, 'a'), (3, 'a'), (1, 'b'), (2, 'b'), (3, 'b') ]
    >>> [1,2,3] | cartesian_product[('a', 'b')][(True,False)]     # => [ (1, 'a', True), ...
    >>> 
    >>> or pass in all args:
    >>> ([1,2,3], ['a', 'b']) | cartesian_product                 # => [ (1, 'a'), (2, 'a'), (3, 'a'), (1, 'b'), (2, 'b'), (3, 'b') ]

    ---- Signature ----
    List[ List[T1], List[T2] ] -> List[ (T1, T2) ]
    (List[T1], List[T2])       -> List[ (T1, T2) ]


    ---- Tags ----
    - operates on: List
    - used for: combinatorics
    - related zefop: combinations
    - related zefop: permutations
    """    
    from itertools import product
    if second is None:
        return product(*x)
    return product( *(x, second, *args) )


def cartesian_product_tp(a, second, *args):
    return VT.List




#---------------------------------------- permutations -----------------------------------------------
def permutations_imp(v, n=None):
    """ 
    Given a list of items, return a list of lists with all 
    permutations lazily. Order within the returned combination 
    does not play a role.

    If given, the second argument is the length of each output.

    ---- Examples ----
    >>> ['a', 'b', 'c'] | permutations 
    >>> # returns:
    >>> # [
    >>> #     ['a', 'b', 'c'],
    >>> #     ['a', 'c', 'b'],
    >>> #     ['b', 'a', 'c'],
    >>> #     ['b', 'c', 'a'],
    >>> #     ['c', 'a', 'b'],
    >>> #     ['c', 'b', 'a']
    >>> # ]

    >>> [1,2,3] | permutations[2]    # specify the number of elements in each sample
    >>> # returns: [(1, 2), (1, 3), (2, 1), (2, 3), (3, 1), (3, 2)]

    ---- Signature ----
    List[T] -> List[List[T]]
    (List[T], Int) -> List[List[T]]

    ---- Tags ----
    - operates on: List
    - used for: combinatorics
    - related zefop: combinations
    - related zefop: cartesian_product
    """
    from itertools import permutations
    return permutations(v, r=n)


def permutations_tp(x):
    return VT.List[x]




#---------------------------------------- combinations -----------------------------------------------
def combinations_imp(v, n):
    """
    Given a list of m elements, return all combinations
    of length n. Order within the returned combination 
    does not play a role.

    ---- Examples ----
    [1,2,3] | combinations[2]      # => [(1, 2), (1, 3), (2, 3)]

    ---- Signature ----
    (List[T], Int) -> List[List[T]]
    """
    import itertools
    return tuple(itertools.combinations(v, n))


def combinations_tp(v, n):
    return VT.List


#---------------------------------------- always -----------------------------------------------
def always_imp(x, value, *args):
    """
    Regardless of the input, always return the curried in value.

    ---- Examples ----
    [1,2,3] | map[always[True]]      # => [True, True, True]

    match[
        (equals[5], "special case"),
        (always[True], "default")
    ]

    ---- Signature ----
    (T, T2) -> T2
    """
    
    return value

def always_tp(x):
    return VT.Any


#---------------------------------------- absorbed -----------------------------------------------
def absorbed_imp(x):
    """
    Extract the absorbed values from any absorbing type.

    ---- Examples ----
    >>> reduce[add[1]][42] | absorbed    # => (add[1], 42)
    
    ---- Tags ----
    - used for: control flow
    - operates on: ZefOp, Value Types, Entity, Relation, AttributeEntity, ZefRef, EZefRef
    - related zefop: without_absorbed
    - related zefop: inject
    - related zefop: inject_list
    - related zefop: reverse_args
    """
    # if isinstance(x, (EntityType, RelationType, AttributeEntityType, Keyword, Delegate)):
    #     if '_absorbed' not in x.__dict__:
    #         return ()
    #     else:
    #         return x._absorbed
    if False:
        pass
    
    elif isinstance(x, (EntityRef, RelationRef, AttributeEntityRef)):
        return x.d['absorbed']

    elif isinstance(x, ZefOp):
        if len(x.el_ops) != 1: 
            return Error(f'"absorbed" can only be called on an elementary ZefOp, i.e. one of length 1. It was called on x={x}')
        return x.el_ops[0][1]

    elif isinstance(x, ZefRef) or isinstance(x, EZefRef):
        return ()
    
    elif isinstance(x, ValueType):
        return x._d['absorbed']

    else:
        return Error(f'absorbed called on type(x)={type(x)}   x={x}')






#---------------------------------------- without_absorbed -----------------------------------------------
def without_absorbed_imp(x):
    """
    Return the bare Type as if nothing had ever been absorbed.

    ---- Examples ----
    >>> LazyValue(reduce[add[1]][42]) | without_absorbed    # => reduce
    
    ---- Tags ----
    - used for: control flow
    - operates on: ZefOp, Value Types, Entity, Relation, AttributeEntity, ZefRef, EZefRef
    - related zefop: absorbed
    - related zefop: inject
    - related zefop: inject_list
    - related zefop: reverse_args
    """
    # if isinstance(x, EntityType):
    #     if '_absorbed' not in x.__dict__:
    #         return x
    #     else:
    #         new_et = EntityType(x.value)
    #         return new_et
    
    # elif isinstance(x, RelationType):
    #     if '_absorbed' not in x.__dict__:
    #         return x
    #     else:
    #         new_rt = RelationType(x.value)
    #         return new_rt
        
    # elif isinstance(x, AttributeEntityType):
    #     if x.complex_value:
    #         return AttributeEntityType(x.complex_value)
    #     else:
    #         return AttributeEntityType(x.rep_type)
                
    # elif isinstance(x, Keyword):
    #     if '_absorbed' not in x.__dict__:
    #         return x
    #     else:
    #         new_kw = Keyword(x.value)
    #         return new_kw

    # elif isinstance(x, Delegate):
    #     if '_absorbed' not in x.__dict__:
    #         return x
    #     else:
    #         new_delegate = Delegate(x.order, x.item)
    #         return new_delegate
    if False:
        pass

    elif isinstance(x, ZefOp):
        if len(x.el_ops) != 1: 
            return Error(f'"without_absorbed_imp" can only be called on an elementary ZefOp, i.e. one of length 1. It was called on x={x}')
        return ZefOp( ((x.el_ops[0][0], ()),) )

    elif isinstance(x, ValueType):
        return x._replace(absorbed=())

    elif isinstance(x, (EntityRef, AttributeEntityRef, RelationRef)):
        return type(x)(remove(x.d, 'absorbed'))
    return Error('Not Implemented')






#---------------------------------------- sum -----------------------------------------------
def sum_imp(v):
    """
    Sums up all elements in a List.
    Similar to the `add` zefop, but to be used in the case of a 
    single argument being passed as a list.
    `add` is used when exactly two elements are passed individually.

    ---- Signature ----
    List[T] -> T

    ---- Tags ----
    used for: maths
    operates on: List
    related zefop: add
    related zefop: product
    """
    import builtins
    return builtins.sum(v)


#---------------------------------------- product -----------------------------------------------
def product_imp(v):
    """
    Multiplies up all elements in a List.
    Similar to the `multiply` zefop, but to be used in the case of a 
    single argument being passed as a list.
    `multiply` is used when exactly two elements are passed individually.

    ---- Signature ----
    List[T] -> T

    ---- Tags ----
    used for: maths
    operates on: List
    related zefop: cartesian_product
    related zefop: multiply
    related zefop: sum
    """
    from functools import reduce
    return reduce(lambda x, y: x * y, v)   # uses 1 as initial val


#---------------------------------------- add -----------------------------------------------
def add_imp(a, b):
    """
    Adds two elements. Neither is a list.
    Don't use this when summing up a list, use `sum`
    in that case.

    ---- Examples ----
    40 | add[2]    # => 42

    ---- Signature ----
    (T1, T2) -> T1 | T2

    ---- Tags ----
    used for: maths
    operates on: Int
    operates on: Float
    related zefop: sum
    related zefop: subtract
    related zefop: multiply
    related zefop: divide
    """
    if type(a) in {list, tuple} or type(b) in {list, tuple}:
        raise TypeError(f"`add` is a binary operator, i.e. always takes two arguments. If you want to sum up all elements in a list, use `sum`")
    return a+b
    
    
def add_tp(a, second, *args):
    return VT.Any    
    
    
    
#---------------------------------------- subtract -----------------------------------------------
def subtract_imp(a, b):
    """
    Binary operator to subtract two elements. Neither is a list.
    If am operator is needed that can act on a tuple of length 2,
    this can be wrapped in `unpack`.

    ---- Examples ----
    44 | subtract[2]               # => 42
    (42, 10) | unpack[subtract]    # => 32

    ---- Signature ----
    (T1, T2) -> T1 | T2

    ---- Tags ----
    used for: maths
    operates on: Int
    operates on: Float
    related zefop: add
    related zefop: multiply
    related zefop: divide
    related zefop: unpack
    """
    if type(a) in {list, tuple} or type(b) in {list, tuple}:
        raise TypeError(f"`subtract` is a binary operator, i.e. always takes two arguments.")
    return a-b
    
    
def subtract_tp(a, second):
    return VT.Any    
    
    
    

#---------------------------------------- multiply -----------------------------------------------
def multiply_imp(a, second=None, *args):
    """ 
    Binary operator only. For a list of numbers, use `product`.

    ---- Examples ----
    >>> 2 | multiply[3]    # => 6

    ---- Signature ----
    (Int, Int) - > Int
    (Float, Float) - > Float

    ---- Tags ----
    used for: maths
    operates on: Int
    operates on: Float
    related zefop: product
    related zefop: add
    related zefop: divide
    related zefop: subtract
    related zefop: unpack
    """
    from functools import reduce
    if second is None:
        return reduce(lambda x, y: x * y, a)
    return reduce(lambda x, y: x * y, [a, second, *args])
    

def multiply_tp(a, second, *args):
    return VT.Any


#---------------------------------------- divide -----------------------------------------------
def divide_imp(a, b=None):    
    """
    Binary operator to divide two elements. Neither is a list.
    If an operator is needed that can act on a tuple of length 2,
    this can be wrapped in `unpack`.

    ---- Examples ----
    10 | divide[2]                # => 5

    ---- Signature ----
    (T1, T2) -> Float

    ---- Tags ----
    used for: maths
    operates on: Int
    operates on: Float
    related zefop: add
    related zefop: multiply
    related zefop: add
    related zefop: subtract
    related zefop: unpack
    """
    if type(a) in {list, tuple} or type(b) in {list, tuple}:
        raise TypeError(f"`subtract` is a binary operator, i.e. always takes two arguments.")
    return a/b
    
    
def divide_tp(a, second):
    return VT.Any    
    




#---------------------------------------- mean -----------------------------------------------
def mean_imp(v):
    """
    Calculates the arithmetic mean of a given List / Set of numbers.
   
    ---- Examples ----
    >>> [3,7,20] | mean    # => 10

    ---- Signature ----
    List[Int] -> Float
    Set[Int] -> Float
    List[Float] -> Float
    Set[Float] -> Float

    ---- Tags ----
    used for: maths
    operates on: List    
    operates on: Set
    related zefop: variance
    """
    return sum(v) / length(v)


def mean_tp(op, curr_type):
    return VT.Any


#---------------------------------------- variance -----------------------------------------------
def variance_imp(v):
    """
    Calculates the variance of a given List / Set of numbers.

    ---- Examples ----
    >>> [-1,0,1] | variance    # => 0.6666..

    ---- Signature ----
    List[Int] -> Float
    Set[Int] -> Float
    List[Float] -> Float
    Set[Float] -> Float

    ---- Tags ----
    used for: maths
    operates on: List    
    operates on: Set
    related zefop: mean
    """
    le = length(v)
    return sum( (x*x for x in v) )/le - (sum(v)/le)**2


def variance_tp(v):
    return VT.Any


#---------------------------------------- power -----------------------------------------------
def power_imp(x, b):
    """
    Take the first argument to the power (exponential) of the second argument.
   
    ---- Examples ----
    >>> 2 | power[8]    # => 256

    ---- Signature ----
    (Int, Int) -> Int
    (Float | Int, Float | Int) -> Float

    ---- Tags ----
    used for: maths
    operates on: Int    
    operates on: Float    
    related zefop: exponential
    related zefop: multiply
    """
    return x**b

def power_tp(op, curr_type):
    return VT.Any


#---------------------------------------- exponential -----------------------------------------------
def exponential_imp(x):
    """
    Take the first argument to the power of the second argument.
   
    ---- Examples ----
    >>> 2 | exponential[8]    # => 256

    ---- Signature ----
    (Float, Float) -> Float

    ---- Tags ----
    used for: maths
    
    related zefop: logarithm
    related zefop: power
    related zefop: multiply
    """
    import math
    return math.exp(x)


def exponential_tp(x):
    return VT.Any


#---------------------------------------- logarithm -----------------------------------------------
def logarithm_imp(x, base=None):
    """
    Take the logarithm, optionally specify the base.
    base = None is synonymous with base = 2.718281828459045...
   
    ---- Examples ----
    >>> 10 | logarithm         # => 2.302585092994046
    >>> 100 | logarithm[10]    # => 2

    ---- Signature ----
    (Float, Float) -> Float

    ---- Tags ----
    used for: maths    
    related zefop: exponential
    related zefop: power
    related zefop: multiply
    """
    """ 
    
    """
    import math
    return (math.log(x) if base is None else math.log(x)/math.log(base))


def logarithm_tp(x,base):
    return VT.Any


#---------------------------------------- max -----------------------------------------------
def max_imp(*args):
    return builtins.max(*args)
    
    
def max_tp(*args):
    return VT.Any
    
    
    

#---------------------------------------- max_by -----------------------------------------------
def max_by_imp(v, max_by_function=None):
    if max_by_function is None:
        raise RuntimeError(f'A function needs to be provided when using max_by. Called for v={v}')
    return builtins.max(v, key=max_by_function)
    
    
def max_by_tp(v_tp, max_by_function_tp):
    return VT.Any
    
    
#---------------------------------------- min -----------------------------------------------
def min_imp(*args):
    return builtins.min(*args)
    
    
def min_tp(a, second, *args):
    return VT.Any
    
    
    

#---------------------------------------- min_by -----------------------------------------------
def min_by_imp(v, min_by_function=None):
    if min_by_function is None:
        raise RuntimeError(f'A function needs to be provided when using min_by. Called for v={v}')
    return builtins.min(v, key=min_by_function)
    
    
def min_by_tp(v_tp, min_by_function_tp):
    return VT.Any
    
    
    
    

#---------------------------------------- clamp -----------------------------------------------
def clamp_imp(x, x_min, x_max):
    """
    Clamp x to be between x_min and x_max.

    ---- Examples ----
    2 | clamp[0][42]        # => 2
    -2 | clamp[0][42]       # => 0
    100 | clamp[0][42]      # => 42

    ---- Signature ----
    (Int|Float, Int|Float, Int|Float) -> Int|Float

    ---- Tags ----
    used for: maths
    operates on: Int
    operates on: Float
    related zefop: min
    related zefop: max
    """
    if x_max < x_min: return Error(f'clamp: x_min={x_min} must be less than or equal to x_max={x_max}')
    return max(x_min, min(x, x_max))

    
    
#---------------------------------------- equals -----------------------------------------------
def equals_imp(a, b):
    """
    Binary operator equivalent to calling "==", but can be chained.
   
    ---- Examples ----
    >>> 42 | equals[42]     # => True
    >>> 42 | Not[equals][41]     # => True
    >>> equals('a', 'a')     # => True

    ---- Signature ----
    (T, T) -> Bool

    ---- Tags ----
    used for: logic
    
    related zefop: Not
    related zefop: greater_than
    related zefop: less_than
    """
    return a == b
    
    
def equals_tp(a, b):
    return VT.Bool

    
    
#---------------------------------------- greater_than -----------------------------------------------
def greater_than_imp(a, b):
    """
    Binary operator equivalent to calling ">", but can be chained.
   
    ---- Examples ----
    >>> 42 | greater_than[41]     # => True
    >>> 42 | greater_than[42]     # => False
    
    ---- Signature ----
    (T, T) -> Bool

    ---- Tags ----
    used for: logic    
    used for: maths    
    related zefop: Not
    related zefop: equals
    related zefop: less_than
    related zefop: greater_than_or_equal
    """
    return a > b
    
    
def greater_than_tp(a, b):
    return VT.Bool    
    
    
    
#---------------------------------------- less_than -----------------------------------------------
def less_than_imp(a, b):
    """
    Binary operator equivalent to calling "<", but can be chained.
   
    ---- Examples ----
    >>> 41 | less_than[42]     # => True
    >>> 42 | less_than[42]     # => False
    
    ---- Signature ----
    (T, T) -> Bool

    ---- Tags ----
    used for: logic    
    used for: maths    
    related zefop: Not
    related zefop: equals
    related zefop: greater_than
    related zefop: less_than_or_equal
    """
    return a < b
    
    
def less_than_tp(a, b):
    return VT.Bool 


    
#---------------------------------------- greater_than_or_equal -----------------------------------------------
def greater_than_or_equal_imp(a, b):
    """
    Binary operator equivalent to calling ">=", but can be chained.
   
    ---- Examples ----
    >>> 43 | greater_than_or_equal[42]     # => True
    >>> 42 | greater_than_or_equal[42]     # => True
    
    ---- Signature ----
    (T, T) -> Bool

    ---- Tags ----
    used for: logic    
    used for: maths    
    related zefop: Not
    related zefop: equals
    related zefop: greater_than
    related zefop: less_than_or_equal
    """
    return a >= b
    

def greater_than_or_equal_to(a, b):
    return VT.Bool 


    
#---------------------------------------- less_than_or_equal -----------------------------------------------
def less_than_or_equal_imp(a, b):
    """
    Binary operator equivalent to calling ">=", but can be chained.
   
    ---- Examples ----
    >>> 41 | less_than_or_equal[42]     # => True
    >>> 42 | less_than_or_equal[42]     # => True
    
    ---- Signature ----
    (T, T) -> Bool

    ---- Tags ----
    used for: logic    
    used for: maths    
    related zefop: Not
    related zefop: equals
    related zefop: greater_than_or_equal
    related zefop: less_than
    """
    return a <= b


def less_than_or_equal_to(a, b):
    return VT.Bool 



#---------------------------------------- Not -----------------------------------------------
def not_imp(x, pred_fct=lambda x: x):
    """ 
    A logic combinator: takes a single predicate function 
    (with any signature) and returns the negated predicate 
    function with the same signature.
    
    ---- Examples ----
    >>> 41 | Not[equals][42]     # => True
    
    ---- Signature ----
    (T, T->Bool) -> Bool

    ---- Tags ----
    used for: logic    
    used for: function composition
    operates on: predicate function
    related zefop: Not
    related zefop: Or
    related zefop: And
    related zefop: xor
    """
    pred_fct = make_predicate(pred_fct)
    return not pred_fct(x)



def not_tp(a, b):
    return VT.Bool 



#---------------------------------------- And -----------------------------------------------
def and_imp(x, *args):
    """
    This operator can be used in two ways:    

    B) Multiary combinator to compose predicate functions.
       Given multiple predicate functions p_1, p_, ...p_m  with 
       signature S->Bool, the expression And[]
       a new predicate function of signature S->Bool is returned.
       Short circuiting is accounted for and the functions are
       evaluated in the order they are listed.
    
    A) binary operator on boolean values

    ---- Examples ----
    >>> 10 | And[greater_than[0]][less_than[42]]     # => True
    >>> {'x': 42, 'b': 'yo'} | And[contains['x']][ get['b] | length | equals[2] ]
    
    >>> [9,3,1,16] | map[And[greater_than[0]][less_than[5]]]     # => [False, True, True, False]

    ---- Signature ----
    (T, T->Bool, T->Bool, ..., T->Bool ) -> Bool

    ---- Tags ----
    used for: logic
    used for: predicate
    operates on: predicate function
    related zefop: Or
    related zefop: Not
    related: Intersection
    """
    # include short circuiting
    if args == ():     # x | And     should always answer True (no predicate function provided)
        return True
        
    a0 = args[0]
    if (a0 is True or a0 is False):        
        # Bools coming in: And used as direct operator, not combinator on predicates
        if not isinstance(x, Bool):
            raise TypeError(f"'And' was called to act on a boolean (based on the non-flow args = {args}), but the flow arg was not a bool {x}")
        if not len(args) == 1:
            raise TypeError(f"'And' was called to act on a boolean, but the number of total arguments was more than 2: x={x} and args={args}. You may want to call `all` in this case.")
        return x and args[0]
              
    # used as combinator on predicates
    for fct in args:
        res = fct(x)
        assert isinstance(res, Bool)
        if res is False:
            return False
    return True
    

def and_tp(x, *args):
    return VT.Any
    
    
    
    
#---------------------------------------- Or -----------------------------------------------
def or_imp(x, *args):
    """
    This operator can be used in two ways:    

    B) Multiary combinator to compose predicate functions.
       Given multiple predicate functions p_1, p_, ...p_m  with 
       signature S->Bool, the expression Or[]
       a new predicate function of signature S->Bool is returned.
       Short circuiting is accounted for and the functions are
       evaluated in the order they are listed.
    
    A) binary operator on boolean values

    ---- Examples ----
    >>> 100 | Or[greater_than[0]][less_than[42]]     # => False    
    >>> [12,7,1] | map[Or[greater_than[10]][less_than[5]]]     # => [True, False, True]

    ---- Signature ----
    (T, T->Bool, T->Bool, ..., T->Bool) -> Bool

    ---- Tags ----
    used for: logic
    used for: predicate
    operates on: predicate function
    related zefop: Or
    related zefop: Not
    related: Intersection
    """
    # include short circuiting
    if args == ():            # x | And     should always answer False (no predicate function provided)
        return False
        
    a0 = args[0]
    if (a0 is True or a0 is False):        
        # Bools coming in: And used as direct operator, not combinator on predicates
        if not isinstance(x, Bool):
            raise TypeError(f"'And' was called to act on a boolean (based on the non-flow args = {args}), but the flow arg was not a bool {x}")
        if not len(args) == 1:
            raise TypeError(f"'And' was called to act on a boolean, but the number of total arguments was more than 2: x={x} and args={args}. You may want to call `all` in this case.")
        return x and args[0]
              
    # used as combinator on predicates
    for fct in args:
        fct = make_predicate(fct)
        res = fct(x)
        assert isinstance(res, Bool)
        if res is True:
            return True
    return False
    

def or_tp(x, *args):
    return VT.Any
    
       
   
#---------------------------------------- xor -----------------------------------------------
def xor_imp(x, *args):
    """ 
    A logic combinator: takes two predicate functions
    (with the same signature) and returns one predicate 
    function with the same signature.
    
    ---- Examples ----
    >>> 41 | xor[p1][p2]
    
    ---- Signature ----
    (T, T->Bool, T->Bool) -> Bool

    ---- Tags ----
    used for: logic    
    used for: function composition
    operates on: predicate function
    related zefop: Not
    related zefop: Or
    related zefop: And
    related zefop: xor
    """
    assert len(args) == 2
    f1, f2 = args
    res1, res2 = (f1(x), f2(x))
    assert isinstance(res1, Bool)
    assert isinstance(res2, Bool)
    return res1 ^ res2   # xor for boolean values
    

def xor_tp(x, *args):
    return VT.Any
    
       
   


#---------------------------------------- skip -----------------------------------------------
def skip_imp(v, n: int):
    """
    Skip the first n elements of the sequence. 
    This can be done lazily.
    
    For negative n the counting is done from the back.
    This cannot be done lazily: one needs to know when the 
    list / iterator / stream ends before emitting the 
    first element.
    
    ---- Examples ----
    >>> range(10) | skip[3]                         # => [3,4,5,6,7,8,9]
    >>> ['a', 'b', 'c', 'd'] | skip[-2]             # => ['a', 'b']
    >>> 'hello' | skip[-2]                          # => 'hel'

    ---- Signature ----
    (List[T], Int) -> List[T]
    (String, Int) -> String

    ---- Tags ----
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    - related zefop: take
    - related zefop: slice
    - related zefop: reverse
    - related zefop: first
    - related zefop: last
    - related zefop: nth
    """

    # handle strings separately: don't return a list, but a string
    if isinstance(v, String):
        if n>=0:
            return v[n:]
        else:
            return v[:n]

    if n>=0:
        def wrapper():
            it = iter(v)
            for _ in range(n):
                next(it)
            yield from it
        return ZefGenerator(wrapper)    
    # n<0
    else:
        # we need to consume all to know where the end is
        cached = tuple(v)    
        return cached[:n]
    

def skip_tp(op, curr_type):
    return curr_type




#---------------------------------------- scan -----------------------------------------------
def scan_implementation(iterable, fct, initial_val=None):
    """
    An operator that take a list of values together with an initial state 
    an emits a sequence of resulting states.
    Similar to 'reduce', but also emits all previous states if required.

    This implies "scan[f][state_ini] | last = reduce[f][state_ini]".

    Equivalent to Python's builtin 'accumulate', but named differently
    to be consistent with ReactiveX and the nomenclature in the 
    live data streaming community.

    This is also called 'scan' in reactiveX and Scala, 'scanl' in 
    Haskell and accumulate in python itertools. Similar to reduce, 
    but returns a sequence with each intermediate result. Essentially 
    this is the operator equivalent to event sourcing or the state model 
    in React/Redux and Elm.

    Note: On the very first call s='a' and el='b' are used

    ---- Examples ----
    >>> ['a', 'b', 'c', 'd', 'e'] | scan[lambda s, el: s+el]    
    >>> # => [    'a', 'ab', 'abc', 'abcd', 'abcde']
    
    ---- Signature ----    
    (List[T1], ((T2, T1)->T2), T2)  ->  List[T2]
    """
    return ZefGenerator(lambda: itertools.accumulate(iterable, fct, initial=initial_val))




def scan_type_info(op, curr_type):
    func = op[1][0]
    assert isfunction(func) or isinstance(func, ZefOp) or (isinstance(func, ZefRef) and ET(func) == ET.ZEF_Function)
    if not isfunction(func) and (isinstance(func, ZefRef) and ET(func) == ET.ZEF_Function): 
        func = get_compiled_zeffunction(func)
        insp = getfullargspec(func)  
        return type_spec(insp.annotations.get("return", None), True)
    else:
        return VT.List[VT.Any]




#---------------------------------------- iterate -----------------------------------------------
def iterate_implementation(x, f):
    """for a homomorphism f and initial val x: [x, f(x), f(f(x)), ...].
    Inspired by the excellent talk by Joel Grus on functional Python.
    
    ---- Examples ----
    >>> 42 | iterate[lambda x: x+1]                                         # => [42, 43, 44, ...]
    >>> 
    >>> # find all of Joe's friends on a graph up to third degree
    >>> ([z_joe, ] 
    >>>   | iterate[lambda z: z >> L[RT.FriendOf] | concat | distinct]      # flatten out and remove duplicates
    >>>   | take[3]
    >>> )


    ---- Signature ----
    (T, (T->T)) -> List[T]
    """
    def wrapper():
        current_val = x
        try:
            yield current_val
            while True:
                new_val = f(current_val)
                current_val = new_val
                yield new_val
        except StopIteration:
            return

    return ZefGenerator(wrapper)


def iterate_type_info(op, curr_type):
    return VT.List


def make_predicate(maybe_predicate):
    # Wrap ValueType or any RAE Type in is_a
    if isinstance(maybe_predicate, ValueType):
        predicate = is_a[maybe_predicate]
    
    # If a set is passed check the existance of the passed element in the set
    elif isinstance(maybe_predicate, Set): 
        predicate = lambda x: x in maybe_predicate

    # ZefOps, ZefFunctions, Lambdas, Python Functions
    elif callable(maybe_predicate) and not isinstance(maybe_predicate, Int): 
        predicate = maybe_predicate
    
    # Anything that didn't match will be matched for equality 
    else:
        log.warning(f"A value {repr(maybe_predicate)} was passed to be used as a ValueType. You should use " + \
         "{" + repr(maybe_predicate) + "} or SetOf[" + repr(maybe_predicate) + "] instead!")
        predicate = lambda x: x == maybe_predicate
        # raise RuntimeError('aaaaarg')
    
    return predicate


#---------------------------------------- skip_while -----------------------------------------------
def skip_while_imp(it, predicate):
    """ 
    Skips the elements of the sequence while the predicate is true.    
    
    ---- Examples ----
    >>> range(10) | skip_while[lambda x: x<4]       # => [4,5,6,7,8,9]

    ---- Signature ----
    (List[T], (T->Bool)) -> List[T]

    ---- Tags ----
    - operates on: List
    - used for: list manipulation
    - related zefop: take
    - related zefop: take_until
    - related zefop: skip
    - related zefop: skip_until
    - uses: Logic Type
    - also named (in itertools): dropwhile
    """
    import itertools
    predicate = make_predicate(predicate)
    res = itertools.dropwhile(predicate, it)
    def wrapper():
            it = iter(res)
            for el in it:
                yield el                
    return ZefGenerator(wrapper)
def skip_while_tp(it_tp, pred_type):
    return it_tp


#---------------------------------------- take -----------------------------------------------
def take_implementation(v, n):
    """
    positive n: take n first items. Operates lazily and supports infinite iterators.
    negative n: take n last items. Must evaluate entire iterable, does not terminate 
    for infinite iterables.

    ---- Examples ----
    >>> ['a', 'b', 'c'] | take[2]       # => ['a', 'b']
    >>> ['a', 'b', 'c'] | take[-2]      # => ['b', 'c']
    >>> 'hello' | take[3]               # => 'hel'
    >>> 'hello' | take[-3]              # => 'llo'

    ---- Signature ----
    (List[T], Int) -> List[T]
    (String, Int)  -> String

    ---- Tags ----
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    - related zefop: slice
    - related zefop: reverse
    - related zefop: first
    - related zefop: last
    - related zefop: nth    
    """
    if isinstance(v, ZefRef) or isinstance(v, EZefRef):
        return take(v, n)
    elif isinstance(v, String):
        return v[:n] if n>=0 else v[n:]
    else:
        if n >= 0:
            def wrapper():
                it = iter(v)
                try:
                    for _ in range(n):
                        yield next(it)
                except StopIteration:
                    return

            return ZefGenerator(wrapper)
        else:
            if isinstance(v, String): return v[n:]
            vv = tuple(v)
            return vv[n:]



        
        
def take_type_info(op, curr_type):
    return curr_type



#---------------------------------------- take_while -----------------------------------------------
def take_while_imp(v, predicate):
    """ 
    Similar to take_until, but with positive predicate and 
    it does not include the bounding element.
    
    ---- Examples ----
    >>> range(10) | take_while[lambda x: x<4]       # => [0, 1, 2, 3]

    ---- Signature ----
    (List[T], (T->Bool)) -> List[T]


    ---- Tags ----
    - operates on: List
    - used for: list manipulation
    - related zefop: take
    - related zefop: take_until
    - related zefop: skip
    - related zefop: skip_while
    - related zefop: skip_until
    - uses: Logic Type
    """
    predicate = make_predicate(predicate)
    def wrapper():
        it = iter(v)
        for el in it:
            if predicate(el):
                yield el                
            else:
                return
    return ZefGenerator(wrapper)

def take_while_tp(it_tp, pred_type):
    return it_tp




#---------------------------------------- take_until -----------------------------------------------
def take_until_imp(v, predicate):
    """ 
    Similar to take_while, but with negated predicate and 
    it includes the bounding element. Useful in some cases,
    but take_while is more common.
    
    ---- Examples ----
    >>> range(10) | take_until[lambda x: x>4]       # => [0, 1, 2, 3, 4, 5]

    ---- Signature ----
    (List[T], (T->Bool)) -> List[T]

    ---- Tags ----
    - operates on: List
    - used for: list manipulation
    - related zefop: take
    - related zefop: take_while
    - related zefop: skip
    - related zefop: skip_while
    - uses: Logic Type
    """
    predicate = make_predicate(predicate)
    def wrapper():
        it = iter(v)
        for el in it:
            if not predicate(el):
                yield el
            else:
                yield el
                break
    return ZefGenerator(wrapper)


def take_until_tp(it_tp, pred_type):
    return it_tp




#---------------------------------------- take_until -----------------------------------------------
def skip_until_imp(v: List, predicate):
    """ 
    Skips the elements of the sequence while the predicate is true.    
    
    ---- Examples ----
    >>> range(10) | skip_until[lambda x: x>5]   # => [6, 7, 8, 9]

    ---- Signature ----
    (List[T], (T->Bool)) -> List[T]

    ---- Tags ----
    - operates on: List
    - used for: list manipulation
    - related zefop: skip
    - related zefop: skip_while
    - related zefop: take_until
    - related zefop: take_while
    - uses: Logic Type    
    """
    predicate = make_predicate(predicate)
    def wrapper():
        it = iter(v)
        while True:
            try:
                x = next(it)
                if predicate(x):
                    yield x
                    break                
            except StopIteration:
                return None
        yield from it
        return None

    return ZefGenerator(wrapper)



#---------------------------------------- take_until -----------------------------------------------
def take_while_pair_imp(iterable, binary_predicate):
    """ use when termination of sequence depends on two successive elements.
    Generates items until the predicate function returns false for the first time.
    Emits both elements given to the predicate function upon the last call.

    ---- Examples ----
    >>> (2 
    >>>   | ops.iterate[lambda n: n+1 if n < 10 else 10] 
    >>>   | take_while_pair[lambda m,n: m!=n]    
    >>>   ) # =>   [2, 3, 4, 5, 6, 7, 8, 9, 10]

    ---- Signature ----
    (List[T], ((T*T)->Bool)) -> List[T]
    """
    # the following almost does what we want, but not quite. It does not emit the very last element
    # return iterable | ops.pairwise | ops.take_while[lambda p: binary_predicate(*p)] | ops.map[lambda p: p[0]]
    class Sentinel:
        pass
    sentinel = Sentinel()       # don't reuse any value the user could have given
    _prev_value = sentinel
    def wrapper():
        it = iter(iterable)
        val = next(it)
        yield val
        while True:
            _prev_value = val
            try:
                val = next(it)
            except StopIteration:
                return
            if not binary_predicate(_prev_value, val):
                return
            yield val

    return ZefGenerator(wrapper)



def take_while_pair_tp(it_tp, pred_type):
    return it_tp




#---------------------------------------- single -----------------------------------------------
def single_imp(itr):
    """ 
    Return the element from a list containing a single element only.

    ---- Examples ----
    >>> [42,] | single          # => 42
    >>> [42, 43] | single       # => Error

    ---- Signature ----
    List[T] -> Union[T, Error]
    """
    input_type = parse_input_type(type_spec(itr))
    if input_type == "zef":
        return pyzefops.only(itr)
    else:
        iterable = iter(itr)
        val = next(iterable)
        try:
            next(iterable)
        except StopIteration:
            return val    
        raise AssertionError("The passed iterable is longer than 1 element!")


def single_tp(op, curr_type):
    if curr_type in ref_types:
        curr_type = downing_d[curr_type]
    else:
        try:
            curr_type = absorbed(curr_type)[0]
        except AttributeError as e:
            raise Exception(f"An operator that downs the degree of a Nestable object was called on a Degree-0 Object {curr_type}: {e}")
    return curr_type


#---------------------------------------- single_or -----------------------------------------------
def single_or_imp(itr, default):
    """ 
    Given an iterable which should contain either 0 or 1 elements and a default, return the element if it exists, otherwise return the default. Lists that are 2 or more elements long cause an Error

    ---- Examples ----
    >>> [42] | single_or[None]          # => 42
    >>> [] | single_or[None]            # => None
    >>> [42, 43] | single_or[None]      # => Error

    ---- Signature ----
    List[T],T2 -> Union[T, T2, Error]
    """
    iterable = iter(itr)
    try:
        val = next(iterable)
        try:
            next(iterable)
            raise Exception("single_or detected more than one item in iterator")
        except StopIteration:
            return val
    except StopIteration:
        return default

def single_or_tp(op, curr_type):
    return VT.Any


#---------------------------------------- first -----------------------------------------------

def first_imp(iterable):
    """
    Return the first element of a list.

    ---- Examples ----
    >>> [1,2,3] | first          # => 1
    >>> [] | first               # => Error

    ---- Signature ----
    List[T] -> Union[T, Error]
    """
    if isinstance(iterable, ZefRef) and is_a[ET.ZEF_List](iterable):
        return iterable | all | first | collect


    it = iter(iterable)
    try:
        return next(it)
    except StopIteration:
        return Error("Empty iterable when asking for first element.")



def first_tp(op, curr_type):
    if curr_type in ref_types:
        curr_type = downing_d[curr_type]
    else:
        try:
            curr_type = absorbed(curr_type)[0]
        except AttributeError as e:
            raise Exception(f"An operator that downs the degree of a Nestable object was called on a Degree-0 Object {curr_type}: {e}")
    return curr_type




#---------------------------------------- second -----------------------------------------------

def second_imp(iterable):
    """
    Return the second element of a list.

    ---- Examples ----
    >>> [1,2,3] | second          # => 2
    >>> [1,] | second             # => Error

    ---- Signature ----
    List[T] -> Union[T, Error]
    """
    if isinstance(iterable, ZefRef) and is_a[ET.ZEF_List](iterable):
        return iterable | all | second | collect
    
    it = iter(iterable)
    try:
        _ = next(it)
        return next(it)
    except StopIteration:
        return Error("Iterable is not long enough to find a second element")
        # raise ValueError("Iterable is not long enough to find a second element")


def second_tp(op, curr_type):
    if curr_type in ref_types:
        curr_type = downing_d[curr_type]
    else:
        try:
            curr_type = absorbed(curr_type)[0]
        except AttributeError as e:
            raise Exception(f"An operator that downs the degree of a Nestable object was called on a Degree-0 Object {curr_type}: {e}")
    return curr_type




#---------------------------------------- last -----------------------------------------------

def last_imp(iterable):
    """
    Return the last element of a list.

    ---- Examples ----
    >>> [1,2,3] | first          # => 3
    >>> [] | first               # => Error

    ---- Signature ----
    List[T] -> Union[T, Error]
    """
    if isinstance(iterable, ZefRef) and is_a[ET.ZEF_List](iterable):
        return iterable | all | last | collect

    input_type = parse_input_type(type_spec(iterable))
    if "zef" in input_type:
        return curry_args_in_zefop(pyzefops.last, iterable)
    elif input_type == "awaitable":
        observable_chain = iterable
        return observable_chain.pipe(rxops.last())
    else:
        item = None
        for item in iterable:
            pass
        return item


def last_tp(op, curr_type):
    if curr_type in ref_types:
        curr_type = downing_d[curr_type]
    else:
        try:
            curr_type = absorbed(curr_type)[0]
        except AttributeError as e:
            raise Exception(f"An operator that downs the degree of a Nestable object was called on a Degree-0 Object {curr_type}: {e}")
    return curr_type



#---------------------------------------- frequencies -----------------------------------------------
def frequencies_imp(v):
    from collections import defaultdict
    d = defaultdict(int)
    for el in v:
        d[el] += 1
    return dict(d)


def frequencies_tp(v_tp):
    return VT.Dict



#---------------------------------------- Z -----------------------------------------------
def Z_imp(z):
    return z

def Z_tp(op, curr_type):
    assert curr_type in ref_types
    return curr_type


#---------------------------------------- Root -----------------------------------------------
def root_imp(x):
    """
    Retrieve the root node for a Graph or Graph Slice.

    ---- Examples ----
    >>> g | root
    >>> g | now | root

    ---- Signature ----
    Graph -> EZefRef
    GraphSlice -> ZefRef

    ---- Tags ----
    - used for: graph traversal
    """
    if isinstance(x, (Graph, GraphSlice)):
        return x[pyzef.internals.root_node_blob_index()]
    raise Exception(f"Don't know how to find root of type {type(g)}")


def root_tp(op, curr_type):
    return None





# ------------------------------------------ sign ---------------------------------------------



def sign_imp(x):
    """
    Utility function that returns the sign of a number (+1/-1).
    also: sign(0)=0

    ---- Signature ----
    Union[Int, Float] -> SetOf[-1,0,1] | Error
    """
    if x > 0: return 1 
    elif x == 0: return 0
    elif x < 0: return -1
    raise ValueError()


def sign_tp(op, curr_type):
    return VT.Int




# --------------------------------------- If ------------------------------------------------
def If_imp(x, pred, true_case_func, false_case_func):
    """
    Dispatches to one of two provided functions based on the boolean 
    result of the predicate function wrapped as a logic type, given
    the input value.
    The input value into the zefop is used by the predicate and 
    forwarded to the relevant case function.

    ---- Examples ----
    >>> Evens = Is[modulo[2] | equals[0]]
    >>> 4 | If[ Evens ][add[1]][add[2]]            # => 5

    ---- Signature ----
    ((T->Bool), (T->T1), (T->T2)) -> Union[T1, T2]

    ---- Tags ----
    - used for: control flow
    - used for: logic
    - related zefop: group_by
    - related zefop: match
    - related zefop: filter
    """
    try:
        pred = make_predicate(pred)
        case = pred(x)
    except Exception as e:            
        raise RuntimeError(f'\nError within `If` zefop evaluating predicate function: `{pred}` for value  `{x}`: {e}')
    try:
        return true_case_func(x) if case else false_case_func(x)
    except Exception as e:            
        raise RuntimeError(f'\nError within `If` zefop evaluating the apply function for value  `{x}`: {e}')




# ------------------------------------------ attempt ---------------------------------------------
def attempt_imp(x, op, alternative_value):
    """
    Wrap an operator to catch errors. If anything goes wrong,
    return the user provided alternative value.

    ---- Examples ----
    >>> risky_chain = (
    >>>     map[lambda x: 1/x] 
    >>>   | filter[greater_than[0]] 
    >>>   | single
    >>> )
    >>> alternative_val = 42
    >>> 
    >>> [-1,10,-3] | attempt[risky_chain][alternative_val]     # => 0.1
    >>> [-1,10,3] | attempt[risky_chain][alternative_val]      # => 42      (single called on list with 2 items)
    >>> [-1,0,-3] | attempt[risky_chain][alternative_val]      # => 42      (division by zero)
    
    ---- Signature ----
    (T, (T-> Union[T1, Error]), T2) -> Union[T1, T2]

    ---- Tags ----
    - used for: control flow
    - related zefop: if_then_else
    - related zefop: expect
    - related zefop: ensure
    - related zefop: bypass
    """
    try:
        res = op(x)
        return res
    except:                
        return alternative_value


def attempt_tp(x_tp, op_tp, alternative_value_tp):
    return VT.Any


# ------------------------------------------ bypass ---------------------------------------------

def bypass_imp(x, bypass_type, fct):
    """ 
    specify one or a tuple of types that will be bypassed. Otherwise the specified 
    function will be called and the result returned.
    
    ---- Examples ----
    >>> my_stream | bypass[Error][my_func]        # any Error will just be forwarded
    >>> my_stream | bypass[Error, Int][my_func]
    >>> my_stream | bypass[set_of[_1 < 42]][my_func]

    ---- Signature ----
    (List[T, T2], Type(T2), (T -> T3)) -> List[Union[T2, T3]]

    ---- Tags ----
    used for: control flow
    operates on: ZefOp
    """
    type_tup = bypass_type if isinstance(bypass_type, tuple) else (bypass_type,)
    return If(
        x, 
        type_tup | reduce[lambda op, el: op[el]][is_a],
        lambda x: x,
        fct,
        )


def bypass_tp(x, bypass_type, fct):
    return VT.Any






# ----------------------------------------- pattern ----------------------------------------------

def pattern_imp(x, p):
    """ 
    generates a predicate function given a parameterized pattern for
    dicts or list.
    It is often used inside the "match" operator.
    
    ---- Examples ----
    >>> [1,2,3,4,5] | pattern[_any, 2, _any, 4]               # => True
    >>> [1,2,3,4,5] | pattern[_any, 2, _any, 5]               # => False
    >>> 
    >>> {'a': 1, 'b': 2} | pattern[{'a': Z}]                  # => True: is there any key a?
    >>> {'a': 1, 'b': 2} | pattern[{'a': Z, 'b': 2}]          # => True

    ---- Signature ----
    (List[T], List[Union[T, _Any]]) -> Bool
    (Dict[T1, T2], Dict[Union[T1, _Any], Union[T2, _Any]]) -> Bool

    ---- Tags ----
    - operates on: List
    - operates on: Dict
    - used for: control flow
    - related zefop: match
    - related zefop: match_apply
    - related zefop: distinct_by
    """
    print(f"pattern will be retired: use `Pattern` and if a predicate is required `is_a[Pattern[...]]` instead. 🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱 ")
    from zef.ops import _any
    class Sentinel:
        pass
    sentinel = Sentinel()      # make sure this doesn't match any value provided by the user
    
    assert (
        (isinstance(x, Dict) and isinstance(p, Dict)) or 
        (type(x) in {list, tuple} and type(p) in {list, tuple}) or
        (isinstance(x, Set) and isinstance(p, Set)) or 
        True
    )
    if isinstance(x, Dict):
        assert  isinstance(p, Dict)        
        for k, v in p.items():            
            r = x.get(k, sentinel)
            if r is sentinel:
                return False
            if not ((isinstance(v, ZefOp) and v == Z) or v == r or v is _any):
                return False            
        return True
        
    elif isinstance(x, list) or isinstance(x, tuple):
        assert isinstance(p, list) or isinstance(p, tuple)
        for p_e,x_e in zip(p, x): # Creates tuples of pairwise elements from both lists
            if p_e != x_e and p_e != _any:  # i.e (_any, 1)->True , (1, 1)->True , (4, 1)->False
                return False
        return True
    
    elif isinstance(x, Set):
        assert  isinstance(p, Set)  
        return p.issubset(x)      


    
    raise TypeError(f"types of x and p don't match in pattern: {x} of type {type(x)} and {p} of type {type(p)}")
    

def pattern_tp(x, p):
    return VT.Bool



# ---------------------------------------- distinct -----------------------------------------------

def distinct_imp(v):
    """
    Remove multiple occurrences of the same element (determined 
    via == comparison evaluating to True). Do this lazily, such 
    that this can also be applied to lazily.

    ---- Examples ----
    >>> [4,5,4,1,5,4] | distinct        # => [4, 5, 1]

    ---- Arguments ----
    v: an iterable with elements that can be compared

    ---- Signature ----
    List[T] -> List[T]
    LazyValue[List[T]] -> LazyValue[List[T]]

    ---- Tags ----
    - operates on: Stream
    - operates on: List
    - used for: list manipulation
    - related zefop: is_distinct
    - related zefop: distinct_by
    """
    observed = set()
    unhashable = []
    it = iter(v)
    from typing import Hashable
    def wrapper():
        try:
            while True:
                el = next(it)
                if isinstance(el, Hashable):
                    if el not in observed:
                        observed.add(el)
                        yield el
                else:
                    if el not in unhashable:
                        unhashable.append(el)
                        yield el
        except StopIteration:
            return

    return ZefGenerator(wrapper)



def distinct_tp(x):
    return x


# ---------------------------------------- distinct_by -----------------------------------------------

def distinct_by_imp(v, fct):
    """
    Remove multiple occurrences of the same element determining equality
    after passing through the function passed in.    
    Do this lazily, such that this can also be applied to lazily.

    ---- Examples ----
    >>> [-1,2,1,-2] | distinct_by[lambda x: x*x]        # => [-1, 2]

    ---- Arguments ----
    v: an iterable with elements that can be compared
    comparison_function: (T, T) -> Bool

    ---- Signature ----
    (List[T], (T->Any)) -> List[T]
    LazyValue[(List[T], (T->Any))] -> LazyValue[List[T]]

    ---- Tags ----
    - operates on: List
    - operates on: Stream
    - used for: list manipulation
    - related zefop: is_distinct_by
    - related zefop: distinct
    """
    observed = set()
    it = iter(v)
    def wrapper():
        try:
            while True:
                el = next(it)
                f_of_el = fct(el)
                if f_of_el not in observed:
                    observed.add(f_of_el)
                    yield el
        except StopIteration:
            return

    return ZefGenerator(wrapper)


def distinct_by_tp(x):
    return x


# ---------------------------------------- distinct -----------------------------------------------

def is_distinct_imp(v):
    """
    Used on an iterable / stream of values and
    returns a boolean indicating whether all
    values are distinct. i.e. as soon as any value
    appears more than once, False is returned.

    ---- Examples ----
    >>> [1,2,3] | is_distinct        # => True
    >>> [1,2,3,2] | is_distinct      # => False

    ---- Arguments ----
    v: an iterable with elements that can be compared

    ---- Signature ----
    List[T] -> Bool
    Stream[T] -> Bool
    LazyValue[List[T]] -> Bool

    ---- Tags ----
    - operates on: String
    - operates on: List
    - used for: set theory
    - related zefop: is_distinct_by
    - related zefop: distinct
    """
    vv = tuple(v)
    return len(vv) == len(set(vv))



def is_distinct_tp(x):
    return VT.Bool


# ---------------------------------------- distinct -----------------------------------------------

def is_distinct_by_imp(v, fn):
    """
    Very similar to `is_distinct` zefop, but takes a user
    provided function `fn` to compare the equality testing with.

    ---- Examples ----
    >>> [1,2] | is_distinct_by[lambda x: x%2]      # => True
    >>> [1,2,3] | is_distinct_by[lambda x: x%2]    # => False

    ---- Arguments ----
    v: an iterable with elements that can be compared
    fn: the function to be applied elementwise

    ---- Signature ----
    List[T], Callable -> Bool
    Stream[T], Callable -> Bool

    ---- Tags ----
    - operates on: String
    - operates on: List
    - used for: set theory
    - related zefop: distinct_by
    - related zefop: is_distinct
    """
    w = [fn(x) for x in v]
    return len(w) == len(set(w))        # TODO: this can clearly be made more efficient by exiting early.



def is_distinct_by_tp(x):
    return VT.Bool





# ---------------------------------------- replace -----------------------------------------------
def replace_imp(collection, old_val, new_val):
    """
    Replace any specified old value with a new value in a List.
    
    ---- Examples ----
    >>> ['a', 'b', 'c', 'd'] | replace['b'][42]    # ['a', 42, 'c', 'd'] 
    >>> 'the zen of python' | replace['n']['f']    # 'the zef of pythof'

    ---- Signature ----
    List[T1], T1, T2 -> List[T1 | T2]
    Char = String & Length[1]
    String, Char, Char -> String
    
    ---- Tags ----
    - operates on: String
    - operates on: List
    - used for: list manipulation
    - used for: string manipulation
    - related zefop: replace_at
    - related zefop: insert_in
    - related zefop: insert
    - related zefop: remove
    """
    from typing import Iterable
    def rep(el):
        return new_val if old_val == el else el
    
    if isinstance(collection, String):
        return collection.replace(old_val, new_val)
    
    if isinstance(collection, Set):
        return set((rep(el) for el in collection))
    
    if isinstance(collection, Dict):
        raise TypeError('Dont use "replace", but "insert" to replace a key value pair in a dictionary')
  
    if isinstance(collection, Iterable):
        def wrapper():
            it = iter(collection)
            try:
                while True:
                    yield rep(next(it))            
            except StopIteration:
                return
        return ZefGenerator(wrapper)
    
    raise TypeError('not implemented')


def replace_tp(*args):
    return VT.Any
    



# ----------------------------------------- shuffle ----------------------------------------------
def shuffle_imp(collection, seed: int = None):
    """ 
    Shuffles the elements in a list. If a seed is
    provided, this is a pure function.
    If no seed is set to None, the system's 
    RNG is used to draw one. In this case this
    operation is impure!
    

    TODO: implement coeffects to get randomness from FX system.
    
    ---- Examples ----
    >>> ['a', 'b', 'c', 'd'] | shuffle[189237]    # => ['d', 'b', 'a', 'c']

    ---- Tags ----
    - operates on: List
    - used for: list manipulation
    - used for: randomness
    """
    import random
    if seed is not None:
        random.seed(seed)
    cpy = [*collection]
    random.shuffle(cpy)     # this mutates the argument
    return cpy


def shuffle_tp(*args):
    return VT.Any




# ---------------------------------------- slice -----------------------------------------------
def slice_imp(v, start_end: tuple):
    """ 
    factor out python-style slicing: ...[5:8].
    Negative indexes count from the back.
    If three arguments are given from the range, the 
    last one denotes the step size.

    ---- Examples ----
    >>> ['a', 'b', 'c', 'd'] | slice[1:2]    # => ['b', 'c']
    >>> 'abcdefgh' | slice[1,6,2]            # => 'bdf'
    >>> 'hello' | slice[1,3]                 # => 'el'
    >>> 'hello' | slice[1,-1]                # => 'ello'
    
    ---- Tags ----
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    - related zefop: take
    - related zefop: reverse
    - related zefop: first
    - related zefop: last
    - related zefop: nth    
    """
    from typing import Generator
    import itertools
    assert isinstance(start_end, tuple)
    if not len(start_end) in {2,3}:
        raise RuntimeError('start_end must be a Tuple[Int] of len 2 or 3')
    
    if type(v) in {list, tuple, str}:
        if len(start_end) == 2:
            end = start_end[1] if start_end[1]>=0 else start_end[1]+1
            return v[start_end[0]: end] if start_end[1] !=-1 else v[start_end[0]:]
        if len(start_end) == 3:
            return v[start_end[0]: start_end[1] : start_end[2]]
    elif isinstance(v, Generator) or isinstance(v, ZefGenerator):
        start, end = start_end
        # don't returns a custom slice object, but a generator to make it uniform.
        def wrapper():
            yield from itertools.islice(v, start, end)        
        return ZefGenerator(wrapper)
    else:
        raise TypeError(f"`slice` not implemented for type {type(v)}: {v}")


def slice_tp(*args):
    return VT.Any




# ---------------------------------------- split -----------------------------------------------

def split_imp(collection, val, max_split=-1):
    """ 
    Split a List into a List[List] based on the occurrence of val.
    The value that is split on is not contained in any of the output lists.

    If max_split is set to a positive integer, the split is truncated after
    that number. If max_split is set to -1, the split is unlimited.

    ---- Examples ----
    >>> 'abcdeabfb' | split['b']            # => ['a', 'cdea', 'f', '']
    >>> 'abcdeabfb' | split['b'][1]         # => ['a', 'cdeabfb']
    >>> [0,1,6,2,3,4,2,] | split[2]         # => [[0, 1, 6], [3, 4], []]  
    >>> '..hello..' | split['..']         # => ['', 'hello', '']
    >>> '..hello..' | split['.']         # => ['','', 'hello', '', '']

    ---- Signature ----
    (List[T], T) -> List[List[T]]

    ---- Tags ----
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    - related zefop: split_left
    - related zefop: split_right
    - related zefop: trim
    """
    if isinstance(collection, String):
        return collection.split(val, max_split)

    split_function = make_predicate(val)
    if max_split == -1:       
        def wrapper():
            it = iter(collection)
            try:
                while True:
                    s = []
                    next_val = next(it)
                    while not split_function(next_val):                
                        s.append(next_val)
                        next_val = next(it)                    
                    yield s
            except StopIteration:
                yield s
                return
        return ZefGenerator(wrapper)

    else:
        assert (max_split >= 0)
        def wrapper2():
            run_no=0
            it = iter(collection)
            try:
                while run_no < max_split:
                    s = []
                    next_val = next(it)
                    while not split_function(next_val):                
                        s.append(next_val)
                        next_val = next(it)                    
                    yield s
                    run_no += 1
                yield list(it)
            except StopIteration:
                yield s
                return

        return ZefGenerator(wrapper2)


def split_tp(*args):
    return VT.Any


# ---------------------------------------- split_left -----------------------------------------------
def split_left_imp(v, val):
    """ 
    Split a List into a List[List] based on the occurrence of val.
    In contrast to `split`, it adds the element to split on to the
    following sublist - i.e. it splits on the left side.
    
    ---- Examples ----
    >>> 'abcdeabfb' | split_left['b']            # => ['a', 'bcdea', 'bf', 'b']
    >>> [0,1,6,2,3,4,2,] | split_left[2]         # => [[0, 1, 6], [2, 3, 4], [2]]    

    ---- Signature ----
    (List[T], T) -> List[List[T]]

    ---- Tags ----
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    - related zefop: split
    - related zefop: split_right
    - related zefop: trim
    """
    if isinstance(v, String):
        return split_left_imp(iter(v), val) | map[concat] | collect

    split_function = make_predicate(val)
    def wrapper():
        it = iter(v)
        try:
            next_val = next(it)
            while True:
                s = []
                s.append(next_val)
                next_val = next(it)
                while not split_function(next_val):                
                    s.append(next_val)
                    next_val = next(it)                    
                yield s
        except StopIteration:
            if s!=[]: yield s
            return
    return ZefGenerator(wrapper)


# ---------------------------------------- split_right -----------------------------------------------
def split_right_imp(v, val):
    """ 
    Split a List into a List[List] based on the occurrence of val.
    In contrast to `split`, it adds the element to split on to the
    left sublist - i.e. it splits on the right side.
    
    ---- Examples ----
    >>> 'abcdeabfb' | split_right['b']            # => ['ab', 'cdeab', 'fb']
    >>> [0,1,6,2,3,4,2,] | split_right[2]         # => [[0, 1, 6, 2], [3, 4, 2]]    

    ---- Signature ----
    (List[T], T) -> List[List[T]]

    ---- Tags ----
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    - related zefop: split
    - related zefop: split_left
    - related zefop: trim
    """
    if isinstance(v, String):
        return split_right_imp(iter(v), val) | map[concat] | collect

    split_function = make_predicate(val)
    def wrapper():
        it = iter(v)
        try:
            while True:
                s = []
                next_val = next(it)
                s.append(next_val)
                while not split_function(next_val):                
                    next_val = next(it)                    
                    s.append(next_val)
                yield s
        except StopIteration:
            if s!=[]: yield s
            return
    return ZefGenerator(wrapper)


# --------------------------------------- now ------------------------------------------------

def now_implementation(*args):
    """
    Caution: impure!
    
    The 'now' operator provides the link between the physical 
    time at which the computation is executed and the "time"
    that is part of the inherent data that is operated on and
    that is also often saved as data on graphs.
    
    It can be used in a variety of contexts:
    a)  As a nullary function (i.e. calling it without arguments) 
        returns the current physical time.
    b)  As a transformational operator that changes the reference 
        frame to the currently executing process, i.e. the thread
        performing the computation at the very time this operation 
        is called.

    It is impure, since calling it at different physical times will
    give different outputs.
    Impurity is transitive: any function calling it is also impure.

    ---- Examples ----
    now()                                   # returns current time (of type Time) 
    g | now                                 # latest graph slice
    gs | now                                # latest graph slice
    z_zr_my_entity | now                    # ZefRef -> ZefRef:     fast forward to very latest reference frame at time of execution
    z_ezr_my_entity | now                   # EZefRef -> ZefRef:    fast forward to very latest reference frame at time of execution
    z_ezr_my_entity[allow_tombstone] | now  # EZefRef -> ZefRef:    flag allows representing RAEs that were terminated.

    ---- Signature ----
    () -> Time
    Graph -> GraphSlice
    GraphSlice -> GraphSlice
    ZefRef -> ZefRef
    EZefRef -> ZefRef
    (ZefRef, ZefOp) -> ZefRef
    (EZefRef, ZefOp) -> ZefRef

    """
    if len(args) == 0:
        return pyzefops.now()
    if len(args) == 1:
        if isinstance(args[0], Graph):
            return GraphSlice(pyzefops.now(args[0]))

        if isinstance(args[0], GraphSlice):
            return GraphSlice(pyzefops.now(Graph(args[0].tx)))

        if isinstance(args[0], ZefRef):
            return pyzefops.now(args[0])

        if isinstance(args[0], EZefRef):
            return pyzefops.now(args[0])

    if len(args) == 2:
        assert args[1] == allow_tombstone
        if isinstance(args[0], Graph):
            return GraphSlice(pyzefops.now(args[0]))
        
        if isinstance(args[0], GraphSlice):
            return GraphSlice(pyzefops.now(Graph(args[0].tx)))

        if isinstance(args[0], ZefRef):
            z=args[0]
            return z | in_frame[allow_tombstone][now(Graph(z))] | collect

        if isinstance(args[0], EZefRef):
            z=args[0]
            return z | in_frame[allow_tombstone][now(Graph(z))] | collect

    raise TypeError(f'now called with unsuitable types: args={args}')

            
def now_type_info(*args_tp):
    return VT.Any




# ---------------------------------------- time_slice -----------------------------------------------

def time_slice_implementation(gs: GraphSlice, *curried_args):
    """ 
    Returns the time slice as an special `TimeSlice` object for a given  GraphSlice.
    
    Note: we may introduce z_tx | time_slice if we can find a convincing use case.
    We're leaving it out for now, since this builds on the prior confusion
    of identifying transactions themselves with time slice.
    The new mental image is that reference frames are somewhat distinct from 
    TXs and a time slice applies to a reference frame, not a TX directly.

    ---- Examples ----
    >>> my_graph_slice | time_slice

    ---- Signature ----
    GraphSlice -> TimeSlice
    """
    print(f"😔😔😔😔😔😔 RThis zefop will be deprecated. The TimeSlice data structure \
         (which it returns) has been conceptually replaced with 'GraphSlice'.")
    if isinstance(gs, GraphSlice):
        return pyzefops.time_slice(gs.tx)

    raise TypeError(f"The 'time_slice' ZefOp can only be used on GraphSlices. Use 'frame | time_slice' or 'to_graph_slice | time_slice' ")  


def time_slice_type_info(op, curr_type):
    return VT.TimeSlice


# ---------------------------------------- graph_slice_index -----------------------------------------------

def graph_slice_index_imp(gs: GraphSlice, *curried_args):
    """ 
    Returns the index of a GraphSlice as an Int. 
    
    Historical note: the GraphSlice data structure itself replaces 
    what was previously the TimeSlice.

    Historical note 2: There was initial confusion in conflating the 
    transactions themselves with time slices / graph slices.
    These are distinct concepts: TXs are sets of changes, 
    i.e. events: "what happened?"
    
    GraphSlices are states: "What is?" They are also known as 
    "reference frames"
    
    ---- Examples ----
    >>> my_graph_slice | graph_slice_index

    ---- Signature ----
    (GraphSlice, ) -> Int
    """
    if isinstance(gs, GraphSlice):
        return int(pyzefops.time_slice(gs.tx))

    raise TypeError(f"The 'graph_slice_index' ZefOp can only be used on GraphSlices. You may want to use the 'frame' or 'to_graph_slice' zefops")  



# ---------------------------------------- next_tx -----------------------------------------------
def next_tx_imp(z_tx):
    """
    Given a ZefRef/EZefRef to a tx, move to the next tx.
    For a ZefRef with a reference frame, one cannot look 
    into the future, i.e. get to a TX that was not known 
    in that reference frame. Nil is returned in that case.
    
    For Both ZeFfRef/EZefRef, Nil is returned when called
    on the very latest TX.

    ---- Examples ----
    z2_tx = z_tx | next_tx | collect

    ---- Signature ----
    ZefRef -> Union[ZefRef, Nil]
    EZefRef -> Union[EZefRef, Nil]

    ---- Tags ----
    - used for: time traversal
    - related zefop: previous_tx
    - related zefop: time_travel
    """
    def next_tx_ezr(zz):
        try:
            return zz | Out[BT.NEXT_TX_EDGE] | collect
        except RuntimeError:
            return None

    if isinstance(z_tx, EZefRef):
        return next_tx_ezr(z_tx)
    if isinstance(z_tx, ZefRef):
        z_frame_ezr = (z_tx | frame | collect).tx
        z_moved = next_tx_ezr(pyzefops.to_ezefref(z_tx))
        ts = pyzefops.time_slice
        if z_moved is None or ts(z_moved)>ts(z_frame_ezr):
            return None 
        else:
            return ZefRef(z_moved, z_frame_ezr)

    raise TypeError(f'next_tx can only take ZefRef and EZefRef, got z_tx={z_tx} type(z_tx)={type(z_tx)}')


def next_tx_tp(z_tp):
    return None



# ---------------------------------------- previous_tx -----------------------------------------------
def previous_tx_imp(z_tx):
    """
    Given a ZefRef/EZefRef to a tx, move to the previous tx.
    If one tries to go back from the very first tx, nil is 
    returned.    
    
    ---- Examples ----
    z2_tx = z_tx | previous_tx | collect

    ---- Signature ----
    ZefRef -> Union[ZefRef, Nil]
    EZefRef -> Union[EZefRef, Nil]
    
    ---- Tags ----
    - used for: time traversal
    - related zefop: previous_tx
    - related zefop: time_travel
    """
    def previous_tx_ezr(zz):
        try:
            return zz | In[BT.NEXT_TX_EDGE] | collect
        except RuntimeError:
            return None

    if isinstance(z_tx, EZefRef):
        return previous_tx_ezr(z_tx)
    if isinstance(z_tx, ZefRef):
        z_frame_ezr = (z_tx | frame | collect).tx
        z_moved = previous_tx_ezr(pyzefops.to_ezefref(z_tx))
        ts = pyzefops.time_slice
        if z_moved is None or ts(z_moved)>ts(z_frame_ezr):
            return None 
        else:
            return ZefRef(z_moved, z_frame_ezr)

    raise TypeError(f'previous_tx can only take ZefRef and EZefRef, got z_tx={z_tx} type(z_tx)={type(z_tx)}')


def previous_tx_tp(z_tp):
    return None


# ----------------------------------------- preceding_events --------------------------------------------

def preceding_events_imp(x, filter_on=None):
    """ 
    Given a TX as a (E)ZefRef, return all events that occurred in that TX.
    Given a ZefRef to a RAE, return all the events that happend on the RAE.

    - filter_on allows for filtering on the type of the events; by default it is none which
    returns all events.

    ---- Examples ----
    >>> my_graph_slice | events                  => [instantiated[z1], terminated[z3], assigned[z8][41][42] ]
    >>> z_rae | events                          => [instantiated[z1], assigned[z8][41][42], terminated[z3]]
    >>> z_rae | events[Instantiated]            => [instantiated[z1]]
    >>> z_rae | events[Instantiated | Assigned] => [instantiated[z1], assigned[z8][41][42]]

    ---- Signature ----
    Union[ZefRef[TX], EZefRef[TX]]  ->  List[ZefOp[Union[Instantiated[ZefRef], Terminated[ZefRef], ValueAssigned[ZefRef, T]]]]
    """
    from zef.pyzef import zefops as pyzefops
    # Note: we can't use the python to_frame here as that calls into us.
    
    if isinstance(x, GraphSlice):
        return 'TODO!!!!!!!!!!!!!!!'

    if BT(x) == BT.TX_EVENT_NODE:
        raise TypeError(f"`preceding_events` can only be called on RAEs and GraphSlices and lists all relevant events form the past. It was called on a TX. You may be looking for the `events` operator, which lists all events that happened in a TX.")
        
    if BT(x) not in [
            BT.ENTITY_NODE,
            BT.RELATION_EDGE,
            BT.ATTRIBUTE_ENTITY_NODE,
            ]:
        raise TypeError(f"`preceding_events` can only be called on RAEs and GraphSlices and lists all relevant events form the past.")

    if internals.is_delegate(x):
        ezr = to_ezefref(x)
        to_del = ezr | in_rel[BT.TO_DELEGATE_EDGE] | collect
        insts = to_del | Ins[BT.DELEGATE_INSTANTIATION_EDGE]
        retirements = to_del | Ins[BT.DELEGATE_RETIREMENT_EDGE]
        if type(ezr) == ZefRef:
            gs = frame(ezr)
            insts = insts | filter[graph_slice_index | less_than_or_equal[graph_slice_index(gs)]]
            retirements = insts | filter[graph_slice_index | less_than_or_equal[graph_slice_index(gs)]]

        insts = insts | map[lambda tx: instantiated[pyzefops.to_frame(ezr, tx, True)]] | collect
        retirements = retirements | map[lambda tx: terminated[pyzefops.to_frame(ezr, tx, True)]] | collect
        full_list = insts+retirements
    else:
        zr = x

        from ..graph_events import instantiated, terminated, assigned

        def make_val_as_from_tx(tx):
            aet_at_frame = pyzefops.to_frame(zr, tx)
            try:
                prev_tx  = tx | previous_tx | collect                                   # Will fail if tx is already first TX
                prev_val = pyzefops.to_frame(zr, prev_tx) | value | collect     # Will fail if aet didn't exist at prev_tx
            except:
                prev_val = None
            return assigned[aet_at_frame][prev_val][value(aet_at_frame)]

        inst        =  [pyzefops.instantiation_tx(zr)]   | map[lambda tx: instantiated[pyzefops.to_frame(zr, tx) ]] | collect
        val_assigns =  pyzefops.value_assignment_txs(zr) | map[make_val_as_from_tx] | collect
        # TODO termination_tx returns even if zr is a zefref with a timeslice where it wasn't terminated yet
        termination =  [pyzefops.termination_tx(zr)]     | filter[lambda b: BT(b) != BT.ROOT_NODE] | map[lambda tx: terminated[pyzefops.to_frame(zr, tx, True)]] | collect
        full_list = inst + val_assigns + termination 

    if filter_on: return full_list | filter[lambda z: is_a_implementation(z, filter_on)] | collect
    return full_list




# ----------------------------------------- events --------------------------------------------

def events_imp(z_tx_or_rae, filter_on=None):
    """ 
    Given a TX as a (E)ZefRef, return all events that occurred in that TX.
    Given a ZefRef to a RAE, return all the events that happend on the RAE.

    - filter_on allows for filtering on the type of the events; by default it is none which
    returns all events.

    ---- Examples ----
    >>> z_tx  | events                          => [instantiated[z1], terminated[z3], assigned[z8][41][42] ]


    ---- Signature ----
    ZefRef[TX] | EZefRef[TX]  ->  List[ZefOp[Union[Instantiated[ZefRef], Terminated[ZefRef], ValueAssigned[ZefRef, Any, Any]]]]

    ---- Tags ----
    - related zefop: preceding_events
    - related zefop: instantiation_txs
    - related zefop: termination_txs
    - related zefop: assignment_txs

    """
    from zef.pyzef import zefops as pyzefops
    # TODO: can remove this once imports are sorted out
    from ..graph_events import instantiated, terminated, assigned
    # Note: we can't use the python to_frame here as that calls into us.
    

    if internals.is_delegate(z_tx_or_rae):
        ezr = to_ezefref(z_tx_or_rae)
        to_del = ezr | in_rel[BT.TO_DELEGATE_EDGE] | collect
        insts = to_del | Ins[BT.DELEGATE_INSTANTIATION_EDGE]
        retirements = to_del | Ins[BT.DELEGATE_RETIREMENT_EDGE]
        if type(ezr) == ZefRef:
            gs = frame(ezr)
            insts = insts | filter[graph_slice_index | less_than_or_equal[graph_slice_index(gs)]]
            retirements = insts | filter[graph_slice_index | less_than_or_equal[graph_slice_index(gs)]]

        insts = insts | map[lambda tx: instantiated[pyzefops.to_frame(ezr, tx, True)]] | collect
        retirements = retirements | map[lambda tx: terminated[pyzefops.to_frame(ezr, tx, True)]] | collect
        full_list = insts+retirements

    elif BT(z_tx_or_rae) == BT.TX_EVENT_NODE:
        zr = to_ezefref(z_tx_or_rae)
        gs = zr | to_graph_slice | collect

        def make_val_as_for_aet(aet):
            aet_at_frame = pyzefops.to_frame(aet, zr)
            try:
                prev_tx  = zr | previous_tx | collect                                  # Will fail if tx is already first TX
                prev_val = pyzefops.to_frame(aet, prev_tx) | value | collect   # Will fail if aet didn't exist at prev_tx
            except:
                prev_val = None
            return assigned[aet_at_frame][prev_val][value(aet_at_frame)]

        insts        = zr | pyzefops.instantiated   | map[lambda zz: instantiated[pyzefops.to_frame(zz, gs.tx) ]] | collect
        val_assigns  = zr | pyzefops.value_assigned | map[make_val_as_for_aet] | collect
        terminations = zr | pyzefops.terminated     | map[lambda zz: terminated[pyzefops.to_frame(zz, gs.tx, True) ]] | collect
        full_list = insts+val_assigns+terminations
    else:
        print(f"🥱🥱🥱🥱🥱🥱 Change: The `events` ZefOp can only be used on (E)ZefRef to TXs. To look at the past of any RAE from a given frame, use `preceding_events`. ")
        raise Exception()
        # TODO: retire this block
        zr = z_tx_or_rae

        def make_val_as_from_tx(tx):
            aet_at_frame = pyzefops.to_frame(zr, tx)
            try:
                prev_tx  = tx | previous_tx | collect                                   # Will fail if tx is already first TX
                prev_val = pyzefops.to_frame(zr, prev_tx) | value | collect     # Will fail if aet didn't exist at prev_tx
            except:
                prev_val = None
            return value_assigned[aet_at_frame][prev_val][value(aet_at_frame)]

        inst        =  [pyzefops.instantiation_tx(zr)]   | map[lambda tx: instantiated[pyzefops.to_frame(zr, tx) ]] | collect
        val_assigns =  pyzefops.value_assignment_txs(zr) | map[make_val_as_from_tx] | collect
        # TODO termination_tx returns even if zr is a zefref with a timeslice where it wasn't terminated yet
        termination =  [pyzefops.termination_tx(zr)]     | filter[lambda b: BT(b) != BT.ROOT_NODE] | map[lambda tx: terminated[pyzefops.to_frame(zr, tx, True)]] | collect
        full_list = inst + val_assigns + termination 

    if filter_on: return full_list | filter[lambda z: is_a_implementation(z, filter_on)] | collect
    return full_list


def events_tp(x):
    return VT.List[VT.ZefOp]


# ----------------------------------------- frame --------------------------------------------
def frame_imp(x):
    """ 
    Extract the reference frame (subject) of a ZefRef.
    Return as a GraphSlice.
    
    ---- Examples ----
    >>> z_tx_zr | frame        # returns the subject (not object pointed to!) / reference frame as a GraphSlice
    >>> z_et | frame           # returns the reference frame (GraphSlice)
    >>> z_aet | frame          # returns the reference frame (GraphSlice)
    >>> z_rt | frame           # returns the reference frame (GraphSlice)

    ---- Signature ----
    ZefRef -> GraphSlice
    """
    return GraphSlice(pyzefops.tx(x))


def frame_tp(x):
    return VT.GraphSlice


# ----------------------------------------- to_frame --------------------------------------------
def in_frame_imp(z, *args):
    """ 
    Represent a RAE in a specified reference frame.
    No changes are made to the graph and an error is returned if the operation is not possible,

     - optional arguments: in_frame[allow_tombstone]

    ---- Examples ----
    >>> z | in_frame[my_graph_slice]                    # z: ZefRef changes the reference frame, also if z itself points to a tx
    >>> z | in_frame[allow_tombstone][my_graph_slice]   # allow can opt in to represent RAEs that were terminated in a future state

    ---- Signature ----
    (ZefRef, GraphSlice)        -> Union[ZefRef, Error]
    (EZefRef[TRAE], GraphSlice) -> Union[ZefRef, Error]    
    """
    # if one additional arg is passed in, it must be a frame
    if len(args)==1:
        target_frame = args[0]
        tombstone_allowed = False
        assert isinstance(target_frame, GraphSlice)

    # if two additional args are passed in, one must be a frame and the other 'allow_tombstone'
    elif len(args)==2:
        if isinstance(args[0], GraphSlice):
            assert isinstance(args[1], ZefOp) and args[1] == allow_tombstone
            target_frame = args[0]
            tombstone_allowed = True
        elif isinstance(args[1], GraphSlice):
            assert isinstance(args[0], ZefOp) and args[0] == allow_tombstone
            target_frame = args[1]
            tombstone_allowed = True
        else:
            raise RuntimeError("'in_frame' can only be called with ...| in_frame[my_gs]  or ... | in_frame[allow_tombstone][my_gs] ")
    else:
        raise RuntimeError("'in_frame' can only be called with ...| in_frame[my_gs]  or ... | in_frame[allow_tombstone][my_gs] ")    
    if not (isinstance(z, ZefRef) or isinstance(z, EZefRef)):
        raise NotImplementedError(f"No in_frame yet for type {type(z)}")
    zz = to_ezefref(z)
    g_frame = Graph(target_frame)
    # z's origin lives in the frame graph
    is_same_g = g_frame == Graph(zz)    # will be reused
    if is_same_g or origin_uid(zz).graph_uid == uid(g_frame):
        z_obj = to_ezefref(z) if is_same_g else g_frame[origin_uid(zz)]
        # exit early if we are looking in a frame prior to the objects instantiation: this is not even allowed when allow_tombstone=True
        if z_obj | Not[aware_of[target_frame]] | collect:
            raise RuntimeError(f"Causality error: you cannot point to an object from a frame prior to its existence / the first time that frame learned about it.")            
        if not tombstone_allowed:
            if not exists_at(z_obj, target_frame):
                raise RuntimeError(f"The RAE was terminated and no longer exists in the frame that the reference frame was about to be moved to. It is still possible to perform this action if you really want to by specifying 'to_frame[allow_tombstone][my_z]'")
        return ZefRef(z_obj, target_frame.tx)

    # z's origin does not live in the frame graph
    else:
        the_origin_uid = origin_uid(zz)        
        if the_origin_uid not in g_frame:
            # this graph never knew about a RAE with this origin uid
            raise RuntimeError('origin node not found in reference frame graph!')

        zz = g_frame[the_origin_uid]
        if BT(zz) in {BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE}:
            z_candidates = zz | Ins[BT.ORIGIN_RAE_EDGE] | map[target] | filter[exists_at[target_frame]] | collect
            if len(z_candidates) > 1:
                raise RuntimeError(f"Error: More than one instance alive found for RAE with origin uid {the_origin_uid}")
            elif len(z_candidates) == 1:
                return z_candidates | single | to_frame[target_frame] | collect
            else:
                return None     # no instance alive at the moment
        
        raise NotImplementedError("We should not have landed here")



def in_frame_tp(op, curr_type):
    return VT.Any


# ----------------------------------------- discard_frame --------------------------------------------
def discard_frame_imp(x):
    """
    Given any kind of reference referring to a RAE,
    it returns the frame-independent representation.

    ---- Signature ----
    ZefRef[ET[T1]] -> Entity[T1]
    ZefRef[AET[T1]] -> AttributeEntity[T1]
    ZefRef[RT[T1]] -> Relation[T1]
    ZefRef[BT.VALUE_NODE] -> Val
    ZefRef[BT.TX] -> TXNode
    ZefRef[BT.Root] -> Graph      # TODO

    EZefRef[ET[T1]] -> Entity[T1]
    EZefRef[AET[T1]] -> AttributeEntity[T1]
    EZefRef[RT[T1]] -> Relation[T1]
    EZefRef[BT.VALUE_NODE] -> Val
    EZefRef[BT.TX] -> TXNode
    EZefRef[BT.Root] -> Graph     # TODO

    Entity -> Entity
    AttributeEntity -> AttributeEntity
    Relation -> Relation
    TXNode -> TXNode
    Root -> Root

    BlobPtr & Delegate -> DelegateRef
    DelegateRef -> DelegateRef


    ---- Tags ----
    
    """
    if isinstance(x, AtomRef):
        return x
    if isinstance(x, BlobPtr):
        if internals.is_delegate(x):
            return to_delegate(x)
        if BT(x) == BT.ENTITY_NODE:
            return EntityRef(x)
        elif BT(x) == BT.RELATION_EDGE:
            return RelationRef(x)
        elif BT(x) == BT.ATTRIBUTE_ENTITY_NODE:
            return AttributeEntityRef(x)
        elif BT(x) == BT.TX_EVENT_NODE:
            return TXNodeRef(x)
        elif BT(x) == BT.ROOT_NODE:
            return RootRef(x)
        elif BT(x) == BT.VALUE_NODE:
            return Val(value(x))
        raise Exception("Not a ZefRef that is a concrete RAE")
    if is_a_implementation(x, Delegate):
        return x
    raise TypeError(f"'discard_frame' not implemented for type {type(x)}: it was passed {x}")


# ----------------------------------------- to_graph_slice --------------------------------------------
def to_graph_slice_imp(*args):
    """
    Return the object (tx pointed to by a ZefRef[TX]) as a GraphSlice.
    Works for ZefRef[TX] and EZefRef[TX] as well as (Graph, Time) pairs in both orders.
    z_tx_zr | to_graph_slice           # ZefRef  -> GraphSlice,     discards reference frame, returns a GraphSlice
    z_tx_ezr | to_graph_slice          # EZefRef -> GraphSlice      returns a GraphSlice

    ---- Examples ----
    >>> t1: Time = now()
    >>> g | to_graph_slice[t1]
    >>> t1 | to_graph_slice[g]
    """
    return GraphSlice(*args)


def to_graph_slice_tp(x):
    return VT.GraphSlice



# ----------------------------------------- to_tx --------------------------------------------
def to_tx_imp(x, t=None):
    """ 
    Given a GraphSlice (reference frame), return a ZefRef[TX] to the 
    transaction that led to this state.
    The ZefRef has the same transaction as object and subject (ref frame).
    
    ---- Examples ----
    >>> my_graph_slice | to_tx     # => ZefRef[TX]

    ---- Signature ----
    GraphSlice    -> ZefRef[TX]
    (Graph, Time) -> ZefRef[TX]
    """
    if isinstance(x, GraphSlice):
        zz = x.tx
        assert t is None
        return ZefRef(zz, zz)
    elif isinstance(x, Graph):
        c = collect
        # inefficient implementation for now
        txs = x | all[TX] | c
        if (txs | first | time | greater_than[t] | c) or t > now():
            return None
        if t < now() and t >= (txs | last | time  | c):
            return txs | last | c
        return txs | take_until[time | greater_than_or_equal[t]] | last | c
    raise TypeError(f'"x | to_tx" can only be called for x being a GraphSlice or a Graph. x={x}')


def to_tx_tp(x):
    return VT.ZefRef        # TODO: can we specify VT.ZefRef[TX]?







# ------------------------------------------ time_travel ---------------------------------------------

def time_travel_imp(x, *args):
    """
    Move the reference frame in time only.
    The temporal movement can absolute: p is a Time
    or relative: p is a Int (move this number of graph slices) or a duration
    i.e. the eternal graph of the reference frame
    remains contant.    

    ---- Examples ----
    >>> #       ---- relative time travel ----
    >>> zr | time_travel[-3]                    # how many time slices to move
    ... zr | time_travel[-3.5*units.seconds]  
    ... my_graph_slice | time_travel[-3]
     
    >>> #       ---- time travel to fixed time ----
    >>> t1 = Time('October 20 2020 14:00 (+0100)')
    ... g | time_travel[t1]
    ... gs | time_travel[t1]
    ... ezr | time_travel[t1]
    ... zr | time_travel[t1]
    ... ezr | time_travel[allow_tombstone][t1]
    ... zr | time_travel[allow_tombstone][-3]
    ... zr | time_travel[allow_tombstone][-3.5*units.seconds]
    ... zr | time_travel[allow_tombstone][t1]

    ---- Signature ----
    (ZefRef, Int)                           -> Union[ZefRef, Nil]           # nil or error? What is the process / criterion?
    (ZefRef, Duration)                      -> Union[ZefRef, Nil]
    (GraphSlice, Int)                       -> Union[GraphSlice, Nil]
    (GraphSlice, Duration)                  -> Union[GraphSlice, Nil]

    (ZefRef, Time)                          -> Union[ZefRef, Nil]
    (EZefRef, Time)                         -> Union[ZefRef, Nil]
    (Graph, Time)                           -> Union[GraphSlice, Nil]
    (GraphSlice, Time)                      -> Union[GraphSlice, Nil]

    ---- Tags ----
    - used for: time traversal
    - related zefop: time_slice    
    - related zefop: next_tx
    - related zefop: previous_tx
    - related zefop: time
    """
    c = collect 

    def is_duration(xx) -> bool:
        return (
            isinstance(xx, QuantityFloat) or isinstance(xx, QuantityInt)
            ) and xx.unit == EN.Unit.seconds

    if len(args)==0:
        p = None
        tombstone_allowed = False
    elif len(args)==1:
        tombstone_allowed = False
        p = args[0]
    elif len(args)==2:
        if args[0] == allow_tombstone:
            tombstone_allowed = True
            p = args[1]
        elif args[1] == allow_tombstone:
            tombstone_allowed = True
            p = args[0]
        else:
            raise RuntimeError(f"If two args are curried into 'time_travel', one of them MUST be 'ops.allow_tombstone'. Given: args={args}")
    else:
        raise RuntimeError(f"At most two args may be curried into time_travel. Given: args={args}")

    try:
        if isinstance(x, ZefRef):
            if isinstance(p, Int):
                return (x | pyzefops.time_travel[allow_tombstone][p]) if tombstone_allowed else (x | pyzefops.time_travel[p])
            elif isinstance(p, Time):
                new_frame = Graph(x) | to_tx[p] | to_graph_slice | c
                if new_frame is None:
                    raise RuntimeError(f"could not determine suitable reference frame / graph slice in x | time_travel[p] for x={x}  p={p}")
                return (x | in_frame[allow_tombstone][new_frame] | c) if tombstone_allowed else (x | in_frame[new_frame] | c)
            elif is_duration(p):
                t = (x | frame | time | c) + p
                new_frame = Graph(x) | to_tx[t] | to_graph_slice | c
                if new_frame is None:
                    raise RuntimeError(f"could not determine suitable reference frame / graph slice in x | time_travel[p] for x={x}  p={p}")
                return (x | in_frame[allow_tombstone][new_frame] | c) if tombstone_allowed else (x | in_frame[new_frame] | c)

        if isinstance(x, GraphSlice):
            if isinstance(p, Int):
                tx_zr = ZefRef(Graph(x.tx)[42], x.tx)       # hacky: we want some ZefRef that we can time travel with. Use root for now
                # some gymnastics using the old ops to get to the frame that we want
                return GraphSlice(tx_zr | pyzefops.time_travel[p] | pyzefops.tx | pyzefops.to_ezefref)
            elif isinstance(p, Time):
                return Graph(x.tx) | to_tx[p] | to_graph_slice | c
            elif is_duration(p):
                t = (x.tx | time | c) + p                
                return Graph(x.tx) | to_tx[t] | to_graph_slice | c                
        
        elif isinstance(x, EZefRef):        
            if isinstance(p, Time):
                new_frame = Graph(x) | to_tx[p] | to_graph_slice | c
                if new_frame is None:
                    raise RuntimeError(f"could not determine suitable reference frame / graph slice in x | time_travel[p] for x={x}  p={p}")
                return (x | in_frame[allow_tombstone][new_frame] | c) if tombstone_allowed else (x | in_frame[new_frame] | c)
        
        elif isinstance(x, Graph):        
            if isinstance(p, Time):
                return x | to_tx[p] | to_graph_slice | c
    
    except Exception as e:
        raise RuntimeError(f'Error applying time_travel operator for x={x}  and  p={p} - e={e}')
    raise TypeError(f"x | time_travel[p] was called for x={x}  p={p}")



def time_travel_tp(x, p):
    return VT.Any


# ------------------------------------------ origin_uid ---------------------------------------------



def origin_uid_imp(z) -> EternalUID:
    """used in constructing GraphDelta, could be useful elsewhere"""
    if isinstance(z, AtomRef):
        return uid(z)
    assert BT(z) in {BT.ENTITY_NODE, BT.ATTRIBUTE_ENTITY_NODE, BT.RELATION_EDGE, BT.TX_EVENT_NODE, BT.ROOT_NODE}
    if internals.is_delegate(z):
        return uid(to_ezefref(z))
    if BT(z) in {BT.TX_EVENT_NODE, BT.ROOT_NODE}:
        return uid(to_ezefref(z))
    origin_candidates = z | to_ezefref | in_rel[BT.RAE_INSTANCE_EDGE] | Outs[BT.ORIGIN_RAE_EDGE] | collect    
    if len(origin_candidates) == 0:
        # z itself is the origin
        return uid(to_ezefref(z))
    z_or = origin_candidates | only | collect
    if BT(z_or) in {BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE}:
        # the origin was from a different graph
        g_origin_uid = z_or | Out[BT.ORIGIN_GRAPH_EDGE] | base_uid | collect
        return EternalUID(z_or | base_uid | collect, g_origin_uid)
    else:
        # z itself is not the origin, but the origin came from this graph.
        # The origin must have been terminated at some point and z is of the
        # same lineage
        return z_or | target | to_ezefref | uid | collect
    

def origin_uid_tp(x):
    return VT.String
    
# ---------------------------------------------------------------------------------------









def fill_or_attach_implementation(z, rt, val):
    return LazyValue(z) | fill_or_attach[rt][val]

def fill_or_attach_type_info(op, curr_type):
    return curr_type

def set_field_implementation(z, rt, val, incoming=False):
    return LazyValue(z) | set_field[rt][val][incoming]

def set_field_type_info(op, curr_type):
    return curr_type

def assert_implementation(z, predicate=None, message=None):
    if predicate is None:
        success = z
    else:
        success = predicate(z)
    if not success:
        if message is None:
            if predicate is None:
                message = "not True"
            else:
                message = "{z} failed check {predicate}"
        elif isinstance(message, String):
            pass
        else:
            try:
                message = message(z)
            except:
                message = "exception when generating this message"
        raise Exception(f"Assertion failed: {message}")

    return z

def assert_type_info(op, curr_type):
    return curr_type
    
def run_effect_implementation(eff):
    from ..fx import _effect_handlers
    
    # if we create the effect with short hand notation, e.g. (ET.Dog, ET.Person | g | run)
    # we want to directly unpack the instances in the same structure as the types that went in
    # To decouple the layers, we need to return based on whether 'unpacking_template' is present
    # as a key in the receipt

    
    if not isinstance(eff, Dict):
        raise TypeError(f"run(x) called with invalid type for x: {type(eff)}. You can only run a wish, which are represented as dictionaries.")
    handler = _effect_handlers[eff['type'].d]
    return handler(eff)



def hasout_implementation(zr, rt):
    return curry_args_in_zefop(pyzefops.has_out, zr, (internals.get_c_token(rt),))

def hasin_implementation(zr, rt):
    return curry_args_in_zefop(pyzefops.has_in, zr, (internals.get_c_token(rt),))



# -------------------------------- apply_functions -------------------------------------------------
def apply_functions_imp(x, fns):
    """ 
    Given a list of values and an associated list of functions,
    apply the nth function to the nth value list element.

    It is strongly recommended not to use impure functions inside
    this operator.

    ---- Examples ----
    >>> ['Hello', 'World'] | apply_functions[to_upper_case, to_lower_case]    # => ['HELLO', 'world']
    >>> (1,2) | apply_functions[add[1], add[10]]      # => (2, 12)

    ---- Signature ----
    (Tuple[T1, ...,TN], Tuple[T1->TT1, ..., TN->TTN] ) -> Tuple[TT1, ...,TTN]

    ---- Tags ----
    - used for: control flow
    - operates on: ZefOp
    - operates on: Function
    - related zefop: reverse_args
    - related zefop: map
    - related zefop: apply
    """
    import builtins
    from typing import Generator
    if not (isinstance(fns, list) or isinstance(fns, tuple)):
        raise TypeError(f"apply_functions must be given a tuple of functions.")
    if not (isinstance(x, list) or isinstance(x, tuple) or isinstance(x, (Generator, ZefGenerator))):
        raise TypeError(f"apply_functions must be given a tuple of input args.")
    xx = tuple(x)
    if not len(fns) == len(xx):
        raise ValueError(f"len(fs) must be equal to len(x)")
        
    return tuple((f(el) for f, el in builtins.zip(fns, x)))




# -------------------------------- map -------------------------------------------------
def map_implementation(v, f):
    """
    Apply a pure function elementwise to each item of a collection.
    It can be used both for iterables and streams (observables).
    
    Note: It is strongly recommended not to use impure functions 
    (with side effects, i.e. that change any state of the program)
    inside "map". You may be looking for "for_each" or "tap" in that 
    case.    

    ---- Examples -----
    >>> [3, 4, 5] | map[add[1]]                            # => [4, 5, 6]
    >>> [1, 2, 3] | map[str, add[100]]                     # => [('1', 101), ('2', 102), ('3', 103)]
    >>> {'a': 1, 'b': 2} | map[lambda k, v: (k+'!', v+1)]  # => {'a!': 2, 'b!': 3}

    ---- Signature ----
    (List[T1], (T1 -> T2))  -> List[T2]

    ---- Tags ----
    - used for: control flow
    - used for: function application
    - related zefop: apply_functions
    - related zefop: apply
    """
    import builtins
    input_type = parse_input_type(type_spec(v))

    if is_a(v, Dict):
        return dict( (f(k,v) for k,v in v.items() ) )

    if input_type == "awaitable":
        observable_chain = v
        # Ugly hack for ZefOp
        f._allow_bool = True
        return observable_chain.pipe(rxops.map(f))
    
    if type(f) in (list, tuple):
        n = len(f)
        def wrapper_list():
            for w in v:
                yield tuple(ff(w) for ff in f)
        return ZefGenerator(wrapper_list)

    def wrapper():
        for el in v:
            # yield apply(el, f)
            # yield call_wrap_errors_as_unexpected(f, el)
            try:
                yield call_wrap_errors_as_unexpected(f, el)
            except Error_ as exc:
                exc = add_error_context(exc, {"metadata": {"last_input": el}})
                raise exc from None

    return ZefGenerator(wrapper)


# -------------------------------- reduce -------------------------------------------------
 

def reduce_implementation(iterable, fct, init):
    import functools
    return functools.reduce(fct, iterable, init)



# -------------------------------- group_by -------------------------------------------------
def group_by_implementation(iterable, key_fct, categories=None):
    """
    categories is optional and specifies additional keys/categories 
    to create, even if there are no occurrences. The categories can
    be implicitly inferred from the data or explicitly specified 
    (useful for dispatch and allowing for empty catergories).

    ---- Examples ----
    >>> range(10) | group_by[modulo[3]]   # => [(0, [0, 3, 6, 9]), (1, [1, 4, 7]), (2, [2, 5, 8])]
    
    ---- Tags ----
    - used for: control flow
    - operates on: List
    - operates on: Stream
    - related zefop: group
    """        
    # map to list first in case the generator state changes
    from collections import defaultdict
    my_list = list(iterable)
    keys = [key_fct(x) for x in my_list]
    if categories is not None:
        cat_set = set(categories)
        for key in keys:
            if key not in cat_set: raise KeyError(f"Error in zef.ops.group_by: a predefined set of categories was specified, but a key not contained in this key was found: key={key}   categories={categories}")
        
    d = defaultdict(list)    
    for k, v in zip(keys, my_list):
        d[k].append(v)
    return [(k, d[k]) for k in (d.keys() if categories is None else categories)]    



# -------------------------------- group -------------------------------------------------
def group_imp(v, f=lambda x: x):
    """
    Examine successive elements in a List and return 
    a lazy list of lists: the given function f maps 
    the elements within each sublist map onto the 
    same value, i.e. the splitting occurs where mapped
    elements differ.

    ---- Examples ----
    >>> ['a','a','a','b','b','c','a', 'a'] | group      # => [('a', 'a', 'a'), ('b', 'b'), ('c',), ('a', 'a')]
    >>> [2,4,6,1,3,2,5,7,9,10] | group[lambda x: x%2]   # => [(2, 4, 6), (1, 3), (2,), (5, 7, 9), (10,)]

    ---- Signature ----
    (List[T], (T)->T2) -> List[List[T]]
    
    ---- Tags ----
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    - related zefop: group_by
    - related zefop: chunk
    """
    def wrapper():        
        it = iter(v)
        current_list = []
        try:
            last_val = next(it)
            last_f_val = f(last_val)
            while True:
                current_list = [last_val]
                while True:                    
                    val = next(it)
                    f_val = f(val)
                    if f_val != last_f_val:
                        yield tuple(current_list)
                        last_val = val
                        last_f_val = f_val
                        break
                    current_list.append(val)
                    last_val = val
                    last_f_val = f_val
        except StopIteration:
            if current_list != []:
                yield tuple(current_list)
            return

    return ZefGenerator(wrapper)






# -------------------------------- identity -------------------------------------------------

def identity_implementation(x):
    """
    The identity function: always returns the input argument

    ---- Examples ----
    >>> 42 | identity     # => 42

    ---- Tags ----
    operates on: Any
    used for: function composition
    """
    return x


# -------------------------------- length -------------------------------------------------
def length_implementation(iterable):
    """
    Zef's version of the length function.
    Similar to Python's `len` and sometimes
    called `count` in other languages.

    ---- Examples ----
    >>> ['a', 'b', 'c'] | length     # => 3

    ---- Signature ----
    (List[T]) -> Int

    ---- Tags ----
    related zefop: count
    operates on: List
    operates on: Stream
    """
    if hasattr(iterable, "__len__"):
        return len(iterable)
    else:
        # benchark: this is faster than iterating with a counter
        return len(list(iterable))



# -------------------------------- nth -------------------------------------------------
def nth_implementation(iterable, n):
    """
    Returns the nth element of an iterable or a stream.
    Using negative 'n' takes from the back with 'nth[-1]' 
    being the last element. An Error value is returned if 
    the index is out of bounds.
    This is sometimes called 'get` in other languages, but
    we want to distinguish it from key lookup.
    Note: this uses a zero-indexed convention.

    ---- Examples ----
    >>>['a', 'b', 'c', 'd'] | nth[1]       # => 'b'
    >>>['a', 'b', 'c', 'd'] | nth[-2]      # => 'c'
    >>>['a', 'b', 'c', 'd'] | nth[10]      # => Error['nth: index out of range using']
    
    ---- Arguments ----
    iterable: a List[T] / LazyValue[List[T]] / Awaitable[List[T]]
    n (Int): a non-negative integer specifying the index of the element to return.

    ----- Signature ----
    (List[T], Int) -> Union[T, Error]
    (LazyValue[List[T]], Int) -> LazyValue[Union[T, Error]]
    (Awaitable[List[T]], Int) -> Awaitable[Union[T, Error]]    
    """
    if isinstance(iterable, Dict): 
        raise TypeError(f"`nth` was called on a dict, but is only supported for Lists")

    if isinstance(iterable, ZefRef) and is_a[ET.ZEF_List](iterable):
        return iterable | all | nth[n] | collect
        
    if isinstance(iterable, list) or isinstance(iterable, tuple) or isinstance(iterable, String):
        return iterable[n]
    
    # it must be a generator or zef generator
    if n<0: 
        return tuple(iterable)[n]    # if we're taking from the back, it must be non-infinite in length
    
    it = iter(iterable)
    for cc in range(n):
        next(it)
    return next(it)    




#---------------------------------------- select_keys -----------------------------------------------
def select_keys_imp(d: dict, *keys: list):
    """
    Given a dictionary, return a new dictionary containing 
    the same key value pairs, but for a specified subset of
    keys only.

    Based on Clojure's select-keys 
    https://clojuredocs.org/clojure.core/select-keys

    ---- Examples ----
    >>> {'a': 3, 'b': 2, 'c': 1} | select_keys['a']['f']      # => {'a': 3}

    ----- Signature ----
    (Dict[T1, T2], T1, ...) -> Dict[T1, T2]
    
    ---- Tags ----
    - operates on: Dict
    - related zefop: filter
    - related zefop: get
    - related zefop: get_in
    - related zefop: merge
    """
    return {k: v for k, v in d.items() if k in keys}




#---------------------------------------- modulo -----------------------------------------------
def modulo_imp(m: int, n: int):
    """
    The modulo function.

    ---- Examples ----
    >>> 12 | mod[10]       # => 2

    ----- Signature ----
    (Int, Int) -> Int
    
    ---- Tags ----
    - operates on: Int
    - topic: Maths
    """
    return m % n

    


#---------------------------------------- filter -----------------------------------------------
def filter_implementation(itr, pred_or_vt):
    """
    Filters an iterable or stream lazily.

    ---- Examples ----
    >>> [1,2,3,4,5] | filter[lambda x: x%2 == 0]       # => [2, 4]
    >>> [1,2,3,4,5] | filter[greater_than[2]]          # => [3, 4, 5]
    
    ---- Arguments ----
    itr: a List[T] / LazyValue[List[T]] / Awaitable[List[T]]
    pred (Func[(T,), Bool]): a predicate function

    ----- Signature ----
    (List[T], (T->Bool)) -> List[T]

    ---- Tags ----
    - used for: control flow
    - operates on: List
    """
    pred = make_predicate(pred_or_vt)
    input_type = parse_input_type(type_spec(itr))
    if input_type == "tools":
        # As this is an intermediate, we return an explicit generator
        # return (x for x in builtins.filter(pred, itr))
        def wrapper():
            return (x for x in builtins.filter(pred, itr))
        return ZefGenerator(wrapper)
            
    elif input_type == "zef":
        return pyzefops.filter(itr, pred)
    elif input_type == "awaitable":
        observable_chain = itr
        return observable_chain.pipe(rxops.filter(pred))
    else:
        raise Exception(f"Filter can't work on input_type={input_type}")




#---------------------------------------- select_by_field -----------------------------------------------
def select_by_field_imp(zrs : Iterable[ZefRef], rt: RT, val):
    """An optimized equivalent of calling:
    zrs | filter[Z >> O[rt] | value_or[None] | equals[val]]
    although the case of val=None is not permitted.
    
    This is implemented in C++. In the future when the native version is as
    fast, this will deprecated.

    ----- Signature ----
    (List[ZefRef], RelationType, Any) -> List[ZefRef]

    ---- Tags ----
    - operates on: Graph
    - related zefop: filter
    - related zefop: value    
    """
    return pyzefops.select_by_field_impl(zrs, internals.get_c_token(rt), val)

def select_by_field_tp(v_tp):
    return VT.Any






def sort_implementation(v, key_fct=None, reverse=False):
    """
    An optional key function for sorting may be provided, e.g.
    list_of_strs | sort[len]

    ---- Signature ----
    List[T] -> List[T]
    (List[T], (T->T2)) -> List[T]       # T2 orderable
    """
    # if isinstance(v, ZefRefs) or isinstance(v, EZefRefs):
    #     if reverse:
    #         raise Exception("Can't reverse sort ZefRefs")
    #     return pyzefops.sort(v)
    # else:
    #     return sorted(v, key=key_fct, reverse=reverse)
    return sorted(v, key=key_fct, reverse=reverse)



def to_delegate_implementation(first_arg, *curried_args):
    """
    Convert the Graph delegate representation to the Delegate representation,
    which is not specific to any graph, or vice versa.
    
    Converting from a Graph EZefRef to a Delegate requires no additional curried
    arguments.

    Converting from a Delegate to a Graph EZefRef requires one argument of a
    Graph and can take an additional argument specifying whether the EZefRef
    should be created if it doesn't already exist. If this is not given, and the
    delegate doesn't exist, then this function returns None.

    ---- Examples ----
    >>> z_entity_del | to_delegate      # A Delegate(...) of the entity
    >>> delegate | to_delegate[g][True] # A EZefRef of the delegate
    >>> delegate | to_delegate          # => delegate

    ---- Signature ----
    Delegate -> Delegate
    (Delegate, Graph) -> EZefRef | Nil
    (Delegate, Graph, True) -> EZefRef
    EZefRef[Delegate] -> Delegate

    ---- Tags ----
    - related zefop: delegate_of

    """

    from ..delegates import to_delegate_imp
    return to_delegate_imp(first_arg, *curried_args)

def to_delegate_type_info(op, curr_type):
    return None


def delegate_of_implementation(x, arg1=None, arg2=None):
    """
    With no additional arguments, takes the input and produces an abstract
    Delegate from it. The input could be a ZefRef on a graph, an
    ET/RT/AET/RelationTriple type, or another delegate. The output will always
    be one order of delegate higher than the input.
    
    With one additional argument, which must be a Graph or GraphSlice, this will
    lookup the ZefRef of the equivalent delegate on the graph, if it exists. A
    second additional argument, set to True, forces the creation of the ZefRef
    on the graph.
    
    This and `to_delegate` can be used interchangably in many ways.

    ---- Examples ----
    >>> z | delegate_of
    >>> z | delegate_of | to_delegate == z | to_delegate | delegate_of
    >>> ET.Machine | delegate_of == DelegateRef(ET.Machine) | delegate_of
    >>> (ET.A, RT.B, ET.C) | delegate_of | source == ET.A | delegate_of
    >>> z | delegate_of | all    # Get all instances that are the same type as z
    
    >>> ET.Machine | delegate_of[g] | now | all == g | now | all[ET.Machine]
    >>> ET.Machine | delegate_of[g][True]   # Creates the delegate ZefRef on the graph g
    
    >>> z = ET.Machine | g | run
    ... dz = z | delegate_of | collect
    ... a,b,c = (dz, RT.Meta, "metadata") | g | run
    ... b | delegate_of | to_ezefref == (delegate_of(ET.Machine), RT.Meta, VRT.String) | delegate_of[g]

    ---- Signature ----
    Delegate -> Delegate
    AnyRAEType -> Delegate
    ZefRef -> ZefRef | Nil
    EZefRef -> EZefRef | Nil
    (Delegate | AnyRAEType, Graph) -> EZefRef | Nil
    (Delegate | AnyRAEType, Graph, Bool) -> EZefRef

    where AnyRAEType = ET | AET | RT | RelationTriple

    ---- Tags ----
    - related zefop: to_delegate
    """
    from ..delegates import delegate_of_imp
    return delegate_of_imp(x, arg1, arg2)

def delegate_of_type_info(op, curr_type):
    return None




#---------------------------------------- Out -----------------------------------------------
def Out_imp(z, rt=VT.Any, target_filter= None):
    """
    Traverse along a unique outgoing relation to the
    thing attached to the target of that relation.
    If there is no or multiple outgoing relations, 
    it will return an Error.
    This function can also be used by specifying logical
    subtypes. The edge type `rt` to traverse on can 
    be seen as the type to filter the instances of
    outgoing edges on, i.e. as special case of pattern 
    matching.

    The default value for `rt` is VT.Any, i.e. no filtering
    on the relation type is performed and it is assumed
    that a single outgoing relation of any type exists.

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friend = z1 | Out[RT.FriendOf]
    >>> z1s_friend = z1 | Out     

    ---- Signature ----
    ZefRef -> ZefRef | Error
    EZefRef -> EZefRef | Error

    ---- Tags ----
    - used for: graph traversal
    - operates on: ZefRef, EZefRef
    - related zefop: In
    - related zefop: Ins
    - related zefop: Outs
    - related zefop: ins_and_outs
    - related zefop: target
    - related zefop: source
    """
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "outout", "single")
    # if isinstance(rt, RelationType):
    #     try:
    #         return traverse_out_node(z, rt)
    #     except RuntimeError as exc:
    #         # create a summary of what went wrong
    #         existing_rels = (outs(z)
    #             | map[RT] 
    #             | frequencies       # which relation types are present how often?
    #             | items 
    #             | map[lambda p: f"{repr(p[0])}: {repr(p[1])}"] 
    #             | join['\n'] 
    #             | collect
    #             )
    #         return Error(f"traversing {z} via relation type {rt}. The existing outgoing edges here are: {existing_rels}")    
    # elif rt==VT.Any or rt==RT:
    #     my_outs = outs(z)
    #     if len(my_outs)==1:
    #         return my_outs[0]
    #     else:
    #         return Error(f"traversing {z} using 'out': there were {len(my_outs)} outgoing edges: {my_outs}")
    # else:
    #     return Error(f'Invalid type "{rt}" specified in Out[...]')
    # res = target(out_rel_imp(z, rt))
    # if target_filter and not is_a_implementation(res, target_filter):
    return target_implementation(out_rel_imp(z, rt, target_filter))




#---------------------------------------- Outs -----------------------------------------------
def Outs_imp(z, rt=None, target_filter = None):
    """
    Traverse along all outgoing relations of the specified 
    type to the thing attached to the target of each relation.

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friend = z1 | Outs[RT.FriendOf]

    ---- Signature ----
    ZefRef -> List[ZefRef]
    EZefRef -> List[EZefRef]
    
    ---- Tags ----
    - used for: graph traversal
    - operates on: ZefRef, EZefRef
    - related zefop: Ins
    - related zefop: Out
    - related zefop: In
    - related zefop: ins_and_outs
    """
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "outout", "multi")

    return out_rels_imp(z, rt, target_filter) | map[target] | collect


#---------------------------------------- In -----------------------------------------------
def In_imp(z, rt=None, source_filter = None):
    """
    Traverse along a unique Incoming relation to the
    thing attached to the source of that relation.
    If there is no or multiple incoming relations, 
    it will return an Error.

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friend = z1 | In[RT.FriendOf]

    ---- Signature ----
    ZefRef -> ZefRef | Error
    EZefRef -> EZefRef | Error

    ---- Tags ----
    - used for: graph traversal
    - operates on: ZefRef, EZefRef
    - related zefop: Out
    - related zefop: Ins
    - related zefop: Outs
    - related zefop: ins_and_outs
    - related zefop: target
    - related zefop: source
    """
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "inin", "single")

    return source_implementation(in_rel_imp(z, rt, source_filter))


#---------------------------------------- Ins -----------------------------------------------
def Ins_imp(z, rt=None, source_filter = None):
    """
    Traverse along all incoming relations of the specified 
    type to the thing attached to the source of each relation.

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friend = z1 | Ins[RT.FriendOf]

    ---- Signature ----
    ZefRef -> List[ZefRef]
    EZefRef -> List[EZefRef]
    
    ---- Tags ----
    - used for: graph traversal
    - operates on: ZefRef, EZefRef
    - related zefop: Outs
    - related zefop: In
    - related zefop: Out
    - related zefop: ins_and_outs
    """
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "inin", "multi")

    return in_rels_imp(z, rt, source_filter) | map[source] | collect

#---------------------------------------- isn_and_outs -----------------------------------------------
def ins_and_outs_imp(z, rel_type=RT):
    """
    Traverse along all incoming AND outgoing relations of 
    the specified type to the thing attached to the source 
    of each relation.

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friend = z1 | ins_and_outs[RT.FriendOf]

    ---- Signature ----
    ZefRef -> List[ZefRef]
    EZefRef -> List[EZefRef]
    
    ---- Tags ----
    - used for: graph traversal
    - operates on: ZefRef, EZefRef
    - related zefop: Ins, Outs
    """

    return [*Outs(z, rel_type), *Ins(z, rel_type)]


#---------------------------------------- out_rel -----------------------------------------------
def out_rel_imp(z, rt=None, target_filter = None):
    """
    Traverse onto a unique outgoing relation of the specified 
    type and return the relation (*NOT* the target).
    In case of no or multiple outgoing relations of 
    the specified type, it will return an Error.

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friend = z1 | out_rel[RT.FriendOf]

    ---- Signature ----
    ZefRef -> ZefRef | Error
    EZefRef -> EZefRef | Error

    ---- Tags ----
    - used for: graph traversal
    - operates on: ZefRef, EZefRef
    - related zefop: out_rels
    - related zefop: in_rel
    - related zefop: Out
    - related zefop: Outs
    """
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "out", "single")


    opts = out_rels_imp(z, rt, target_filter)
    if len(opts) != 1:
        # We use a function here for ease of control flow
        def help_hint():
            hint = ""
            if len(opts) > 0:
                hint += f"out_rel found too many relations that satisfy RT⊆{rt!r}"
                if target_filter is not None:
                    hint += f" and target={target_filter!r}"
                return hint 
            else:
                hint += f"out_rel did not find any relations that satisfy RT⊆{rt!r}"
                if target_filter is not None:
                    hint += f" and target={target_filter!r}"
                    num = len(out_rels_imp(z, rt, None))
                    if num > 0:
                        hint += f"\nThere are {num} relations of kind {rt!r}, maybe you did not mean to include the target filter?"
                        return hint 

                if len(in_rels_imp(z, rt, target_filter)) > 0:
                    hint += f"\nThere are incoming relations of this kind, maybe you meant to write in_rel or In instead."
                    return hint

                return hint

        raise Exception(help_hint())
    return single(opts)


#---------------------------------------- out_rels -----------------------------------------------
def out_rels_imp(z, rt_or_bt=None, target_filter=None):
    """
    Traverse onto all outgoing relations of the specified 
    type and return the relations (it does NOT proceed 
    to the targets).

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friends = z1 | out_rels[RT.FriendOf]

    ---- Signature ----
    ZefRef -> ZefRefs
    EZefRef -> EZefRefs

    ---- Tags ----
    - used for: graph traversal
    - operates on: ZefRef, EZefRef
    - related zefop: in_rels
    - related zefop: out_rel
    - related zefop: Out
    - related zefop: Outs
    """
    assert is_a(z, ZefRef) or is_a(z, EZefRef) or is_a(z, FlatRef)
    if is_a(z, FlatRef): return traverse_flatref_imp(z, rt_or_bt, "out", "multi")

    if rt_or_bt == RT or rt_or_bt is None: res = pyzefops.outs(z) | filter[is_a[BT.RELATION_EDGE]] | collect
    elif rt_or_bt == BT: res =  pyzefops.outs(z | to_ezefref | collect)
    else:
        if isinstance(rt_or_bt, RT) and isinstance(internals.get_c_token(rt_or_bt), RelationTypeToken):
            res = pyzefops.traverse_out_edge_multi(z, internals.get_c_token(rt_or_bt))
        elif isinstance(rt_or_bt, BT) and isinstance(internals.get_c_token(rt_or_bt), BlobTypeToken):
            res = pyzefops.traverse_out_edge_multi(z, internals.get_c_token(rt_or_bt))
        else:
            raise Exception("TODO: Need to implement non-specific relation types for out_rels")
    if target_filter: 
        if isinstance(target_filter, ZefOp): target_filter = Is[target_filter]
        return res | filter[target | is_a[target_filter]] | collect 
    return res



#---------------------------------------- in_rel -----------------------------------------------
def in_rel_imp(z, rt=None, source_filter = None):
    """
    Traverse onto a unique incoming relation of the specified 
    type and return the relation (it does NOT proceed to the source).
    In case of no or multiple incoming relations of 
    the specified type, it will return an Error.

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friend = z1 | in_rel[RT.FriendOf]

    ---- Signature ----
    ZefRef -> ZefRef | Error
    EZefRef -> EZefRef | Error

    ---- Tags ----
    - used for: graph traversal
    - operates on: ZefRef, EZefRef
    - related zefop: in_rels
    - related zefop: out_rel
    - related zefop: In
    - related zefop: Ins
    """
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "in", "single")


    opts = in_rels_imp(z, rt, source_filter)
    if len(opts) != 1:
        # We use a function here for ease of control flow
        def help_hint():
            hint = ""
            if len(opts) > 0:
                hint += f"in_rel found too many relations that satisfy RT⊆{rt!r}"
                if source_filter is not None:
                    hint += f" and source={source_filter!r}"
                return hint 
            else:
                hint += f"in_rel did not find any relations that satisfy RT⊆{rt!r}"
                if source_filter is not None:
                    hint += f" and source={source_filter!r}"
                    num = len(in_rels_imp(z, rt, None))
                    if num > 0:
                        hint += f"\nThere are {num} relations of kind {rt!r}, maybe you did not mean to include the source filter?"
                        return hint 

                if len(out_rels_imp(z, rt, source_filter)) > 0:
                    hint += f"\nThere are outgoing relations of this kind, maybe you meant to write out_rel or Out instead."
                    return hint

                return hint

        raise Exception(help_hint())
    return single(opts)


#---------------------------------------- in_rels -----------------------------------------------
def in_rels_imp(z, rt_or_bt=None, source_filter=None):
    """
    Traverse onto all incoming relations of the specified 
    type and return the relations (it does NOT proceed 
    to the sources).

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friends = z1 | in_rels[RT.FriendOf]

    ---- Signature ----
    ZefRef -> ZefRefs
    EZefRef -> EZefRefs

    ---- Tags ----
    - used for: graph traversal
    - operates on: ZefRef, EZefRef
    - related zefop: out_rels
    - related zefop: in_rel
    - related zefop: In
    - related zefop: Ins
    """
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt_or_bt, "in", "multi")
    if rt_or_bt == RT or rt_or_bt is None: res = pyzefops.ins(z) | filter[is_a[BT.RELATION_EDGE]] | collect
    elif rt_or_bt == BT: res = pyzefops.ins(z | to_ezefref | collect)
    else:
        if isinstance(rt_or_bt, RT) and isinstance(internals.get_c_token(rt_or_bt), RelationTypeToken):
            res = pyzefops.traverse_in_edge_multi(z, internals.get_c_token(rt_or_bt))
        elif isinstance(rt_or_bt, BT) and isinstance(internals.get_c_token(rt_or_bt), BlobTypeToken):
            res = pyzefops.traverse_in_edge_multi(z, internals.get_c_token(rt_or_bt))
        else:
            raise Exception("TODO: Need to implement non-specific relation types for out_rels")
    if source_filter: 
        if isinstance(source_filter, ZefOp): source_filter = Is[source_filter]
        return res | filter[source | is_a[source_filter]] | collect 
    return res  




def source_implementation(zr, *curried_args):
    if is_a(zr, Entity):
        raise Exception(f"Can't take the source of an entity (have {zr}), only relations have sources/targets")
    if isinstance(zr, FlatRef):
        return fr_source_imp(zr)
    if isinstance(zr, RelationRef):
        return zr.d["source"]
    if isinstance(zr, DelegateRef):
        from ...pyzef.internals import DelegateRelationTriple
        if not isinstance(zr.item, DelegateRelationTriple):
            raise Exception(f"Can't take the source of a non-relation-triple Delegate ({zr})")
        return internals.Delegate(zr.order + zr.item.source.order, zr.item.source.item)
    return (pyzefops.source)(zr, *curried_args)

def target_implementation(zr):
    if is_a(zr, Entity):
        raise Exception(f"Can't take the target of an entity (have {zr}), only relations have sources/targets")
    if isinstance(zr, FlatRef):
        return fr_target_imp(zr)
    if isinstance(zr, RelationRef):
        return zr.d["target"]
    if isinstance(zr, DelegateRef):
        from ...pyzef.internals import DelegateRelationTriple
        if not isinstance(zr.item, DelegateRelationTriple):
            raise Exception(f"Can't take the target of a non-relation-triple Delegate ({zr})")
        return instance.Delegate(zr.order + zr.item.target.order, zr.item.target.item)
    return pyzefops.target(zr)

def value_implementation(zr, maybe_tx=None):
    if isinstance(zr, FlatRef):
        return fr_value_imp(zr)
    if maybe_tx is None:
        val = pyzefops.value(zr)
    elif isinstance(maybe_tx, GraphSlice):
        val = pyzefops.value(zr, maybe_tx.tx)
    else:
        val = pyzefops.value(zr, maybe_tx)

    from ...pyzef.zefops import SerializedValue
    if isinstance(val, SerializedValue):
        val = val.deserialize()
    if isinstance(val, AttributeEntityTypeToken):
        val = AET[val]
    return val

def time_implementation(x, *curried_args):
    """
    Return the time of the object.

    ---- Signature ----
    ZefRef[TX]  -> Time
    EZefRef[TX] -> Time
    GraphSlice  -> Time
    """
    if isinstance(x, GraphSlice):
        return pyzefops.time(to_tx(x))
    return (pyzefops.time)(x, *curried_args)



def instantiation_tx_implementation(z):
    return z | preceding_events[VT.Instantiated] | single | absorbed | single | frame | to_tx | collect

def termination_tx_implementation(z):
    root_node = Graph(z)[42] 
    return z | preceding_events[VT.Terminated] | attempt[single | absorbed | single | frame | to_tx][root_node] | collect


    
def uid_implementation(arg):
    if isinstance(arg, String):
        return to_uid(arg)
    if isinstance(arg, AtomRef):
        return arg.d["uid"]
    if is_a(arg, UID):
        return arg
    return pyzefops.uid(arg)

def base_uid_implementation(first_arg):
    if isinstance(first_arg, EternalUID) or isinstance(first_arg, ZefRefUID):
        return (lambda x: x.blob_uid)(first_arg)
    if isinstance(first_arg, BaseUID):
        return first_arg
    return base_uid(uid(first_arg))

def zef_id_imp(x):
    if isinstance(x, BlobPtr):
        if BT(x) == BT.VALUE_NODE:
            from ...pyzef.zefops import SerializedValue
            return f"hash: {internals.value_hash(SerializedValue.serialize(value(x)))})"
        elif internals.is_delegate(x):
            return zef_id_imp(to_delegate(x))
        elif internals.has_uid(to_ezefref(x)):
            return uid(x)
    elif isinstance(x, AtomRef):
        return origin_uid(x)
    elif isinstance(x, DelegateRef):
        return str(x)
            
    raise Exception(f"Don't know how to represent UID of object ({x})")

def exists_at_implementation(z, frame):
    assert isinstance(frame, GraphSlice)
    return (pyzefops.exists_at)(z, frame.tx)

def first_tx_for_low_level_blob(z):
    # Delegate detection comes first, as several of the other things below can
    # be delegates and would need their own logic.
    if internals.is_delegate(z):
        # Chronological order is mandatory so we find the first instantiation edge
        return first_tx_for_low_level_blob(z | in_rel[BT.TO_DELEGATE_EDGE] | collect)
    elif BT(z) in [BT.ENTITY_NODE,
                   BT.RELATION_EDGE,
                   BT.ATTRIBUTE_ENTITY_NODE,
                   BT.VALUE_NODE]:
        # Chronological order is mandatory so we find the first instantiation edge
        return first_tx_for_low_level_blob(z | in_rel[BT.RAE_INSTANCE_EDGE] | collect)
    if BT(z) in [BT.DELEGATE_INSTANTIATION_EDGE,
                   BT.DELEGATE_RETIREMENT_EDGE,
                   BT.INSTANTIATION_EDGE,
                   BT.TERMINATION_EDGE,
                   BT.ATOMIC_VALUE_ASSIGNMENT_EDGE,
                   BT.ASSIGN_TAG_NAME_EDGE]:
        # Any of these are low-level blobs that only have a single event of "instantiation" and whose source is the tx
        return source(z)
    elif BT(z) in [BT.ORIGIN_RAE_EDGE,
                   BT.ORIGIN_GRAPH_EDGE,
                   BT.FOREIGN_ENTITY_NODE,
                   BT.FOREIGN_RELATION_EDGE,
                   BT.FOREIGN_ATTRIBUTE_ENTITY_NODE,
                   BT.FOREIGN_GRAPH_NODE]:
        raise TypeError(f"Can't yet determine events for this kind of blob: {BT(z)}")
                   
    # Special cases where there is a TX but it's a bit harder to get to
    elif BT(z) == BT.TX_EVENT_NODE:
        return z
    elif BT(z) == BT.NEXT_TX_EDGE:
        return target(z)
    elif BT(z) == BT.ROOT_NODE:
        # This is a little of an odd choice
        return z | Out[BT.NEXT_TX_EDGE] | collect
    elif BT(z) == BT.RAE_INSTANCE_EDGE:
        return z | Ins[BT.INSTANTIATION_EDGE] | first | collect
    elif BT(z) == BT.TO_DELEGATE_EDGE:
        # Need to get the first TX of all instantiation edges.
        return z | Ins[BT.DELEGATE_INSTANTIATION_EDGE] | first | collect

    raise Exception(f"Not a low level blob we can get a simple tx for: {BT(z)}.")


def aware_of_implementation(z, frame):
    """
    Return whether the Graph is aware of the object `z` in the frame given. This
    differs from exists_at in that a graph is still "aware" of a terminated
    entity, and low-level blobs are also valid inputs to aware_of.
    
    ---- Signature ----
    ((ZefRef | EZefRef), GraphSlice) -> Bool
    """
    z_tx = first_tx_for_low_level_blob(z)
    return (z_tx
            | to_graph_slice
            | graph_slice_index
            | less_than_or_equal[graph_slice_index(frame)]
            | collect)

def is_zefref_promotable_implementation(z):
    return pyzefops.is_zefref_promotable(z)

def to_ezefref_implementation(zr):
    return pyzefops.to_ezefref(zr)

def l_implementation(first_arg, curried_args):
    return (pyzefops.L)(first_arg, *curried_args)

def o_implementation(first_arg, curried_args):
    """
    O[...]  is "optional", meaning it will return either 0,1 results.
    If the RT.Edge  exists it'll traverse that and return a ZefRef
    If the RT.Edge  doesn't exist, it'll return None or nothing .
    If there are multiple RT.Edge  it'll throw an error

    ---- Examples ----
    >>> z1 >> O[RT.Foo]        # of type {ZefRef, None, Error}
    """
    return (pyzefops.O)(first_arg, *curried_args)




#---------------------------------------- value_type -----------------------------------------------
# @register_zefop(RT.ZefType)
def representation_type_imp(x):
    """
    Warning: this function is not complete and its behavior may change!!!!!!!!!!!

    Returns a Zef ValueType_ (a value itself),
    also when used with the python builtin supported
    types, as well as with instances of Zef Values.

    TODO:
    my_ent | representation_type   # Entity[ET.Foo]
    z | representation_type        # ZefRef[ET.Foo]
    ez| representation_type        # EZefRef[ET.Foo]

    ET.Foo | representation_type    # ET
    ET     | representation_type    # ValueType

    """
    from ...core._bytes import Bytes_
    tp = type(x)    
    try:
        return {
            bool: VT.Bool,
            int: VT.Int,
            float: VT.Float,
            str: VT.String,
            list: VT.List,    # TODO: we may want to specialize this, e.g. VT.List[VT.Int]. At least if the list is finite, or saved as metadata
            tuple: VT.List,
            dict: VT.Dict,
            set: VT.Set,
            Time: VT.Time,
            Bytes_: VT.Bytes,
            ValueType_: VT.ValueType,
            # QuantityFloat: VT.QuantityFloat,
            # QuantityInt: VT.QuantityInt,
            # EntityType: VT.EntityType,
            # AttributeEntityType: VT.AttributeEntityType,
            # RelationType: VT.RelationType,
            ZefRef: VT.ZefRef,
            EZefRef: VT.EZefRef,
            # TX: VT.TX
            # Entity: VT.Entity,
            # AttributeEntity: VT.AttributeEntity,
            # Relation: VT.Relation,
            ZefOp: VT.ZefOp,
            Graph: VT.Graph,
            FlatGraph: VT.FlatGraph,
            # Keyword: VT.Keyword,
            # ValueType_: VT.ValueType_,
            # SymbolicExpression: VT.SymbolicExpression,
            # Stream: VT.Stream,
            # Error: VT.Error,
            # Nil: VT.Nil,
            # Effect !!!!! not required, just a dict
            # GraphDelta !!!!! not required, just a dict
        }[tp]
    except KeyError:
        return Error(f'"zef_type" function received a value of type {type(x)} which is not implemented yet.')







def is_a_implementation(x, typ):
    """

    """
    # To handle user passing by int instead of Int by mistake
    if typ in [int, float, bool]:
        py_type_to_vt = {
            bool: Bool,
            int: Int,
            float: Float
        }
        print(f"{repr(typ)} was passed as a type, but what you meant was { py_type_to_vt[typ]}!")
        return is_a(x, py_type_to_vt[typ])

    return isinstance(x, typ)

    def rp_matching(x, rp):
        triple = rp._d['absorbed'][0]
        v = tuple(el == Z for el in triple)
        try:
            if v == (True, False, False):
                return x | Out[triple[1]] | is_a[triple[2]] | collect

            if v == (False, False, True):
                return x | In[triple[1]] | is_a[triple[0]] | collect

            if v == (False, True, False):
                return is_a(source(x), triple[0]) and is_a(target(x), triple[2])
                
            if  v == (False, False, False):
                return is_a(x, triple[1]) and is_a(source(x), triple[0]) and is_a(target(x), triple[2])
        except:
            return False
        raise TypeError(f"invalid pattern to match on in RP: {triple}")


    def has_value_matching(x, vt):
        my_set = vt._d['absorbed'][0]
        try:
            val = value(x)
        except:
            raise TypeError(f"HasValue can only be applied to AETs")
            # TODO: or return false here

        if isinstance(my_set, Set):
            return val in my_set            
        # If we're here, it should be a VT    
        return is_a(val, my_set)

    if isinstance(typ, ValueType):
        if typ._d['type_name'] == "RP":
            return rp_matching(x, typ)

        if typ._d['type_name'] == "HasValue":
            return has_value_matching(x, typ)
        
        if typ._d['type_name'] in  {"Instantiated", "Assigned", "Terminated"}:
            map_ = {"Instantiated": instantiated, "Assigned": assigned, "Terminated": terminated}
            def compare_absorbed(x, typ):
                val_absorbed = absorbed(x)
                typ_absorbed = absorbed(typ)
                for i,typ in enumerate(typ_absorbed):
                    if i >= len(val_absorbed): break               # It means something is wrong, i.e typ= Instantiated[Any][Any]; val=instantiated[z1]
                    if not is_a(val_absorbed[i],typ): return False
                return True
            return without_absorbed(x) == map_[typ._d['type_name']] and compare_absorbed(x, typ)


    if isinstance(x, ZefRef) or isinstance(x, EZefRef):
        if isinstance(typ, BlobType):
            return BT(x) == typ

        if typ == Delegate:
            return internals.is_delegate(x)

        if is_a(typ, Delegate):
            return delegate_of(x) == typ

        if not internals.is_delegate(x):
            # The old route is just for instances only
            if _is_a_instance_delegate_generic(x, typ):
                return True

    if isinstance(x, ZefEnumValue):
        if isinstance(typ, ZefEnumStruct):
            return True
        if isinstance(typ, ZefEnumStructPartial):
            return x.enum_type == typ.__enum_type
        if isinstance(typ, ZefEnumValue):
            return x == typ








def _is_a_instance_delegate_generic(x, typ):
    # This function is for internal use only and does the comparisons against
    # ET, ET.x, RT, (Z, RT.x, ET.y), etc... ignoring whether the reference is an
    # instance or delegate
    if typ == Z:
        return True
    if typ == RAE:
        if BT(x) in [BT.ENTITY_NODE, BT.RELATION_EDGE, BT.ATTRIBUTE_ENTITY_NODE]:
            return True
    if isinstance(typ, EntityType):
        if BT(x) != BT.ENTITY_NODE:
            return False
        return ET(x) == typ
    if isinstance(typ, RelationType):
        if BT(x) != BT.RELATION_EDGE:
            return False
        return RT(x) == typ
    if is_RT_triple(typ):
        if BT(x) != BT.RELATION_EDGE:
            return False
        return (_is_a_instance_delegate_generic(pyzefops.source(x), typ[0])
                and _is_a_instance_delegate_generic(x, typ[1])
                and _is_a_instance_delegate_generic(pyzefops.target(x), typ[2]))
    if is_a(typ, AET):
        if BT(x) != BT.ATTRIBUTE_ENTITY_NODE:
            return False
        return is_a(AET(x), typ)
    if isinstance(typ, EntityTypeStruct):
        return BT(x) == BT.ENTITY_NODE
    if isinstance(typ, RelationTypeStruct):
        return BT(x) == BT.RELATION_EDGE
    if isinstance(typ, AttributeEntityTypeStruct):
        return BT(x) == BT.ATTRIBUTE_ENTITY_NODE

    return False

def has_relation_implementation(z1, rt, z2):
    return pyzefops.has_relation(z1, internals.get_c_token(rt), z2)

def relation_implementation(z1, *args):
    if len(args) == 1:
        return pyzefops.relation(z, *args)
    else:
        rt,z2 = args
        return pyzefops.relation(z1, internals.get_c_token(rt), z2)

def relations_implementation(z, *args):
    if len(args) == 1:
        return pyzefops.relations(z, *args)
    else:
        rt,z2 = args
        return pyzefops.relations(z1, internals.get_c_token(rt), z2)

def rae_type_implementation(z):
    if isinstance(z, EntityRef):
        return z.d["type"]
    if isinstance(z, RelationRef):
        return z.d["type"]
    if isinstance(z, AttributeEntityRef):
        return z.d["type"]
    # return pymain.rae_type(z)
    c_rae = pymain.rae_type(z)
    if isinstance(c_rae, internals.EntityType):
        return ET[c_rae]
    if isinstance(c_rae, internals.RelationType):
        return RT[c_rae]
    if isinstance(c_rae, internals.AttributeEntityType):
        return AET[c_rae]
    if isinstance(c_rae, internals.ValueRepType):
        return VRT[c_rae]
    raise Exception(f"Don't know how to recast {c_rae}")

def abstract_type_implementation(z):
    # This is basically rae_type, but also including TXNode and Root
    if isinstance(z, TXNodeRef):
        return BT.TX_EVENT_NODE
    if isinstance(z, RootRef):
        return BT.ROOT_NODE
    if isinstance(z, (ZefRef, EZefRef)) and BT(z) in {BT.TX_EVENT_NODE, BT.ROOT_NODE}:
        return BT(z)
    return rae_type(z)





#----------------------docstring------------------------- 

def docstring_imp(a) -> str:
    """
    Return the docstring for a given ZefOp or Zef Function.
        
    ---- Examples ----
    >>> docstring(nth)
    """
    from zef.core.op_implementations.dispatch_dictionary import _op_to_functions
    import inspect
    if not isinstance(a, ZefOp):
        return Error('"docstring" can only be called on ZefOps')

    # is this a bare zef function? We want to implement that separately
    if len(a.el_ops) == 1 and a.el_ops[0][0]==RT.Function:
        return Error('"docstring" not implemented for Zef functions yet')


    if len(a.el_ops) == 1:
        if len(a.el_ops) == 1:
            f = _op_to_functions[a.el_ops[0][0]][0]
            doc = inspect.getdoc(f)
            doc = doc if doc else f"No docstring found for given {a}!"
            return doc
    else:
        from zef.core.op_implementations.yo_ascii import make_op_chain_ascii_output
        return make_op_chain_ascii_output(a)







#---------------------- source_code------------------------- 
def source_code_imp(a) -> str:
    """
    Return the function body for a given ZefOp or Zef Function.
        
    ---- Examples ----
    >>> source_code(insert_at) | to_clipboard | run
    """
    from zef.core.op_implementations.dispatch_dictionary import _op_to_functions
    import inspect
    if not isinstance(a, ZefOp):
        return Error('"source_code" can only be called on ZefOps')

    # is this a bare zef function? We want to implement that separately
    if len(a.el_ops) == 1 and a.el_ops[0][0]==RT.Function:
        return Error('"source_code" not implemented for Zef functions yet')


    if len(a.el_ops) == 1:
        if len(a.el_ops) == 1:
            f = _op_to_functions[a.el_ops[0][0]][0]
            body = inspect.getsource(f)
            return body if body else f"No function body found for given {a}!"
    else:
        from zef.core.op_implementations.yo_ascii import make_op_chain_ascii_output
        return make_op_chain_ascii_output(a)





#----------------------Type Functions------------------------- 
from inspect import isfunction, getfullargspec
def map_type_info(op, curr_type):
    func = op[1][0]
    from ..zef_functions import LocalFunction
    assert isfunction(func) or isinstance(func, ZefOp) or isinstance(func, LocalFunction) or (isinstance(func, ZefRef) and ET(func) == ET.ZEF_Function)
    if not isfunction(func) and (isinstance(func, ZefRef) and ET(func) == ET.ZEF_Function): 
        func = get_compiled_zeffunction(func)
        insp = getfullargspec(func)  
        return type_spec(insp.annotations.get("return", None), True)
    else:
        return VT.List[VT.Any]

def reduce_type_info(op, curr_type):
    func = op[1][0]
    assert isfunction(func) or isinstance(func, ZefOp) or (isinstance(func, ZefRef) and ET(func) == ET.ZEF_Function)
    if not isfunction(func) and (isinstance(func, ZefRef) and ET(func) == ET.ZEF_Function): 
        func = get_compiled_zeffunction(func)
        insp = getfullargspec(func)  
        return type_spec(insp.annotations.get("return", None), True)
    else:
        return VT.List[VT.Any]

def group_by_type_info(op, curr_type):
    return VT.List[VT.Tuple[VT.Any]]

def identity_type_info(op, curr_type):
    return curr_type

def length_type_info(op, curr_type):
    return VT.Int



def nth_type_info(op, curr_type):
    try:
        curr_type = absorbed(curr_type)[0]
    except AttributeError as e:
        raise Exception(f"An operator that downs the degree of a Nestable object was called on a Degree-0 Object {curr_type}: {e}")
    return curr_type

def filter_type_info(op, curr_type):
    return curr_type





def sort_type_info(op, curr_type):
    return curr_type

def source_type_info(op, curr_type):
    return curr_type

def target_type_info(op, curr_type):
    return curr_type

def value_type_info(op, curr_type):
    return VT.ValueType

def time_type_info(op, curr_type):
    return VT.Time



def instantiation_tx_type_info(op, curr_type):
    return VT.ZefRef

def termination_tx_type_info(op, curr_type):
    return VT.ZefRef


def uid_type_info(op, curr_type):
    if curr_type == VT.Graph:
        return VT.BaseUID
    elif curr_type == VT.ZefRef:
        return VT.ZefRefUID
    elif curr_type == VT.EZefRef:
        return VT.EternalUID
    return None

def base_uid_type_info(op, curr_type):
    return VT.BaseUID

def exists_at_type_info(op, curr_type):
    return VT.Bool

def is_zefref_promotable_type_info(op, curr_type):
    return VT.Bool


def to_ezefref_type_info(op, curr_type):
    return VT.EZefRef

def o_type_info(op, curr_type):
    return curr_type

def l_type_info(op, curr_type):
    return curr_type

def Out_type_info(op, curr_type):
    return curr_type

def In_type_info(op, curr_type):
    return curr_type

def OutOut_type_info(op, curr_type):
    return curr_type

def InIn_type_info(op, curr_type):
    return curr_type

def terminate_implementation(z, *args):
    from ..graph_delta import PleaseTerminate
    # We need to keep terminate as something that works in the GraphDelta code.
    # So we simply wrap everything up as a LazyValue and return that.
    assert len(args) <= 1
    internal_id = args[0] if len(args) == 1 else None
    return PleaseTerminate(target=z, internal_id=internal_id)

def terminate_type_info(op, curr_type):
    return curr_type

def assign_imp(z, val):
    # We need to keep the assign value as something that works in the GraphDelta
    # code. So we simply wrap everything up as a LazyValue and return that.
    # return LazyValue(z) | assign[val]
    from ..graph_delta import PleaseAssign
    return PleaseAssign({"target": z,
                          "value": val})

def assign_tp(op, curr_type):
    return VT.Any

def ET_implementation(z, *args):
    assert len(args) == 0 or args == (ET,)
    return internals.ET(z)

def ET_type_info(op, curr_type):
    return VT.ET

def RT_implementation(z, *args):
    assert len(args) == 0 or args == (RT,)
    return internals.RT(z)

def RT_type_info(op, curr_type):
    return VT.RT

def BT_implementation(z, *args):
    assert len(args) == 0 or args == (BT,)
    return internals.BT(z)

def BT_type_info(op, curr_type):
    return VT.BT

def AET_implementation(z, *args):
    assert len(args) == 0 or args == (AET,)
    return internals.AET(z)

def AET_type_info(op, curr_type):
    return VT.AET

def is_a_type_info(op, curr_type):
    return VT.Bool

def has_relation_type_info(op, curr_type):
    return VT.Bool

def relation_type_info(op, curr_type):
    return curr_type

def relations_type_info(op, curr_type):
    return VT.Any

def hasout_type_info(op, curr_type):
    return VT.Bool

def hasin_type_info(op, curr_type):
    return VT.Bool

def rae_type_type_info(op, curr_type):
    return VT.BT

def abstract_type_type_info(op, curr_type):
    return VT.BT


def is_represented_as_implementation(arg, vt):
    if isinstance(arg, Bool):
        return vt == VT.Bool
    
    if isinstance(arg, Int):
        return vt == VT.Int

    if isinstance(arg, Float):
        return vt == VT.Float
    
    return Error(f"Is Represented As not implemented for {type(arg)}")


def is_represented_as_type_info(op, curr_type):
    VT.Bool
#---------------------------------------- tag -----------------------------------------------
def tag_imp(x, tag_s: str, *args):
    """ 
    Create an effect to add a tag to either a Graph or a RAE.

    ---- Examples ----
    >>> g | tag["my_graph"] | run
    >>> g2 | tag["my_graph"][True] | run
    >>> z | tag["settings_node"] | g | run
    >>> [z | tag["settings_node"]] | transact[g] | run

    ---- Arguments ----
    x Graph / ZefRef: object to tag
    tag str: the name of the tag to apply
    force bool (=False): whether to allow stealing the tag from another graph or ZefRef.

    ---- Signature ----
    (Graph, str, bool) -> Effect
    (ZefRef, str, bool) -> LazyValue
    """
    if isinstance(x, Graph):
        if len(args) > 0:
            assert len(args) == 1
            force = args[0]
        else:
            force = False
        return {
            'type': FX.Graph.Tag,
            'graph': x,
            'tag': tag_s,
            'force': force,
            'adding': True,
        }
    if isinstance(x, ZefRef) or isinstance(x, EZefRef) or is_a(x, ZefOp[Z]):
        assert len(args) == 0
        return LazyValue(x) | tag[tag_s]

    raise RuntimeError(f"Unknown type for tag: {type(x)}")
    
    
def tag_tp(op, curr_type):
    return VT.Effect

def untag_imp(x, tag: str):
    """ 
    Create an effect to remove a tag from either a Graph or a RAE

    ---- Examples ----
    >>> g | untag["my_graph"] | run
    >>> z | untag["settings_node"] | run

    ---- Signature ----
    (Graph, str) -> Effect
    (ZefRef, str) -> Effect
    """
    if isinstance(x, Graph):
        return {
            'type': FX.Graph.Tag,
            'graph': x,
            'tag': tag,
            'adding': False,
            'force': False,
        }
    if isinstance(x, ZefRef) or isinstance(x, EZefRef) or is_a(x, ZefOp[Z]):
        raise Exception("Untagging a RAE is not supported at the moment.")

    raise RuntimeError(f"Unknown type for tag: {type(x)}")
    
    
def untag_tp(op, curr_type):
    return VT.Effect

#---------------------------------------- sync -----------------------------------------------
def sync_imp(x: VT.Graph, sync_state: bool = True):
    """ 
    Creates an effect to set the sync option on the graph. Defaults to enabling
    sync (sync_state=True) but can be set to False to disable sync.

    ---- Examples ----
    >>> g | sync | run
    >>> g | sync[True] | run
    >>> g | sync[False] | run

    ---- Signature ----
    Graph -> Effect
    (Graph, bool) -> Effect
    """
    return {
        'type': FX.Graph.Sync,
        'graph': x,
        'sync_state': sync_state,
    }
    
def sync_tp(op, curr_type):
    return VT.Effect




#---------------------------------------- merge -----------------------------------------------
def merge_imp(a, second=None, *args):
    """
    Merge a dictionaries: either one list of dicts or
    dicts as multiple args.

    Clojure has a similar operator:
    https://clojuredocs.org/clojure.core/merge

    Also has the ability to merge 2 FlatGraphs together or a list of FlatGraphs.

    ---- Examples -----
    >>> [{'a': 1, 'b': 42}, {'a': 2, 'c': 43}] | merge          # => {'a': 2, 'b': 42, 'c': 43}
    >>> {'a': 1, 'b': 42} | merge[ {'a': 2, 'c': 43} ]
    >>> fg1 | merge[fg2] | collect
    >>> [fg1, fg2, fg3] | merge | collect


    ---- Signature ----
    List[Dict]          -> Dict
    (Dict, Dict)        -> Dict
    (Dict, Dict, Dict)  -> Dict

    ---- Tags ----
    - operates on: Dict
    - related zefop: merge_with
    """
    from typing import Generator
    if is_a(a, FlatGraph) and is_a(second, FlatGraph):
        return fg_merge_imp(a, second)
    elif isinstance(a, list) and is_a(a[0], FlatGraph):
        return fg_merge_imp(a)
    elif second is None:
        assert isinstance(a, tuple) or isinstance(a, list) or isinstance(a, (Generator, ZefGenerator))
        return {k: v for d in a for k, v in d.items()}
    else:
        assert isinstance(a, Dict)
        assert isinstance(second, Dict)
        def tmp_gen():
            yield from a.items()
            yield from second.items()
            for d in args:
                yield from d.items()
        return dict(tmp_gen())


def merge_tp(a, second=None, *args):
    return VT.Dict





#---------------------------------------- merge_with -----------------------------------------------

def merge_with_imp(dict_or_list, merge_func, more_dict_or_list=None):
    """ 
    Merge a list of maps, but give an operation to join the values 
    for matching keys.
    The function provided for the merging must be able to operate on
    the value type in the dictionary (it suffices if it works for the
    value type of the matching keys only).

    Based on the Clojure operator https://clojuredocs.org/clojure.core/merge-with

    ---- Examples ----
    >>> {'a': 1, 'b': 2} | merge_with[add][{'a': 3}]                        # => {'a': 4, 'b': 2}
    >>> {'a': 1, 'b': 2} | merge_with[add][{'a': 3}, {'b': 10, 'c': 5}]     # => {'a': 4, 'b': 12, 'c': 5}    
    >>> [{'a': [1], 'b': [2]}, {'a': [3]}] | merge_with[concat]             # => {'a': [1, 2], 'b': [3]}

    ---- Signature ----
    (Dict[T1, T2], ((T2,T2)->T2), Dict[T1, T2]) -> Dict[T1, T2]
    (List[Dict[T1, T2]], ((T2,T2)->T2)) -> Dict[T1, T2]
    """
    v = [dict_or_list] if isinstance(dict_or_list, Dict) else tuple(dict_or_list)
    if more_dict_or_list is not None:
        if isinstance(more_dict_or_list, Dict):
            v.append(more_dict_or_list)
        elif isinstance(more_dict_or_list, list) or isinstance(more_dict_or_list, tuple):
            v = (*v, *more_dict_or_list)
    if len(v) == 0: return dict()   # this is the only reasonable answer for an empty list? Or should it be an Error?    
    d_out = {**v[0]}
    for dd in v[1:]:
        for k, v in dd.items():
            if k in d_out:
                d_out[k] = merge_func(d_out[k], v)
            else:
                d_out[k] = v
    return d_out


def merge_with_tp(x):
    return VT.Dict

#---------------------------------------- to_clipboard -----------------------------------------------
def to_clipboard_imp(x):
    """
    A shortcut function to create an effect that will copy
    an elementary type to the clipboard.

    ---- Examples ----
    >>> 'hello' | to_clipboard                                  # returns an effect
    >>> my_zef_func | to_clipboard | run                        # copy a single zef function to the clipboard
    >>> g | now | all[ET.ZEF_Function] | to_clipboard | run     # copy all zef function on graph to clipboard

    ---- Signature ----
    String                          -> Effect
    Int                             -> Effect
    Float                           -> Effect
    Bool                            -> Effect
    ZefRef[ET.ZEF_Function]         -> Effect
    List[ZefRef[ET.ZEF_Function]]   -> Effect
    """
    from ..zef_functions import zef_fct_to_source_code_string

    if isinstance(x, ZefRef) and ET(x)==ET.ZEF_Function:
        return to_clipboard(zef_fct_to_source_code_string(x))

    if isinstance(x, list):
        for el in x: 
            assert ET(el)==ET.ZEF_Function
        return to_clipboard(x | map[zef_fct_to_source_code_string] | join['\n'] | collect)

    if is_a(x, UID):
        return to_clipboard(str(x))

    assert type(x) in {str, int, float, bool}
    return {
        'type': FX.Clipboard.CopyTo,
        'content': x
    }


def to_clipboard_tp(x):
    return VT.Effect


#---------------------------------------- from_clipboard -----------------------------------------------
def from_clipboard_imp():
    """
    A shortcut function to create an effect that will request
    the content of a clipboard to be copied
    """
    # assert type(x) in {str, int, float, bool}
    return {
        'type': FX.Clipboard.CopyFrom,
    }

_call_0_args_translation[internals.RT.FromClipboard] = from_clipboard_imp

def from_clipboard_tp(op, curr_type):
    return VT.Effect



#---------------------------------------- text_art -----------------------------------------------
def text_art_imp(s: str) -> str:
    """ 
    convert a string to ascii art text
    Todo: add flag to select comment style

    ---- Signature ----
    String -> String
    """
    from art import text2art        
    def add_comment(s: str) -> str:
        v = (s.split('\n'))[:-1] | map[lambda ro: '#  '+ro]
        return '\n'.join(v)
    
    s2 = s.replace(' ', '   ')
    return add_comment(text2art(s2))

def text_art_tp(x):
    return VT.String

#---------------------------------------- to/from_json -----------------------------------------------
def to_json_imp(v: VT.Any)-> VT.Dict:
    """ 
    Serializes python types,zef types,ops, and custom types to a JSON dictionary

    ---- Examples ----
    >>> zr | to_json | collect
    >>> RT.A | to_json | collect

    ---- Signature ----
    VT.Any -> VT.Dict
    """
    from ..serialization import serialize
    import json
    return json.dumps(serialize(v))
    
def to_json_tp(op, curr_type):
    return VT.Dict

def from_json_imp(d: VT.Dict)-> VT.Any:
    """ 
    Deserializes a serialized object as a JSON dict back to the object.

    ---- Examples ----
    >>> serialized_dict | from_json | collect

    ---- Signature ----
    VT.Dict -> VT.Any
    """
    from ..serialization import deserialize
    import json
    return deserialize(json.loads(d))
    
def from_json_tp(op, curr_type):
    return VT.Any


#------------------------------------------yaml/toml---------------------------------------------
def to_yaml_imp(v: VT.Any)-> VT.Dict:
    import yaml 
    return yaml.safe_dump(v)
    
def to_yaml_tp(op, curr_type):
    return VT.Dict

def from_yaml_imp(d: VT.Dict)-> VT.Any:
    import yaml 
    return yaml.safe_load(d)
    
def from_yaml_tp(op, curr_type):
    return VT.Any


def to_toml_imp(v: VT.Any)-> VT.Dict:
    import toml
    return toml.dumps(v)
    
def to_toml_tp(op, curr_type):
    return VT.Dict

def from_toml_imp(d: VT.Dict)-> VT.Any:
    import toml
    return toml.loads(d)
    
def from_toml_tp(op, curr_type):
    return VT.Any


#---------------------------------------- to/from_csv -----------------------------------------------
def to_csv_imp(df: VT.DataFrame, settings = {}) -> VT.String:
    return df.to_csv(**settings)
    
def to_csv_tp(op, curr_type):
    return VT.String

def from_csv_imp(csv_str: VT.String, settings = {}) -> VT.DataFrame:
    import pandas as pd
    import io
    return pd.read_csv(io.StringIO(csv_str, **settings))
    
def from_csv_tp(op, curr_type):
    return VT.DataFrame


#---------------------------------------- read/write file -----------------------------------------------
def read_file_imp(fname):
    """Reads the file at the given `fname` returning its content as bytes.
    
    This operator produces an effect and must be passed to `run`. The output of
    the effect will contain a "content" key with the file's content.

    ---- Examples ----
    >>> "data.yaml" | read_file | run | get["content"] | from_yaml | collect

    ---- Signature ----
    VT.String -> VT.Effect

    ---- Tags ----
    - related zefop: load_file
    - used for: file io

    """
    return {
        'type': FX.LocalFile.Read,
        'filename' : fname
    }

def read_file_tp(op, curr_type):
    return VT.Effect

def load_file_imp(fname, format = None):
    """Reads the file at the given `fname` and parse its content based on the
    file extension.
    
    This operator produces an effect and must be passed to `run`. The output of
    the effect will contain a "content" key with the transformed object.

    ---- Examples ----
    >>> "data.yaml" | load_file | run | get["content"] | collect

    ---- Signature ----
    VT.String -> VT.Effect

    ---- Tags ----
    - related zefop: read_file
    - used for: file io

    """
    return {
        'type'     : FX.LocalFile.Load,
        'filename' : fname,
        'format'   : format
    }

def load_file_tp(op, curr_type):
    return VT.Effect

def save_file_imp(content, fname, settings = {}):
    """The counterpart to `load_file`. Takes the given `content` and writes it to the file at the filename `fname`. The content is converted based on the extension of the file.
    
    The options for content as described further in the `FX.LocalFile.Save` effect.
    
    This operator doesn't do the writing itself, it only produces an effect
    which must be passed to run.
    
    Settings is used for cases where we would need to pass additional flags to the underlying conversion.
    An example is for csv, we can pass a dict {"na_values": True} to keep NA values in the Pandas DF.

    ---- Examples ----
    >>> data_as_dict | save_file["data.yaml"] | run
    
    ---- Possible issues ---
    data_as_dict | to_yaml | save_file["data.yaml"] | run
    will create a yaml file containing a single string, which is unlikely to be
    what you want.

    ---- Signature ----
    (VT.Any, VT.String) -> VT.Effect

    """
    return {
        'type': FX.LocalFile.Save,
        'filename' : fname,
        'content': content,
        'settings': settings,
    }

def save_file_tp(op, curr_type):
    return VT.Effect


def write_file_imp(content, fname):
    """The counterpart to `read_file`. Takes the given `content` and writes it to the file at the filename `fname`. The content is converted based on the extension of the file.
    
    The options for content as described further in the `FX.LocalFile.Write` effect.
    
    This operator doesn't do the writing itself, it only produces an effect
    which must be passed to run.
    
    ---- Examples ----
    >>> "Hello" | write_file["data.txt"] | run
    
    ---- Signature ----
    (VT.Any, VT.String) -> VT.Effect

    """
    return {
        'type': FX.LocalFile.Write,
        'filename' : fname,
        'content': content,
    }

def write_file_tp(op, curr_type):
    return VT.Effect

#---------------------------------------- dataframe to graph delta -----------------------------------------------
def pandas_to_gd_imp(df: VT.DataFrame, mapping: VT.Dict) -> VT.List:
    """ 
    Takes a pandas dataframe and a mapping Dict and returns a list of commands.

    ---- Signature ----
    (VT.DataFrame,  VT.Dict) -> VT.List
    """
    # step 1: column mapping
    cols = df.columns.to_list()
    if "columns" not in mapping: mapping["columns"] = {}
    for c in cols | filter[lambda x: x not in mapping['columns']] | collect:
            mapping['columns'][c] = c | split[' '] | map[lambda s: s[0].upper() + s[1:]] | join | collect
    cols = cols | map[lambda c: mapping['columns'][c]] | collect

    # step 2: create GraphDelta changes
    entity = mapping.get('row', "Row")
    actions = (
            df.values.tolist()
            | enumerate
            | map[lambda idx_row: idx_row[1] | enumerate | map[lambda i_v: (Any[f'_{idx_row[0]}'], RT(cols[i_v[0]]), i_v[1])] | collect]
            | prepend[range(len(df.values.tolist())) | map[lambda i: ET(entity)[f'_{i}']] | collect]
            | concat
            | collect
    )
    return actions

def pandas_to_gd_tp(op, curr_type):
    return VT.List




#---------------------------------------- to_pipeline -----------------------------------------------
def to_pipeline_imp(ops: list):
    """ 
    Given a list of operators, return one operator by constructing
    an operator pipeline in that order.

    ---- Examples ----
    >>> (nth[42], repeat, enumerate) | to_pipeline      # => nth[42] | repeat | enumerate

    ---- Tags ----
    - used for: control flow
    - operates on: ZefOp, Value Types, Entity, Relation, AttributeEntity, ZefRef, EZefRef
    - related zefop: inject
    - related zefop: inject_list
    - related zefop: absorbed
    - related zefop: without_absorbed
    - related zefop: reverse_args
    - related zefop: bypass
    """
    from typing import Generator, Iterable, Iterator
    if isinstance(ops, (Generator, ZefGenerator)) or isinstance(ops, Iterator): ops = [op for op in ops]
    return identity if len(ops) == 0 else (ops[1:] | reduce[lambda v, el: v | el][ops[0]] | collect)


def to_pipeline_tp():
    return VT.ZefOp



#---------------------------------------- inject -----------------------------------------------
def inject_imp(x, injectee):
    """
    Small helper function to inject the inflowing data via [...]

    ---- Examples ----
    >>> 42 | inject[equals]         # => equals[42]

    ---- Tags ----
    - used for: control flow
    - operates on: ZefOp, Value Types, Entity, Relation, AttributeEntity, ZefRef, EZefRef
    - related zefop: inject_list
    - related zefop: absorbed
    - related zefop: without_absorbed
    - related zefop: reverse_args
    - related zefop: to_pipeline
    """
    return injectee[x]


def inject_tp():
    return VT.ZefOp



#---------------------------------------- inject_list -----------------------------------------------

def inject_list_imp(v, injectee):
    """
    Small helper function to inject a list of inflowing data 
    as multiple [...][...]...

    ---- Examples ----
    >>> [pred1, pred2, pred3] | inject_list[And]         # => And[pred1][pred2][pred3]

    ---- Tags ----
    - used for: control flow
    - operates on: ZefOp, Value Types, Entity, Relation, AttributeEntity, ZefRef, EZefRef
    - related zefop: inject
    - related zefop: absorbed
    - related zefop: without_absorbed
    - related zefop: reverse_args
    - related zefop: to_pipeline
    """
    return v | reduce[lambda a, el: a[el]][injectee] | collect


def inject_list_tp():
    return VT.ZefOp




#---------------------------------------- Zascii -----------------------------------------------
def zascii_to_asg_imp(zascii_str: VT.String) -> VT.Dict:
    """ 
    Takes a zascii string and returns an asg dict.

    ---- Signature ----
    (VT.String) -> VT.Dict

    ---- Examples ----
    >>> s = \"""
    >>>                   RT.RatingScore
    >>>   ET.Dropdown─────────────────────────►1.0
    >>>   \"""
    >>> zascii_to_asg(s)


    ---- Tags ----
    - used for: parsing ascii
    - operates on: String
    - related zefop: zascii_to_flatgraph
    - related zefop: zascii_to_blueprint_fg
    """
    from ...deprecated.tools.zascii import parse_zascii_to_asg
    asg, _  = parse_zascii_to_asg(zascii_str)
    return asg

def zascii_to_asg_tp(op, curr_type):
    return VT.Dict

def zascii_to_flatgraph_imp(zascii_str: VT.String) -> VT.FlatGraph:
    """ 
    Takes a zascii string representing a Graph and returns a FlatGraph containing all the RAEs 
    appearing in the string.

    ---- Examples ----
    >>> s = \"""
    >>>                   RT.RatingScore
    >>>   ET.Dropdown─────────────────────────►1.0
    >>>   \"""
    >>> zascii_to_flatgraph(s)
    FlatGraph(
    (temp_id_0002=>0)
    (temp_id_0003=>1)
    (temp_id_0004=>2)
    -------
    (0, ET.Dropdown, [2], None)
    (1, AET.Float, [-2], None, 1.0)
    (2, RT.RatingScore, [], None, 0, 1)
    )

    ---- Signature ----
    (VT.String) -> VT.FlatGraph

    ---- Tags ----
    - used for: parsing ascii
    - operates on: String
    - related zefop: zascii_to_asg
    - related zefop: zascii_to_blueprint_fg
    """
    from ...core.internals import is_any_UID

    asg = zascii_to_asg_imp(zascii_str)

    aet_mapping = {'Float': AET.Float, 'Int': AET.Int, 'Bool': AET.Bool, 'String': AET.String}
    scalar_types_for_aets = {'Float', 'Int', 'Bool', 'String'}
    elements = list(asg.items())
    filter_with_temp_id =filter[lambda p: not is_any_UID(p[1].get('existing_uid', ''))]

    def ensure_valid_aet_type(d: dict):
        if d[1]['value'] not in scalar_types_for_aets: raise TypeError(f'An AET type "AET.{d.get("value", None)}" is not a parsable AET type')
        return d

    ets = (elements 
    | filter[lambda p: p[1]['type'] == 'ET'] 
    | filter_with_temp_id
    | map[lambda p: ET(p[1]['value'])[p[0]]]
    | collect
    )

    aets = (elements 
    | filter[lambda p: p[1]['type'] == 'AET'] 
    | filter_with_temp_id
    | map[lambda p: ensure_valid_aet_type(p)]
    | map[lambda p: aet_mapping[p[1]['value']][p[0]]]
    | collect
    )

    aets_from_vals = (elements 
    | filter[lambda p: p[1]['type'] in scalar_types_for_aets]
    | filter_with_temp_id
    | map[lambda p: aet_mapping[p[1]['type']][p[0]] <= p[1]['value']]
    | collect
    )
    ids = {absorbed(x)[0] for x in ets + aets}.union({absorbed(x.initial_val)[0] for x in aets_from_vals})

    rels = (elements 
    | filter[lambda p: p[1]['type'] == 'Edge']
    | filter_with_temp_id
    | map[lambda p: (Any[p[1]['source']], RT(asg[p[1]['labeled_by']]['value'])[p[0]], Any[p[1]['target']])]
    | collect
    )    
    sorted_rels = []
    while rels:
        for r in rels:
            if absorbed(r[0])[0] in ids and absorbed(r[2])[0] in ids:
                sorted_rels.append(r)
                ids.add(absorbed(r[1])[0])
                rels.remove(r)

    actions = ets + aets + aets_from_vals + sorted_rels
    return FlatGraph(actions)

def zascii_to_flatgraph_tp(op, curr_type):
    return VT.FlatGraph


def zascii_to_blueprint_fg_imp(zascii_str: VT.String) -> VT.FlatGraph:
    """ 
    Takes a zascii string representing a Graph Blueprint and returns a FlatGraph containing all the delegates 
    appearing in the string.

    ---- Examples ----
    >>> s = \"""
    >>>                   RT.RatingScore
    >>>   ET.Dropdown─────────────────────────►AET.Int
    >>>   \"""
    >>> zascii_to_blueprint_fg(s)
    FlatGraph(
    (D1(ET.Dropdown)=>0)
    (D1(AET.Int)=>1)
    (D1({D1(ET.Dropdown)>RT.RatingScore>D1(AET.Int)})=>2)
    -------
    (0, D1(ET.Dropdown), [2], None)
    (1, D1(AET.Int), [-2], None)
    (2, D1({D1(ET.Dropdown)>RT.RatingScore>D1(AET.Int)}), [], None, 0, 1)
    )    

    ---- Signature ----
    (VT.String) -> VT.FlatGraph

    ---- Tags ----
    - used for: parsing ascii
    - operates on: String
    - related zefop: zascii_to_asg
    - related zefop: zascii_to_flatgraph
    """
    from ...core.internals import is_any_UID
    asg = zascii_to_asg(zascii_str)

    aet_mapping = {'Float': AET.Float, 'Int': AET.Int, 'Bool': AET.Bool, 'String': AET.String}
    scalar_types_for_aets = {'Float', 'Int', 'Bool', 'String'}
    elements = list(asg.items())
    filter_with_temp_id =filter[lambda p: not is_any_UID(p[1].get('existing_uid', ''))]

    def ensure_valid_aet_type(d: dict):
        if d[1]['value'] not in scalar_types_for_aets: raise TypeError(f'An AET type "AET.{d.get("value", None)}" is not a parsable AET type')
        return d

    ets = (elements 
    | filter[lambda p: p[1]['type'] == 'ET'] 
    | filter_with_temp_id
    | map[lambda p: ET(p[1]['value'])[p[0]]]
    | collect
    )

    aets = (elements 
    | filter[lambda p: p[1]['type'] == 'AET'] 
    | filter_with_temp_id
    | map[lambda p: ensure_valid_aet_type(p)]
    | map[lambda p: aet_mapping[p[1]['value']][p[0]]]
    | collect
    )

    # Don't include the values, could be changes in the future to point to instances
    aets_without_vals = (elements 
    | filter[lambda p: p[1]['type'] in scalar_types_for_aets]
    | filter_with_temp_id
    | map[lambda p: aet_mapping[p[1]['type']][p[0]] ]
    | collect
    )

    id_to_rae = {absorbed(x)[0]: x for x in ets + aets + aets_without_vals}

    rels = (elements 
    | filter[lambda p: p[1]['type'] == 'Edge']
    | filter_with_temp_id
    | map[lambda p: (Any[p[1]['source']], RT(asg[p[1]['labeled_by']]['value'])[p[0]], Any[p[1]['target']])]
    | collect
    )    

    # TopoSort the RTs
    sorted_rels = []
    while rels:
        for r in rels:
            if absorbed(r[0])[0] in id_to_rae and absorbed(r[2])[0] in id_to_rae:
                sorted_rels.append(r)
                id_to_rae[absorbed(r[1])[0]] = r[1]
                rels.remove(r)
    
    instance_rep = Delegate

    def get_label(p):
        return LazyValue(p) | absorbed | single_or[None] | collect

    @func
    def get_template_representation(p, id_lookup):
        label = result = None
        if type(p) == EntityType or type(p) == AttributeEntityType:
            result = instance_rep(p)
            label = get_label(p)
        elif type(p) == tuple:
            s = get_item(p[0], id_lookup)
            t = get_item(p[2], id_lookup)
            result = instance_rep(s, p[1], t)
            label = get_label(p[1])

        if label is not None:
            id_lookup[label] = result
        return result

    def get_item(x, id_lookup):
        if is_a(x, ZefOp[Z]):
            return id_lookup[get_label(x)]
        if type(x) == Delegate:
            return x
        return instance_rep(x)
    
    items = concat([ets + aets + aets_without_vals + sorted_rels])
    reps = items | map[get_template_representation[id_to_rae]] | collect
    dels = reps | map[delegate_of] | collect
    return FlatGraph(dels)

def zascii_to_blueprint_fg_tp(op, curr_type):
    return VT.FlatGraph

#--------------------------------------------------------------------------------------
def replace_at_imp(str_or_list, index, new_el):
    """ 
    Given a list replace element at an index with new_el.
    Given a string return a new string with element at index replaced with new_el.
    

    ---- Examples ----
    >>> ['a','b','c'] | replace_at[1]['x']    # => ['a','x','c']
    >>> 'hello' | replace_at[1]['a'] | c      # 'hallo'

    ---- Signature ----
    (VT.String, VT.Int, VT.String) -> VT.String
    (VT.List, VT.Int, VT.Any) -> VT.List

    ---- Tags ----
    - related zefop: replace
    - used for: list manipulation
    - operates on: String
    - operates on: List
    
    """
    from typing import Generator
    if isinstance(str_or_list, String):
        s = str_or_list
        char = str(new_el)
        if index >= len(s) or index < 0: return s
        if index == len(s) - 1: return s[:index] + char
        return s[:index] + char + s[index+1:] 
    elif isinstance(str_or_list, list) or isinstance(str_or_list, tuple) or isinstance(str_or_list, Generator) or isinstance(str_or_list, ZefGenerator):
        def wrapper():
            it = iter(str_or_list)
            c = 0
            try:
                while c < index:
                    yield next(it)
                    c += 1
                next(it)
                yield new_el
                yield from it
            except StopIteration:
                return
        return ZefGenerator(wrapper)
    else:
        return Error.TypeError(f"Expected an string or a list. Got {type(str_or_list)} instead.")



def replace_at_tp(op, curr_type):
    return VT.String





#---------------------------------------- floor -----------------------------------------------
def floor_imp(x):
    """
    The mathematical floor function.
    Rounds down to the next smallest integer.

    ---- Examples ----
    >>> 5.1 | floor      # => 5
    >>> 5.9 | floor      # => 5
    >>> 5.0 | floor      # => 5

    ---- Signature ----
    Float -> Int
    Int -> Int

    ---- Tags ----
    operates on: Float
    used for: maths
    related zefop: ceil
    related zefop: round
    """
    import math
    return math.floor(x)

#---------------------------------------- ceil -----------------------------------------------
def ceil_imp(x):
    """
    The mathematical ceil function.
    Rounds down to the next biggest integer.

    ---- Examples ----
    >>> 5.1 | ceil      # => 6
    >>> 5.9 | ceil      # => 6
    >>> 5.0 | ceil      # => 5

    ---- Signature ----
    Float -> Int
    Int -> Int

    ---- Tags ----
    operates on: Float
    used for: maths
    related zefop: floor
    related zefop: round
    """
    import math
    return math.ceil(x)


#---------------------------------------- round -----------------------------------------------
def round_imp(x):
    """
    The mathematical round function.
    Rounds down to the next integer.

    ---- Examples ----
    >>> 5.1 | round      # => 5
    >>> 5.9 | round      # => 6
    >>> 5.5 | round      # => 5
    >>> 5.0 | round      # => 5

    ---- Signature ----
    Float -> Int
    Int -> Int

    ---- Tags ----
    operates on: Float
    used for: maths
    related zefop: floor
    related zefop: ceil
    """
    import builtins
    return builtins.round(x)




#---------------------------------------- pad_left -----------------------------------------------
def pad_left_imp(s, target_length: int, pad_element=' '):
    """
    Pads a string to a specified length by inserting
    the pad_element on the left.
    If the input string is longer than the specified
    length, the original string is returned.
    The pad_element is optional, with ' ' being the
    default.
    
    ---- Examples ----
    >>> 'hi' | pad_left[5]['.']      # => '...hi'
    >>> 'hi' | pad_left[5]           # => '   hi'

    ---- Signature ----
    String -> String

    ---- Tags ----
    operates on: String
    used for: string manipulation
    related zefop: pad_right
    related zefop: pad_center
    related zefop: slice
    """
    if not isinstance(s, String): raise NotImplementedError()
    if len(pad_element) != 1: raise ValueError("pad_element must be of length 1 in 'pad_left'")
    l = len(s)
    return s if l>= target_length else f"{pad_element*(target_length-l)}{s}"



#---------------------------------------- pad_right -----------------------------------------------
def pad_right_imp(s, target_length: int, pad_element=' '):
    """
    Pads a string to a specified length by inserting
    the pad_element on the right.
    If the input string is longer than the specified
    length, the original string is returned.
    The pad_element is optional, with ' ' being the
    default.
    
    ---- Examples ----
    >>> 'hi' | pad_right[5]['.']      # => 'hi...'
    >>> 'hi' | pad_right[5]           # => 'hi   '

    ---- Signature ----
    String -> String

    ---- Tags ----
    operates on: String
    used for: string manipulation
    related zefop: pad_left
    related zefop: pad_center
    related zefop: slice
    """
    if not isinstance(s, String): raise NotImplementedError()
    if len(pad_element) != 1: raise ValueError("pad_element must be of length 1 in 'pad_right'")
    l = len(s)
    return s if l>= target_length else f"{s}{pad_element*(target_length-l)}"


#---------------------------------------- pad_center -----------------------------------------------
def pad_center_imp(s, target_length: int, pad_element=' '):
    """
    Pads a string to a specified length by inserting
    the pad_element on both sides, equally.
    If an odd number of pad_elements must be distributed,
    one more is added to the left.
    If the input string is longer than the specified
    length, the original string is returned.
    The pad_element is optional, with ' ' being the
    default.
    
    ---- Examples ----
    >>> 'hi' | pad_center[5]['.']      # => '..hi.'
    >>> 'hi' | pad_center[5]           # => '  hi '

    ---- Signature ----
    String -> String

    ---- Tags ----
    operates on: String
    used for: string manipulation
    related zefop: pad_left
    related zefop: pad_right
    related zefop: slice
    """
    from math import floor, ceil
    if not isinstance(s, String): raise NotImplementedError()
    if len(pad_element) != 1: raise ValueError("pad_element must be of length 1 in 'pad_center'")
    l = len(s)
    diff2 = (target_length-l)/2
    return s if l>= target_length else f"{pad_element*(ceil(diff2))}{s}{pad_element*(floor(diff2))}"





#---------------------------------------- random_pick -----------------------------------------------


def random_pick_imp(itr: VT.Any) -> VT.Any:
    """ 
    Given a list, tuple, string: returns a random item from that iterable.
    """
    import random
    return random.SystemRandom().choice(itr)

def random_pick_tp(op, curr_type):
    return VT.Any



#---------------------------------------- int_to_alpha -----------------------------------------------
def int_to_alpha_imp(n: int) -> str:
    """
    Map an integer n to the nth letter of the alphabet.
    Always lower case.

    ---- Examples ----
    >>> 3 | int_to_alpha    # => 'c'

    ---- Tags ----
    - operates on: Int
    - used for: string manipulation
    """
    d = 'abcdefghijklmnopqrstuvwxyz'
    return d[n]




#---------------------------------------- permute_to -----------------------------------------------
def permute_to_imp(v, indices: VT.List[VT.Int]):
    """
    given a input list, as well as a list of indices, 
    return the list of elements arranged according to the 
    list of indices.

    >>> ['a', 'b', 'c', 'd'] | permute_to[2,0,1]    # => ['c', 'a', 'b']

    ---- Tags ----
    - operates on: List
    - used for: list manipulation
    """
    cached = tuple(v | take[max(indices)+1])
    return (cached[n] for n in indices)



#---------------------------------------- is_alpha -----------------------------------------------
def is_alpha_imp(s: VT.String) -> VT.Bool:
    """ 
    Given a string return if it is only composed of Alphabet characters
    """
    return s.isalpha()

def is_alpha_tp(op, curr_type):
    return VT.Bool




#---------------------------------------- is_numeric --------------------------------------------------

def is_numeric_imp(x):
    """
    Given a string, determine if all characters are numeric, i.e. string
    representations of integers.

    ---- Examples ----
    >>> 'a' | is_numeric          # => False
    >>> '4' | is_numeric          # => True
    >>> '42' | is_numeric         # => True
    >>> '42.6' | is_numeric       # => False

    ---- Signature ----
    String -> Bool

    ---- Tags ----
    - related zefop: is_alpha
    - related zefop: is_alpha_numeric
    - operates on: String
    - used for: predicate function    
    """
    assert isinstance(x, String)
    return x.isnumeric()



#---------------------------------------- is_alpha_numeric --------------------------------------------

def is_alpha_numeric_imp(x):
    """
    Given a string, determine if all characters are numeric or 
    alphabetical letters. Any special symbols or characters 
    would cause this function to return False.

    ---- Examples ----
    >>> 'a' | is_alpha_numeric          # => True
    >>> '4' | is_alpha_numeric          # => True
    >>> '42abc' | is_alpha_numeric      # => True
    >>> 'hello!' | is_alpha_numeric     # => False
    >>> 'good morning' | is_alpha_numeric     # => False
    

    ---- Signature ----
    String -> Bool

    ---- Tags ----
    - related zefop: is_alpha
    - related zefop: is_numeric
    - operates on: String
    - used for: predicate function    
    """
    assert isinstance(x, String)
    return x.isalnum()


#---------------------------------------- to_upper_case -----------------------------------------------
def to_upper_case_imp(s: VT.String) -> VT.Bool:
    """ 
    Given a string, capitalize each character.

    ---- Examples ----
    >>> 'aBc' | to_upper_case          # => 'ABC'

    ---- Signature ----
    VT.String -> VT.String

    ---- Tags ----
    - related zefop: to_lower_case
    - related zefop: to_pascal_case
    - related zefop: to_camel_case
    - related zefop: to_kebab_case
    - related zefop: to_snake_case
    - related zefop: to_screaming_snake_case
    - operates on: String
    - used for: string manipulation
    """
    return s.upper()

def to_upper_case_tp(op, curr_type):
    return VT.Bool




#---------------------------------------- to_lower_case -----------------------------------------------
def to_lower_case_imp(s: VT.String) -> VT.Bool:
    """ 
    Given a string, convert each character to lower case.

    ---- Examples ----
    >>> 'aBc' | to_upper_case          # => 'abc'

    ---- Signature ----
    VT.String -> VT.String
    

    ---- Tags ----
    - related zefop: to_upper_case
    - related zefop: to_pascal_case
    - related zefop: to_camel_case
    - related zefop: to_kebab_case
    - related zefop: to_snake_case
    - related zefop: to_screaming_snake_case
    - operates on: String
    - used for: string manipulation
    """
    return s.lower()

def to_lower_case_tp(op, curr_type):
    return VT.Bool


#---------------------------------------- to_pascal_case -----------------------------------------------

def to_pascal_case_imp(s: VT.String) -> VT.String:
    """Convert a string to PascalCase style. Uses the caseconverter module.
    
    This is intended for use in producing variable names. It is also useful for
    generating token (e.g. `ET` or `RT`) names.

    ---- Examples ----
    >>> 'person_name' | to_pascal_case   # => "PersonName"
    >>> 'jsObject' | to_pascal_case      # => "JsObject"
    >>> 'external. data-with  UNUSUAL@characters' | to_pascal_case   # => "ExternalDataWithUnusualcharacters"

    ---- Signature ----
    String -> String

    ---- Tags ----
    - related zefop: to_lower_case
    - related zefop: to_upper_case
    - related zefop: to_camel_case
    - related zefop: to_kebab_case
    - related zefop: to_snake_case
    - related zefop: to_screaming_snake_case
    - operates on: String
    - used for: string manipulation
    """
    import caseconverter
    return caseconverter.pascalcase(s)

def to_pascal_case_tp(op, curr_type):
    return VT.String


#---------------------------------------- to_camel_case -----------------------------------------------
def to_camel_case_imp(s: VT.String) -> VT.String:
    """Convert a string to camelCase style. Uses the caseconverter module.
    
    This is intended for use in producing variable names.

    ---- Examples ----
    >>> 'person_name' | to_camel_case   # => "personName"
    >>> 'TokenName' | to_camel_case    # => "tokenName"
    >>> 'external. data-with  UNUSUAL@characters' | to_camel_case   # => "externalDataWithUnusualcharacters"

    ---- Signature ----
    String -> String

    ---- Tags ----
    - related zefop: to_lower_case
    - related zefop: to_upper_case
    - related zefop: to_pascal_case
    - related zefop: to_kebab_case
    - related zefop: to_snake_case
    - related zefop: to_screaming_snake_case
    - operates on: String
    - used for: string manipulation
    """
    import caseconverter
    return caseconverter.camelcase(s)

def to_camel_case_tp(op, curr_type):
    return VT.String


#---------------------------------------- to_kebab_case -----------------------------------------------
def to_kebab_case_imp(s: VT.String) -> VT.String:
    """Convert a string to kebab-case style. Uses the caseconverter module.
    
    This is intended for use in producing variable names.

    ---- Examples ----
    >>> 'person_name' | to_kebab_case   # => "person-name"
    >>> 'TokenName' | to_kebab_case    # => "token-name"
    >>> 'external. data-with  UNUSUAL@characters' | to_kebab_case   # => "external-data-with-unusualcharacters"

    ---- Signature ----
    String -> String

    ---- Tags ----
    - related zefop: to_lower_case
    - related zefop: to_upper_case
    - related zefop: to_pascal_case
    - related zefop: to_camel_case
    - related zefop: to_snake_case
    - related zefop: to_screaming_snake_case
    - operates on: String
    - used for: string manipulation
    """
    import caseconverter
    return caseconverter.kebabcase(s)

def to_kebab_case_tp(op, curr_type):
    return VT.String


#---------------------------------------- to_snake_case -----------------------------------------------
def to_snake_case_imp(s: VT.String) -> VT.String:
    """Convert a string to snake_case style. Uses the caseconverter module.
    
    This is intended for use in producing variable names.

    ---- Examples ----
    >>> 'yaml-keyword' | to_snake_case   # => "yaml_keyword"
    >>> 'TokenName' | to_snake_case    # => "token_name"
    >>> 'external. data-with  UNUSUAL@characters' | to_snake_case   # => "external_data_with_unusualcharacters"

    ---- Signature ----
    String -> String

    ---- Tags ----
    - related zefop: to_lower_case
    - related zefop: to_upper_case
    - related zefop: to_pascal_case
    - related zefop: to_camel_case
    - related zefop: to_kebab_case
    - related zefop: to_screaming_snake_case
    - operates on: String
    - used for: string manipulation
    """
    import caseconverter
    return caseconverter.snakecase(s)

def to_snake_case_tp(op, curr_type):
    return VT.String


#---------------------------------------- to_screaming_snake_case -----------------------------------------------
def to_screaming_snake_case_imp(s: VT.String) -> VT.String:
    """Convert a string to SCREAMING_SNAKE_CASE style. Uses the caseconverter module.
    
    This is intended for use in producing variable names.

    ---- Examples ----
    >>> 'yaml-keyword' | to_screaming_snake_case   # => "YAML_KEYWORD"
    >>> 'TokenName' | to_screaming_snake_case    # => "TOKEN_NAME"
    >>> 'external. data-with  UNUSUAL@characters' | to_screaming_snake_case   # => "EXTERNAL_DATA_WITH_UNUSUALCHARACTERS"

    ---- Signature ----
    String -> String

    ---- Tags ----
    - related zefop: to_lower_case
    - related zefop: to_upper_case
    - related zefop: to_pascal_case
    - related zefop: to_camel_case
    - related zefop: to_kebab_case
    - related zefop: to_snake_case
    - operates on: String
    - used for: string manipulation
    """
    import caseconverter
    return caseconverter.macrocase(s)

def to_screaming_snake_case_tp(op, curr_type):
    return VT.String

def make_request_imp(url, method='GET', params={}, data={}):
    return {
            "type":     FX.HTTP.Request,
            "url":      url,
            "method":   method,
            "params":   params,
            "data":     data,
    }

def make_request_tp(op, curr_type):
    return VT.Effect


#---------------------------------------- blake3 -----------------------------------------------
def blake3_imp(obj) -> VT.String:
    """
    Cryptographic hash function.
    Can be applied to strings and Bytes.

    ---- Examples ----
    >>> 'hello' | blake3    
    >>> b'hello' | blake3    
    
    ---- Signature ----
    String -> String
    Bytes -> String

    ---- Tags ----
    - used for: Cryptography
    - used for: Hashing
    - operates on: String
    - operates on: Bytes
    """
    # Hash some input all at once. The input can be bytes, a bytearray, or a memoryview.
    from blake3 import blake3 as b3
    if isinstance(obj, bytes):
        return b3(obj).hexdigest()
    elif isinstance(obj, String):
        return b3(obj.encode()).hexdigest()
    else:
        raise ValueError(f"Expected Bytes or Str but got {obj} instead.")

def blake3_tp(op, curr_type):
    return VT.String


def value_hash_imp(obj) -> VT.String:
    """
    Cryptographic hash function using blake3 
    for non-str/bytes values including Zef Values.

    ---- Examples ----
    >>> 5 | value_hash    
    >>> g | value_hash    
    
    ---- Signature ----
    Any -> String

    ---- Tags ----
    - used for: Cryptography
    - used for: Hashing
    - operates on: Values
    - operates on: Any
    """
    if isinstance(obj, Dict): # Handle dict specially to fix key,value pair order.
        return blake3_imp("dict" + str(sort(obj.items())))
    try:
        from ..op_structs import type_spec
        type_str = str(type_spec(obj))
    except:
        type_str = str(type(obj))
    return blake3_imp(type_str + str(obj))

def value_hash_tp(op, curr_type):
    return VT.String


#-------------------------------ZEF LIST------------------------------------------
def to_zef_list_imp(elements: list):
    all_zef = elements | map[lambda v: isinstance(v, ZefRef) or isinstance(v, EZefRef)] | all | collect
    if not all_zef: return Error("to_zef_list only takes ZefRef or EZefRef.")
    is_any_terminated = elements | map[events[VT.Terminated]] | filter[None] | length | greater_than[0] | collect 
    if is_any_terminated: return Error("Cannot create a Zef List Element from a terminated ZefRef")
    rels_to_els = (elements 
            | enumerate 
            | map[lambda p: (Any['zef_list'], RT.ZEF_ListElement[str(p[0])], p[1])] 
            | collect
            )

    new_rels = rels_to_els | map[second | absorbed | first | inject[Any] ] | collect
    next_rels = new_rels | sliding[2] | attempt[map[lambda p: (p[0], RT.ZEF_NextElement, p[1])]][[]] | collect


    return [
        [ET.ZEF_List['zef_list']],
        next_rels,
        rels_to_els
    ] | concat | collect

def to_zef_list_tp(op, curr_type):
    return VT.List



# -------------------------------- transact -------------------------------------------------
def transact_imp(data, g, **kwargs):
    from typing import Generator
    from ..graph_delta import construct_commands
    if is_a(data, FlatGraph):
        if isinstance(g, Graph):
            commands = flatgraph_to_commands(data)
        else:
            fg = data
            commands = g
            return fg_insert_imp(fg, commands)
    elif is_a(g, FlatGraph):
        fg = g
        commands = data
        return fg_insert_imp(fg, commands)
    elif type(data) in {list, tuple}:
        commands = construct_commands(data)
    elif isinstance(data, (Generator, ZefGenerator)):
        commands = construct_commands(tuple(data))
    else:
        raise ValueError(f"Expected FlatGraph or [] or () for transact, but got {data} instead.")

    return {
            "type": FX.Graph.Transact,
            "target_graph": g,
            "commands":commands
    }

def transact_tp(op, curr_type):
    return VT.Effect



#-----------------------------Range----------------------------------------
def range_imp(*args):
    """
    Introduce a separate operator from Python's range.
    
    1)  The lazy output value type should be uniform for other
    zefops to deal with. `range` does not return a general Python
    generator, but a special range object. We don't want to  make
    all other zefops that can operate on a lazy List[Int] to special
    case range.

    2) we want the ability to pipe the boundary values into `Range`.
    Range can take a single int or a pair of ints and returns a lazy
    List[Int].

    3) In contrast to Python's builtin `range`, Zef's `Range` can
    be called with infinity as an upper bound.

    ---- Signature ----
    Int -> List[Int]
    (Int, Int) -> List[Int]

    ---- Examples ----
    >>> 4 | Range         # => [0,1,2,3]   # but as a generator
    >>> (2, 5) | Range    # => [2,3,4]
    >>> Range(4)          # => [0,1,2,3]   # not lazy! The parentheses trigger evaluation for ZefOps!
    >>> infinity | Range  # => [0,1,2,3, ... # lazy and infinitely long
    >>> (4, infinity) | Range  # => [4,5,6, ... # lazy and infinitely long

    ---- Gotchas ----
    - calling with parentheses triggers evaluation. If this is 
      called for an infinite lazy sequence, the evaluation will not terminate.

    ---- Tags ----
    - used for: List manipulation

    """

    if len(args) == 1:
        a = args[0]
        # 10 | Range | ...
        if isinstance(a, Int) or a==infinity:
            lo, hi = 0, a        
        # (2, 5) | Range | ...
        elif len(a) == 2:
            if not (isinstance(a[0], Int) and (isinstance(a[1], Int) or a[1]==infinity)):
                raise TypeError(f'When called with two argument, Range takes two ints. Got: {a}')
            else: lo, hi = a
        else:
            raise TypeError(f'Range can be called with one or two integers. It got called with the wrong number of args: {a}')
    # Range(2, 5) | ...
    elif len(args) == 2:
        a = args
        if not (isinstance(a[0], Int) and (isinstance(a[1], Int) or a[1]==infinity)):
                raise TypeError(f'When called with two argument, Range takes two ints. Got: {a}')
        else: lo, hi = a
    else:
        raise TypeError(f'Range can be called with one or two integers. It got called with the wrong number of args: {args}')

    def generator_wrapper2():
        count2 = lo
        while True:
            yield count2
            count2 += 1

    def generator_wrapper1():
        yield from range(lo, hi)
    # don't expose the yield directly, keep this a function
    return ZefGenerator(generator_wrapper2 if hi==infinity else generator_wrapper1)


def range_tp(op, curr_type):
    return None


# -----------------------------Old Traversals-----------------------------
def traverse_implementation(first_arg, *curried_args, func_only, func_multi, func_optional, func_RT, func_BT, traverse_direction):
    translation_dict = {
        "outout": "Out",
        "out"  : "out_rel",
        "inin": "In",
        "in": "in_rel",
    }
    print(f"Old traversal style will be retired: use `{translation_dict[traverse_direction]}` instead. 🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱🥱 ")
    if isinstance(first_arg, FlatRef):
        return traverse_flatref_imp(first_arg, curried_args, traverse_direction)
    elif isinstance(first_arg, FlatRefs):
        return [traverse_flatref_imp(FlatRef(first_arg.fg, idx), curried_args, traverse_direction) for idx in first_arg.idxs]
    

    spec = type_spec(first_arg)         # TODO: why is this called "spec"? Not sure which specification it refers to.
    if len(curried_args) == 1:
        # "only" traverse
        if spec not in zef_types:
            raise Exception(f"Can only traverse ref types, not {spec}")
        return func_only(first_arg, curried_args[0])
    traverse_type,(rel,) = curried_args
    if traverse_type == RT.L:
        if spec not in zef_types:
            raise Exception(f"Can only traverse ref types, not {spec}")
        if rel==RT:
            return func_RT(first_arg)       # if only z1 > L[RT]   is passed: don't filter
        if rel==BT:
            return func_BT(first_arg)       # if only z1 > L[BT]   is passed: don't filter
        return func_multi(first_arg, rel)
    if traverse_type == RT.O:
        if spec == VT.Nil:
            return None
        elif spec in zef_types:
            return func_optional(first_arg, rel)
        else:
            raise Exception(f"Can only traverse ref types or None, not {spec}")
    raise Exception(f"Unknown traverse type {traverse_type}")
        
def out_rts(obj):
    return pyzefops.filter(obj | pyzefops.outs, BT.RELATION_EDGE)
def in_rts(obj):
    return pyzefops.filter(obj | pyzefops.ins, BT.RELATION_EDGE)
from functools import partial
OutOld_implementation = partial(traverse_implementation,
                             func_only=pyzefops.traverse_out_edge,
                             func_multi=pyzefops.traverse_out_edge_multi,
                             func_optional=pyzefops.traverse_out_edge_optional,
                             func_RT=out_rts,
                             func_BT=lambda zz: pyzefops.outs(pyzefops.to_ezefref(zz)),
                             traverse_direction="out",
                             )
InOld_implementation = partial(traverse_implementation,
                            func_only=pyzefops.traverse_in_edge,
                            func_multi=pyzefops.traverse_in_edge_multi,
                            func_optional=pyzefops.traverse_in_edge_optional,
                            func_RT=in_rts,
                            func_BT=lambda zz: pyzefops.ins(pyzefops.to_ezefref(zz)),
                            traverse_direction="in",
                            )
OutOutOld_implementation = partial(traverse_implementation,
                                func_only=pyzefops.traverse_out_node,
                                func_multi=pyzefops.traverse_out_node_multi,
                                func_optional=pyzefops.traverse_out_node_optional,
                                func_RT=lambda zz: pyzefops.target(out_rts(zz)),
                                func_BT=lambda zz: pyzefops.target(pyzefops.outs(pyzefops.to_ezefref(zz))),
                                traverse_direction="outout",
                                )
InInOld_implementation = partial(traverse_implementation,
                              func_only=pyzefops.traverse_in_node,
                              func_multi=pyzefops.traverse_in_node_multi,
                              func_optional=pyzefops.traverse_in_node_optional,
                              func_RT=lambda zz: pyzefops.target(in_rts(zz)),
                              func_BT=lambda zz: pyzefops.target(pyzefops.ins(pyzefops.to_ezefref(zz))),
                              traverse_direction="inin",
                              )








# ----------------------------- zstandard_compress -----------------------------



def zstandard_compress_imp(x: bytes, compression_level=0.1) -> Bytes:
    """
    Compress a Bytes value using ZStandard.
    The compression level can be specified optionally.
    
    ---- Example ----
    >>> 'hello' | to_bytes | zstandard_compress

    ---- Signature ----
    Bytes -> Bytes

    ---- Tags ----
    used for: data compression    
    operates on: Bytes
    related zefop: zstandard_compress
    """
    import zstd
    level = 1 + round(max(min(compression_level, 1), 0)*21)
    if isinstance(x, String): raise TypeError('zstandard_compress can`t be called with a string. Must be bytes')
    return Bytes(zstd.compress(bytes(x), level))






# ----------------------------- zstandard_decompress -----------------------------
def zstandard_decompress_imp(x: Bytes) -> Bytes:
    """
    Decompress a Bytes value using ZStandard.
    
    ---- Example ----
    >>> 'hello' | to_bytes | zstandard_compress | zstandard_decompress

    ---- Signature ----
    Bytes -> Bytes

    ---- Tags ----
    used for: data compression    
    operates on: Bytes
    related zefop: zstandard_decompress
    """
    import zstd
    if isinstance(x, String): raise TypeError('zstandard_decompress can`t be called with a string. Must be bytes')
    return Bytes(zstd.decompress(bytes(x)))
    





# ----------------------------- to_bytes -----------------------------
def to_bytes_imp(x: String) -> Bytes:
    """
    Convert a string to a ValueType Bytes using utf8 encoding.
    Python bytes are wrapped and Bytes are forwarded.
    
    ---- Example ----
    >>> 'hello' | bytes_to_base64string     # equivalent to Bytes(b'hello')

    ---- Signature ----
    String | Bytes -> Bytes

    ---- Tags ----
    used for: type conversion
    operates on: String
    related zefop: utf8bytes_to_string
    """
    from zef.core._bytes import Bytes_
    if isinstance(x, String): return Bytes(x.encode())     # default: utf8
    if isinstance(x, bytes): return Bytes(x)
    if isinstance(x, Bytes_): return x
    else: raise NotImplementedError()


# ----------------------------- utf8bytes_to_string -----------------------------
def utf8bytes_to_string_imp(b: Bytes) -> String | VT.Error:
    """
    Convert a Byes value that is a utf8 encoded string,
    return the string. Not all bytes values are valid utf8.

    
    ---- Example ----
    >>> 'hello' | to_bytes | utf8bytes_to_string     # => 'hello'

    ---- Signature ----
    Bytes -> String | Error

    ---- Tags ----
    used for: type conversion
    operates on: Bytes
    related zefop: to_bytes
    """
    return bytes(b).decode()




# ----------------------------- base64string_to_bytes -----------------------------
def base64string_to_bytes_imp(s: str) -> Bytes | VT.Error:
    """
    Convert data that is in a valid base64 encoding as a string to bytes.
    Not all strings are valid base64 encoded strings.
    Returns the decoded Bytes object
    
    ---- Example ----
    >>> 'hello' | bytes_to_base64string

    ---- Signature ----
    String | Bytes -> Bytes

    ---- Tags ----
    used for: type conversion
    operates on: String
    """
    import base64
    return Bytes(base64.b64decode(s))






# ----------------------------- bytes_to_base64string -----------------------------
def bytes_to_base64string_imp(b: Bytes) -> String:
    """
    Convert any bytes object into a base64 encoded string.
    
    ---- Example ----
    >>> b'hello' | bytes_to_base64string

    ---- Signature ----
    Bytes -> String

    ---- Tags ----
    used for: type conversion
    operates on: Bytes
    """
    import base64
    if isinstance(b, bytes): return base64.b64encode(b).decode()
    if b | is_a[Bytes]: return base64.b64encode(bytes(b)).decode()
    raise TypeError()
    




# ----------------------------- is_between_imp -----------------------------
def is_between_imp(x, lo, hi) -> Bool:
    """
    Checks whether a specified value lies within the 
    specified range for some orderable data type.
    Like the SQL counterpart, this operator is INCLUSIVE.

    ---- Examples ----
    >>> 5 | is_between[1][9]     # => True
    >>> 42 | is_between[1][9]    # => False
    >>> 9 | is_between[1][9]     # => True

    ---- Signature ----
    Tuple[List[T], T, T] -> Bool
    
    ---- Tags ----
    used for: logic
    used for: maths
    operates on: List[Int]
    operates on: List[Float]
    operates on: List[QuantityInt]
    operates on: List[QuantityFloat]
    """
    return x>=lo and x<=hi


def map_cat_imp(x, f):
    """ 
    Returns the flatten results of f applied to each item of x. Equivalent to:
    `x | map[f] | concat`.
 
    ---- Examples ----
    >>> [1,0,3] | flat_map[lambda x: x | repeat[x]]  # => [1, 3, 3, 3]
    >>> [z1,z2] | flat_map[Outs[RT]]             # All outgoing relations on z1 and z2.
 
    ---- Signature ----
    (Iterable, Function) => List
    """
    # This can be made more efficient
    return concat_implementation(map_implementation(x, f))

def map_cat_tp(x):
    return VT.List

def without_imp(x, y):
    """ 
    Returns the piped iterable without any items that are contained in the
    argument. As a special case, dictionaries are returned without any keys
    contained in the argument.

    Note the syntax of `| without[1]` is not supported. The argument must be an
    iterable.

    Note that the type of the output is determined by the following operators,
    e.g. `{1,2,3} | without[x] | collect` will return a list, even though the
    input is a set.
 
    ---- Examples ----
    >>> [1,2,3] | without[[2]]            # => [1,3]
    >>> {'a', 5, 10} | without['abc']     # => [10, 5]
    >>> {'a': 1, 'b': 2} | without[['a']] # => {'b': 2}
 
    ---- Signature ----
    (Iterable, Iterable) => List
    (Dict, Iterable) => Dict
    """

    # Because this will likely be surprising, make a special case of it here.
    # Though note that technically y doesn't have to be iterable for this to
    # work... TODO: make this more general and test for 'contains' support.
    try:
        y_itr = iter(y)
    except:
        return Error("The given argument to `without` is not iterable. If you have passed a single value, then you must wrap it in a list first, e.g. `| without[[1]]` instead of `| without[1]`.")
        
    if isinstance(x, Dict):
        return x | items | filter[first | Not[contained_in[y]]] | func[dict] | collect
    else:
        return x | filter[Not[contained_in[y]]] | collect

def without_tp(x):
    return VT.Any

def blueprint_imp(x, include_edges=False):
    """ 
    WARNING: the name/interface of this zefop is not stable and is likely to change in the future.

    Returns all delegate nodes on the graph or alive in the given GraphSlice.
    These are the delegate entities/relations which reference all instances on
    the graph.

    The optional argument `include_edges` can be set to True to also return the
    low-level edges between these nodes, which is useful for plotting with
    `graphviz`.
 
    ---- Examples ----
    >>> g | blueprint[True] | graphviz          # Shows the blueprint of graph g.
    >>> g | now | blueprint[True] | graphviz    # Shows the blueprint of graph g in the current time slice.

    >>> # A blank graph already has one TX delegate node.
    >>> g = Graph()
    ... g | blueprint | collect
    [<EZefRef #65 DELEGATE TX at slice=0>]
 
    ---- Signature ----
    (Graph, Bool) => List[EZefRef]
    (GraphSlice, Bool) => List[ZefRef]
    """

    if isinstance(x, Graph):
        g = x

        rules = {"from_source": [
            (Any, BT.TO_DELEGATE_EDGE, Any)
        ]}

        all_items = g | root | gather[rules] | collect

        all_equal = sliding[2] | map[unpack[equals]] | all
        is_delegate_rel_group = BT.RELATION_EDGE & Is[apply[identity, source, target] | all_equal]

        trim_items = [g | root | collect] + (all_items | filter[is_delegate_rel_group] | collect)
        trim_items += trim_items | map_cat[out_rels[BT.TO_DELEGATE_EDGE]] | collect

        all_items = all_items | without[trim_items] | collect

        if not include_edges:
            all_items = all_items | filter[Not[is_a[BT.TO_DELEGATE_EDGE]]] | collect

        return all_items

    elif isinstance(x, GraphSlice):
        rae_satisfies = match[
            (Delegate, exists_at[x]),
            (Any, aware_of[x])
        ]
        satisfies = match[
            (BT.TO_DELEGATE_EDGE, target),
            (Any, identity)
        ] | rae_satisfies
        return (blueprint_imp(Graph(x), include_edges)
                # We need to be careful of the filtering here. Only RAEs (including delegates) can be used with exists_at.
                 | filter[satisfies]
                # allow_tombstone is set here to bypass exists_at checking
                 | map[in_frame[x][allow_tombstone]]
                | collect)

    return Error(f"Don't know how to handle type of {type(x)} in blueprint zefop")


# ----------------------------- field -----------------------------

@func
def field_imp(z, rt):
    """
    Opinionated operator that is part of high level zef.

    1) If the final node at the end of the path is an AET,
       the `value` zefop is automatically applied. i.e. The
       value and not the ZefRef to the node is returned.

    2) Automatically maps to the right nesting order, it
       essentially wraps with the according number of "map"s
    >>> [z1, z2, z3] | F.Name      # equivalent to 
    >>> [z1, z2, z3] | map[Out[RT.Name] | value]


    ***** TODO: allow traversing the "implied graphs", given ********
          a base graph and set of rules.

    rules = [
            Implies[ (ET.Person['p'], RT.Directed, ET.Movie['m']), (ET.Movie['m'], RT.Director, ET.Person['p']) ],

            Implies[ (Any['y'], RT.BirthYear, Any['x']), (Any['y'], RT.YearOfBirth, Any['x']) ],
            ]
    
    
    ---- Signature ----
    (ZefRef, RT) -> ZefRef
    (List[ZefRef], RT) -> List[ZefRef]
    (List[List[ZefRef]], RT) -> List[List[ZefRef]]

    ----Examples ----
    >>> z1 | F.Name         # equivalent to z1 | Out[RT.Name] | value     # if target is an AET
    >>> z1 | F.FriendOf     # equivalent to z1 | Out[RT.Name]             # if target is an ET or RT

    ---- Tags ----
    - related zefop: fields
    - related zefop: Out
    - used for: graph traversal
    """
    def val_maybe(x):
        if is_a(x, AttributeEntity): return value(x)
        else: return x

    if isinstance(z, ZefRef):
        return val_maybe(Out(z, rt))

    if isinstance(z, list) or isinstance(z, tuple):
        return [field(zz, rt) for zz in z]

    raise TypeError(f"Field operator not implemented for type(z)={type(z)}    z={z}")


# ----------------------------- fields -----------------------------


@func
def fields_imp(z, rt):
    """
    Closely related to the `field` operator, but use this for
    traversing a variable number of outgoing relations, similar
    to `Outs`.

    If any of the target nodes is an AET, the value is automatically
    used.

    ---- Signature ----
    (ZefRef, RT) -> List[ZefRef]
    (List[ZefRef], RT) -> List[List[ZefRef]]

    ----Examples ----
    >>> z1 | F.Name         # equivalent to z1 | Out[RT.Name] | value     # if target is an AET
    >>> z1 | F.FriendOf     # equivalent to z1 | Out[RT.Name]             # if target is an ET or RT

    ---- Tags ----
    - related zefop: field
    - related zefop: Outs
    - used for: graph traversal
    """
    def val_maybe(x):
        if is_a(x, AET): return value(x)
        else: return x

    if isinstance(z, ZefRef):
        return [val_maybe(x) for x in Outs(z, rt)]

    if isinstance(z, list) or isinstance(z, tuple):
        return [fields(zz, rt) for zz in z]

    raise TypeError(f"`fields` operator not implemented for type(z)={type(z)}    z={z}")




# ----------------------------- apply -----------------------------
def apply_imp(x, f):
    """
    Very similar to func to apply a function
    to an input argument.
    In addition, a tuple of functions (f1,..,.fn) can 
    be specified and the output is is the tuple of
    the individual output values.
    Note that this procedure is distinct from map, as
    it acts on the single flow input argument once.

    ---- Signature ----
    (T,  (T->T2) ) -> T2    # if f is a single function
    (T, List[(T->T2)] ) -> List[T2]

    ---- Examples ----
    >>> 40 | apply[
    ...    add[1],
    ...    add[2],
    ...    String,
    ... ]
    (41, 42, '40')
    
    ---- Tags ----
    - related zefop: map
    - related zefop: apply_functions
    - related zefop: func
    - related zefop: call
    - used for: control flow
    - used for: function application
    """
    if isinstance(f, tuple) or isinstance(f, list):
        return tuple(apply(x, ff) for ff in f)
    else:
        return call_wrap_errors_as_unexpected(f, x)
            
        




# ----------------------------- split_on_next -----------------------------
def split_on_next_imp(s, el_to_split_on):
    """
    Split a List or a string at one point only

    ---- Examples ----
    >>> before, after = split_on_next('good morning, good afternoon, good night', ', ')     # => ['good morning', 'good afternoon, good night']
    >>> [1,2,3,4,5,3,8] | split_on_next[3]    # => (1,2), (4, 5, 3, 8)

    ---- Signature ----
    (String, String) -> String
    (List[T], T) -> List[T]

    ---- Tags ----
    - related zefop: split
    - related zefop: chunk
    - operates on: List
    - operates on: String
    - used for: list manipulation
    - used for: string manipulation
    """
    from typing import Generator
    if isinstance(s, String):
        ind = s.find(el_to_split_on)
        if ind == -1: return s, ''    # not found
        return s[:ind], s[ind+len(el_to_split_on):]
    if isinstance(s, list) or isinstance(s, tuple) or isinstance(s, Generator) or isinstance(s, ZefGenerator):
        def outer_wrapper():
            it = iter(s)
            part1 = []
            while True:
                val = next(it)
                if val == el_to_split_on: break
                part1.append(val)
            yield part1
            def wrapper():      # keep split_on_next a function
                yield from it
            yield wrapper()

        return ZefGenerator(outer_wrapper)

    raise TypeError(f"expected a String or a List in `split_on_next`, got a {type(s)}")



# ----------------------------- ops acting on op doctstring -----------------------------
def examples_imp(op: VT.ZefOp) -> VT.List[VT.Tuple]:
    """
    Returns the examples portion of a docstring as a list of tuples mapping
    example string to a possible output string.

    It is able to handle multiline result, but not multine examples. This
    will be later improved.

    ---- Examples ----
    >>> examples(to_snake_case)
    [("'yaml-keyword' | to_snake_case", '"yaml_keyword"'),
    ("'TokenName' | to_snake_case", '"token_name"'),
    ("'external. data-with  UNUSUAL@characters' | to_snake_case",
    '"external_data_with_unusualcharacters"')]

    ---- Signature ----
    (ZefOp) -> List[Tuple[String | Nil]]

    ---- Tags ----
    - related zefop: tags
    - related zefop: signature
    - related zefop: related_ops
    - related zefop: operates_on
    - related zefop: used_for
    - operates on: ZefOp
    - used for: op usage
    """
    def parse_example(s):
        s = trim(s, " ")

        # Case of empty lines
        if len(s) == 0: return None

        # Case of comments
        if s[0] == "#": raise ValueError(f"Comments aren't allowed on new-lines without >>> before them so not to be confused with an output that starts with #! {s}")

        # ... <example one-liner 2>
        if s[:3] == "...":
            example = s[3:] | trim[" "] | collect
            comment_idx = example.find('#')
            if comment_idx != -1: example = example[:comment_idx] | trim[" "] | collect
            return (example, None, "multi-example")

        # case of example lines
        if s[:3] == ">>>":
            result = None
            s = s | trim[">>>"] | trim[" "] | collect

            # case of '>>>' only
            if len(s) == 0: return None                 

            # case of '>>> <example one-liner 1>       # => <result of the one-liner (a value)>'
            s = s.replace("# ->", "# =>").replace("=>", "# =>") # TODO relax this later i.e make it break on malformation
            result_idx = s.find('# =>')
            if result_idx != -1:
                result = s[result_idx + 4:] | trim[" "] | collect
                comment_idx = result.find('#')
                if comment_idx != -1: result = result[:comment_idx] | trim[" "] | collect
                s = s[:result_idx] | trim[" "] | collect

            # case of '>>> <example one-liner 2>       # <comment on the behaviour (not a value)>'
            comment_idx = s.find('#')
            if comment_idx != -1:
                s = s[:comment_idx] | trim[" "] | collect
                if len(s) == 0: return None

            return s, result

        # case of '(single line or multine output )'
        else:
            return (None, s, "multi-result")

    def process_examples(tups):
        def join_if_not_empty(s1, s2):
            if s2: return s1 + "\n" + s2
            return s1

        result = []
        curr_result = curr_example = ""
        for t in reversed(list(tups)):
            if t[-1] and t[-1] == "multi-result": 
                curr_result = join_if_not_empty(t[1], curr_result)
                # curr_result = t[1] + "\n" + curr_result
            elif t[-1] and t[-1] == "multi-example": 
                curr_example = join_if_not_empty(t[0], curr_example)
                # curr_example = t[0] + "\n" + curr_example
            elif not t[-1]:
                example = join_if_not_empty(t[0], curr_example)
                result.append((example, curr_result))
                curr_result = curr_example = ""
            else:
                if curr_example or curr_result: raise ValueError(f"Malformed example. A multiline output or example contained also an inline comment. This isn't allowed! {t}")
                result.append(t)
        return result[::-1]


    s = docstring(op) | split["\n"] | collect
    example_idx = s.index("---- Examples ----")
    return (
        s 
        | skip[example_idx + 1] 
        | take_while[lambda l: l[:4] != "----"] 
        | map[parse_example]
        | filter[lambda l: l != None]
        | func[process_examples]
        | collect
    )


def signature_imp(op: VT.ZefOp) -> VT.List[VT.String]:
    """
    Given the signature portion of a docstring is valid, returns back a List of Strings.

    ---- Examples ----
    >>> signature(apply)
    ['(T,  (T->T2) ) -> T2', '(T, List[(T->T2)] ) -> List[T2]']

    >>> signature(schema)
    ['(Graph, Bool) -> List[EZefRef]', '(GraphSlice, Bool) -> List[ZefRef]']

    ---- Signature ----
    (ZefOp) -> List[String]

    ---- Tags ----
    - related zefop: tags
    - related zefop: examples
    - related zefop: related_ops
    - related zefop: operates_on
    - related zefop: used_for
    - operates on: ZefOp
    - used for: op usage
    """
    def clean_and_eval(s):
        s = s | replace['=>']['->'] | collect
        commment_idx = s.find("#")
        if commment_idx != -1: s = s[:commment_idx]
        return s | trim[' '] | join | collect
        # extra_eval = lambda expr: eval(expr, globals(), {**VT.__dict__})
        # try:
            # val_type = extra_eval(s)
        # except:
            # raise ValueError(f"The following '{s}'' signature is malformed and can't be parsed! Make sure all types present in the signature are valid ValueTypes")
        # return val_type


    s = LazyValue(op) | docstring | split["\n"] | collect
    try:
        signature_idx = s.index("---- Signature ----")
    except:
        raise ValueError(f"The docstring for {op} is either malformed or missing a Signature section!")  from None
    signature = (
        s 
        | skip[signature_idx + 1] 
        | take_while[lambda l: l[:4] != "----"] 
        | filter[lambda l: l != ""]
        | map[clean_and_eval]
        | collect
    )
    return signature


def tags_imp(op: VT.ZefOp) -> VT.List:
    """
    Returns the tags portion of a docstring as a list of list of strings.

    ---- Examples ----
    >>> tags(map)
    [['used for', 'control flow'],
    ['used for', 'function application'],
    ['related zefop', 'apply_functions']]

    ---- Signature ----
    (ZefOp) -> List[String]

    ---- Tags ----
    - related zefop: signature
    - related zefop: examples
    - related zefop: related_ops
    - related zefop: operates_on
    - related zefop: used_for
    - operates on: ZefOp
    - used for: op usage
    """
    try:
        return (LazyValue(op) 
        | docstring 
        | split["\n"]
        | skip_while[Not[equals['---- Tags ----']]] 
        | skip[1]
        | take_while[lambda s: s[:4]!='----']
        | map[trim_left['-']]
        | filter[~SetOf('')]
        | map[split[':'] | map[trim[' ']]]
        | collect
    )
    except:
        return []


def related_ops_imp(op: VT.ZefOp) -> VT.List[VT.ZefOp]:
    """
    Extracts the related ops from the tags portion of the docstring of the op.
    It returns back a list of ZefOp. It also discards invalid ZefOps.

    ---- Examples ----
    >>> related_ops(to_snake_case)
    [to_lower_case, to_upper_case, to_pascal_case, to_camel_case, to_kebab_case, to_screaming_snake_case]

    ---- Signature ----
    (ZefOp) -> List[ZefOp]

    ---- Tags ----
    - related zefop: signature
    - related zefop: examples
    - related zefop: tags
    - related zefop: operates_on
    - related zefop: used_for
    - operates on: ZefOp
    - used for: op usage
    """
    from ... import ops
    tags_lines = tags(op) | collect
    return (
        tags_lines 
        | filter[first |equals["related zefop"]]
        | map[second | func[lambda s: ops.__dict__.get(s, None)]]
        | filter[lambda el: el != None]
        | collect
    )

def operates_on_imp(op: VT.ZefOp) -> VT.List[VT.ValueType]:
    """
    Extracts the operates on Types from the tags portion of the docstring of the op.
    It returns back a list of ValueTypes. It also discards invalid ValueTypes.

    ---- Examples ----
    >>> operates_on(blake3)
    [String, Bytes]

    ---- Signature ----
    (ZefOp) -> List[ValueType]

    ---- Tags ----
    - related zefop: signature
    - related zefop: examples
    - related zefop: tags
    - related zefop: related_ops
    - related zefop: used_for
    - operates on: ZefOp
    - used for: op usage
    """
    tags_lines = tags(op) | collect
    return (
        tags_lines 
        | filter[first |equals["operates on"]]
        | map[second | func[lambda s: VT.__dict__.get(s, None)]]
        | filter[lambda el: el != None]
        | collect
    )

def used_for_imp(op: VT.ZefOp) -> VT.List[VT.String]:
    """
    Extracts the used for from the tags portion of the docstring of the op.

    ---- Examples ----
    >>> used_for(append)   # => ["list manipulation", "string manipulation"]

    ---- Signature ----
    (ZefOp) -> List[String]

    ---- Tags ----
    - related zefop: signature
    - related zefop: examples
    - related zefop: tags
    - related zefop: related_ops
    - related zefop: used_for
    - operates on: ZefOp
    - used for: op usage
    """
    tags_lines = tags(op) | collect
    return (
        tags_lines 
        | filter[first |equals["used for"]]
        | map[second]
        | collect
    )




def indexes_of_imp(v, ElType):
    """
    Given a list, returns the indexes of the elements which are of
    a specified logic type / set.

    ---- Signature ----
    (List[Any], ValueType) -> List[Int]

    ---- Examples ----
    >>> me = ["foo", 5,"bar", "baz", 'bar'] | indexes_of[SetOf["bar",]] | c  # => [2, 4]

    ---- Tags ----
    operates on: List
    """
    return v | enumerate | filter[second | is_a[ElType]] | map[first] | collect








def gather_imp(initial: List[ZefRef | EZefRef] | ZefRef, rules, max_step = float('inf')) -> Set[ZefRef]:
    """ 
    An operator that given a launch point on a graph, gathers up 
    a subgraph by traversing based on a rules pattern that is specified.
    There are two main modes of usage: 

    ---------------- A: Explicit Rules -----------------
    Giving an explicit rule set:
    >>> rules = {
    >>>     'from_source': [
    >>>             (Any, RT.Director, ET.Person),
    >>>             (Any,  RT.FirstName, AET.String),
    >>>     ],
    >>>     'from_target': [
    >>>         (Any, RT.Writer, ET.Person,),
    >>>     ]
    >>> }
    >>> z1 | gather[rules] | collect

    will iteratively traverse the graph until there is nothing more to be collected.
    What do the rules specify in this case? For example, the first rule (ET.A, RT.R1, ET.B)
    being part of 'from_source' mean that if one were to stand on a node of type "ET.A" as a source,
    with an outgoing relation of type "RT.R1" which leads to a "ET.B", then take that step
    and include both the edge and the "ET.B" in the gathered set.

    ---------------- B: Implicit Rules -----------------
    For certain entity types, predefined rule sets are associated.
    B1) For ET.ZEF_Function, a rule set to gather all things related to that function is predefined.
    Future: Also for ET.ZEF_EffectModule, ET.ZEF_Op
    """


    # ------------------------------ catch all cases for implicit rules ----------------------------------
    # TODO enable this once ZefFunctions are stable
    # if rules is None:
    #     if z_initial | is_a[ET.ZEF_Function] | collect:
    #         zef_fct_rules =  [
    #                     (ET.ZEF_Function, RT.PythonSourceCode, AET.String),
    #                     (ET.ZEF_Function, RT.OriginalName,     AET.String),
    #                     (ET.ZEF_Function, RT.Binding,          ET.ZEF_Function),
    #                     (RT.Binding,      RT.Name,             AET.String),
    #                     (RT.Binding,      RT.UseTimeSlice,     AET.String),
    #                 ]
    #         return gather_imp(z_initial, zef_fct_rules)           
    #     else:
    #         raise TypeError(f"Implicit rules not defined in 'gather' operator for {rae_type(z_initial)}")


    # --------------------------------- verify the rules data structure-------------------------------
    # TODO: once type checking is in place, hoist this out of the body into the function signature
    ValidTriple    = Tuple & Is[length | equals[3]]
    ValidRuleList  = List & Is[ map[is_a[ValidTriple]] | all  ] 
    ValidRulesDict = Dict & (
        (Is[contains['from_source']] & Is[get['from_source'] | is_a[ValidRuleList]]) | 
        (Is[contains['from_target']] & Is[get['from_target'] | is_a[ValidRuleList]])
    )

    if not rules | is_a[ValidRulesDict] | collect:
        return Error(f'`gather` called with an invalid set of rules: {rules}')


    # --------------------------------- explicit rules -------------------------------
    def traverse_rae_step(z: ZefRef) -> List[ZefRef]:

        def from_single_rule_from_source(triple) -> List[ZefRef]:
            src_tp, rel_tp, trg_tp = triple
            if not is_a[src_tp](z):
                return []
            return z | out_rels[rel_tp][trg_tp] | map[lambda rel: (rel, target(rel))] | concat | collect

        def from_single_rule_from_target(triple) -> List[ZefRef]:
            src_tp, rel_tp, trg_tp = triple
            if not is_a[trg_tp](z):
                return []
            return z | in_rels[rel_tp][src_tp] | map[lambda rel: (rel, source(rel))] | concat | collect
 
        from_source_rules = rules.get('from_source', [])
        from_target_rules = rules.get('from_target', [])
        return (
            from_source_rules | map[from_single_rule_from_source] | concat | collect,
            from_target_rules | map[from_single_rule_from_target] | concat | collect,
            ) | concat | collect

    def step(d: dict) -> dict:
        new_gathered = {*d['gathered'], *d['frontier']}
        new_frontier = d['frontier'] | map[traverse_rae_step] | concat | filter[Not[contained_in[new_gathered]]] | collect
        return {
            'gathered': new_gathered,
            'frontier': new_frontier,
            'iteration': d['iteration'] + 1.0,
        }
    
    frontier_is_empty = get['frontier'] | length | equals[0]
    max_step_reached  = get['iteration'] | equals[max_step]
    stop_condition    = Or[frontier_is_empty][max_step_reached]


    if isinstance(initial, (list,set)):
        initial = set(initial)
    elif is_a(initial, ZefRef) or is_a(initial, EZefRef):
        initial = {initial}
    else:
        raise TypeError(f'`gather` called with an invalid initial value: {initial}')

    return {
        'gathered': set(),
        'frontier': initial,
        'iteration': 0.0,
    } | iterate[step] | take_until[stop_condition] | last | get['gathered'] | collect



# ----------------------------- alias -----------------------------
def alias_imp(vt, name: str):
    """ 
    Give a ValueType an alias: a name to show in the
    repr's output to shorten and make the output more readable.

    ---- Examples ----
    >>> StringOrFloat = (String | Float) | alias('StringOrFloat')

    ---- Signature ----
    (ValueType, String)    -> ValueType

    ---- Tags ----
    - operates on: ValueType
    - used for: readability
    """
    vt2 = ValueType_(type_name=vt._d['type_name'], absorbed=vt._d['absorbed'])
    vt2._d['alias'] = name
    return vt2





# ----------------------------- splice -----------------------------
def splice_imp(x, start_pos, els_to_replace, replacement):
    """
    "splice in" or insert a replacement sequence or string into
    a sequence at a given position.

    ---- Examples ----
    >>> 'good morning world' | splice[5][7]['afternoon']         # 'good afternoon world'
    >>> Range(0, 5) | splice[2][1][('a','b', 'c')]               # (0, 1, 'a', 'b', 'c', 3, 4)

    ---- Signature ----
    (Graph, str, bool) -> Effect
    (ZefRef, str, bool) -> LazyValue

    ---- Tags ----
    - operates on: String
    - operates on: List
    - used for: list manipulation
    - used for: string manipulation
    - related zefop: replace_at
    - related zefop: replace
    """
    if isinstance(x, String):
        return x[:start_pos] + replacement + x[start_pos + els_to_replace:]
    elif isinstance(x, list) or isinstance(x, tuple):
        return (*x[:start_pos], *replacement, *x[start_pos + els_to_replace:])
    
    elif isinstance(x, ZefGenerator):        
        # handle both the dataflow arg and the replacement as generators
        def wrapper():
            it1 = iter(x)
            it2 = iter(replacement)
            for _ in range(start_pos):
                yield next(it1)
            yield from it2
            for _ in range(els_to_replace):
                next(it1)     # throw these away
            yield from it1
        return ZefGenerator(wrapper)

    else:
        return Error(f'Unsupported type passed to "splice": {type(x)}')






def parse_imp(data: str, grammar: str) -> FlatGraph:
    """
    Parse a string with a given grammar and return a flat graph.
    Uses the Lark parser.

    
    ---- Examples ----
    >>> grammar = '''
    ... ?start: sum
    ... 
    ... ?sum: term
    ...   | sum "+" term        -> add
    ...   | sum "-" term        -> subtract
    ... 
    ... ?term: item
    ...   | term "*"  item      -> multiply
    ...   | term "/"  item      -> divide
    ... 
    ... ?item: NUMBER           -> number
    ...   | "-" item            -> negative
    ...   | CNAME               -> variable
    ...   | "(" start ")"
    ... 
    ... %import common.NUMBER
    ... %import common.WS
    ... %import common.CNAME
    ... %ignore WS
    ... '''
    ... 
    ... "(5 * (3 / x)) + y - 1 + 7" | parse[grammar] | graphviz | c


    ---- Tags ----
    - operates on: String
    - used for: parsing
    """    
    import lark
    from .to_flatgraph import to_flatgraph_imp
    def lark_2_dict(tr):
        """convert lark tree to dict"""

        get_val_as_str = lambda x: x if isinstance(x, str) else x.value

        if isinstance(tr, lark.tree.Tree):
            return {
                # 'type': tr.data.value,        # we don't need to know that this is a RULE. 
                'type': get_val_as_str(tr.data),        # we don't need to know that this is a RULE. 
                'children': [lark_2_dict(c) for c in tr.children]
            }
        elif isinstance(tr, lark.lexer.Token):
            return {
                'type': tr.type,
                'value': tr.value
            }

    parser = lark.Lark(grammar)
    tree = parser.parse(data)    
    return to_flatgraph_imp(lark_2_dict(tree))





def flatten_dict_imp(y):
    """
    Given a potentially nested plain data structure 
    (anything isomorphic to JSON), return a flattened 
    version where the keys are paths given as tuples.

    ---- Examples ----
    >>> {'a': 42} | flatten_dict     # => [ ('a',): 42]
    >>> {'a': 42, 'b': {'x': 'hello'}} | flatten_dict     # => [ ('a',): 42, ('b', 'x'): 'hello']    

    ---- Tags ----
    related zefop: unflatten_dict
    related zefop: get_in
    related zefop: insert_in
    operates on: Dict
    operates on: List    
    used for: dict manipulation
    used for: list manipulation
    """
    def is_terminal(x):
        return type(x) in {int, float, str, bool, type(None)}
    
    def f(d: Dict, path: tuple) -> tuple:
        if isinstance(d, Dict):
            return tuple(
                    (((*path, k), v), ) if is_terminal(v) else f(v, (*path, k)) for k, v in d.items()
                ) | concat
        elif type(d) in {list, tuple}:
            return tuple(
                    (((*path, i), v), ) if is_terminal(v) else f(v, (*path, i)) for i, v in enumerate(d)
                ) | concat            
        else:
            raise TypeError(f'{type(d)} is not a dict or a list')

    if is_terminal(y):
        return {(): y}
    return dict(f(y, ()))





def unflatten_dict_imp(d: Dict) -> Dict:
    """
    Given a flattened dictionary, return a nested version.
    
    *Caution*: integer keys are also used as dictionary keys
    for now, i.e. no list is constructed. This may change in 
    the future

    ---- Examples ----
    >>> {('a',): 42, ('b', 'd'): 'hello'} | unflatten_dict   # => {'a': 42, 'b': {'d': 'hello'}}

    ---- Tags ----
    related zefop: flatten_dict
    operates on: Dict
    used for: dict manipulation
    """
    @func
    def insert_in_dict_with_idx(d: dict, path, value):
        assert isinstance(path, list) or isinstance(path, tuple)
        res = {**d}    
        # purely side effectful
        def insert(obj, path):      

            def _insert_list(ll, path):
                # If the next element in the path is an int then we need to insert a new list
                if isinstance(path[1], Int):
                    ll.append([])

                # If the next element in the path is a string then we could need to insert a new dict
                else:
                    if path[0] == len(ll): # Only insert a new dict if it wasn't inserted before in this path.
                        ll.append({})
                
            def _insert_dict(new_d, path):
                if path[0] not in new_d:

                    if isinstance(path[1], Int):
                        new_d[path[0]] = []
                    else: 
                        new_d[path[0]] = {}
                else:
                    if not isinstance(path[1], Int):
                        new_d[path[0]] = {**new_d[path[0]]}

            
            # If it is the only remaining object, then it is the object to be added or appended
            if len(path) == 1:

                if isinstance(path[0], Int): 
                    obj.append(value)
                else: 
                    obj[path[0]] = value

            # Depending on the current state of our path, we could either have a list or a dict
            else: 
                if isinstance(obj, Dict):
                    _insert_dict(obj, path)
                elif isinstance(obj, list): 
                    _insert_list(obj, path)

                insert(obj[path[0]], path[1:])        

        insert(res, path)        
        return res

    if () in d:
        return d[()]    
    op = d | items | map[inject_list[insert_in_dict_with_idx]] | to_pipeline | collect
    return op({})


def token_name_imp(raet: RAET) -> String:
    if isinstance(raet, (ET, RT, AET, BT)):
        from ..VT.rae_types import RAET_get_token
        token = RAET_get_token(raet)
        if token is None:
            raise Exception("No token inside of RAET")
        assert isinstance(token, (EntityTypeToken, RelationTypeToken, AttributeEntityTypeToken, BlobTypeToken))
    else:
        assert isinstance(raet, (EntityTypeToken, RelationTypeToken, AttributeEntityTypeToken, BlobTypeToken))
        token = raet

    if isinstance(token, BlobTypeToken):
        return str(token)
    else:
        return token.name