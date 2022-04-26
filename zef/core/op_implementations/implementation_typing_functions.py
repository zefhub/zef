import builtins
from typing import Generator, Iterable, Iterator


from rx import operators as rxops
from ..VT.value_type import ValueType

# This is the only submodule that is allowed to do this. It can assume that everything else has been made available so that it functions as a "user" of the core module.
from .. import *
from ..op_structs import _call_0_args_translation, type_spec
from .._ops import *
from ..abstract_raes import abstract_rae_from_rae_type_and_uid

from ...pyzef import zefops as pyzefops, main as pymain
from ..internals import BaseUID, EternalUID, ZefRefUID, BlobType, EntityTypeStruct, AtomicEntityTypeStruct, RelationTypeStruct, to_uid, ZefEnumStruct, ZefEnumStructPartial
from .. import internals
import itertools
from typing import Generator, Iterable, Iterator

zef_types = {VT.Graph, VT.ZefRef, VT.ZefRefs, VT.ZefRefss, VT.EZefRef, VT.EZefRefs, VT.EZefRefss}
ref_types = {VT.ZefRef, VT.ZefRefs, VT.ZefRefss, VT.EZefRef, VT.EZefRefs, VT.EZefRefss}


#--utils---
def curry_args_in_zefop(zefop, first_arg, nargs = ()):
    for arg in nargs: zefop = zefop[arg]
    return zefop(first_arg) if callable(zefop) else zefop[first_arg]

def parse_input_type(input_type: ValueType) -> str:
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
 
    <description of following example>
    >>> <example one-liner with multiline result>
    ... <result line 1>
    ... <result line 2>
 
    <description of following example>
    >>> <multiline example line 1>
    >>> <multiline example line 2>
    >>> <multiline example line 3>  # => <result of multiline example>
 
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
        return fct(x0, *args, **kwargs)
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
    >>> g | on[value_assigned[AET.String]]              # value_assigned[z3]['hello!']      c.f. with action: assign_value[z3]['hello!']
    >>> g | on[terminated[z2]]                          # terminated[z2], followed by completion_event
    >>> g | on[instantiated[ET.Foo]]                    # instantiated[z5]
    >>> 
    >>> # listening for new relations
    >>> # old syntax:  zz | subscribe[on_instantiation[outgoing][RT.Foo]][my_callback]       old syntax
    >>> g | on[ instantiated[(zz, RT.Foo, Z)] ]         # Z matches on anything, i.e. takes on role of "_"
    >>>
    >>> One can also add more precise requirements
    >>> g | on[ instantiated[(zz, RT.Foo, ET.Bar)] ]    # an element "instantiated[z_rel]" is pushed into the stream. Instances are represented by a single ZefRef, types by a triple for relations
    >>> g | on[ terminated[(zz, RT.Foo, Z)] ]
    >>>
    >>> g | on[terminated[z_rel]]

    ---- Signature ----
    (Graph, ZefOp[ValueAssigned]) -> Stream[ZefOp[ValueAssigned[ZefRef][Any]]]
    (Graph, ZefOp[Instantiated]) -> Stream[ZefOp[Instantiated[ZefRef]]]
    (Graph, ZefOp[Terminated]) -> Stream[ZefOp[Terminated[ZefRef]]]
    
    ...


    """
    assert isinstance(op, ZefOp)
    assert len(op.el_ops) == 1
    from ...pyzef import zefops as internal
    from ..fx import FX, Effect

    stream =  Effect({'type': FX.Stream.CreatePushableStream}) | run
    sub_decl = internal.subscribe[internal.keep_alive[True]]
    
    if isinstance(g, Graph):
        op_kind = op.el_ops[0][0]
        op_args = op.el_ops[0][1]        
        if op_kind in {RT.Terminated,RT.Instantiated}:
            selected_types = {RT.Terminated: (internal.on_termination, terminated), RT.Instantiated: (internal.on_instantiation, instantiated)}[op_kind]
            
            if not isinstance(op_args[0], tuple):
                rae_or_zr = op_args[0]
                # Type 1: a specific entity i.e on[terminated[zr]]  !! Cannot be on[instantiated[zr]] because doesnt logically make sense !!
                if isinstance(rae_or_zr, ZefRef): 
                    assert op_kind == RT.Terminated, "Cannot listen for a specfic ZefRef to be instantiated! Doesn't make sense."
                    def filter_func(root_node): root_node | frame | to_tx  | terminated | filter[lambda x: to_ezefref(x) == to_ezefref(rae_or_zr)] |  for_each[lambda x: LazyValue(terminated[x]) | push[stream] | run ] 
                    sub_decl = sub_decl[filter_func]
                    sub = g | sub_decl                
                # Type 2: any RAE  i.e on[terminated[ET.Dog]] or on[instantiated[RT.owns]] 
                elif type(rae_or_zr) in {AtomicEntityType, EntityType, RelationType}: 
                    def filter_func(root_node): root_node | frame | to_tx | selected_types[1] | filter[lambda x: rae_type(x) == rae_or_zr] |  for_each[lambda x: LazyValue(selected_types[1][x]) | push[stream] | run ] 
                    sub_decl = sub_decl[filter_func]
                    sub = g | sub_decl
                else:
                    raise Exception(f"Unhandled type in on[{selected_types[1]}] where the input was {rae_or_zr}")

            # Type 3: An instantiated or terminated relation outgoing or incoming from a specific zr 
            elif len(op_args[0]) == 3: 
                src, rt, _ = op_args[0] # TODO don't ignore matching on the Target! To be implemented!
                sub_decl = sub_decl[selected_types[0][internal.outgoing][rt]][lambda x: LazyValue(selected_types[1][x]) | push[stream] | run] # TODO: is it always outgoing?
                sub = src | sub_decl            
            else:
                raise Exception(f"Unhandled type in on[{selected_types[1]}] where the curried args were {op_args}")
        elif op_kind == RT.ValueAssigned:
            assert len(op_args) == 1
            aet_or_zr = op_args[0]
            # Type 1: a specific zefref to an AET i.e on[value_assigned[zr_to_aet]]
            if isinstance(aet_or_zr, ZefRef): 
                sub_decl = sub_decl[internal.on_value_assignment][lambda x: LazyValue(value_assigned[x][x|value|collect]) | push[stream] | run]
                sub = aet_or_zr | sub_decl
            # Type 2: any AET.* i.e on[value_assigned[AET.String]]
            elif isinstance(aet_or_zr, AtomicEntityType): 
                def filter_func(root_node): root_node | frame | to_tx | value_assigned | filter[aet_or_zr] | map[lambda x: value_assigned[x][x|value|collect]] |  for_each[lambda x: run(LazyValue(x) | push[stream]) ]  
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

    ---- Examples ----
    >>> [ [2,3,4], [5,6,7] ]                    # => [ [2, 5], [3,6], [4,7] ]
    >>> 
    >>> # terminates upon the shortest one:
    >>> [ range(2, inf), [5,6], [15,16,17] ]    # => [[2, 5, 15], [3, 6, 16]]
    """
    its = [iter(el) for el in iterable]
    while True:
        try:
            yield [next(it) for it in its]
        except StopIteration:
            # if any one of the iterators completes, this completes
            return


def transpose_tp(op, curr_type):
    return VT.List


#---------------------------------------- match -----------------------------------------------
def match_imp(item, patterns):
    """
    Given an item and a list of Tuples[predicate, output]. The item is checked
    sequentially on each predicate until one matches. If non-matches an exception is raised.

    ---- Examples ----
    >>> -9 | match[
    >>>     (less_than[-10], 'very cold'),
    >>>     (less_than[0], 'cold'),
    >>>     (greater_than_or_equal[0], 'ok'),
    >>> ] | collect                            => 'cold'

    ---- Arguments ----
    item: the incoming value
    patterns: (T, T) -> Bool

    ---- Signature ----
    (T, (T->Any)) -> T
    """
    for pred, return_val in patterns:
        if pred(item): return return_val
    raise RuntimeError(f'None of the specified patterns matched for value {item} in "match operator": pattern: {patterns}')
    

def match_tp(op, curr_type):
    return VT.Any


def match_apply_imp(item, patterns):
    """
    This is apply version of match operator. Where the pattern tuple's second item contains a function to be applied
    on the inputed item.

    ---- Examples ----
    >>> -9 | match[
    >>>     (less_than[-10], add[10]),
    >>>     (less_than[0], multiply[2]),
    >>>     (greater_than_or_equal[0], add[1]),
    >>> ] | collect                            => -18

    ---- Arguments ----
    item: the incoming value
    patterns: (T, T) -> Bool

    ---- Signature ----
    (T, (T->Any)) -> T
    """
    for pred, applied_func in patterns:
        if pred(item):
            assert callable(applied_func)
            return applied_func(item)
    raise RuntimeError(f'None of the specified patterns matched for value {item} in "match operator": pattern: {patterns}')
    

def match_apply_tp(op, curr_type):
    return VT.Any
#---------------------------------------- peel -----------------------------------------------
def peel_imp(el, *args):
    from ..fx.fx_types import _Effect_Class
    if isinstance(el, ValueType):
        return el.nested()
    elif isinstance(el, _Effect_Class):
        return el.d
    elif isinstance(el, ZefOp):
        # TODO !!!!!!!!!!!!! Change this to always return elementary ZefOps and to use list(el) to get current behavior
        if len(el.el_ops) > 1:
            return [ZefOp((op,)) for op in el.el_ops]
        return el.el_ops
    elif isinstance(el, LazyValue):
        return (el.initial_val, el.el_ops)
    else:
        raise NotImplementedError(f"Tried to peel an unsupported type {type(el)}")


def peel_tp(op, curr_type):
    if curr_type == VT.Effect:
        return VT.Dict[VT.String, VT.Any]
    elif curr_type in {VT.ZefOp, VT.LazyValue}:
        return VT.Record
    else:
        return curr_type.nested()


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
        return wrapper1()
        
    def wrapper2():
        yield from builtins.zip( *(x, second, *args) )    
    return wrapper2()



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
    >>>     [1,2,3],                                    
    >>>     ['a', 'b', 'c', 'd'],
    >>> ] | concat                                      # =>  [1, 2, 3, 'a', 'b', 'c', 'd']
    >>> 
    >>> # B) One list piped in, other lists to concatenated curried into op
    >>> [1,2,3] | concat[ (42,43) ][('a','b','c')]      # =>  [1, 2, 3, 42, 43, 'a', 'b', 'c']
    """
    if isinstance(v, ZefRefss) or isinstance(v, EZefRefss):
        return pyzefops.flatten(v)
    elif (isinstance(v, list) or isinstance(v, tuple)) and len(v)>0 and isinstance(v[0], str):
        if not all((isinstance(el, str) for el in v)):
            raise TypeError(f'A list starting with a string was passed to concat, but not all other elements were strings. {v}')
        return v | join[''] | collect
    elif isinstance(v, str):
        if first_curried_list_maybe is None:
            return v
        else:
            return [v, first_curried_list_maybe, *args] | join[''] | collect
    else:
        import more_itertools as mi
        import itertools
        if first_curried_list_maybe is None:
            return (el for sublist in v for el in sublist)
        else:
            return (el for sublist in (v, first_curried_list_maybe, *args)  for el in sublist)




def concat_type_info(op, curr_type):
    if curr_type in ref_types:
        curr_type = downing_d[curr_type]
    else:
        try:
            curr_type = curr_type.nested()
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
    (List[T], T)      -> List[T]
    (String, String)  -> String
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
        is_any_terminated = elements_to_prepend | map[terminated] | filter[None] | length | greater_than[0] | collect 
        if is_any_terminated: return Error("Cannot append a terminated ZefRef")

        
        g = Graph(z_list)
        rels = z_list > L[RT.ZEF_ListElement] | collect
        rels1 = (elements_to_prepend 
                | enumerate 
                | map[lambda p: (z_list, RT.ZEF_ListElement[str(p[0])], p[1])] 
                | collect
                )

        new_rels = rels1 | map[second | peel | first | second | second | inject[Z] ] | collect
        next_rels = new_rels | sliding[2] | attempt[map[lambda p: (p[0], RT.ZEF_NextElement, p[1])]][[]] | collect

        # Do we need a single connecting RT.ZEF_NextElement between the last element of the existing list and the first new element?
        
        # if there are elements in the list, use the last one. Otherwise return an empty list.
        first_existing_el_rel = (
            rels 
            | filter[lambda r: r < L[RT.ZEF_NextElement] | length | equals[0] | collect] 
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
    elif isinstance(v, EZefRefs):
        return EZefRefs([item, *v])
    elif isinstance(v, ZefRefs):
        return ZefRefs([item, *v])
    elif isinstance(v, str):
        return item + v
    elif isinstance(v, Generator) or isinstance(v, Iterator):
        def generator_wrapper():
            yield item
            yield from v
        return generator_wrapper()
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
    (List[T], T)      -> List[T]
    (String, String)  -> String
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
        is_any_terminated = elements_to_append | map[terminated] | filter[None] | length | greater_than[0] | collect 
        if is_any_terminated: return Error("Cannot append a terminated ZefRef")

        
        g = Graph(z_list)
        rels = z_list > L[RT.ZEF_ListElement] | collect
        rels1 = (elements_to_append 
                | enumerate 
                | map[lambda p: (z_list, RT.ZEF_ListElement[str(p[0])], p[1])] 
                | collect
                )

        new_rels = rels1 | map[second | peel | first | second | second | inject[Z] ] | collect
        next_rels = new_rels | sliding[2] | attempt[map[lambda p: (p[0], RT.ZEF_NextElement, p[1])]][[]] | collect

        # Do we need a single connecting RT.ZEF_NextElement between the last element of the existing list and the first new element?
        
        # if there are elements in the list, use the last one. Otherwise return an empty list.
        last_existing_ZEF_ListElement_rel = (
            rels 
            | filter[lambda r: r > L[RT.ZEF_NextElement] | length | equals[0] | collect] 
            | attempt[
                single
                | func[lambda x: [(x, RT.ZEF_NextElement, Z['0'])]]
                ][[]] 
            | collect)

        actions = ( 
                rels1,                              # the RT.ZEF_ListElement between the ET.ZEF_List and the RAEs
                next_rels,                          # the RT.ZEF_NextElement between each pair of new RT.ZEF_ListElement 
                last_existing_ZEF_ListElement_rel,  # list with single connecting rel to previous list or empty
            ) | concat | collect
        return  actions     
 

    elif isinstance(v, EZefRefs):
        return EZefRefs([*v, item])
    elif isinstance(v, ZefRefs):
        return ZefRefs([*v, item])
    elif isinstance(v, str):
        return v + item
    elif isinstance(v, Generator) or isinstance(v, Iterator):
        def generator_wrapper():
            yield from v
            yield item
        return generator_wrapper()
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
    """
    assert isinstance(path, list) or isinstance(path, tuple)
    if len(path) == 0: return d
    return get_in(d.get(path[0], default_val), path[1:], default_val)

    
def get_in_tp(d_tp, path_tp, default_val_tp):
    return VT.Dict




#---------------------------------------- insert_in -----------------------------------------------
def insert_in_imp(d: dict, path, value):
    """
    ---- Examples ----
    >>> {'a': 1, 'b': {'c': 1}} | insert_in[('b', 'd')][42]   # => {'a': 1, 'b': {'c': 1, 'd': 42}} 
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
    ---- Examples ----
    >>> {'a': 1, 'b': {'c': 1}} | remove_in[('b', 'c')]   # => {'a': 1, 'b': {}} 
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
    ---- Examples ----
    >>> {'a': 1, 'b': {'c': 1}} | update_in[('b', 'c')][add[1]]   # => {'a': 1, 'b': {'c': 2}} 

    ---- Signature ----
    (Dict, List, T1->T2) -> Dict

    ---- Tags ----
    - related zefop: update_at
    - related zefop: insert_in
    - related zefop: remove_in
    - related zefop: get_in
    - operates on: Dict
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

    """
    assert isinstance(n, int)
    assert callable(f)

    if n>=0:
        return (f(el) if m==n else el for m, el in enumerate(v))
    else:
        return Error(f'update_at not implemented for negative indexes n yet')



def update_at_tp(op, curr_type):
    return curr_type

    

#---------------------------------------- update -----------------------------------------------
def update_imp(d: dict, k, fn):
    """
    Change the value for a given key / index by applying a
    user provided function to the old value at that location.

    Note: the equivalent ZefOp to use on Lists is "update_at"
    (to be consistent with "remove_at" for Lists and 
    disambiguate acting on the value)

    {'a': 5, 'b': 6} | update['a'][add[10]]   # => {'a': 15, 'b': 6}    
    """
    if not isinstance(d, dict): raise TypeError('"update" only acts on dictionaries. Use "update_at" for Lists or "update_in" for nested access.')
    r = {**d}
    r[k] = fn(d[k])
    return r



#---------------------------------------- remove_at -----------------------------------------------
def remove_at_imp(v, *nn):
    """
    Using "remove" based on indexes would be confusing,
    as Python's list remove, searches for the first 
    occurrence of that value and removes it.

    ['a', 'b', 'c'] | remove_at[1]       # => ['a', 'c']
    ['a', 'b', 'c'] | remove_at[1][0]    # => ['c']
    """
    if isinstance(v, dict): raise TypeError('"remove_at" only acts on iterables. Use "remove" for dictionaries. For nested access, use "remove_in".')
    return (el for m, el in enumerate(v) if m not in nn)




#---------------------------------------- interleave -----------------------------------------------

def interleave_imp(v, first_curried_list_maybe=None, *args):
    """
    Interleaves elements of an arbitrary number M of lists.
    Length of output is determined by the length of the 
    shortest input list N_shortest: M*N_shortest
    
    ---- Examples ----
    >>> # Either called with a list of of lists (or equivalent for streams)
    >>> [
    >>>     [1,2,3],
    >>>     ['a', 'b', 'c', 'd']        
    >>> ] | interleave          # =>  [1, 'a', 2, 'b', 3, 'c']
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
    - operates on: List

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
    return v[::step]


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
    >>> range(8) | chunk[3]     #  [[0, 1, 2], [3, 4, 5], [6, 7]]
    
    ---- Signature ----
    (List[T], Int) -> List[List[T]]

    ---- Tags ----
    related zefop: stride
    related zefop: sliding
    related zefop: slice
    """
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




def chunk_tp(v_tp, step_tp):
    return VT.List


#---------------------------------------- insert -----------------------------------------------
def sliding_imp(iterable, window_size: int, stride_step: int=1):
    """ 
    Given a list, return a list of internal lists of length "window_size".
    "stride_step" may be specified (optional) and determines how
    many elements are taken forward in each step.
    Default stride_step if not specified otherwise: 1.

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

    implementation follows Scala's, see https://stackoverflow.com/questions/32874419/scala-slidingn-n-vs-groupedn
    """
    it = iter(iterable)
    try:
        w = []
        for c in range(window_size):
            w.append(next(it))
        yield tuple(w)
    except StopIteration:
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
    
        

def sliding_tp(v_tp, step_tp):
    return VT.List






#---------------------------------------- insert -----------------------------------------------
def insert_imp(d: dict, key, val=None):
    """ 
    ---- Examples ----
    >>> {'a': 1} | insert['b'][2]   ->  {'a': 1, 'b': 2} 
    >>> FlatGraph() | insert[ET.God, RT.Favorite, Val(1/137)] | insert[ET.BigBang]
    """
    if isinstance(d, FlatGraph):
        fg = d
        new_el = key
        return fg_insert_imp(fg, new_el)
    elif isinstance(d, dict):
        return {**d, key: val}
    else:
        return Error(f'"insert" zefop called with unhandled type {d=} {key=} {val=}')


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

    ---- Tags ----
    - level: advanced
    - used for: control flow
    - operates on: ZefOps, Functions
    - related zefop: func
    - related zefop: apply_functions
    """
    return op(*(*args[::-1], flow_arg))



#---------------------------------------- insert_into -----------------------------------------------

def insert_into_imp(key_val_pair, x):
    """
    Unclear whether we need this. To insert a key value
    pair into a dictionary, one could cast to a dict and use merge.
    But this may be shorter.

    This function could also be used on Lists. 
    
    ---- Examples ----
    >>> (2, 'a') | insert_into[ range(4) ]    # [0,1,'a',2,3]

    ---- Signature ----
    ((T1, T2), Dict[T3][T4]) -> Dict[T1|T3][T2|T4]

    # TODO: make this work: (10, 'a') | insert_into[range(10) | map[add[100]]] | take[5] | c

    """
    if not isinstance(key_val_pair, (list, tuple)):
        return Error(f'in "insert_into": key_val_pair must be a list or tuple. It was {type(x)=}     {x=}')
    
    k, v = key_val_pair
    if isinstance(x, dict):
        return {**x, k:v}
    if type(x) in {list, tuple, range}:
        assert isinstance(k, int)
        # So much laziness!
        it = iter(x)
        def tmp():
            for c in range(k):
                yield next(it)
            yield v
            yield from it
        return tmp()





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
    - related zefop: remote_at
    - related zefop: insert
    - related zefop: get
    """
    if isinstance(d, FlatGraph):
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
    
    ---- Tags ----
    - operates on: Dict
    - related zefop: get_in
    - related zefop: insert
    - related zefop: remove
    - related zefop: select_in
    """
    from typing import Generator
    if isinstance(d, FlatGraph):
        return fg_get_imp(d, key)
    elif isinstance(d, dict):
        return d.get(key, default)
    elif isinstance(d, list) or isinstance(d, tuple) or isinstance(d, Generator):
        return Error(f"get called on a list. Use 'nth' to get an element at a specified index.")
    else:
        return Error(f"get called with unsupported type {type(d)}.")

def get_tp(d_tp, key_tp):
    return VT.Any



#---------------------------------------- enumerate -----------------------------------------------
def enumerate_imp(v):
    import builtins
    return builtins.enumerate(v)


def enumerate_tp(input_arg0_tp):
    return VT.List



#---------------------------------------- items -----------------------------------------------
def items_imp(d):
    return tuple(d.items())


def items_tp(input_arg0_tp):
    return VT.List



#---------------------------------------- values -----------------------------------------------
def values_imp(d):
    return tuple(d.values())


def values_tp(input_arg0_tp):
    return VT.List



#---------------------------------------- keys -----------------------------------------------
def keys_imp(d):
    return tuple(d.keys())


def keys_tp(input_arg0_tp):
    return VT.List



#---------------------------------------- reverse -----------------------------------------------
def reverse_imp(v):
    from typing import Generator
    if isinstance(v, Generator): return (tuple(v))[::-1]
    if isinstance(v, str): return v[::-1]
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
    
    e.g. [2,3] | cycle[3]  -> [2,3,2,3,2,3]
    cycle[None]:     means never terminate
    """
    if n==0:
        return
    cached = []
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



def cycle_tp(iterable_tp, n_tp):
    return iterable_tp



#---------------------------------------- repeat -----------------------------------------------
def repeat_imp(iterable, n=None):
    import itertools
    if isinstance(iterable, Generator) or isinstance(iterable, Iterator): iterable = [i for i in iterable]
    return itertools.repeat(iterable) if n is None else itertools.repeat(iterable, n)


def repeat_tp(iterable_tp, n_tp):
    return VT.List



#---------------------------------------- contains -----------------------------------------------
def contains_imp(x, el):
    return el in x    
                        

def contains_tp(x_tp, el_tp):
    return VT.Bool



#---------------------------------------- contained_in -----------------------------------------------
def contained_in_imp(x, el):
    if isinstance(x, Generator) or isinstance(x, Iterator): x = [i for i in x]
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
 
    ---- Related ----
    - predicates
    """

    import builtins
    from typing import Generator, Iterator   
    if isinstance(args[0], FlatGraph):
        return fg_all_imp(*args)
    if isinstance(args[0], ZefRef) and is_a[ET.ZEF_List](args[0]):
        z_list = args[0]
        rels = z_list > L[RT.ZEF_ListElement] | collect
        first_el = rels | attempt[filter[lambda r: r < L[RT.ZEF_NextElement] | length | equals[0] | collect] | single | collect][None] | collect
        return (x for x in first_el | iterate[attempt[lambda r: r >> RT.ZEF_NextElement | collect][None]] | take_while[Not[equals[None]]] | map[target])

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
        if isinstance(fil, EntityType) or isinstance(fil, AtomicEntityType):
            # Note: returning list rather than ZefRefs as more compatible
            # return list(gs.tx | pyzefops.instances[fil])
            return gs.tx | pyzefops.instances[fil]

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
        if fil == VT.TX:
            return pyzefops.tx(g)

        # These options have C++ backing so try them first
        # The specific all[ET.x/AET.x] options (not all[RT.x] though)
        # Not using as this is not correct in filtering out the delegates
        # if isinstance(fil, EntityType) or isinstance(fil, AtomicEntityType):
        #     return g | pyzefops.instances_eternal[fil]

        # The remaining options will just use the generic filter and is_a
        return filter(blobs(g), lambda x: is_a(x, fil))

    if isinstance(args[0], ZefRef):
        assert len(args) == 1
        z, = args
        assert internals.is_delegate(z)
        return z | pyzefops.instances
    
    # once we're here, we interpret it like the Python "all"
    v = args[0]
    assert len(args) == 1
    assert isinstance(v, list) or isinstance(v, tuple) or isinstance(v, Generator) or isinstance(v, Iterator)
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
    join a list of strings with a binding character.
    >>> ['foo', 'bar'] | join['-']              # => 'foo-bar'
    """
    return x.join(list_of_strings)


def join_tp(v, x):
    return VT.String



#---------------------------------------- trim_left -----------------------------------------------
def trim_left_imp(v, el_to_trim):
    if isinstance(v, str):        
        return v.lstrip(el_to_trim)
    def wrapper():
        it = iter(v)
        try:
            next_el = next(it)
            while next_el == el_to_trim:
                next_el = next(it)
            yield next_el
            while True:
                yield next(it)        
        except StopIteration:
            return
    return wrapper()


def trim_left_tp(v_tp, el_to_trim_tp):
    return v_tp
        
    
    
#---------------------------------------- trim_right -----------------------------------------------
def trim_right_imp(v, el_to_trim):
    import itertools 
    if isinstance(v, str):
        return v.rstrip(el_to_trim)
    # we need to know all elements before deciding what is at the end
    vv = tuple(v)
    vv_rev = vv[::-1]
    ind = len(list(itertools.takewhile(lambda x: x==el_to_trim, vv_rev)))
    return vv if ind==0 else vv[:-ind]


def trim_right_tp(v_tp, el_to_trim_tp):
    return v_tp
        
        

#---------------------------------------- trim -----------------------------------------------
def trim_imp(v, el_to_trim):
    import itertools 
    if isinstance(v, str):
        return v.strip(el_to_trim)
    # we need to know all elements before deciding what is at the end
    vv = tuple(v)
    vv_rev = vv[::-1]
    ind_left = len(list(itertools.takewhile(lambda x: x==el_to_trim, vv)))
    ind_right = len(list(itertools.takewhile(lambda x: x==el_to_trim, vv_rev)))    
    return vv[ind_left:] if ind_right==0 else vv[ind_left:-ind_right]


def trim_tp(v_tp, el_to_trim_tp):
    return v_tp
        

#---------------------------------------- tap -----------------------------------------------
def tap_imp(x, fct):
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
        return Effect({
            'type': FX.Stream.Push,
            'stream': stream,
            'item': item,
        })
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
    - operates on: ZefOps, Value Types, Entity, Relation, AtomicEntity, ZefRef, EZefRef
    - related zefop: without_absorbed
    - related zefop: inject
    - related zefop: inject_list
    - related zefop: reverse_args
    """
    if isinstance(x, EntityType) or isinstance(x, RelationType) or isinstance(x, AtomicEntityType)  or isinstance(x, Keyword):
        if '_absorbed' not in x.__dict__:
            return ()
        else:
            return x._absorbed
    
    elif type(x) in {Entity, Relation, AtomicEntity}:
        return x.d['absorbed']

    elif isinstance(x, ZefOp):
        if len(x.el_ops) != 1: 
            return Error(f'"absorbed" can only be called on an elementary ZefOp, i.e. one of length 1. It was called on {x=}')
        return x.el_ops[0][1]
    elif isinstance(x, ZefRef) or isinstance(x, EZefRef):
        return ()
    else:
        return Error(f'absorbed called on {type(x)=}   {x=}')






#---------------------------------------- without_absorbed -----------------------------------------------
def without_absorbed_imp(x):
    """
    Return the bare Type as if nothing had ever been absorbed.

    ---- Examples ----
    >>> reduce[add[1]][42] | without_absorbed    # => reduce
    
    ---- Tags ----
    - used for: control flow
    - operates on: ZefOps, Value Types, Entity, Relation, AtomicEntity, ZefRef, EZefRef
    - related zefop: absorbed
    - related zefop: inject
    - related zefop: inject_list
    - related zefop: reverse_args
    """
    if isinstance(x, EntityType):
        if '_absorbed' not in x.__dict__:
            return x
        else:
            new_et = EntityType(x.value)
            return new_et
    
    elif isinstance(x, RelationType):
        if '_absorbed' not in x.__dict__:
            return x
        else:
            new_rt = RelationType(x.value)
            return new_rt
        
    elif isinstance(x, AtomicEntityType):
        return AtomicEntityType(x.value)
                
    elif isinstance(x, Keyword):
        if '_absorbed' not in x.__dict__:
            return x
        else:
            new_kw = Keyword(x.value)
            return new_kw

    return Error('Not Implemented')






#---------------------------------------- add -----------------------------------------------
def add_imp(a, second=None, *args):
    from functools import reduce
    if second is None:
        return reduce(lambda x, y: x + y, a)
    return reduce(lambda x, y: x + y, [a, second, *args])
    
    
def add_tp(a, second, *args):
    return VT.Any    
    
    
    
#---------------------------------------- subtract -----------------------------------------------
def subtract_imp(a, second=None):    
    if second is None:
        assert len(a) == 2
        return a[0]-a[1]
    return a-second
    
    
def subtract_tp(a, second):
    return VT.Any    
    
    
    

#---------------------------------------- multiply -----------------------------------------------
def multiply_imp(a, second=None, *args):
    """ 
    Multiplies a list of numbers.
    We don't use 'product' to name this, as that term is too overloaded.
    e.g. in Python itertools this denotes the Cartesian product.
    """
    from functools import reduce
    if second is None:
        return reduce(lambda x, y: x * y, a)
    return reduce(lambda x, y: x * y, [a, second, *args])
    

def multiply_tp(a, second, *args):
    return VT.Any


#---------------------------------------- divide -----------------------------------------------
def divide_imp(a, second=None):    
    if second is None:
        assert len(a) == 2
        return a[0]/a[1]
    return a/second
    
    
def divide_tp(a, second):
    return VT.Any    
    




#---------------------------------------- mean -----------------------------------------------
def mean_imp(v):
    """
    Calculates the arithmetic mean of a given List / Set of numbers.

    ---- Signature ----
    List[Int] -> Float
    Set[Int] -> Float
    List[Float] -> Float
    Set[Float] -> Float
    """
    return sum(v) / length(v)


def mean_tp(op, curr_type):
    return VT.Any


#---------------------------------------- variance -----------------------------------------------
def variance_imp(v):
    """
    Calculates the variance of a given List / Set of numbers.

    ---- Signature ----
    List[Int] -> Float
    Set[Int] -> Float
    List[Float] -> Float
    Set[Float] -> Float
    """
    le = length(v)
    return sum( (x*x for x in v) )/le - (sum(v)/le)**2


def variance_tp(v):
    return VT.Any


#---------------------------------------- power -----------------------------------------------
def power_imp(x, b):
    return x**b

def power_tp(op, curr_type):
    return VT.Any


#---------------------------------------- exponential -----------------------------------------------
def exponential_imp(x):
    import math
    return math.exp(x)


def exponential_tp(x):
    return VT.Any


#---------------------------------------- logarithm -----------------------------------------------
def logarithm_imp(x, base=None):
    """ 
    base = None is synonymous with base = 2.718281828459045...
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
        raise RuntimeError(f'A function needs to be provided when using max_by. Called for {v=}')
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
        raise RuntimeError(f'A function needs to be provided when using min_by. Called for {v=}')
    return builtins.min(v, key=min_by_function)
    
    
def min_by_tp(v_tp, min_by_function_tp):
    return VT.Any
    
    
    

    
    
#---------------------------------------- equals -----------------------------------------------
def equals_imp(a, b):
    return a == b
    
    
def equals_tp(a, b):
    return VT.Bool

    
    
#---------------------------------------- greater_than -----------------------------------------------
def greater_than_imp(a, b):
    return a > b
    
    
def greater_than_tp(a, b):
    return VT.Bool    
    
    
    
#---------------------------------------- less_than -----------------------------------------------
def less_than_imp(a, b):
    return a < b
    
    
def less_than_tp(a, b):
    return VT.Bool 


    
#---------------------------------------- larger_or_equal_than -----------------------------------------------
def greater_than_or_equal_imp(a, b):
    return a >= b
    

def greater_than_or_equal_to(a, b):
    return VT.Bool 


    
#---------------------------------------- less_or_equal_than -----------------------------------------------
def less_than_or_equal_imp(a, b):
    return a <= b


def less_than_or_equal_to(a, b):
    return VT.Bool 



#---------------------------------------- Not -----------------------------------------------
def not_imp(x, pred_fct=lambda x: x):
    """ 
    Takes a single predicate function and returns the negated predicate function.    
    """    
    return not pred_fct(x)



def not_tp(a, b):
    return VT.Bool 



#---------------------------------------- And -----------------------------------------------
def and_imp(x, *args):
    """Takes the input and tests it against the predicates given in the other
    arguments one at a time, returning True only if all predicates return True.
    Similarly to `all` will return True if the list of predicates is empty.

    `and` performs shortcircuiting, stopping evaluation as soon as the first
    predicate returns False. All arguments to and should be functions which
    return a Bool. As a convenience, bools passed instead of a predicate are
    used directly.

    ---- Examples ----
    >>> 5 | and[greater_than[2]][less_than[10]] # => True
    
    >>> x | and[False][test_func][another_pred] # => False

    >>> False | and # => True

    ---- Signature ----
    (T, Callable[[T], Bool]...) -> Bool
 
    ---- Related ----
    - predicates

    """
    # include short circuiting
    for fct in args:
        if fct is False: return False
        if fct is True: continue
        res = fct(x)
        assert isinstance(res, bool)
        if res is False:
            return False
    return True
    

def and_tp(x, *args):
    return VT.Any
    
    
    
    
#---------------------------------------- Or -----------------------------------------------
def or_imp(x, *args):
    # include short circuiting
    for fct in args:
        if fct is True: return True
        if fct is False: continue
        res = fct(x)
        assert isinstance(res, bool)
        if res is True:
            return True
    return False
    

def or_tp(x, *args):
    return VT.Any
    
       
   
#---------------------------------------- xor -----------------------------------------------
def xor_imp(x, *args):
    assert len(args) == 2
    f1, f2 = args
    res1, res2 = (f1(x), f2(x))
    assert isinstance(res1, bool)
    assert isinstance(res2, bool)
    return res1 ^ res2   # xor for boolean values
    

def xor_tp(x, *args):
    return VT.Any
    
       
   

#---------------------------------------- drop -----------------------------------------------
def drop_imp(v, n: int):
    """
    Drop the first n elements of the sequence
    """
    print("Deprecation: 😞😞😞😞😞 ops.drop is deprecated. Use 'skip'  instead 😞😞😞😞😞")
    if isinstance(v, tuple) or isinstance(v, list) or isinstance(v, str):
        return itertools.islice(v, n, None)  # None is 'inf' for the upper limit
    # if isinstance(v, ZefRefs) or isinstance(v, EZefRefs):
    #     return zefops.drop(v, n)
    if n>=0:
        it = iter(v)
        for _ in range(n):
            next(it)
        def gen():
            yield from it
        return gen()    
    # n<0
    else:
        cached = tuple(v)
        return cached[:n]
    

def drop_tp(op, curr_type):
    return curr_type



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

    ---- Signature ----
    (List[T], Int) -> List[T]
    """
    if isinstance(v, tuple) or isinstance(v, list) or isinstance(v,str):
        return itertools.islice(v, n, None)  # None is 'inf' for the upper limit
    # if isinstance(v, ZefRefs) or isinstance(v, EZefRefs):
    #     return pyzefops.drop(v, n)
    if n>=0:
        it = iter(v)
        for _ in range(n):
            next(it)
        def gen():
            yield from it
        return gen()    
    # n<0
    else:
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
    return itertools.accumulate(iterable, fct, initial=initial_val)




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
    # TODO: assert that fct is an endomorphism on zef_type(x)
    return itertools.accumulate(itertools.repeat(x), lambda fx, _: f(fx))


def iterate_type_info(op, curr_type):
    return VT.List



#---------------------------------------- skip_while -----------------------------------------------
def skip_while_imp(it, predicate):
    import itertools
    return itertools.dropwhile(predicate, it)

def skip_while_tp(it_tp, pred_type):
    return it_tp


#---------------------------------------- take -----------------------------------------------
def take_implementation(v, n):
    """
    positive n: take n first items. Operates lazily and supports infinite iterators.
    negative n: take n last items. Must evaluate entire iterable, does not terminate 
    for infinite iterables.


    ---- Signature ----
    (List[T], Int) -> List[T]
    """
    import itertools
    if isinstance(v, ZefRef) or isinstance(v, EZefRef):
        return take(v, n)
    else:
        if n >= 0:            
            return (x for x in itertools.islice(v, n))
        else:
            vv = list(v)
            return vv[n:]
        
        
def take_type_info(op, curr_type):
    return curr_type



#---------------------------------------- take_while -----------------------------------------------
def take_while_imp(it, predicate):
    import itertools
    return itertools.takewhile(predicate, it)

def take_while_tp(it_tp, pred_type):
    return it_tp




#---------------------------------------- take_until -----------------------------------------------
def take_until_imp(it, predicate):
    """ 
    Similar to take_while, but with negated predicate and 
    it includes the bounding element. Useful in some cases,
    but take_while is more common.
    
    ---- Examples ----
    >>> range(10) | take_until[lambda x: x>4] | collect       # => [0, 1, 2, 3, 4, 5]

    ---- Signature ----
    (List[T], (T->Bool)) -> List[T]
    """
    for el in it:
        if not predicate(el):
            yield el
        else:
            yield el
            break

def take_until_tp(it_tp, pred_type):
    return it_tp




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
            curr_type = curr_type.nested()
        except AttributeError as e:
            raise Exception(f"An operator that downs the degree of a Nestable object was called on a Degree-0 Object {curr_type}: {e}")
    return curr_type



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
            curr_type = curr_type.nested()
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
            curr_type = curr_type.nested()
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
            curr_type = curr_type.nested()
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




#---------------------------------------- GraphViz -----------------------------------------------
def to_graphviz_imp(x, curried_args):
    return f"TODO..... {x}"


def to_graphviz_tp(op, curr_type):
    return None




#---------------------------------------- Blobs -----------------------------------------------
def blobs_imp(x):    
    print('😞😞😞😞 Deprecation of blobs operator / function: use "g | all" 😞😞😞😞')
    return pymain.blobs(x)


def blobs_tp(op, curr_type):
    return VT.List[VT.ZefRef]


#---------------------------------------- Z -----------------------------------------------
def Z_imp(z):
    return z

def Z_tp(op, curr_type):
    assert curr_type in ref_types
    return curr_type


#---------------------------------------- Root -----------------------------------------------
def root_imp(g):
    assert isinstance(g, Graph)
    return g[42]            # TODO: don't hard code


def root_tp(op, curr_type):
    return None





# ------------------------------------------ sign ---------------------------------------------



def sign_imp(x):
    """
    Utility function that returns the sign of a number (+1/-1).
    also: sign(0)=0

    ---- Signature ----
    Union[Int, Float] -> Int
    """
    if x > 0: return 1 
    elif x == 0: return 0
    elif x < 0: return -1
    raise ValueError()


def sign_tp(op, curr_type):
    return VT.Int




# --------------------------------------- if_then_else ------------------------------------------------
def if_then_else_apply_imp(x, pred, true_case_func, false_case_func):
    """
    Dispatches to one of two provided functions based on the boolean 
    result of the predicate function, given the input value.
    The input value into the zefop is used by the predicate and 
    forwarded to the relevant case function.

    ---- Examples ----
    >>> add_one, add_two = add[1], add[2]
    >>> 4 | if_then_else[is_even][add_one][add_two]         # => 5

    ---- Signature ----
    ((T->Bool), (T->T1), (T->T2)) -> Union[T1, T2]

    ---- Tags ----
    - used for: control flow
    - used for: logic
    - related zefop: if_then_else
    - related zefop: group_by
    - related zefop: match
    - related zefop: match_apply
    - related zefop: filter
    """
    return true_case_func(x) if pred(x) else false_case_func(x)


def if_then_else_apply_tp(x, pred, true_case_func, false_case_func):
    return VT.Any


def if_then_else_imp(x, pred, value1, value2):
    """
    Unlike the apply version of this function. If pred evaluates
    to true, value1 is returned otherwise value2.

    ---- Examples ----
    >>> 4 | if_then_else[is_even]["even"]["odd"]       # => "even"

    ---- Signature ----
    (Any, (T->Bool), (Any), (Any)) -> Any

    ---- Tags ----
    - used for: control flow
    - used for: logic
    - related zefop: if_then_else_apply
    - related zefop: group_by
    - related zefop: match
    - related zefop: match_apply
    - related zefop: filter
    """
    return value1 if pred(x) else value2


def if_then_else_tp(x, pred, true_case_func, false_case_func):
    return VT.Any


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
    """
    type_tup = bypass_type if isinstance(bypass_type, tuple) else (bypass_type,)
    return if_then_else(
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
    """
    from zef.ops import _any
    class Sentinel:
        pass
    sentinel = Sentinel()      # make sure this doesn't match any value provided by the user
    
    assert (
        (isinstance(x, dict) and isinstance(p, dict)) or 
        (type(x) in {list, tuple} and type(p) in {list, tuple}) or
        (isinstance(x, set) and isinstance(p, set)) or 
        True
    )
    if isinstance(x, dict):
        assert  isinstance(p, dict)        
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
    
    elif isinstance(x, set):
        assert  isinstance(p, set)  
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
    """
    observed = set()
    it = iter(v)
    def wrapper():
        try:
            while True:
                el = next(it)
                if el not in observed:
                    observed.add(el)
                    yield el
        except StopIteration:
            return

    return wrapper()


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

    return wrapper()


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
    """
    vv = tuple(v)
    return len(vv) == len(set(vv))



def is_distinct_tp(x):
    return VT.Bool


# ---------------------------------------- distinct -----------------------------------------------

def is_distinct_by_imp(v, fn):
    """
    Used on an iterable / stream of values and
    returns a boolean indicating whether all
    values are distinct after tranformation with 
    a given function. i.e. as soon as any value
    appears more than once, False is returned.

    ---- Examples ----
    >>> [1,2] | is_distinct_by[lambda x: x%2]      # => True
    >>> [1,2,3] | is_distinct_by[lambda x: x%2]    # => False

    ---- Arguments ----
    v: an iterable with elements that can be compared
    fn: the function to be applied elementwise

    ---- Signature ----
    List[T], Callable -> Bool
    Stream[T], Callable -> Bool
    """
    w = [fn(x) for x in v]
    return len(w) == len(set(w))        # TODO: this can clearly be made more efficient by exiting early.



def is_distinct_by_tp(x):
    return VT.Bool





# ---------------------------------------- replace -----------------------------------------------
def replace_imp(collection, old_new_pair):
    from collections.abc import Iterable
    assert isinstance(old_new_pair, tuple)
    def rep(el):
        return old_new_pair[1] if old_new_pair[0] == el else el
    
    if isinstance(collection, set):
        return set((rep(el) for el in collection))
    
    if isinstance(collection, dict):
        raise TypeError('Dont use "replace", but "insert" to replace a key value pair in a dictionary')
    
    
    # if isinstance(collection, list):
    #     return [rep(el) for el in collection]
    
    # if isinstance(collection, tuple):
    #     return tuple((rep(el) for el in collection))
    
    # if we're here, it must be a tuple/list/... 
    # perform lazily
    
    if isinstance(collection, Iterable):
        it = iter(collection)
        def my_gen():
            try:
                while True:
                    yield rep(next(it))            
            except StopIteration:
                return
            
        return my_gen()
    
    
    
    raise TypeError('not implemented')


def replace_tp(*args):
    return VT.Any
    



# ----------------------------------------- shuffle ----------------------------------------------
def shuffle_imp(collection, seed: int = None):
    """ 
    A seed can be provided optionally.
    TODO: implement coeffects to get randomness from FX system.
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
def slice_imp(collection, start_end: tuple):
    """ 
    factor out python-style slicing: ...[5:8].
    Negative indexes count from the back    

    ---- Examples ----
    >>> ['a', 'b', 'c', 'd'] | Slice[1:2]    # => ['b', 'c']    
    """
    assert isinstance(start_end, tuple)
    if len(start_end) == 2:
        return collection[start_end[0]: start_end[1]]
    if len(start_end) == 3:
        return collection[start_end[0]: start_end[1] : start_end[2]]
    raise RuntimeError('start_end must be a Tuple[Int] of len 2 or 3')


def slice_tp(*args):
    return VT.Any




# ---------------------------------------- split -----------------------------------------------

def split_imp(collection, val):
    """ 
    Split a List into a List[List] based on the occurrence of val.
    The value that is split on is not contained in any of the output lists.

    ---- Examples ----
    >>> 'abcdeabfb' | split['b']            # => ['a', 'cdea', 'f', '']
    >>> [0,1,6,2,3,4,2,] | split[2]         # => [[0, 1, 6], [3, 4], []]    

    ---- Signature ----
    (List[T], T) -> List[List[T]]
    """
    if isinstance(collection, str):
        return collection.split(val)
    def wrapper():
        it = iter(collection)
        try:
            while True:
                s = []
                next_val = next(it)
                while next_val != val:                
                    s.append(next_val)
                    next_val = next(it)                    
                yield s
        except StopIteration:
            yield s
            return
    return wrapper()
    

def split_tp(*args):
    return VT.Any



# ------------------------------------------ tx ---------------------------------------------

def tx_imp(*args):
    """
    let zr: ZefRef  ezr: EZefRef
    zr | tx[instantiated]       # keeps ref. frame of zr, returns a ZefRef: 
    zr | tx[terminated]
    """
    import traceback
    traceback.print_stack()
    print(f"Deprecation warning: the 'tx' operator will be deprecated. Its usage was too broad and mixed up concepts. Use 'to_graph_slice' and 'to_tx' instead")
    # print(f">>>>> {args[0]=}")
    # print(f">>>>> {args[1]=}")
    # print(f">>>>> {args[1]== lazy_zefops.instantiated}")
    x = args[0]
    if len(args) == 1:          
        if isinstance(x, GraphSlice):
            zz = x.tx
            return ZefRef(zz, zz)
        if isinstance(x, ZefRef):
            return pyzefops.tx(x)
        if isinstance(x, ZefRefs):
            return pyzefops.tx(x)
    
    if len(args) == 2:        
        if type(args[1]) == ZefOp:
            if args[1].el_ops[0][0] == RT.Instantiated:                                
                return pyzefops.instantiation_tx(x)
            if args[1].el_ops[0][0] == RT.Terminated:                                
                return pyzefops.termination_tx(x)

    raise Exception("Unknown type for tx op {type(args[0])=}")


def tx_tp(op, curr_type):
    if curr_type == VT.Graph:
        curr_type = VT.EZefRefs
    else:
        curr_type = VT.ZefRefs if "ZefRef" in curr_type.d[0] else VT.EZefRefs
    return curr_type



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

        if isinstance(args[0], ZefRefs):
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

    raise TypeError(f'now called with unsuitable types: {args=}')

            
def now_type_info(*args_tp):
    return VT.Any




# ---------------------------------------- time_slice -----------------------------------------------

def time_slice_implementation(first_arg, *curried_args):
    """ 
    Returns the time slice as an Int for GraphSlice.
    
    Note: we may introduce z_tx | time_slice if we can find a convincing use case.
    We're leaving it out for now, since this builds on the prior confusion
    of identifying transactions themselves with time slice.
    The new mental image is that reference frames are somewhat distinct from 
    TXs and a time slice applies to a reference frame, not a TX directly.

    ---- Examples ----
    >>> my_graph_slice | time_slice

    ---- Signature ----
    GraphSlice -> Int
    """
    if isinstance(first_arg, GraphSlice):
        return int(first_arg)       # casting a GraphSlice onto an int implemented

    # Old usage:
    if not is_a(first_arg, BT.TX_EVENT_NODE):
        print(f"😞😞😞😞😞  The use of the 'time_slice' operator with {type(first_arg)=} is deprecated: 😞😞😞😞😞")    
    return (pyzefops.time_slice)(first_arg, *curried_args).value


def time_slice_type_info(op, curr_type):
    return VT.TimeSlice



# ---------------------------------------- next_tx -----------------------------------------------
def next_tx_imp(z_tx):
    """
    Given a ZefRef/EZefRef to a tx, move to the next tx.
    For a ZefRef with a reference frame, one cannot look 
    into the future, i.e. get to a TX that was not known 
    in that reference frame. Nil is returned in that case.
    
    For Both ZeFfRef/EZefRef, Nil is returned when called
    on the very lastest TX.

    ---- Examples ----
    z2_tx = z_tx | next_tx | collect

    ---- Signature ----
    ZefRef -> Union[ZefRef, Nil]
    EZefRef -> Union[EZefRef, Nil]    
    """
    def next_tx_ezr(zz):
        try:
            return zz >> BT.NEXT_TX_EDGE | collect
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

    raise TypeError(f'next_tx can only take ZefRef and EZefRef, got {z_tx=} {type(z_tx)=}')


def next_tx_tp(z_tp):
    return None



# ---------------------------------------- previous_tx -----------------------------------------------
def previous_tx_imp(z_tx):
    """
    Given a ZefRef/EZefRef to a tx, move to the previous tx.
    If one tries to go back from the very first tx, nil is 
    returned.    
    
    ---- Signature ----
    ZefRef -> Union[ZefRef, Nil]
    EZefRef -> Union[EZefRef, Nil]
    
    z2_tx = z_tx | previous_tx | collect
    """
    def previous_tx_ezr(zz):
        try:
            return zz << BT.NEXT_TX_EDGE | collect
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

    raise TypeError(f'previous_tx can only take ZefRef and EZefRef, got {z_tx=} {type(z_tx)=}')


def previous_tx_tp(z_tp):
    return None



# ---------------------------------------- instantiated -----------------------------------------------
def instantiated_imp(x):
    """ 
    ---- Signature ----
    ZefRef[TX]     -> List[ZefRef[RAE]]
    EZefRef[TX]    -> List[EZefRef[RAE]]
    ZefRef[RAE]    -> ZefRef[TX]
    EZefRef[RAE]   -> EZefRef[TX]
    """
    if isinstance(x, GraphSlice):
        raise TypeError("`instantiated` cannot be used with a GraphSlice. Please convert to a TX first. For example `g | now | to_tx | instantiated`")
    assert isinstance(x, ZefRef) or isinstance(x, EZefRef)
    if BT(x) == BT.TX_EVENT_NODE:
        if isinstance(x, ZefRef):
            # zefops.to_frame(z1, zefops.to_ezefref(z_tx))
            return pyzefops.instantiated(x) | map[in_frame[frame(x)]] | collect
        elif isinstance(x, EZefRef):
            return pyzefops.instantiated(x) | to_ezefref
    elif internals.is_delegate(x):
        raise Exception("Can't ask for instantiation time of delegate (there are multiple")
    else:
        return pyzefops.instantiation_tx(x)


def instantiated_tp(op, curr_type):
    return VT.Any #VT.ZefRefs if isinstance(op, ZefRef) else VT.EZefRefs


# ----------------------------------------- terminated ------------------------------------------------
def terminated_imp(x):
    """
    ---- Signature ----
    ZefRef[TX]     -> List[ZefRef[RAE]]
    EZefRef[TX]    -> List[EZefRef[RAE]]
    ZefRef[RAE]    -> Union[ZefRef[TX], Nil]
    EZefRef[RAE]   -> Union[EZefRef[TX], Nil]
    """
    if isinstance(x, GraphSlice):
        raise TypeError("`terminated` cannot be used with a GraphSlice. Please convert to a TX first. For example `g | now | to_tx | terminated`")
    assert isinstance(x, ZefRef) or isinstance(x, EZefRef)
    if BT(x) == BT.TX_EVENT_NODE:
        if isinstance(x, ZefRef):
            return pyzefops.terminated(x) | map[in_frame[frame(x)]] | collect
        elif isinstance(x, EZefRef):
            return pyzefops.terminated(x) | to_ezefref
    else:
        res = pyzefops.termination_tx(x)  # this returns node 42 (root) as a sentinel if there is none        
        if BT(res) == BT.ROOT_NODE:
            return None  
        # TODO (Ulf): fix this.
        if isinstance(x, ZefRef) and ((x | frame | time_slice | collect) > (res | time_slice | collect)):
            return None
        else:
            return res
    

def terminated_tp(x):
    return VT.Any #VT.ZefRefs if isinstance(op, ZefRef) else VT.EZefRefs

# ----------------------------------------- value_assigned --------------------------------------------
def value_assigned_imp(x):
    """
    ---- Signature ----
    ZefRef[TX]   -> List[ZefRef[AET]]
    EZefRef[TX]  -> List[EZefRef[AET]]
    ZefRef[AET]  -> List[ZefRef[TX]]
    EZefRef[AET] -> List[EZefRef[TX]]
    """
    if isinstance(x, GraphSlice):
        raise TypeError("`value_assigned` cannot be used with a GraphSlice. Please convert to a TX first. For example `g | now | to_tx | value_assigned`")
    assert isinstance(x, ZefRef) or isinstance(x, EZefRef)
    if BT(x) == BT.TX_EVENT_NODE:
        if isinstance(x, ZefRef):
            # Note: value_assigned preserves the tx
            return pyzefops.value_assigned(x)
        elif isinstance(x, EZefRef):
            return pyzefops.value_assigned(x) | to_ezefref
    else:
        return pyzefops.value_assignment_txs(x)


def value_assigned_tp(x):
    return VT.Any




# ----------------------------------------- merged --------------------------------------------
def _is_merged(z):
    inst_edge = collect(to_ezefref(z) < BT.RAE_INSTANCE_EDGE)
    return has_out(inst_edge, BT.ORIGIN_RAE_EDGE)

def merged_imp(x):    
    """ 
    A more specific requirement than instantiated.
    A merge event also shows up as instantiated, but not 
    vice versa.
    
    ---- Signature ----
    ZefRef     -> GraphSlice
    GraphSlice -> List[ZefRef]
    """
    if isinstance(x, GraphSlice):
        raise TypeError("`merged` cannot be used with a GraphSlice. Please convert to a TX first. For example `g | now | to_tx | merged`")
    assert isinstance(x, ZefRef) or isinstance(x, EZefRef)
    if BT(x) == BT.TX_EVENT_NODE:
        return filter(pyzefops.instantiated(x), lambda z: _is_merged(z))
    else:
        assert _is_merged(x)
        return pyzefops.instantiation_tx(x)

def merged_tp(x):    
    return VT.Any


# ----------------------------------------- affected --------------------------------------------
def affected_imp(x):    
    """
    Given a transaction, return a list of all ZefRef
    that were in some way affected by the changes in
    that tx.

    When the tx is passed in as a ZefRef, the reference
    frame is kept by this operator.
    When traversing the eternal graph, this behavior is 
    also propagated by returning the EZefRef.

    Related function: use "events" if a list events,
    e.g. [instantiated[z1], terminated[z2], ...] is wanted.

    TODO
    syntax proposal, since "instantiated" et al. will become entities:
    adjust the syntax and use types?
    here: z_tx | raes[Instantiated]                 # returns a List[ZefRef]
          z_tx | raes[Instantiated | Assigned]
          z_tx | events[Instantiated | Assigned]    # returns a List[Instantiated | Terminated] 
    which would be analogous to
    z1 | txs[Assigned] 
    

    ---- Signature ----
    ZefRef[TX]   -> List[ZefRef]
    EZefRef[TX]  -> List[EZefRef]
    """
    # assert isinstance(x, GraphSlice)

    return concat(instantiated(x), terminated(x), value_assigned(x))

def affected_tp(x):
    return VT.Any
    


# ----------------------------------------- events --------------------------------------------

def events_imp(z_tx):
    """ 
    Given a TX as a (E)ZefRef, return all events that occurred.

    ---- Examples ----
    z_tx | events            # => [instantiated[z1], terminated[z3], value_assigned[z8][42] ]

    ---- Signature ----
    Union[ZefRef[TX], EZefRef[TX]]  ->  List[ZefOp[Union[Instantiated[ZefRef], Terminated[ZefRef], ValueAssigned[ZefRef, T]]]]
    """
    z = to_ezefref(z_tx)
    assert BT(z) == BT.TX_EVENT_NODE
    gs = z | to_graph_slice | collect

    def make_val_ev(zz):
        zzz = to_frame[gs](zz)
        return value_assigned[zzz][value(zzz)]

    insts = z | instantiated | map[lambda zz: instantiated[to_frame[gs](zz) ]] | collect
    val_assigns = z | value_assigned | map[make_val_ev] | collect
    terminations = z | terminated | map[lambda zz: terminated[to_frame[allow_tombstone][gs](zz) ]] | collect
    return insts+val_assigns+terminations


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
        frame = args[0]
        tombstone_allowed = False
        assert isinstance(frame, GraphSlice)

    # if two additional args are passed in, one must be a frame and the other 'allow_tombstone'
    elif len(args)==2:
        if isinstance(args[0], GraphSlice):
            assert isinstance(args[1], ZefOp) and args[1] == allow_tombstone
            frame = args[0]
            tombstone_allowed = True
        elif isinstance(args[1], GraphSlice):
            assert isinstance(args[0], ZefOp) and args[0] == allow_tombstone
            frame = args[1]
            tombstone_allowed = True
        else:
            raise RuntimeError("'in_frame' can only be called with ...| in_frame[my_gs]  or ... | in_frame[allow_tombstone][my_gs] ")
    else:
        raise RuntimeError("'in_frame' can only be called with ...| in_frame[my_gs]  or ... | in_frame[allow_tombstone][my_gs] ")    
    z_tx = frame.tx
    if not (isinstance(z, ZefRef) or isinstance(z, EZefRef)):
        raise NotImplementedError(f"No in_frame yet for type {type(z)}")
    zz = to_ezefref(z)
    g_frame = Graph(z_tx)
    # z's origin lives in the frame graph
    cond1 = g_frame == Graph(zz)    # will be reused
    if cond1 or origin_uid(zz).graph_uid == uid(g_frame):
        z_obj = to_ezefref(z) if cond1 else g_frame[origin_uid(zz)]
        # exit early if we are looking in a frame prior to the objects instantiation: this is not even allowed when allow_tombstone=True
        if internals.is_delegate(z_obj):
            from ..logger import log
            log.warn("Need to fix, assuming delegate exists from the beginning of time.")
            instantiation_ts = 0
        else:
            instantiation_ts = time_slice(instantiated(z_obj))
        if (time_slice(z_tx)) < instantiation_ts:
            raise RuntimeError(f"Causality error: you cannot point to an object from a frame prior to its existence / the first time that frame learned about it.")            
        if not tombstone_allowed:
            # assert that the frame is prior to the termination tx
            if internals.is_delegate(z_obj):
                from ..logger import log
                log.warn("Need to fix, assuming delegate exists to the end of time.")
                z_tx_term = None
            else:
                z_tx_term = terminated(z_obj)
            if z_tx_term is not None and index(z_tx_term)!=internals.root_node_blob_index():  # if there is a termination tx
                termination_ts = time_slice(z_tx_term)
                if (time_slice(z_tx)) >= termination_ts:
                    raise RuntimeError(f"The RAE was terminated and no longer exists in the frame that the reference frame was about to be moved to. It is still possible to perform this action if you really want to by specifying 'to_frame[allow_tombstone][my_z]'")
        return ZefRef(z_obj, z_tx)

    # z's origin does not live in the frame graph
    else:
        the_origin_uid = origin_uid(zz)        
        if the_origin_uid not in g_frame:
            # this graph never knew about a RAE with this origin uid
            raise RuntimeError('origin node not found in reference frame graph!')

        zz = g_frame[the_origin_uid]
        if BT(zz) in {BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATOMIC_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE}:
            z_candidates = zz << L[BT.ORIGIN_RAE_EDGE] | target | filter[exists_at[frame]] | collect
            if len(z_candidates) > 1:
                raise RuntimeError(f"Error: More than one instance alive found for RAE with origin uid {the_origin_uid}")
            elif len(z_candidates) == 1:
                return z_candidates | single | to_frame[frame] | collect
            else:
                return None     # no instance alive at the moment
        
        raise NotImplementedError("We should not have landed here")



def in_frame_tp(op, curr_type):
    return VT.Any


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
    raise TypeError(f'"x | to_tx" can only be called for x being a GraphSlice or a Graph. {x=}')


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
    >>> zr | time_travel[-3.5*units.seconds]  
    >>> my_graph_slice | time_travel[-3]
    >>> 
    >>> #       ---- absolute time travel ----
    >>> t1 = Time('October 20 2020 14:00 (+0100)')
    >>> g | time_travel[t1]
    >>> gs | time_travel[t1]
    >>> ezr | time_travel[t1]
    >>> zr | time_travel[t1]
    >>>
    >>> ezr | time_travel[allow_tombstone][t1]
    >>> zr | time_travel[allow_tombstone][-3]
    >>> zr | time_travel[allow_tombstone][-3.5*units.seconds]
    >>> zr | time_travel[allow_tombstone][t1]

    ---- Signature ----
    (ZefRef, Int)                           -> Union[ZefRef, Nil]           # nil or error? What is the process / criterion?
    (ZefRef, Duration)                      -> Union[ZefRef, Nil]
    (GraphSlice, Int)                       -> Union[GraphSlice, Nil]
    (GraphSlice, Duration)                  -> Union[GraphSlice, Nil]

    (ZefRef, Time)                          -> Union[ZefRef, Nil]
    (EZefRef, Time)                         -> Union[ZefRef, Nil]
    (Graph, Time)                           -> Union[GraphSlice, Nil]
    (GraphSlice, Time)                      -> Union[GraphSlice, Nil]
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
            raise RuntimeError(f"If two args are curried into 'time_travel', one of them MUST be 'ops.allow_tombstone'. Given: {args=}")
    else:
        raise RuntimeError(f"At most two args may be curried into time_travel. Given: {args=}")

    try:
        if isinstance(x, ZefRef):
            if isinstance(p, int):
                return (x | pyzefops.time_travel[allow_tombstone][p]) if tombstone_allowed else (x | pyzefops.time_travel[p])
            elif isinstance(p, Time):
                new_frame = Graph(x) | to_tx[p] | to_graph_slice | c
                if new_frame is None:
                    raise RuntimeError(f"could not determine suitable reference frame / graph slice in x | time_travel[p] for {x=}  {p=}")
                return (x | in_frame[allow_tombstone][new_frame] | c) if tombstone_allowed else (x | in_frame[new_frame] | c)
            elif is_duration(p):
                t = (x | frame | time | c) + p
                new_frame = Graph(x) | to_tx[t] | to_graph_slice | c
                if new_frame is None:
                    raise RuntimeError(f"could not determine suitable reference frame / graph slice in x | time_travel[p] for {x=}  {p=}")
                return (x | in_frame[allow_tombstone][new_frame] | c) if tombstone_allowed else (x | in_frame[new_frame] | c)

        if isinstance(x, GraphSlice):
            if isinstance(p, int):
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
                    raise RuntimeError(f"could not determine suitable reference frame / graph slice in x | time_travel[p] for {x=}  {p=}")
                return (x | in_frame[allow_tombstone][new_frame] | c) if tombstone_allowed else (x | in_frame[new_frame] | c)
        
        elif isinstance(x, Graph):        
            if isinstance(p, Time):
                return x | to_tx[p] | to_graph_slice | c
    
    except Exception as e:
        raise RuntimeError(f'Error applying time_travel operator for {x=}  and  {p=} - {e=}')
    raise TypeError(f"x | time_travel[p] was called for {x=}  {p=}")



def time_travel_tp(x, p):
    return VT.Any


# ------------------------------------------ origin_uid ---------------------------------------------



def origin_uid_imp(z) -> EternalUID:
    """used in constructing GraphDelta, could be useful elsewhere"""
    if type(z) in [Entity, AtomicEntity, Relation]:
        return uid(z)
    assert BT(z) in {BT.ENTITY_NODE, BT.ATOMIC_ENTITY_NODE, BT.RELATION_EDGE}
    if internals.is_delegate(z):
        return uid(to_ezefref(z))
    origin_candidates = z | to_ezefref | in_rel[BT.RAE_INSTANCE_EDGE] | Outs[BT.ORIGIN_RAE_EDGE] | collect    
    if len(origin_candidates) == 0:
        # z itself is the origin
        return uid(to_ezefref(z))
    z_or = origin_candidates | only | collect
    if BT(z_or) in {BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATOMIC_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE}:
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
    
# ------------------------------------------ origin_rae ---------------------------------------------

def origin_rae_imp(x):
    """For RAEs, return an abstract entity, relation or atomic entity. For delegates, acts as the identity.""" 
    if type(x) in [Entity, AtomicEntity, Relation]:
        return x
    if isinstance(x, ZefRef) or isinstance(x, EZefRef):
        if internals.is_delegate(x):
            raise Exception("TODO: Implement origin_rae(ZefRef) when ZefRef is a delegate")
        if BT(x) == BT.ENTITY_NODE:
            return Entity(x)
        elif BT(x) == BT.RELATION_EDGE:
            return Relation(x)
        elif BT(x) == BT.ATOMIC_ENTITY_NODE:
            return AtomicEntity(x)
        raise Exception("Not a ZefRef that is a concrete RAE")
    if is_a_implementation(x, Delegate):
        return x
    raise Exception(f"Not a valid type for origin_rae: {type(x)}")
    

def origin_rae_tp(x):
    return VT.Any
    

# ---------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------









def fill_or_attach_implementation(*args):
    from ...deprecated import zefops
    return curry_args_in_zefop(zefops.fill_or_attach, args[0], args[1:])

def fill_or_attach_type_info(op, curr_type):
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
        elif isinstance(message, str):
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
    from ..fx.fx_types import _Effect_Class
    
    # if we create the effect with short hand notation, e.g. (ET.Dog, ET.Person | g | run)
    # we want to directly unpack the instances in the same structure as the types that went in
    # To decouple the layers, we need to return based on whether 'unpacking_template' is present
    # as a key in the receipt

    
    if not isinstance(eff, _Effect_Class):
        raise TypeError(f"run(x) called with invalid type for x: {type(eff)}. You can only run an Effect.")
    handler = _effect_handlers[eff.d['type'].d]
    eff._been_run = True
    return handler(eff)



def hasout_implementation(zr, rt):
    return curry_args_in_zefop(pyzefops.has_out, zr, (rt,))

def hasin_implementation(zr, rt):
    return curry_args_in_zefop(pyzefops.has_in, zr, (rt,))



# -------------------------------- apply_functions -------------------------------------------------
def apply_functions_imp(x, *fns):
    """ 
    Apply different functions to different elements in a list.
    The use case is similar to using Zef's map[f1,f2,f3] with multiple
    functions, but fills in the gap when the input is not a list / stream.
    In this sense, it is closer to func.
    The input list and list of functions must be of the same length.

    ---- Examples ----
    ['Hello', 'World'] | apply_functions[to_upper_case][to_lower_case]    # => ['HELLO', 'world']

    ---- Signature ----
    VT.List -> VT.List

    ---- Tags ----
    - used for: control flow
    - operates on: ZefOps, Functions
    - related zefop: func
    - related zefop: reverse_args
    """
    import builtins
    if not (isinstance(x, tuple) or isinstance(x, list)):
        raise TypeError(f"The dataflow argument for apply_functions must be a list/tuple: Got a {type(x)=} Value: {x=}")
    if not len(fns) == len(x):
        raise TypeError(f"the length of the dataflow argument for apply_functions must be the same length as the list of functions provided. Got {x=} and {fns=}" )
    return tuple(
        (f(el) for f, el in builtins.zip(fns, x))
    )




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
    >>> [3, 4, 5] | map[lambda x: x+1]                      # => [4, 5, 6]
    >>> add_one, to_upper = add[1], lambda c: c.upper()
    >>> [(2, 'a'), (3, 'b')] | map[(add_one, to_upper)]     # => [(3, 'A'), (4, 'B')]
    >>>
    >>> my_int_stream | map[lambda x: x+1]                  # returns a Stream[Int]

    ---- Signature ----
    (List[T1], (T1 -> T2))  -> List[T2]

    ---- Tags ----
    - used for: control flow
    """
    import builtins
    input_type = parse_input_type(type_spec(v))
    if input_type == "awaitable":
        observable_chain = v
        # Ugly hack for ZefOp
        f._allow_bool = True
        return observable_chain.pipe(rxops.map(f))
    else:
        if not isinstance(v, Iterable): raise TypeError(f"Map only accepts values that are Iterable. Type {type(v)} was passed")
        if isinstance(f, list) or isinstance(f, tuple):
            n = len(f)
            def wrapper_list():
                for w in v:
                    assert len(w) == n
                    yield tuple((ff(el) for (ff, el) in builtins.zip(f, w)))
            return wrapper_list()
        else:
            def wrapper():
                for el in v:
                    yield f(el)
            return wrapper()       



   

def reduce_implementation(iterable, fct, init):
    import functools
    return functools.reduce(fct, iterable, init)


# def group_by_implementation(first_arg, *curried_args):
#     return (ops.group_by)(first_arg, *curried_args)

def group_by_implementation(iterable, key_fct, categories=None):
    """
    categories is optional and specifies additional keys/categories 
    to create, even if there are no occurrences. Passed as a list 
    [True, False]
    
    ---- Tags ----
    - used for: control flow
    - operates on: List    
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



def identity_implementation(x):
    return x

def length_implementation(iterable):
    if hasattr(iterable, "__len__"):
        return len(iterable)
    else:
        # benchark: this is faster than iterating with a counter
        return len(list(iterable))



def nth_implementation(iterable, n):
    """
    Returns the nth element of an iterable or a stream.
    Using negative 'n' takes from the back with 'nth[-1]' being the last element.
    An Error value is returned if the index is out of bounds.
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
    if isinstance(iterable, ZefRef) and is_a[ET.ZEF_List](iterable):
        return iterable | all | nth[n] | collect
        
    if isinstance(iterable, ZefRefs) or isinstance(iterable, EZefRefs) or isinstance(iterable, list) or isinstance(iterable, tuple) or isinstance(iterable, str):
        return iterable[n]
    it = iter(iterable)
    for cc in range(n):
        next(it)
    return next(it)
    #   TODO: implementation for awaitables
            




#---------------------------------------- select_by_field -----------------------------------------------
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




#---------------------------------------- select_by_field -----------------------------------------------
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

    


#---------------------------------------- select_by_field -----------------------------------------------
def filter_implementation(itr, pred):
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
    input_type = parse_input_type(type_spec(itr))
    if input_type == "tools":
        # As this is an intermediate, we return an explicit generator
        return (x for x in builtins.filter(pred, itr))
    elif input_type == "zef":
        return pyzefops.filter(itr, pred)
    elif input_type == "awaitable":
        observable_chain = itr
        return observable_chain.pipe(rxops.filter(pred))
    else:
        raise Exception(f"Filter can't work on input_type={input_type}")




#---------------------------------------- select_by_field -----------------------------------------------
def select_by_field_imp(zrs : Iterable[ZefRef], rt: RelationType, val):
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
    """
    return pyzefops.select_by_field_impl(zrs, rt, val)

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
    if isinstance(first_arg, Delegate):
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


def to_delegate_type_info(op, curr_type):
    return None


# This is for internal use only - it tries to convert a tuple or single RAET
# into a delegate of order zero.
def attempt_to_delegate(args):
    if isinstance(args, tuple):
        assert len(args) == 3
        return Delegate(Delegate(args[0]), args[1], Delegate(args[2]))
    else:
        return Delegate(args)

def delegate_of_implementation(x, arg1=None, arg2=None):
    if isinstance(x, EZefRef) or isinstance(x, ZefRef):
        assert arg2 is None
        if arg1 is None:
            create = False
        else:
            create = arg1
        assert isinstance(create, bool)

        d = pyzefops.delegate_of(to_delegate(x))
        z = to_delegate(d, Graph(x), create)
        if z is None:
            return None
        if isinstance(x, ZefRef):
            z = z | in_frame[frame(x)] | collect
        return z

    if isinstance(x, Delegate):
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
            return to_delegate(d, g, create)

    # Fallback
    return delegate_of_implementation(attempt_to_delegate(x), arg1, arg2)

def delegate_of_type_info(op, curr_type):
    return None




#---------------------------------------- Out -----------------------------------------------
def Out_imp(z, rt=VT.Any):
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
    """
    from zef.pyzef.zefops import traverse_out_node, outs
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "outout", "single")
    if isinstance(rt, RelationType):
        try:
            return traverse_out_node(z, rt)
        except RuntimeError as exc:
            # create a summary of what went wrong
            existing_rels = (outs(z)
                | map[RT] 
                | frequencies       # which relation types are present how often?
                | items 
                | map[lambda p: f"{repr(p[0])}: {repr(p[1])}"] 
                | join['\n'] 
                | collect
                )
            return Error(f"traversing {z} via relation type {rt}. The existing outgoing edges here are: {existing_rels}")    
    elif rt==VT.Any or rt==RT:
        my_outs = outs(z)
        if len(my_outs)==1:
            return my_outs[0]
        else:
            return Error(f"traversing {z} using 'out': there were {len(my_outs)} outgoing edges: {my_outs}")
    else:
        return Error(f'Invalid type "{rt}" specified in Out[...]')





#---------------------------------------- Outs -----------------------------------------------
def Outs_imp(z, rt):
    """
    Traverse along all outgoing relation of the specified 
    type to the thing attached to the target of each relation.

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friend = z1 | Outs[RT.FriendOf]

    ---- Signature ----
    ZefRef -> ZefRefs
    EZefRef -> EZefRefs
    """
    from zef.pyzef.zefops import traverse_out_node_multi
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "outout", "multi")
    return traverse_out_node_multi(z, rt)





#---------------------------------------- In -----------------------------------------------
def In_imp(z, rt=None):
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
    """
    from zef.pyzef.zefops import traverse_in_node
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "inin", "single")
    return traverse_in_node(z, rt)



#---------------------------------------- Ins -----------------------------------------------
def Ins_imp(z, rt):
    """
    Traverse along all incoming relation of the specified 
    type to the thing attached to the source of each relation.

    For a ZefRef, it will always stay in the same time
    slice of the given graph.
    
    When Used on an EZefRef, the eternal graph is traversed.

    ---- Examples ----
    >>> z1s_friend = z1 | Ins[RT.FriendOf]

    ---- Signature ----
    ZefRef -> ZefRefs
    EZefRef -> EZefRefs
    """
    from zef.pyzef.zefops import traverse_in_node_multi
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "inin", "multi")
    return traverse_in_node_multi(z, rt)


#---------------------------------------- out_rel -----------------------------------------------
def out_rel_imp(z, rt=None):
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
    """
    from zef.pyzef.zefops import traverse_out_edge
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "out", "single")
    return traverse_out_edge(z, rt)


#---------------------------------------- out_rels -----------------------------------------------
def out_rels_imp(z, rt=None):
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
    """
    from zef.pyzef.zefops import traverse_out_edge_multi
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "out", "multi")
    return traverse_out_edge_multi(z, rt)



#---------------------------------------- in_rel -----------------------------------------------
def in_rel_imp(z, rt=None):
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
    """
    from zef.pyzef.zefops import traverse_in_edge
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "in", "single")
    return traverse_in_edge(z, rt)


#---------------------------------------- in_rels -----------------------------------------------
def in_rels_imp(z, rt=None):
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
    """
    from zef.pyzef.zefops import traverse_in_edge_multi
    assert isinstance(z, (ZefRef, EZefRef, FlatRef))
    if isinstance(z, FlatRef): return traverse_flatref_imp(z, rt, "in", "multi")
    return traverse_in_edge_multi(z, rt)









def ins_implementation_old(first_arg, *curried_args):
    print("Deprecation: 😞😞😞😞😞 ops.ins is deprecated. Use 'z1 < L[RT]'  instead of 'z1 | ins' 😞😞😞😞😞")
    if isinstance(first_arg, FlatRef): return fr_ins_imp(first_arg)
    return (pyzefops.ins)(first_arg, *curried_args)

def outs_implementation_old(zr):
    print("Deprecation: 😞😞😞😞😞 ops.outs is deprecated. Use 'z1 > L[RT]'  instead of 'z1 | outs' 😞😞😞😞😞")
    if isinstance(zr, FlatRef): return fr_outs_imp(zr)
    return pyzefops.outs(zr)

def ins_and_outs_implementation_old(first_arg, *curried_args):
    print("Deprecation: 😞😞😞😞😞 ops.ins_and_outs is deprecated. use 'z1 < L[RT]' and 'z1 > L[RT]' instead😞😞😞😞😞")    
    if isinstance(first_arg, FlatRef): return fr_ins_and_outs_imp(first_arg)
    return (pyzefops.ins_and_outs)(first_arg, *curried_args)

def source_implementation(zr, *curried_args):
    if isinstance(zr, FlatRef):
        return fr_source_imp(zr)
    if isinstance(zr, Relation):
        return abstract_rae_from_rae_type_and_uid(zr.d["type"][0], zr.d["uids"][0])
    return (pyzefops.source)(zr, *curried_args)

def target_implementation(zr):
    if isinstance(zr, FlatRef):
        return fr_target_imp(zr)
    if isinstance(zr, Relation):
        return abstract_rae_from_rae_type_and_uid(zr.d["type"][2], zr.d["uids"][2])
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
        if val.type == "tools.serialize":
            from ..serialization import deserialize
            from json import loads
            return deserialize(loads(val.data))
        else:
            raise Exception(f"Don't know how to deserialize a type of {val.type}")
    else:
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



def instantiation_tx_implementation(first_arg, *curried_args):
    return (pyzefops.instantiation_tx)(first_arg, *curried_args)

def termination_tx_implementation(first_arg, *curried_args):
    return (pyzefops.termination_tx)(first_arg, *curried_args)

def instances_implementation(*args):
    print("Deprecation: 😞😞😞😞😞 instances is deprecated. Use 'f | all'  instead 😞😞😞😞😞")
    return all(*args)
    
def uid_implementation(arg):
    if isinstance(arg, str):
        return to_uid(arg)
    if isinstance(arg, Entity):
        return arg.d["uid"]
    if isinstance(arg, Relation):
        return arg.d["uids"][1]
    if isinstance(arg, AtomicEntity):
        return arg.d["uid"]
    else:
        return pyzefops.uid(arg)

def base_uid_implementation(first_arg):
    if isinstance(first_arg, EternalUID) or isinstance(first_arg, ZefRefUID):
        return (lambda x: x.blob_uid)(first_arg)
    if isinstance(first_arg, BaseUID):
        return first_arg
    return base_uid(uid(first_arg))

def exists_at_implementation(first_arg, frame):
    assert isinstance(frame, GraphSlice)
    return (pyzefops.exists_at)(first_arg, frame.tx)

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




#---------------------------------------- zef_type -----------------------------------------------
# @register_zefop(RT.ZefType)
def zef_type_imp(x):
    """
    Warning: this function is not complete and its behavior may change!!!!!!!!!!!

    Returns a Zef ValueType (a value itself),
    also when used with the python builtin supported
    types, as well as with instances of Zef Values.

    Open questions: what should zef_type(ET) return?
    BT
    RT    
    Should the various uid types be exposed to userland?

    """
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
            # QuantityFloat: VT.QuantityFloat,
            # QuantityInt: VT.QuantityInt,
            # EntityType: VT.EntityType,
            # AtomicEntityType: VT.AtomicEntityType,
            # RelationType: VT.RelationType,
            ZefRef: VT.ZefRef,
            EZefRef: VT.EZefRef,
            # TX: VT.TX
            # Entity: VT.Entity,
            # AtomicEntity: VT.AtomicEntity,
            # Relation: VT.Relation,
            ZefOp: VT.ZefOp,
            Graph: VT.Graph,
            # FlatGraph: VT.FlatGraph,
            # Keyword: VT.Keyword,
            # ValueType: VT.ValueType,
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
    from ..error import _ErrorType
    if type(x) == _ErrorType:
        if typ == Error:
            return True
    if type(x) in [BaseUID, EternalUID, ZefRefUID]:
        if is_a(typ, ZefOp) and is_a(typ, uid):
            return True
        if typ in [BaseUID, EternalUID, ZefRefUID]:
            return True
        
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

    if isinstance(x, EntityType):
        if isinstance(typ, EntityTypeStruct):
            return True
        if isinstance(typ, EntityType):
            return x == typ
        if typ == BT.RELATION_EDGE:
            return True

    if isinstance(x, RelationType):
        if isinstance(typ, RelationTypeStruct):
            return True
        if isinstance(typ, RelationType):
            return x == typ
        if typ == BT.RELATION_EDGE:
            return True

    if isinstance(x, AtomicEntityType):
        if isinstance(typ, AtomicEntityTypeStruct):
            return True
        if isinstance(typ, AtomicEntityType):
            return x == typ
        if typ == AET.QuantityFloat:
            return internals.is_aet_a_quantity_float(x)
        if typ == AET.QuantityInt:
            return internals.is_aet_a_quantity_int(x)
        if typ == AET.Enum:
            return internals.is_aet_a_enum(x)
        if typ == BT.ATOMIC_ENTITY_NODE:
            return True

    if isinstance(x, EntityTypeStruct):
        if isinstance(typ, EntityTypeStruct):
            return True

    if isinstance(x, RelationTypeStruct):
        if isinstance(typ, RelationTypeStruct):
            return True

    if isinstance(x, AtomicEntityTypeStruct):
        if isinstance(typ, AtomicEntityTypeStruct):
            return True

    if isinstance(x, ZefOp):
        if isinstance(typ, ZefOp):
            if len(x.el_ops) != 1 or len(typ.el_ops) != 1: return False
            if x.el_ops[0][0] != typ.el_ops[0][0]: return False        
            # compare the elements curried into the operator. Recursively use this subtyping function
            if len(typ.el_ops[0][1]) > len(x.el_ops[0][1]): return False
            for el_x, el_typ in zip(x.el_ops[0][1], typ.el_ops[0][1]):
                if not is_a(el_x, el_typ): return False        
            return True
        
    # TODO CHANGE THIS TO ACCEPT ONLY PYTHON TYPES
    if isinstance(typ, EntityTypeStruct) or isinstance(typ, RelationTypeStruct) or isinstance(x, AtomicEntityTypeStruct):
        return False
    try:
        return isinstance(x, typ)
    except TypeError:
        return False

def _is_a_instance_delegate_generic(x, typ):
    # This function is for internal use only and does the comparisons against
    # ET, ET.x, RT, (Z, RT.x, ET.y), etc... ignoring whether the reference is an
    # instance or delegate
    if typ == Z:
        return True
    if typ == RAE:
        if BT(x) in [BT.ENTITY_NODE, BT.RELATION_EDGE, BT.ATOMIC_ENTITY_NODE]:
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
        if BT(x) != BT.ATOMIC_ENTITY_NODE:
            return False
        return is_a(AET(x), typ)
    if isinstance(typ, EntityTypeStruct):
        return BT(x) == BT.ENTITY_NODE
    if isinstance(typ, RelationTypeStruct):
        return BT(x) == BT.RELATION_EDGE
    if isinstance(typ, AtomicEntityTypeStruct):
        return BT(x) == BT.ATOMIC_ENTITY_NODE

    return False

def has_relation_implementation(z, *args):
    return pyzefops.has_relation(z, *args)

def relation_implementation(z, *args):
    return pyzefops.relation(z, *args)

def relations_implementation(z, *args):
    return pyzefops.relations(z, *args)

def rae_type_implementation(z):
    if isinstance(z, Entity):
        return z.d["type"]
    if isinstance(z, Relation):
        return z.d["type"][1]
    if isinstance(z, AtomicEntity):
        return z.d["type"]
    return pymain.rae_type(z)





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
    return VT.List[VT.Record[VT.Any]]

def identity_type_info(op, curr_type):
    return curr_type

def length_type_info(op, curr_type):
    return VT.Int



downing_d = {
    VT.ZefRefss:    VT.ZefRefs,
    VT.ZefRefs:     VT.ZefRef,
    VT.EZefRefss:   VT.EZefRefs,
    VT.EZefRefs:    VT.EZefRef,
}

def nth_type_info(op, curr_type):
    if curr_type in ref_types:
        curr_type = downing_d[curr_type]
    else:
        try:
            curr_type = curr_type.nested()
        except AttributeError as e:
            raise Exception(f"An operator that downs the degree of a Nestable object was called on a Degree-0 Object {curr_type}: {e}")
    return curr_type

def filter_type_info(op, curr_type):
    return curr_type





def sort_type_info(op, curr_type):
    return curr_type

def ins_type_info(op, curr_type):
    return VT.ZefRefs if "ZefRef" in curr_type.d[0] else VT.EZefRefs

def outs_type_info(op, curr_type):
    return VT.ZefRefs if "ZefRef" in curr_type.d[0] else VT.EZefRefs

def ins_and_outs_type_info(op, curr_type):
    return VT.ZefRefs if "ZefRef" in curr_type.d[0] else VT.EZefRefs

def source_type_info(op, curr_type):
    return curr_type

def target_type_info(op, curr_type):
    return curr_type

def value_type_info(op, curr_type):
    return VT.Any

def time_type_info(op, curr_type):
    return VT.Time



def instantiation_tx_type_info(op, curr_type):
    return VT.ZefRef

def termination_tx_type_info(op, curr_type):
    return VT.ZefRef

def instances_type_info(op, curr_type):
    return VT.ZefRefs

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
    if op[1][0] == RT.L: 
        assert curr_type in zef_types # Can only be used with Zef types
        curr_type = VT.ZefRefs if "ZefRef" in curr_type.d[0] else VT.EZefRefs
    return curr_type

def In_type_info(op, curr_type):
    if op[1][0] == RT.L: 
        assert curr_type in zef_types # Can only be used with Zef types
        curr_type = VT.ZefRefs if "ZefRef" in curr_type.d[0] else VT.EZefRefs
    return curr_type

def OutOut_type_info(op, curr_type):
    if op[1][0] == RT.L: 
        assert curr_type in zef_types # Can only be used with Zef types
        curr_type = VT.ZefRefs if "ZefRef" in curr_type.d[0] else VT.EZefRefs
    return curr_type

def InIn_type_info(op, curr_type):
    if op[1][0] == RT.L: 
        assert curr_type in zef_types # Can only be used with Zef types
        curr_type = VT.ZefRefs if "ZefRef" in curr_type.d[0] else VT.EZefRefs
    return curr_type

def terminate_implementation(z, *args):
    # We need to keep terminate as something that works in the GraphDelta code.
    # So we simply wrap everything up as a LazyValue and return that.
    if len(args) == 1:
        return LazyValue(z) | terminate[args[0]]
    else:
        assert len(args) == 0
        return LazyValue(z) | terminate

def terminate_type_info(op, curr_type):
    return curr_type

def assign_value_imp(z, val):
    # We need to keep the assign value as something that works in the GraphDelta
    # code. So we simply wrap everything up as a LazyValue and return that.
    return LazyValue(z) | assign_value[val]

def assign_value_tp(op, curr_type):
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
    if curr_type == VT.ZefRef:
        return VT.ZefRefs
    if curr_type == VT.EZefRef:
        return VT.EZefRefs
    return VT.Error

def hasout_type_info(op, curr_type):
    return VT.Bool

def hasin_type_info(op, curr_type):
    return VT.Bool

def rae_type_type_info(op, curr_type):
    return VT.BT

#---------------------------------------- tag -----------------------------------------------
def tag_imp(x, tag: str, force: bool = False):
    """ 
    Create an effect to add a tag to either a Graph or a RAE.

    ---- Examples ----
    >>> g | tag["my_graph"] | run
    >>> g2 | tag["my_graph"][True] | run
    >>> z | tag["settings_node"] | run

    ---- Arguments ----
    x Graph / ZefRef: object to tag
    tag str: the name of the tag to apply
    force bool (=False): whether to allow stealing the tag from another graph or ZefRef.

    ---- Signature ----
    (Graph, str, bool) -> Effect
    (ZefRef, str, bool) -> Effect
    """
    if isinstance(x, Graph):
        return Effect({
            'type': FX.Graph.Tag,
            'graph': x,
            'tag': tag,
            'force': force,
            'adding': True,
        })
    if isinstance(x, ZefRef) or isinstance(x, EZefRef):
        return Effect({
            'type': FX.RAE.Tag,
            'rae': to_ezefref(x),
            'tag': tag,
            'force': force,
            'adding': True,
        })

    raise RuntimeError("Unknonwn type for tag: {type(x)}")
    
    
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
        return Effect({
            'type': FX.Graph.Tag,
            'graph': x,
            'tag': tag,
            'adding': False,
            'force': False,
        })
    if isinstance(x, ZefRef) or isinstance(x, EZefRef):
        return Effect({
            'type': FX.RAE.Tag,
            'rae': to_ezefref(x),
            'tag': tag,
            'adding': False,
            'force': False,
        })

    raise RuntimeError("Unknonwn type for tag: {type(x)}")
    
    
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
    return Effect({
        'type': FX.Graph.Sync,
        'graph': x,
        'sync_state': sync_state,
    })
    
def sync_tp(op, curr_type):
    return VT.Effect




#---------------------------------------- merge -----------------------------------------------
def merge_imp(a, second=None, *args):
    """
    Merge a dictionaries: either one list of dicts or
    dicts as multiple args.

    Clojure has a similar operator:
    https://clojuredocs.org/clojure.core/merge

    ---- Examples -----
    [{'a': 1, 'b': 42}, {'a': 2, 'c': 43}] | merge          # => {'a': 2, 'b': 42, 'c': 43}
    {'a': 1, 'b': 42} | merge[ {'a': 2, 'c': 43} ]

    ---- Signature ----
    List[Dict]          -> Dict
    (Dict, Dict)        -> Dict
    (Dict, Dict, Dict)  -> Dict

    ---- Tags ----
    * tool for: dictionaries
    * similar: merge_with
    ...
    """
    from typing import Generator
    if isinstance(a, FlatGraph) and isinstance(second, FlatGraph):
        return fg_merge_imp(a, second)
    elif second is None:
        assert isinstance(a, tuple) or isinstance(a, list) or isinstance(a, Generator)
        return {k: v for d in a for k, v in d.items()}
    else:
        assert isinstance(a, dict)
        assert isinstance(second, dict)
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
    {'a': 1, 'b': 2} | merge_with[add][{'a': 3}]                        # => {'a': 4, 'b': 2}
    {'a': 1, 'b': 2} | merge_with[add][{'a': 3}, {'b': 10, 'c': 5}]     # => {'a': 4, 'b': 12, 'c': 5}    
    [{'a': [1], 'b': [2]}, {'a': [3]}] | merge_with[concat]             # => {'a': [1, 2], 'b': [3]}

    ---- Signature ----
    (Dict[T1, T2], ((T2,T2)->T2), Dict[T1, T2]) -> Dict[T1, T2]
    (List[Dict[T1, T2]], ((T2,T2)->T2)) -> Dict[T1, T2]
    """
    v = [dict_or_list] if isinstance(dict_or_list, dict) else tuple(dict_or_list)
    if more_dict_or_list is not None:
        if isinstance(more_dict_or_list, dict):
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
    'hello' | to_clipboard                                  # returns an effect
    my_zef_func | to_clipboard | run                        # copy a single zef function to the clipboard
    g | now | all[ET.ZEF_Function] | to_clipboard | run     # copy all zef function on graph to clipboard

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

    if isinstance(x, ZefRefs):
        for el in x: 
            assert ET(el)==ET.ZEF_Function
        return to_clipboard(x | map[zef_fct_to_source_code_string] | join['\n'] | collect)

    if is_a(x, uid):
        return to_clipboard(str(x))

    assert type(x) in {str, int, float, bool}
    return Effect({
        'type': FX.Clipboard.CopyTo,
        'value': x
    })


def to_clipboard_tp(x):
    return VT.Effect


#---------------------------------------- from_clipboard -----------------------------------------------
def from_clipboard_imp():
    """
    A shortcut function to create an effect that will request
    the content of a clipboard to be copied
    """
    # assert type(x) in {str, int, float, bool}
    return Effect({
        'type': FX.Clipboard.CopyFrom,
    })

_call_0_args_translation[RT.FromClipboard] = from_clipboard_imp

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
    "data.yaml" | read_file | run | get["content"] | from_yaml | collect

    ---- Signature ----
    VT.String -> VT.Effect

    """
    return Effect({
        'type': FX.LocalFile.Read,
        'filename' : fname
    })

def read_file_tp(op, curr_type):
    return VT.Effect

def load_file_imp(fname, format = None):
    """Reads the file at the given `fname` and parse its content based on the
    file extension.
    
    This operator produces an effect and must be passed to `run`. The output of
    the effect will contain a "content" key with the transformed object.

    ---- Examples ----
    "data.yaml" | load_file | run | get["content"] | collect

    ---- Signature ----
    VT.String -> VT.Effect

    """
    return Effect({
        'type'     : FX.LocalFile.Load,
        'filename' : fname,
        'format'   : format
    })

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
    data_as_dict | save_file["data.yaml"] | run
    
    ---- Possible issues ---
    data_as_dict | to_yaml | save_file["data.yaml"] | run
    will create a yaml file containing a single string, which is unlikely to be
    what you want.

    ---- Signature ----
    (VT.Any, VT.String) -> VT.Effect

    """
    return Effect({
        'type': FX.LocalFile.Save,
        'filename' : fname,
        'content': content,
        'settings': settings,
    })

def save_file_tp(op, curr_type):
    return VT.Effect


def write_file_imp(content, fname):
    """The counterpart to `read_file`. Takes the given `content` and writes it to the file at the filename `fname`. The content is converted based on the extension of the file.
    
    The options for content as described further in the `FX.LocalFile.Write` effect.
    
    This operator doesn't do the writing itself, it only produces an effect
    which must be passed to run.
    
    ---- Examples ----
    "Hello" | write_file["data.txt"] | run
    
    ---- Signature ----
    (VT.Any, VT.String) -> VT.Effect

    """
    return Effect({
        'type': FX.LocalFile.Write,
        'filename' : fname,
        'content': content,
    })

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
            | map[lambda idx_row: idx_row[1] | enumerate | map[lambda i_v: (Z[f'_{idx_row[0]}'], RT(cols[i_v[0]]), i_v[1])] | collect]
            | prepend[range(len(df.values.tolist())) | map[lambda i: ET(entity)[f'_{i}']] | collect]
            | concat
            | collect
    )
    return actions

def pandas_to_gd_tp(op, curr_type):
    return VT.List




#---------------------------------------- to_pipeline -----------------------------------------------
def as_pipeline_imp(ops: list):
    """ 
    Given a list of operators, return one operator by constructing
    an operator pipeline in that order.

    ---- Examples ----
    >>> (nth[42], repeat, enumerate) | as_pipeline      # => nth[42] | repeat | enumerate

    ---- Tags ----
    - used for: control flow
    - operates on: ZefOps, Value Types, Entity, Relation, AtomicEntity, ZefRef, EZefRef
    - related zefop: inject
    - related zefop: inject_list
    - related zefop: absorbed
    - related zefop: without_absorbed
    - related zefop: reverse_args
    - related zefop: bypass
    """
    from typing import Generator, Iterable, Iterator
    if isinstance(ops, Generator) or isinstance(ops, Iterator): ops = [op for op in ops]
    return identity if len(ops) == 0 else (ops[1:] | reduce[lambda v, el: v | el][ops[0]] | collect)


def as_pipeline_tp():
    return VT.ZefOp



#---------------------------------------- inject -----------------------------------------------
def inject_imp(x, injectee):
    """
    Small helper function to inject the inflowing data via [...]

    ---- Examples ----
    >>> 42 | inject[equals]         # => equals[42]

    ---- Tags ----
    - used for: control flow
    - operates on: ZefOps, Value Types, Entity, Relation, AtomicEntity, ZefRef, EZefRef
    - related zefop: inject_list
    - related zefop: absorbed
    - related zefop: without_absorbed
    - related zefop: reverse_args
    - related zefop: as_pipeline
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
    >>> [pred1, pred2, pred3] | inject[And]         # => And[pred1][pred2][pred3]

    ---- Tags ----
    - used for: control flow
    - operates on: ZefOps, Value Types, Entity, Relation, AtomicEntity, ZefRef, EZefRef
    - related zefop: inject
    - related zefop: absorbed
    - related zefop: without_absorbed
    - related zefop: reverse_args
    - related zefop: as_pipeline
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
    """
    from ...deprecated.tools.zascii import parse_zascii_to_asg
    asg, _  = parse_zascii_to_asg(zascii_str)
    return asg

def zascii_to_asg_tp(op, curr_type):
    return VT.Dict

def zascii_to_schema_imp(zascii_str: VT.String) -> VT.GraphDelta:
    """ 
    Takes a zascii string with a schema and returns a GraphDelta.
    ---- Signature ----
    (VT.String) -> VT.GraphDelta
    """
    pass

def zascii_to_schema_tp(op, curr_type):
    return VT.GraphDelta


#--------------------------------------------------------------------------------------
def replace_at_imp(str_or_list, index, new_el):
    """ 
    Given a list replace element at an index with new_el.
    Given a string return a new string with element at index replaced with new_el.
    ---- Signature ----
    (VT.String, VT.Int, VT.String) -> VT.String
    (VT.List, VT.Int, VT.Any) -> VT.List
    """
    if isinstance(str_or_list, str):
        s = str_or_list
        char = str(new_el)
        if index >= len(s) or index < 0: return s
        if index == len(s) - 1: return s[:index] + char
        return s[:index] + char + s[index+1:] 
    elif isinstance(str_or_list, list):
        lst = str_or_list
        if index >= len(lst) or (index < 0 and abs(index) > length(lst)): return lst
        lst[index] = new_el
        return lst
    else:
        return Error.TypeError(f"Expected an string or a list. Got {type(str_or_list)} instead.")


def replace_at_tp(op, curr_type):
    return VT.String

def pad_to_length_imp(s: VT.String, l: VT.Int) -> VT.String:
    """ 
    Pads a string with white space to the right to a specific length l.
    """
    return s + (" " * (l - len(s))) 

def pad_to_length_tp(op, curr_type):
    return VT.String

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
    
    >>> 3 | int_to_alpha    # => 'c'
    """
    d = 'abcdefghijklmnopqrstuvwxyz'
    return d[n]




#---------------------------------------- permute_to -----------------------------------------------
def permute_to_imp(v, indices: list[int]):
    """
    given a input list, as well as a list of indices, 
    return the list of elements arranged according to the 
    list of indices.

    >>> ['a', 'b', 'c', 'd'] | permute_to[2,0,1]    # => ['c', 'a', 'b']

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




#---------------------------------------- to_upper_case -----------------------------------------------
def to_upper_case_imp(s: VT.String) -> VT.Bool:
    """ 
    Given a string, capitalize each character.

    ---- Examples ----
    >>> 'aBc' | to_upper_case          # => 'ABC'

    ---- Signature ----
    VT.String -> VT.String
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
    """
    import caseconverter
    return caseconverter.macrocase(s)

def to_screaming_snake_case_tp(op, curr_type):
    return VT.String

def make_request_imp(url, method='GET', params={}, data={}):
    return Effect({
            "type":     FX.HTTP.Request,
            "url":      url,
            "method":   method,
            "params":   params,
            "data":     data,
    })

def make_request_tp(op, curr_type):
    return VT.Effect


#---------------------------------------- blake3 -----------------------------------------------
def blake3_imp(obj) -> VT.String:
    # Hash some input all at once. The input can be bytes, a bytearray, or a memoryview.
    from blake3 import blake3 as b3
    if isinstance(obj, bytes):
        return b3(obj).hexdigest()
    elif isinstance(obj, str):
        return b3(obj.encode()).hexdigest()
    else:
        try:
            from ..op_structs import type_spec
            type_str = str(type_spec(obj))
        except:
            type_str = str(type(obj))
        return blake3_imp(type_str + str(obj))

def blake3_tp(op, curr_type):
    return VT.String


#-------------------------------ZEF LIST------------------------------------------
def to_zef_list_imp(elements: list):
    all_zef = elements | map[lambda v: isinstance(v, ZefRef) or isinstance(v, EZefRef)] | all | collect
    if not all_zef: return Error("to_zef_list only takes ZefRef or EZefRef.")
    is_any_terminated = elements | map[terminated] | filter[None] | length | greater_than[0] | collect 
    if is_any_terminated: return Error("Cannot create a Zef List Element from a terminated ZefRef")
    rels_to_els = (elements 
            | enumerate 
            | map[lambda p: (Z['zef_list'], RT.ZEF_ListElement[str(p[0])], p[1])] 
            | collect
            )

    new_rels = rels_to_els | map[second | peel | first | second | second | inject[Z] ] | collect
    next_rels = new_rels | sliding[2] | attempt[map[lambda p: (p[0], RT.ZEF_NextElement, p[1])]][[]] | collect


    return [
        [ET.ZEF_List['zef_list']],
        next_rels,
        rels_to_els
    ] | concat | collect

def to_zef_list_tp(op, curr_type):
    return VT.List


#-----------------------------FlatGraph Implementations-----------------------------------
def fg_insert_imp(fg, new_el):
    from ..graph_delta import map_scalar_to_aet_type
    assert isinstance(fg, FlatGraph)
    new_fg = FlatGraph()
    new_blobs, new_key_dict = [*fg.blobs], {**fg.key_dict}

    def idx_generator(n):
        def next_idx():
            nonlocal n
            n = n + 1
            return n
        return next_idx

    next_idx = idx_generator(length(fg.blobs) - 1)

    def inner_zefop_type(zefop, rt):
        return peel(zefop)[0][0] == rt

    def construct_abstract_rae_and_return_idx(rae_type, rae_uid):
        if isinstance(rae_type, RelationType):
            assert rae_uid in new_key_dict, "Can't construct an Abstract Relation!"
            return new_key_dict[rae_uid]
        else:
            rae_class = AtomicEntity if type(rae_type) == AtomicEntityType else Entity
            return common_logic(rae_class({"type": rae_type, "uid": rae_uid}))

    def common_logic(new_el):
        if isinstance(new_el, EntityType):
            idx = next_idx()
            internal_id = new_el | absorbed | attempt[single][None] | collect
            new_el = new_el | without_absorbed | collect
            if internal_id: new_key_dict[internal_id] = idx
            new_blobs.append((idx, new_el, [], None))

        elif isinstance(new_el, AtomicEntityType):
            idx = next_idx()
            internal_id = new_el | absorbed | attempt[single][None] | collect
            new_el = new_el | without_absorbed | collect
            if internal_id: new_key_dict[internal_id] = idx
            new_blobs.append((idx, new_el, [], None, None))

        elif isinstance(new_el, Entity):
            node_type, node_uid = new_el.d['type'], new_el.d['uid']
            if node_uid not in new_key_dict:
                idx = next_idx()
                new_blobs.append((idx, node_type, [], node_uid))
                new_key_dict[node_uid] = idx
            idx = new_key_dict[node_uid]

        elif isinstance(new_el, AtomicEntity):
            node_type, node_uid = new_el.d['type'], new_el.d['uid']
            if node_uid not in new_key_dict:
                idx = next_idx()
                new_blobs.append((idx, node_type, [], node_uid, None))
                new_key_dict[node_uid] = idx
            idx = new_key_dict[node_uid]

        elif isinstance(new_el, ZefOp) and inner_zefop_type(new_el, RT.Instantiated):
            raise ValueError("!!!!SHOULD NO LONGER ARRIVE HERE!!!!")

        elif isinstance(new_el, ZefOp) and inner_zefop_type(new_el, RT.Z):
            key = peel(new_el)| first | second | first | collect
            if key not in new_key_dict: raise KeyError(f"{key} doesn't exist in internally known ids!")
            idx = new_key_dict[key]

        # i.e: z4 <= 42 ; AET.String <= "42" ; AET.String['z1'] <= 42 ; Z['n1'] <= 42
        elif isinstance(new_el, LazyValue) and length(peel(new_el)) == 2:
            first_op = peel(new_el)[0]
            if isinstance(first_op, AtomicEntityType):
                    internal_id = first_op | absorbed | attempt[single][None] | collect
                    aet_maybe = first_op | without_absorbed | collect
                    assert isinstance(aet_maybe, AtomicEntityType), f"{new_el} should be of type AET"
                    aet_value = peel(peel(new_el)[1])[0][1][0]
                    idx = next_idx()
                    new_blobs.append((idx, aet_maybe, [], None, aet_value))
                    if internal_id: new_key_dict[internal_id] = idx
            elif isinstance(first_op, ZefOp):
                if inner_zefop_type(first_op, RT.Z):
                    key = peel(first_op)[0][1][0]
                    aet_value = peel(peel(new_el)[1])[0][1][0]
                    if key not in new_key_dict: raise KeyError(f"{key} doesn't exist in internally known ids!")
                    idx = new_key_dict[key]
                    assert isinstance(new_blobs[idx][1], AtomicEntityType), f"This key must refer to an AET found {new_blobs[idx][1]}"
                    new_blobs[idx] = (*new_blobs[idx][:4], aet_value)
                else:
                    raise ValueError(f"Expected a Z['n1'] <= 42 got {new_el} instead!")
            elif isinstance(first_op, ZefRef) or isinstance(first_op, EZefRef):
                idx = common_logic(first_op)
                assert isinstance(new_blobs[idx][1], AtomicEntityType), f"This key must refer to an AET found {new_blobs[idx][1]}"
                aet_value = peel(peel(new_el)[1])[0][1][0]
                new_blobs[idx] = (*new_blobs[idx][:4], aet_value)
            else:
                raise ValueError(f"Expected a z <= 42 or AET.String <= 42 got {new_el} instead!")

        elif isinstance(new_el, Val):
            new_el = new_el.arg
            hash_vn = blake3(str(new_el))
            if hash_vn not in new_key_dict:
                idx = next_idx()
                new_key_dict[hash_vn] = idx
                new_blobs.append((idx, "BT.ValueNode", [], new_el))  # TODO Don't treat as str once added to Zef types
            idx = new_key_dict[hash_vn]

        elif isinstance(new_el, FlatRef):
            assert new_el.fg == fg, "The FlatRef's origin FlatGraph doesn't match current FlatGraph."
            idx = new_el.idx

        elif type(new_el) in list(map_scalar_to_aet_type.keys()): 
            aet = map_scalar_to_aet_type[type(new_el)](new_el)
            idx = next_idx()
            new_blobs.append((idx, aet, [], None, new_el))

        elif type(new_el) in {ZefRef, EZefRef}:
            idx = common_logic(origin_rae(new_el))
            if isinstance(new_blobs[idx][1], AtomicEntityType) and isinstance(new_el, ZefRef):
                new_blobs[idx] = (*new_blobs[idx][:4], value(new_el))
                
        else:
            idx = None
        return idx

    if type(new_el) in {EntityType, AtomicEntityType, Entity, AtomicEntity, ZefOp, LazyValue,ZefRef, EZefRef, *list(map_scalar_to_aet_type.keys()), Val}:
        common_logic(new_el)
    elif isinstance(new_el, tuple) and len(new_el) == 3:
        src, rt, trgt = new_el
        src_idx = common_logic(src)
        trgt_idx = common_logic(trgt)
        assert isinstance(src_idx, int) and isinstance(trgt_idx, int), "Couldn't find/create src or target!"
        assert type(rt) in {RelationType, ZefOp}, "Tuples must have Relation as second item."
        idx = next_idx()

        # Case of RT.A['a']
        if isinstance(rt, RelationType):
            internal_id = LazyValue(rt) | absorbed | attempt[single][None] | collect
            rt = LazyValue(rt) | without_absorbed | collect
            if internal_id: new_key_dict[internal_id] = idx
        # Case of Z['a']
        elif isinstance(rt, ZefOp) and inner_zefop_type(rt, RT.Z): 
            raise ValueError(f"Cannot reference an internal element to be used as a Relation. {rt}")

        new_blobs.append((idx, rt, [], None, src_idx, trgt_idx))
        if idx not in new_blobs[src_idx][2]: new_blobs[src_idx][2].append(idx)
        if idx not in new_blobs[trgt_idx][2]: new_blobs[trgt_idx][2].append(-idx)
    elif isinstance(new_el, Relation):
        src, rt, trgt = new_el.d['type']
        src_uid, rt_uid, trgt_uid = new_el.d['uids']

        if type(src) == RelationType and src_uid not in new_key_dict: raise ValueError("Source of an abstract Relation can't be a Relation that wasn't inserted before!")
        if type(trgt) == RelationType and trgt_uid not in new_key_dict: raise ValueError("Target of an abstract Relation can't be a Relation that wasn't inserted before!")
        src_idx = construct_abstract_rae_and_return_idx(src, src_uid)
        trgt_idx = construct_abstract_rae_and_return_idx(trgt, trgt_uid)
        idx = next_idx()
        new_blobs.append((idx, rt, [], rt_uid, src_idx, trgt_idx))
        new_key_dict[rt_uid] = idx
        if idx not in new_blobs[src_idx][2]: new_blobs[src_idx][2].append(idx)
        if idx not in new_blobs[trgt_idx][2]: new_blobs[trgt_idx][2].append(-idx)
    else: 
        raise NotImplementedError(f"Insert not implemented for {type(new_el)}.\n{new_el=}")
        
    new_fg.key_dict = new_key_dict
    new_fg.blobs = (*new_blobs,)
    return new_fg

def fg_get_imp(fg, key):
    kdict = fg.key_dict
    if type(key) in {ZefRef, EZefRef} and origin_uid(key) in kdict: return FlatRef(fg, kdict[origin_uid(key)])
    elif type(key) in {Entity, AtomicEntity} and key.d['uid'] in kdict: return FlatRef(fg, kdict[key.d['uid']])
    elif isinstance(key, Relation) and key.d['uids'][1] in kdict:return FlatRef(fg, kdict[key.d['uids'][1]])
    elif isinstance(key, Val) and  blake3(key.arg) in kdict: return FlatRef(fg, kdict[blake3(key.arg)])
    elif key in kdict: return FlatRef(fg, kdict[key])
    else: raise KeyError(f"{key} isn't found in this FlatGraph!")


def fg_remove_imp(fg, key):
    error = KeyError(f"{key} isn't found in this FlatGraph!")
    kdict = fg.key_dict
    if type(key) in {ZefRef, EZefRef}:  
        if origin_uid(key) not in kdict: raise error
        idx = kdict[origin_uid(key)]
        key = origin_uid(key)
    elif type(key) in {Entity, AtomicEntity}: 
        if key.d['uid'] not in kdict: raise error
        idx = kdict[key.d['uid']]
        key = key.d['uid']
    elif isinstance(key, Relation):
        if key.d['uids'][1] not in kdict: raise error
        idx = kdict[key.d['uids'][1]]
        key = key.d['uids'][1]
    elif isinstance(key, Val):
        if blake3(key.arg) not in kdict: raise error
        idx = kdict[blake3(key.arg)]
        key = blake3(key.arg)
    elif key in kdict:  
        idx = kdict[key]
    else: raise error

    idx_key = {idx:key for key,idx in kdict.items()}
    kdict   = {**fg.key_dict}
    blobs   = [*fg.blobs]

    def remove_blob(idx, key = None):
        blob  = blobs[idx]
        if blob:
            blob_type = blob[1]
            ins_outs  = blob[2]
            blobs[idx] = None
            if not key: 
                key = idx_key.get(idx, None)
                if key : del(kdict[key])
            else: del(kdict[key])
            if type(blob_type) == RelationType:
                src_idx, trgt_idx = blob[4:]
                if blobs[src_idx] and idx in blobs[src_idx][2]: blobs[src_idx][2].remove(idx)
                if blobs[trgt_idx] and -idx in blobs[trgt_idx][2]: blobs[trgt_idx][2].remove(-idx)
            ins_outs | map[abs] | for_each[remove_blob]
    remove_blob(idx, key)

    new_fg = FlatGraph()
    new_fg.blobs = blobs
    new_fg.key_dict = kdict
    return new_fg


def flatgraph_to_commands(fg):
    return_elements = []
    idx_key = {idx:key for key,idx in fg.key_dict.items()}
    def dispatch_on_blob(b, for_rt=False):
        idx = b[0]
        if isinstance(b[1], EntityType):
            if idx in idx_key:
                key = idx_key[idx]
                if is_a(key, uid):
                    return Entity({"type": b[1], "uid": key})
                else:
                    if for_rt: return Z[key]
                    return b[1][key]
            elif for_rt or len(b[2]) == 0:
                return b[1][idx]
            else: return None
        elif isinstance(b[1], AtomicEntityType):
            if idx in idx_key:
                key = idx_key[idx]
                if is_a(key, uid):
                    if b[-1]: return AtomicEntity({"type": b[1], "uid": key}) <= b[-1]
                    else:     return AtomicEntity({"type": b[1], "uid": key})
                else:
                    if for_rt: return Z[key]
                    if b[-1]: return b[1][key] <= b[-1]
                    else:     return b[1][key]
            elif for_rt or len(b[2]) == 0:
                if b[-1]: return b[1][idx] <= b[-1]
                else:     return b[1][idx]
            else: return None
        elif isinstance(b[1], RelationType):
            if idx in idx_key: 
                key = idx_key[idx]
                if for_rt: return Z[key]
                if is_a(key, uid):
                    return Relation({"type": (fg.blobs[b[4]][1],b[1], fg.blobs[b[5]][1]), "uids": (idx_key[b[4]], key, idx_key[b[5]])})
            if for_rt: return Z[idx]
            src_blb  = dispatch_on_blob(fg.blobs[b[4]], True)
            trgt_blb = dispatch_on_blob(fg.blobs[b[5]], True)
            if b[0] in idx_key: base = b[1][idx_key[b[0]]]
            else: base = b[1][idx]
            return (src_blb, base, trgt_blb)

        elif isinstance(b[1], str) and b[1][:2] == "BT":
            if for_rt:
                return Z[blake3(b[-1])]
            else:
                from ..graph_delta import map_scalar_to_aet_type
                if type(b[-1]) in map_scalar_to_aet_type:
                    aet = map_scalar_to_aet_type[type(b[-1])](b[-1])
                    return aet[blake3(b[-1])] <= (b[-1])
                else:
                    return AET.Serialized[blake3(b[-1])] <= to_json(b[-1])

        raise NotImplementedError(f"Unhandled type in dispatching {b}")

    for b in fg.blobs | filter[lambda b: b != None] | collect:
        el = dispatch_on_blob(b)
        if el != None: return_elements.append(el)

    from ..graph_delta import construct_commands
    return construct_commands(return_elements)


# ------------------------------FlatRef----------------------------------
def fr_source_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    assert isinstance(blob[1], RelationType), f"| source is only allowed for FlatRef of type RelationType, but {type(blob[1])} was passed!"
    return FlatRef(fr.fg, blob[4])

def fr_target_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    assert isinstance(blob[1], RelationType), f"| target is only allowed for FlatRef of type RelationType, but {type(blob[1])} was passed!"
    return FlatRef(fr.fg, blob[5])

def fr_value_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    assert isinstance(blob[1], AtomicEntityType), "Can only ask for the value of an AET"
    return blob[-1]

def traverse_flatref_imp(fr, rt, direction, traverse_type):
    translation_dict = {
        "outout": "Out",
        "out"  : "out_rel",
        "inin": "In",
        "in": "in_rel",
    }
    assert isinstance(rt, RelationType), f"Passed Argument to traverse should be of type RelationType but got {rt}"
    blob = fr.fg.blobs[fr.idx]
    specific = blob[2] | filter[greater_than[0] if direction in {"out", "outout"} else less_than[0]] | collect
    specific_blobs = specific | map[lambda idx: fr.fg.blobs[abs(idx)]] | filter[lambda b: b[1] == rt] | collect
    if traverse_type == "single" and len(specific_blobs) != 1: return Error.ValueError(f"There isn't exactly one {translation_dict[direction]} RT.{rt} Relation. Did you mean {translation_dict[direction]}s[RT.{rt}]?")
    
    if direction == "inin": idx = 4
    elif direction == "outout": idx = 5
    else: idx = 0 # itself the relation

    if traverse_type == "single": return FlatRef(fr.fg, specific_blobs[0][idx])
    return FlatRefs(fr.fg, specific_blobs | map[lambda b: b[idx]] | collect)

def fr_outs_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    outs = blob[2] | filter[greater_than[-1]] | collect
    return FlatRefs(fr.fg, outs)

def fr_ins_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    ins = blob[2] | filter[less_than[0]] | collect
    return FlatRefs(fr.fg, ins)

def fr_ins_and_outs_imp(fr):
    assert isinstance(fr, FlatRef)
    blob = fr.fg.blobs[fr.idx]
    return FlatRefs(fr.fg, blob[2])

def fg_all_imp(fg, selector=None):
    assert isinstance(fg, FlatGraph)
    if selector:
        selected_blobs = fg.blobs | filter[lambda b: is_a(b[1], selector)] | collect
    else:
        selected_blobs = fg.blobs 
    return FlatRefs(fg, [b[0] for b in selected_blobs])


# -------------------------------- transact -------------------------------------------------
def transact_imp(data, g, **kwargs):
    from ..graph_delta import construct_commands
    if isinstance(data, FlatGraph):
        commands = flatgraph_to_commands(data)

    elif type(data)  in {list, tuple}:
        commands = construct_commands(data)
    
    else:
        raise ValueError(f"Expected FlatGraph or [] or () for transact, but got {data} instead.")

    return Effect({
            "type": FX.TX.Transact,
            "target_graph": g,
            "commands":commands
    })    

def transact_tp(op, curr_type):
    return VT.Effect

def fg_merge_imp(fg1, fg2):
    def idx_generator(n):
        def next_idx():
            nonlocal n
            n = n + 1
            return n
        return next_idx

    blobs, k_dict = [*fg1.blobs], {**fg1.key_dict}
    next_idx = idx_generator(length(blobs) - 1)

    idx_key_2 = {i:k for k,i in fg2.key_dict.items()}
    old_to_new = {}

    def retrieve_or_insert_blob(new_b):
        old_idx = new_b[0]
        key = idx_key_2.get(new_b[0], None)

        if old_idx in old_to_new:
            idx = old_to_new[old_idx]
            new_b = blobs[idx]
        elif (new_b[1] == 'BT.ValueNode' or is_a(new_b[3], uid)) and key in k_dict:
            new_b = blobs[k_dict[key]]
            idx = new_b[0]
        else:
            idx = next_idx()
            if key: k_dict[key] = idx
            new_b = (idx, new_b[1], [], *new_b[3:])
            blobs.append(new_b)
        old_to_new[old_idx] = idx
        return new_b

    for b in fg2.blobs | filter[lambda b: isinstance(b[1], RelationType)] | sort[lambda b: -len(b[2])]:
        rt_key = idx_key_2.get(b[0], None)

        src_b, trgt_b = fg2.blobs[b[4]], fg2.blobs[b[5]]
        src_b  = retrieve_or_insert_blob(src_b)
        trgt_b = retrieve_or_insert_blob(trgt_b)
        
        idx = next_idx()
        rt_b = (idx, b[1], [], None, src_b[0], trgt_b[0])
        src_b[2].append(idx)
        trgt_b[2].append(-idx)
        if rt_key: k_dict[rt_key] = idx
        blobs.append(rt_b)
        old_to_new[b[0]] = idx

            
    for b in fg2.blobs | filter[lambda b: not isinstance(b[1], RelationType)]:
        if b[0] not in old_to_new: retrieve_or_insert_blob(b)

    new_fg = FlatGraph()
    new_fg.blobs = blobs
    new_fg.key_dict = k_dict
    return new_fg




#-----------------------------Range----------------------------------------
def range_imp(*args):
    def generator_wrapper(r):
        yield from iter(r)
    
    return generator_wrapper(range(*args))

def range_tp(op, curr_type):
    return None