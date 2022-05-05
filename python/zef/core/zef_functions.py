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
TODO: make the decorator @zef_function(g, ...) also work if we don't have the primary instance: remote merge if someone else has the primary instance

z_fct can be a ET.ZEF_Function
inputs and outputs should be plain data: elementary python or ZefTensors, / flat graphs
If python Zef Functions are type annotated, the zef runtime can be used to enforce type checking (optional setting)

How does one define a zef function? Use a decorator @zef_function(g, label='square function', f1, f2) above a function definition
What does this do?
A)   side effect: extracts the functions source code, specified meta data, closure over other 
     functions (should we allow variables? e.g. plain data like lists, also ZefRefs) and saves all of this on the specified graph.
B)   What should the decoration process return? 
     Firstly, something that behaves like the function that was decorated. This could be the actual function, 
     but we may want to close over this very zef function in other zef functions. These need to know 

why do we want to overload the __call__ method for a ZefRef? Then the syntax becomes easier: we can just do z_my_zef_fct(42, 'x')

Future options: provide optional specification of exception behavior as well?

-------------------- Example -----------------

@zef_function(
    g,
    graph_spec = z_some_spec,   # specs are just boolean functions

    my_fct = z_fct5,            # binding of other zef functions to names
    my_fct2 = z_fct45,     
)
def my_fct(x: z_spec1,y: z_spec2, z: z_spec3) -> z_spec4:
    return 42

"""





from typing import Optional, Union
import typing
import inspect
import pyperclip
import traceback

from . import internals
from ._core import *
from ._ops import *


# IMPORTANT!!!
# IMPORTANT!!!
# IMPORTANT!!!
# NOTE: This definition of func must live here, separated from the ops package.
# This is to break any dependencies other modules (e.g. the fx package) have on
# func, without themselves depending on ops and causing a cyclic dependency
# chain.
# IMPORTANT!!!
# IMPORTANT!!!

##############################
# * Func op
#----------------------------

# This provides the zefop-like behaviour expected from a func. Needs to support theset hings:
# - z | func[f] | run
#
# - @func
#   def f(x, y):
#     ...
#   z | f[42] | run
#
# - g = Graph()
#   @func(g)
#   def f(x, y):
#     ...
#   z | f[42] | run
#
# - z | func[lambda x: x] | run



# This is not a zefop, it only becomes one when [] is applied to it.
class FunctionConstructor:
    """
    1. -------------------------------------------------------
     func[lambda x: 2*x]     # close over Python function
    
    2. -------------------------------------------------------
    func[z_my_function_zefref]      # why may we want this rather than func(z_my_function_zefref)? Possibly useful if this appears in zefop chain. (?) Unsure 
    
    3. -------------------------------------------------------
    func[z_my_function_ezefref]    # Zef Functions are values, any time slice defines the same one

    4. -------------------------------------------------------
    def ff(x, y):
        return x+2*y

    func(ff)            # close over Python function
    
    
    5. -------------------------------------------------------
    @func
    def ff(x, y):
        return x+2*y

        
    6. -------------------------------------------------------
    @func(g)
    def ff(x, y):
        return x+2*y

    """
    @staticmethod
    def __call__(*args, **kwds):
        from types import FunctionType
        from .abstract_raes import Entity
        if len(kwds) == 0 and len(args) == 1 and isinstance(args[0], FunctionType):
            return ZefOp(((RT.Function, ((1, args[0]), )), ))
        else:
            from zef.core.zef_functions import zef_function_decorator, _local_compiled_zef_functions, time_resolved_hashable
            promote_to_zefref_func = zef_function_decorator(*args, **kwds)
            def inner(func):
                zefref = promote_to_zefref_func(func)
                abstract_entity = Entity(zefref)
                _local_compiled_zef_functions[abstract_entity.d['uid']] = _local_compiled_zef_functions[time_resolved_hashable(zefref)]
                return ZefOp(((RT.Function, ((0, abstract_entity), )), ))
            return inner

    @staticmethod
    def __getitem__(arg):
        # TODO we gotta check if arg is of type Zef Lambda once we implement it
        # return ZefOp(((RT.Function, ((1, arg), )), ))
        return ZefOp(((RT.Function, ((1, arg), )), ))


func = FunctionConstructor()

##################################
# * Implementation
#--------------------------------





_local_compiled_zef_functions = {}
# do not realease the graphs from which zef functions are read to prevent garbage collection. We may need to access meta info
# _graphs_held_on_to = set()



def time_resolved_hashable(z_fct):
    """we can't use zefrefs as keys in a dict: z1==z2 
    comparison was disabled in zefdb for safety. Make a tuple of uzefrefs."""
    return (z_fct | to_ezefref | collect, z_fct | frame | collect)



def compile_in_zef_context(fct_str: str, additional_globals: dict, fake_filename="__zef__"):
    from .. import core
    from .. import ops
    shared_globals = {
        **core.__dict__,
        **ops.__dict__,
        **additional_globals,     # convert the list of tuples to a dict: the first tuple element is the fct name as a str
        } # pass variables in, but compiled fct will be in there too
    code = compile(fct_str, fake_filename, "exec")
    # if type annotations (both input and output) are made, these show up as the 
    # first elements in code.co_names tuple
    # e.g. for def mapper(x: str) -> int: 
    # code.co_names=('str', 'int', 'zef_function_4718c414b5be00ce70b2d59bbda46a2bba8610950c168e78')
    assert len(code.co_names) >= 1, "Function to compile produced multiple top-level names"
    exec(code, shared_globals)     # don't give the fct direct access to our globals here
    fct_name = code.co_names[-1]
    return shared_globals[fct_name]

def compile_zef_function(z_fct: ZefRef):
    """"""
    if time_resolved_hashable(z_fct) in _local_compiled_zef_functions:
        return z_fct

    g = Graph(z_fct)
    # Prevent garbage collection
    set_keep_alive(g, True)

    # first compile all zef functions bound within this zef fct's scope
    z_bound_fcts_pairs = (
        z_fct
        | out_rels[RT.Binding]
        | map[lambda z_rel: (z_rel | Out[RT.Name] | value | collect,
                             z_rel | target | in_frame[
                                 g[z_rel | Out[RT.UseTimeSlice] | value | collect]
                                 | to_graph_slice | collect
                             ] | collect)]
        | map[lambda p: (p[0], compile_zef_function(p[1]))]
        | collect
    )
    fct_str = z_fct | Out[RT.PythonSourceCode] | value | collect
    # what is in the scope of the zef function that will execute?    
    fct = compile_in_zef_context(fct_str, dict(z_bound_fcts_pairs))
    _local_compiled_zef_functions[time_resolved_hashable(z_fct)] = fct
    return fct



def zef_function_signature(z_fct):
    from inspect import signature
    from .core import rae_type, ET, ZefRef
    assert isinstance(z_fct, ZefRef)
    assert rae_type(z_fct) == ET.ZEF_Function
    
    compile_zef_function(z_fct)
    fct = _local_compiled_zef_functions[time_resolved_hashable(z_fct)]
    return signature(fct)



def is_zef_function_name(fct_name: str)->bool:
    if len(fct_name) != 61:
        return False
    if fct_name[0:13] != 'zef_function_':
        return False
    uid_maybe = fct_name[13:]
    if not set(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']
        ).issuperset(set(uid_maybe)):
        return False
    return True


def zef_function_decorator(g: Graph=None, label: str=None, is_pure=False, **kwargs):        
    """
    Use as a decorator above a function to beam it up into the zefnet.
    @zef_function(g, label='my favorite function')
    def my_f(x):
        ...


    - The function name given upon the very first definition is attached as z_zef_fct >> RT.OriginalName
    - if the optional label argument is specified in the decorator (a string), it is attached as z_zef_fct >> RT.Label




                                           RT.UseTimeSlice
                                       ┌──────────────────►AET.String    # hack for now until we can point at txs
                                       │
                                       │  RT.Name
                                       ├──────────────────►AET.String
                                       │
                       RT.Binding[*]   │
                    ┌──────────────────┴────────────────────────────────►ET.ZEF_Function
                    │
                    │
   ET.ZEF_Function──┤
                    │  RT.OriginalName
                    ├────────────────────►AET.String
                    │
                    │  RT.PythonSourceCode
                    ├────────────────────►AET.String
                    │
                    │  RT.Label?
                    ├────────────────────►AET.String
                    │
                    │  RT.IsPure?
                    └────────────────────►AET.Bool


    """
    assert label is None or isinstance(label, str)
    def inner(func):            
        """one more layer of indirection is needed if we want to create a decorator that takes arguments"""
        # assert isinstance(g, core.Graph)
        assert label is None or isinstance(label, str)
        # from .ops import terminate
        # make sure all kw args passed in are zef functions. We may consider binding libraries, 
        # data as parameters etc. in future if there is a compelling reason to do so.
        # For now we should try to import everything explicitly within the function.
        for fct_name, zef_fct_maybe in kwargs.items():
            if not isinstance(zef_fct_maybe, ZefRef):
                raise TypeError(f'Currently we only support binding of other zef functions within a zef function. The type passed in for the name "{fct_name}" was {type(zef_fct_maybe)}')
            if Graph(zef_fct_maybe) != g:
                raise RuntimeError(f'Currently any zef function bound to the local scope of another zef function has to live on the same graph. We are likely to relax this constraint once usage patterns become clear. Attempted to bind the zef function named "{fct_name}" {zef_fct_maybe}')


        # TODO: This is where we should analyze for call signature
        # from inspect import signature
        # print(signature(func).parameters)       # get the names of arguments of func

        if g is not None:
            return make_function_entity(g, label, is_pure, func, **kwargs)
        else:
            return LocalFunction(func, is_pure=is_pure)


    return inner

def make_function_entity(g, label, is_pure, func, **kwargs):        
    s_full = inspect.getsource(func)        
    import re
    # Doing two things here - a) finding the def line, and b) need to strip
    # common whitespace prefix from all lines.
    m = re.search(r"^(\s*)def ", s_full, re.MULTILINE)
    func_line_start = m.start()
    s = s_full[func_line_start:]

    prefix = len(m[1])
    lines = s.split('\n')
    lines = [line[prefix:] for line in lines]
    s = '\n'.join(lines)

    # what is the current function name? If it is 'zef_function_a4s564...', 
    # then we need to match it with an existing zef function. Otherwise create it new.        
    fct_name = func.__name__
    zef_function_exists = is_zef_function_name(fct_name)
    docstring_maybe = inspect.getdoc(func)

    with Transaction(g):
        if zef_function_exists:
            # zef function already exists. matching up
            zef_fct_uid = fct_name[13:]
            if zef_fct_uid not in g:
                raise RuntimeError("The uid extracted from the zef function name was not found in the graph")

            z_zef_fct = (g[zef_fct_uid] | now)
            z_python_str = z_zef_fct | Out[RT.PythonSourceCode] | collect
            # only assign a new value if the contents changed
            if (value(z_python_str)) != s:
                z_python_str | assign_value[s] | g | run

            # If docstring already exists; update it may be
            if len(z_zef_fct | Outs[RT.DocString] |collect) == 1:
                z_doctstring_str = z_zef_fct | Out[RT.DocString] | collect
                # If the Docstring was removed
                if not docstring_maybe:
                    z_zef_fct | out_rel[RT.DocString] | terminate | g | run
                # If Docstring was updated
                elif docstring_maybe != (z_doctstring_str | value | collect):
                    z_doctstring_str | assign_value[docstring_maybe] | collect
            # Case where this function existed before introducting Docstring parsing
            else:
                # If Docstring is defined attach it
                if docstring_maybe: (z_zef_fct, RT.DocString, docstring_maybe) | g | run
        else:
            # zef function does not exist yet. Make a new ET.ZEF_Function and attach the string after processing
            z_zef_fct = ET.ZEF_Function | g | run
            # replace the function name given by the user with a unique one containing the uid of the rel
            s_renamed = s[:s.find('def')] + f"def zef_function_{uid(z_zef_fct)}" + s[s.find('('):]
            (z_zef_fct, RT.PythonSourceCode, s_renamed) | g | run
            if label is not None:
                (z_zef_fct, RT.Label, label) | g | run
            if is_pure is not False:
                (z_zef_fct, RT.IsPure, is_pure) | g | run

            # attach the very first name the programmer gave as metadata (extracting 
            # the fct after this will auto-generate the name using the uid)
            (z_zef_fct, RT.OriginalName, fct_name) | g | run

            # If Docstring is defined attach it
            if docstring_maybe: (z_zef_fct, RT.DocString, docstring_maybe) | g | run


        # TODO: What if only the name of the bound variable was changed?

        # immediately focus on the diffing case, if one needs to establish the required diff of bindings present in the 
        # zef function pre and post                
        bindings_pre = dict((z_zef_fct | out_rels[RT.Binding]) 
                        | map[lambda z_rel: (z_rel | Out[RT.Name] | value, z_rel | target | to_frame[ g[z_rel | Out[RT.UseTimeSlice] | value]] )]
                        | collect
                        )
        bindings_post = kwargs
        # bindings_to_add = bindings_post - bindings_pre
        bindings_to_add = {k:v for k,v in bindings_post.items() if k not in bindings_pre}
        bindings_to_remove = {k:v for k,v in bindings_pre.items() if k not in bindings_post}

        def remove_binding(z_fct: ZefRef, name: str, z_attached_fct: EZefRef):
            z_ed = (z_fct | out_rels[RT.Binding]) | filter[lambda z_ed: z_ed | Out[RT.Name] | value == name] | only | collect
            z_ed | Out[RT.Name] | terminate | g | run
            z_ed | Out[RT.UseTimeSlice] | terminate | g | run
            z_ed | terminate | g | run

        def add_binding(z_fct: ZefRef, name: str, z_attached_fct):                    
            z_rel = (instantiate(z_fct | to_ezefref | collect, RT.Binding, z_attached_fct | to_ezefref | collect, g) 
                    | fill_or_attach[RT.Name, name] 
                    | fill_or_attach[RT.UseTimeSlice, str(base_uid(z_attached_fct | frame | to_tx))]        # the zef function passed in by the user may be pointing to an earlier 
                    | collect
                    # TODO: this should directly point at the transaction once zefDB allows attaching RTs to txs. This is just a hack for now !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    )

        list(bindings_to_remove.items()) | for_each[lambda p: remove_binding(z_zef_fct, *p)]
        list(bindings_to_add.items()) | for_each[lambda p: add_binding(z_zef_fct, *p)]


    # Prevent garbage collection
    set_keep_alive(g, True)
    # from zefdb.tools import pipeable
    # func_pipeable = pipeable(func)
    # func_pipeable._my_zefref = z_zef_fct
    # return func_pipeable
    compile_zef_function(z_zef_fct | now | collect)     # purely runs side effects: compiles fct and adds it to _local_compiled_zef_functions. It is idempotent though.
    # to overload the function that is being called upon z(), we cannot monkey patch the actual object instance.
    # Python calls type(z).__call__(z), which we can monkey patch for all instances of the class            
    return z_zef_fct


class LocalFunction:
    def __init__(self, func, *, is_pure=False):
        self.func = func
        self.is_pure = is_pure

    def __getitem__(self, arg):
        return call[self][arg]

    def __call__(self, *args, **kwds):
        return self.func(*args, **kwds)

    def __or__(self, other):
        # Promotes the function to a ZefOp then pipes with other
        # i.e: @func
        #      def f(x): pass
        # f | map
        return call[self] | other

    def __ror__(self, other):
        # Similar to ror behavior but pipes the other way
        # i.e: map | f
        return other | call[self]
    
        


def zef_fct_to_source_code_string(z_func: ZefRef) -> str:
    from ..ops import text_art

    def _make_header(z_zef_function: ZefRef)->str:
        g = Graph(z_zef_function)         
        # TODO: possibly warn if this is NOT the latest version known at the time of generating this snippet!
        original_fct_name = z_zef_function | Out[RT.OriginalName] | value | collect if z_zef_function | has_out[RT.OriginalName] | collect else None
        original_fct_name_val = original_fct_name if original_fct_name is not None else '*unknown*'
        graph_uid = str(uid(g))
        return (
"#%% " + "-"*37 + f" zef function - original name: '{original_fct_name_val}' " + "-"*37 + "\n"
+ f"# -------------------------- this code snippet was generated at {str(now())} --------------------------\n"
+ text_art(f"---- {original_fct_name_val} ----") 
+ "\n#\n" + "\n".join([
#f"# origin: first created by {'-- ZefDB user --'} at ...",
#f"# last change: by -- ZefDB user -- ...",
# f'# z_zef_function = g["{uid(z_zef_function)}"]["{uid(z_zef_function | frame)}"]',            # TODO: update this to new uid and ref frame syntax
f"# we need to bind this to a var outside for now to prevent the graph being garbage collected.\n"
f"g_{graph_uid[:8]} = Graph('{graph_uid}')     # Graph tags: {g.tags}",
f'@func(',
f'    g = g_{graph_uid[:8]},',
f"    label = '{value(z_zef_function | Out[RT.Label])}'," if z_zef_function | has_out[RT.Label] | collect else None,
f"    is_pure = {value(z_zef_function | Out[RT.IsPure])}," if z_zef_function | has_out[RT.IsPure] | collect else None,
*[f"    {value(z_ed | Out[RT.Name])} = g['{uid(z_ed | target)}'] | to_frame[g['{value(z_ed | Out[RT.UseTimeSlice])}']]," for z_ed in (z_zef_function | out_rels[RT.Binding] | collect) ],
f")\n"
        ] | filter[lambda x: x is not None] | collect
        ))    
    assert isinstance(z_func, ZefRef) and ET(z_func)==ET.ZEF_Function
    fct_body = z_func | Out[RT.PythonSourceCode] | value | collect
    assert isinstance(fct_body, str)       
    return  _make_header(z_func) + fct_body + "\n"*8






# Monkey patching ZefRef to support the call syntax
def _overloaded_zefref_call(self, *args, **kwargs):
    """
    Function that will be monkey patched in upon zefdb init for the ZefRef type object.
    Note that executing z.__call__(z) where z is a ZefRef actually calls type(z).__call__(z)
    self will be the ZefRef"""
    
    if rae_type(self) != ET.ZEF_Function: 
        raise TypeError(f'The call operator for a ZefRef can only be used if for zef functions. It was called from {self}')
    try:    # it's cheaper to ask for forgiveness
        fct = _local_compiled_zef_functions[time_resolved_hashable(self)]
    except KeyError:
        # if its not in the function cache dict, compile it
        fct = compile_zef_function(self)
    try:
        return_val = fct(*args, **kwargs)
    except Exception as exc:
        print(f"Error in executing zef function: {exc}. Reraising...")
        raise exc
    return return_val

from ..pyzef import main 
main.ZefRef.__call__ = _overloaded_zefref_call

def abstract_entity_call(entity, *args, **kwargs):
    from .abstract_raes import Entity
    if not isinstance(entity, Entity): 
        raise TypeError(f'Trying to call using abstract entity but {entity} was given instead')
    try:    # it's cheaper to ask for forgiveness
        fct = _local_compiled_zef_functions[entity.d['uid']]
    except KeyError:
        raise Exception("The abstract entity didn't have any cached compiled function.")
        # TODO: Here gather the ZefFunction and compile it!
        # fct = compile_zef_function(self)
    try:
        return_val = fct(*args, **kwargs)
    except Exception as exc:
        print(f"Error in executing zef function: {exc}. Reraising...")
        raise exc
    return return_val

# Monkey patching ZefRef to support the currying syntax
# def _overloaded_zefref_getitem(self, arg):
#     """
#     Function that will be monkey patched in upon zefdb init for the ZefRef type object.
#     self will be the ZefRef"""
    
#     if rae_type(self) != ET.ZEF_Function: 
#         raise TypeError(f'The [] syntax for a ZefRef can only be used if for zef functions. It was called from {self}')

#     from ._ops import call
#     return call[self][arg]

# from ..pyzef import main 
# main.ZefRef.__getitem__ = _overloaded_zefref_getitem
