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

__all__ = [
    "Namespace",
    "import_ns_static",
]

from .. import report_import
report_import("zef.core.namespaces")

from . import internals
from .VT import *
from .zef_functions import func

special_graph_uid = internals.BaseUID('0000000000000002')
namespace_base_uid = internals.BaseUID('0000000000000001')
python_var_ref_base_uid = internals.BaseUID('0000000000000002')
namespace_ref_base_uid = internals.BaseUID('0000000000000003')




PythonVarRef = UserValueType(
    'PythonVarRef',
    Dict,
    Pattern[{"module_path": String,
             "target": String}],

    forced_uid=str(internals.EternalUID(python_var_ref_base_uid, special_graph_uid)),
)

def PVR_get(pvr: PythonVarRef):
    # This is temporary - so just assume everything will work and will be pure values etc...
    import importlib
    mod = importlib.import_module(pvr.module_path)
    return getattr(mod, pvr.target)
    


# This is to cheat the recursive definition with a delay-execution function.
def NamespaceCheat_is_a(x, typ):
    return isinstance(x, Namespace)
from .VT import make_VT
NamespaceCheat = make_VT("NamespaceCheat", is_a_func=NamespaceCheat_is_a)

# AllowedNamespaceTypes = GraphRef | RAERef | Namespace # | FuncRef | NamespaceRef
# AllowedNamespaceTypes = GraphRef | RAERef # | FuncRef | NamespaceRef
# Just for testing - I don't know if scalar valuess will remain
# AllowedNamespaceTypes = GraphRef | RAERef | NamespaceCheat | String | Int
# Could just allow everything and then let the rest sort itself out
AllowedNamespaceTypes = Any

def Namespace_get_value(x):
    from ._ops import match, value, identity, collect
    from .op_structs import LazyValue
    return LazyValue(x) | match[
        (GraphRef, Graph),
        (PythonVarRef, PVR_get),
        # (NamespaceRef, Namespace),
                (AttributeEntityConcrete, value),
        # TODO: Something with AERefs
        # (AttributeEntityRef, value),
                (Any, identity),
    ] | collect

def Namespace_getattr(self, x):
    val = self._value[x]
    return Namespace_get_value(val)

Namespace = UserValueType(
    'Namespace',
    Dict,
    Dict[String][AllowedNamespaceTypes],

    forced_uid=str(internals.EternalUID(namespace_base_uid, special_graph_uid)),
    
    # this is for the local python syntax
    object_methods = {
        'getattr': Namespace_getattr,
    }
)

# NamespaceRef = UserValueType(
#     'NamespaceRef',
#     AttributeEntityRef,
#     # AET[Namespace],
#     # TODO: The above, when isinstance supports it
#     # AET,
#     AttributeEntity,

#     forced_uid=str(internals.EternalUID(namespace_ref_base_uid, special_graph_uid)),
# )

# Wrapped functions for manipulating namespaces like dictionaries
def make_wrapped_modifier(func_to_wrap):
    @func
    def wrapper(ns, *args):
        d = ns._value
        new_d = func_to_wrap(d, *args)
        return Namespace(new_d)
    return wrapper
def make_wrapped_accessor(func_to_wrap):
    @func
    def wrapper(ns, *args):
        d = ns._value
        return func_to_wrap(d, *args)
    return wrapper

from . import _ops
NS_insert = make_wrapped_modifier(_ops.insert)
NS_remove = make_wrapped_modifier(_ops.remove)
NS_update = make_wrapped_modifier(_ops.update)

NS_get = make_wrapped_accessor(_ops.get)
NS_get_in = make_wrapped_accessor(_ops.get_in)
NS_items = make_wrapped_accessor(_ops.items)
NS_keys = make_wrapped_accessor(_ops.keys)
NS_values = make_wrapped_accessor(_ops.values)
NS_contains = make_wrapped_accessor(_ops.contains)
NS_select_keys = make_wrapped_accessor(_ops.select_keys)

@func
def NS_merge(ns1, ns2):
    d1 = ns1._value
    d2 = ns2._value
    return Namespace(_ops.merge(d1,d2))

@func
def NS_insert_into(pair, ns):
    d = ns._value
    return Namespace(_ops.insert_into(pair, d))

# These need to modify namespaces recursively
# NS_insert_in = make_wrapped_modifier(_ops.insert_in)
# NS_remove_in = make_wrapped_modifier(_ops.remove_in)



def get_upper_scope():
    import inspect
    # We get index 2 here because we want to skip us (0), our caller (1) and get
    # the caller of the caller.
    above = inspect.stack()[2].frame
    return above.f_globals

def import_ns_static(ns: Namespace, scope: Dict|Nil=None):
    if scope is None:
        scope = get_upper_scope()

    for name,val in ns._value.items():
        # scope[name] = val
        scope[name] = Namespace_get_value(val)

def watch_ns(ns_ezr: EZefRef, scope: Dict|Nil=None):
    if scope is None:
        scope = get_upper_scope()

    from ._ops import on, subscribe, value, now
    def do_import(*args):
        print("Importing namespace")
        import_ns_static(value(now(ns_ezr)), scope)
    do_import()
    # TODO: fix this up when it's just simply subscribing to the EZR
    Graph(ns_ezr) | on[Assigned[ns_ezr]] | subscribe[do_import]




class LiveNamespace:
    def __init__(self, arg=None, *, upstream_ezr=None, downstream_scope=None, apply_upstream=False, apply_downstream=True):
        if isinstance(arg, Namespace):
            assert upstream_ezr is None
            starting_ns = arg
        elif isinstance(arg, AttributeEntity):
            assert upstream_ezr is None
            upstream_ezr = arg
        elif arg is None:
            # This will only be used if upstream_ezr is not set
            starting_ns = Namespace({})
        else:
            raise Exception(f"Don't understand main argument: {arg}")

        self._upstream_ezr = upstream_ezr
        self._downstream_scope = downstream_scope
        self._apply_downstream = apply_downstream
        self._apply_upstream = apply_upstream
        self._ns = None

        if upstream_ezr is None:
            self._ns = starting_ns
        else:
            # TODO: Do subtype check to see if namespaces can be included in this ae.
            # assert issubclass(Namespace, AET(upstream_ezr).copmlex_type)
            self._pull_from_upstream()
            # TODO: Setup sub here to listen to future updates
            from ._ops import on,subscribe
            Graph(self._upstream_ezr) | on[Assigned[self._upstream_ezr]] | subscribe[lambda x: self._pull_from_upstream]
        self._update_downstream()

    def _pull_from_upstream(self):
        if self._upstream_ezr is None:
            return
        
        from ._ops import value,now
        new_value = value(now(self._upstream_ezr))
        if not isinstance(new_value, Namespace):
            log.warning(f"Namespace EZR was not a namespace! {self._upstream_ezr}")
            return

        if self._ns == new_value:
            return

        self._ns = new_value
        self._update_downstream()
        
    def _update_downstream(self):
        if self._downstream_scope is None:
            return
        if not self._apply_downstream:
            return
        
        import_ns_static(self._ns, self._downstream_scope)

    def _update_upstream(self):
        if self._upstream_ezr is None:
            return
        if not self._apply_upstream:
            return

        from ._ops import assign, run
        self._upstream_ezr | assign[self._ns] | Graph(self._upstream_ezr) | run

    def __getitem__(self, x):
        return self._ns[x]

    def __getattr__(self, name):
        return getattr(self._ns, name)
    def __dir__(self):
        return dir(self._ns)
    
    def __setattr__(self, name, value):
        if name.startswith("_"):
            return object.__setattr__(self, name, value)
        self._ns = NS_insert(self._ns, name, value)
        self._update_downstream()
        self._update_upstream()

    def __repr__(self):
        s = "LiveNamespace("
        details = []
        if self._upstream_ezr is not None:
            details += [f"upstream: {self._upstream_ezr}"]
        if self._apply_upstream:
            details += [f"listening"]
        if self._apply_downstream and self._downstream_scope is not None:
            details += [f"watched by scope"]
        details += [str(self._ns)]

        s += ', '.join(details)
        s += ")"

        return s





# # Idea for self-referential types
# NamespaceInternal = Where[T][UserValueType(
#     'Namespace',
#     Dict,
#     Where[T][Dict[String][AllowedNamespaceTypes | T]],

#     forced_uid=str(internals.EternalUID(namespace_base_uid, special_graph_uid)),
    
#     # this is for the local python syntax
#     # object_methods = {
#     #     'get_item': lambda self, key: self[key],
#     #     'contains': lambda self, key: key in self,
#     # }
# )]
# Namespace = NamespaceInternal[Myself]

# # A different idea for self-referential types
# #
# # `Myself` refers to next-scoped Where. Here Where is used without any
# # parameters to just facilitate the scoping.
# #
# # For value hashing, Myself is a dual value. If asked for the hash of Namespace,
# # Myself is replaced with "xxxxxx" a given uid. If asked for the hash of the
# # element type of namespace, Myself is replaced with the value hash of
# # Namespace. (Because Dict | Myself could be different for two different types
# # even though it is symbollically the same)
# #
# # Perhaps we can define these recursive types in a different scope, like an AST
# # parsed function?
# Namespace = Where[()][UserValueType(
#     'Namespace',
#     Dict,
#     Dict[String][AllowedNamespaceTypes | Myself],

#     forced_uid=str(internals.EternalUID(namespace_base_uid, special_graph_uid)),
    
#     # this is for the local python syntax
#     # object_methods = {
#     #     'get_item': lambda self, key: self[key],
#     #     'contains': lambda self, key: key in self,
#     # }
# )]


# # Idea for parsing using AST
# @parse_definitions
# def dummy():
#     Namespace = UserValueType(
#         'Namespace',
#         Dict,
#         Dict[String][AllowedNamespaceTypes | Namespace],

#         forced_uid=str(internals.EternalUID(namespace_base_uid, special_graph_uid)),

#         # this is for the local python syntax
#         # object_methods = {
#         #     'get_item': lambda self, key: self[key],
#         #     'contains': lambda self, key: key in self,
#         # }
#     )