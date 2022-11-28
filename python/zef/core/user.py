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
    "get_user_graph",
    "get_user_namespace",
]

from ._ops import *
# Relying on this not being imported till later
from . import Namespace, GraphRef, Graph, AET, set_keep_alive, FlatGraph, ET
from .namespaces import PythonVarRef, LiveNamespace

# def get_user_graph():
#     from ..pyzef.main import get_user_graph

#     return get_user_graph()
def get_user_graph():
    # This is a temporary stub while we are on the older version of zefhub
    from ..pyzef import main as pymain, zefops as pyzo
    config_dir = pymain.zefdb_config_path()
    import os
    user_graph_filename = os.path.join(config_dir, "user_graph_stub.zefgraph")
    g = Graph("file://" + user_graph_filename)
    # We will fake this being synced by updating on any change - this won't work well with multiple sessions though.
    g | pyzo.subscribe[lambda x: pymain.save_local(g)]
    return g

bootstrap_ns = Namespace(
    zo=Namespace(
        map=map,
        match=match,
        value=value,
        now=now,
        uid=uid,
    ),
    # ioft=GraphRef(uid("c099f5bad7653717")),
    NS_insert=PythonVarRef(module_path="zef.core.namespaces", target="NS_insert"),
    NS_remove=PythonVarRef(module_path="zef.core.namespaces", target="NS_remove"),
    test_int=42,
    test_string="stringy",
    test_fg=FlatGraph() | insert[ET.Dummy] | collect,
)

def get_user_namespace():
    gref = get_user_graph()
    # While we are on the older version of zefhub, we'll just load this locally
    g = Graph(gref)
    if "main namespace" not in now(g):
        ns_ent = AET[Namespace] | assign[bootstrap_ns] | g | run
        ns_ent | tag["main namespace"] | g | run
    else:
        ns_ent = g | now | get["main namespace"] | collect

    return to_ezefref(ns_ent)
    

def watch_user_ns(scope=None):
    from .namespaces import watch_ns, get_upper_scope
    if scope is None:
        scope = get_upper_scope()

    # TODO: Shouldn't need to keep doing this for general subscriptions, but
    # here we need to keep the graph alive so the subscription continues through
    # sensibly.
    set_keep_alive(Graph(get_user_graph()), True)

    # watch_ns(get_user_namespace(), scope)
    ns = LiveNamespace(get_user_namespace(), downstream_scope=scope, apply_upstream=True)

    return ns