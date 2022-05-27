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

from zef import *
from zef.ops import *
from .ariadne_utils import gqlify, de_gqlify
from ..core.logger import log

# TODO attach Delegates


# General Utils #
def find_field_with_name(gql_type, name):
    for f in gql_type > L[RT(gqlify("field"))] | collect:
        if de_gqlify(f >> RT.Name | value | collect, False) == name:
            return f


def connect_direct_resolvers_to_delegates(g, gql_type, data_type, field_to_rt):
    print("#TODO This is deprecated use connect_delegate_resolvers instead")
    for field_name, rt_name in field_to_rt.items():
        field_zr = find_field_with_name(gql_type, field_name) | to_ezefref | collect
        data_delegate = ET(data_type) | delegate_of[g] > RT(rt_name) | collect
        create_resolver_to_field(g, field_zr, data_delegate)

# This is deprecated use connect_delegate_resolvers instead
def connect_direct_resolvers_to_specific_delegate(g, gql_type, data_type, tups_of_specific_relations):
    print("#TODO This is deprecated use connect_delegate_resolvers instead")
    for tup in tups_of_specific_relations:
        field_name, rt_name, target_type = tup
        field_zr = find_field_with_name(gql_type, field_name) | to_ezefref | collect
        data_delegate = (ET(data_type) | delegate_of[g] > L[RT(rt_name)] | collect) | filter[lambda x: x | target | AET | collect == target_type] | only | collect 
        create_resolver_to_field(g, field_zr, data_delegate)


def connect_zef_function_resolvers(g, gql_type, field_resolving_dict):
    for field_name, handler_func in field_resolving_dict.items():
        gql_rt = find_field_with_name(gql_type, field_name)
        create_zef_function_resolver(g, gql_rt, handler_func)

def connect_delegate_resolvers(g, gql_type, field_resolving_dict):
    for field_name, delegate_details in field_resolving_dict.items():
        delegate_triple, is_out = delegate_details["triple"], delegate_details.get("is_out", True)
        field_zr = find_field_with_name(gql_type, field_name) | to_ezefref | collect
        data_delegate = [delegate_of(delegate_triple)] | transact[g] | run | get[delegate_of(delegate_triple)] | collect
        create_resolver_to_field(g, field_zr, data_delegate, is_out)

# Resolve With Connectors#
def create_zefscript_resolver(g, gql_rt, script):
    if length(now(gql_rt) >> L[RT(gqlify("resolve_with_script"))] | collect) == 0:
        aet_str = instantiate(AET.String, g)
        (aet_str <= script) | g | run
        zef_scr = instantiate(ET.ZEF_Script, g)
        instantiate(zef_scr, RT.ZEF_Python, aet_str, g)
        instantiate(gql_rt, RT(gqlify("resolve_with_script")), zef_scr, g)
    else:
        log.warn(f"The bindings for resolve_with_script on {gql_rt | uid} has been updated")
        (((gql_rt >> RT(gqlify("resolve_with_script")) | collect) >> RT.ZEF_Python) <= script) | g | run

def create_body_resolver(g, gql_rt, script):
    if length(now(gql_rt) >> L[RT(gqlify("resolve_with_body"))]) == 0:
        aet_str = instantiate(AET.String, g)
        (aet_str <= script) | g | run
        instantiate(gql_rt, RT(gqlify("resolve_with_body")), aet_str, g)
    else:
        log.warn(f"The bindings for resolve_with_body on {gql_rt | uid} has been updated")
        ((gql_rt >> RT(gqlify("resolve_with_body")) | collect ) <= script) | g | run


def create_intermediate_resolver_to_field(g, gql_rt, del_rt):
    if length(now(gql_rt) >> L[RT(gqlify("resolve_with_intermediate"))]) == 0:
        gql_rt, del_rt = gql_rt | to_ezefref | collect, del_rt | to_ezefref | collect
        instantiate(gql_rt, RT(gqlify("resolve_with_intermediate")), del_rt, g)


def create_resolver_to_field(g, gql_rt, del_rt, is_out = False):
    if length(now(gql_rt) >> L[RT(gqlify("resolve_with"))]) == 0:
        gql_rt, del_rt = gql_rt | to_ezefref | collect, del_rt | to_ezefref | collect
        rt = instantiate(gql_rt, RT(gqlify("resolve_with")), del_rt, g)
        aet = instantiate(AET.Bool, g)
        (aet <= is_out) | g | run
        rt = instantiate(rt, RT.IsOut, aet, g)


def create_zef_function_resolver(g, gql_rt, z_func):
    if length(now(gql_rt) >> L[RT(gqlify("resolve_with_zef_function"))]) == 0:
        (gql_rt, RT(gqlify("resolve_with_zef_function")), z_func) | g | run

# Relay #
def apply_cursor_to_edges(edges, before=None, after=None):
    def check_edge_location(edges, cursor, return_cursor):
        for idx, elm in enumerate(edges):
            if elm | uid == cursor:
                return idx
        return return_cursor

    prior_to_after = False
    if after:
        after_Edge = check_edge_location(edges, after, 0)
        if after_Edge != 0:
            edges = edges[after_Edge + 1:]
            prior_to_after = True

    following_before = False
    if before:
        before_edge = check_edge_location(edges, before, length(edges))
        if before_edge != length(edges):
            edges = edges[:before_edge]
            following_before = True

    return edges, prior_to_after, following_before


def edges_to_return(edges, first=None, last=None):
    if first and last:
        raise Exception("Both first and last are passed")

    if first:
        if first < 0:
            raise Exception("First is less than 0")

        if length(edges) > first:
            edges = edges[:first]

    if last:
        if last < 0:
            raise Exception("last is less than 0")

        if length(edges) > last:
            edges = edges[length(edges) - last:]

    return edges


def has_previous(edges, prior_to_after, after=None, last=None):
    if last:
        if length(edges) > last:
            return True
        return False

    elif after:
        # If the server can efficiently determine that elements exist prior to after, return true.
        return prior_to_after
    else:
        return False


def has_next(edges, following_before, before=None, first=None):
    if first:
        if length(edges) > first:
            return True
        return False

    elif before:
        # If the server can efficiently determine that elements exist following before, return true.
        return following_before
    else:
        return False


def pagination_algorithm(edges, before=None, after=None, first=None, last=None):
    if not edges:
        return {"pageInfo": {
            "hasPreviousPage": False,
            "hasNextPage": False,
            "startCursor": "",
            "endCursor": ""
        },
            "edges": []}
    edges = [e for e in edges]
    edges, prior_to_after, following_before = apply_cursor_to_edges(edges, before, after)
    return_edges = edges_to_return(edges, first, last)

    return {"pageInfo": {
        "hasPreviousPage": has_previous(edges, prior_to_after, after, last),
        "hasNextPage": has_next(edges, following_before, before, first),
        "startCursor": return_edges[0] | uid,
        "endCursor": return_edges[length(return_edges) - 1] | uid
    },
        "edges": return_edges}

