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

from ..core import *
from ..ops import *
from typing import *
from .auto import auto_generate_gql
from ..core.logger import log

def schema_node_checks(schema_node: ZefRef):
    if type(schema_node) != ZefRef:
        raise TypeError("Passed schema_node should be of type ZefRef")

    if (schema_node | BT | collect != BT.ENTITY_NODE):
        raise TypeError("Passed schema_node should be of type BT.ENTITY_NODE")

    if (schema_node | ET | collect != ET.GQL_Schema):
        raise TypeError("Passed schema_node should be of type ET.GQL_Schema")


def gql_type_checks(gql_type: ZefRef):
    if type(gql_type) != ZefRef:
        raise TypeError("Passed gql_type should be of type ZefRef")

    if (gql_type | BT | collect != BT.ENTITY_NODE):
        raise TypeError("Passed gql_type should be of type BT.ENTITY_NODE")

    if (gql_type | ET | collect != ET.GQL_Type):
        raise TypeError("Passed gql_type should be of type ET.GQL_Type")

def gql_field_checks(gql_field: ZefRef):
    if type(gql_field) != ZefRef:
        raise TypeError("Passed gql_field should be of type ZefRef")

    if (gql_field | BT | collect != BT.RELATION_EDGE):
        raise TypeError("Passed gql_field should be of type BT.RELATION_EDGE")

    if (gql_field | RT | collect != RT.GQL_Field):
        raise TypeError("Passed gql_field should be of type ET.GQL_Field")


@func
def gql_schema(g: Graph) -> ZefRef:
    if type(g) != Graph:
        raise TypeError("Passed g should be of type Graph")

    schema_node = g | now | all[ET.GQL_Schema] | collect
    if len(schema_node) != 1:
        raise Exception(f"There wasn't exactly one schema_node of type ET.GQL_Schema")
    return schema_node | only | collect

@func
def gql_types(schema_node: ZefRef) -> ZefRefs:
    schema_node_checks(schema_node)
    return (schema_node >> L[RT.GQL_Type]) | collect

@func
def gql_interfaces(schema_node: ZefRef) -> ZefRefs:
    schema_node_checks(schema_node)
    return (schema_node >> L[RT.GQL_Interface]) | collect

@func
def gql_scalars(schema_node: ZefRef) -> ZefRefs:
    schema_node_checks(schema_node)
    return (schema_node >> L[RT.GQL_Scalar]) | collect

@func
def gql_enums(schema_node: ZefRef) -> ZefRefs:
    schema_node_checks(schema_node)
    return (schema_node >> L[RT.GQL_Enum]) | collect


@func
def gql_types_dict(schema_node: ZefRef) -> dict:
    schema_node_checks(schema_node)
    return {str(t >> RT.Name | value | collect) : t for t in schema_node | outs | target | filter[lambda x: x | BT | collect == BT.ENTITY_NODE] | collect}

@func
def gql_fields_rt_dict(gql_type: ZefRef) -> dict:
    gql_type_checks(gql_type)
    return {str(t >> RT.Name | value) : t for t in gql_type > L[RT.GQL_Field] | collect}

@func
def gql_field_resolver(gql_field: ZefRef) -> ZefRef:
    gql_field_checks(gql_field)
    resolver_types = [RT.GQL_Resolve_with, RT.GQL_Resolve_with_intermediate, RT.GQL_Resolve_with_body, RT.GQL_Resolve_with_zcript]
    resolvers = gql_field | outs | filter[lambda z: z | RT in resolver_types] | collect
    if len(resolvers) != 1:
        raise Exception(f"There wasn't exactly one gql_resolver associated with field {gql_field}")
    return resolvers | only | collect

@func
def gql_info(schema_node: ZefRef) -> str:
    schema_node_checks(schema_node)
    bl = blobs(Graph(schema_node))
    all_rts = bl | filter[BT.RELATION_EDGE] | collect
    return f"""
======================================================================================================================
==================================================== GQL Summary =====================================================
======================================================================================================================

ET.GQL_Type:                        {len(schema_node | gql_types)}
ET.GQL_Interface:                   {len(schema_node | gql_interfaces)}
ET.GQL_Scalar:                      {len(schema_node | gql_scalars)}
ET.GQL_Enum:                        {len(schema_node | gql_enums)}
RT.GQL_Resolve_with:                {len(all_rts| filter[lambda z: z | RT == RT.GQL_Resolve_with])}
RT.GQL_Resolve_with_intermediate:   {len(all_rts| filter[lambda z: z | RT == RT.GQL_Resolve_with_intermediate])}
RT.GQL_Resolve_with_zcript:         {len(all_rts| filter[lambda z: z | RT == RT.GQL_Resolve_with_zcript])}
\n
"""
