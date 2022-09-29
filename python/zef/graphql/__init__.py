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

from .resolvers_generator import generate_resolvers, fill_types_default_resolvers
from .schema_generator import generate_schema_str
from .schema_parser import generate_schema_dict

__all__ = [
    "generate_resolvers",
    "generate_schema_str",
    "generate_schema_dict",
    "make_graphql_api",
    "fill_types_default_resolvers"
]

def make_graphql_api(schema_dict: dict, g = None):
    """
    Given a schema dictionary, generate Ariadne's GraphQL Executable Schema.
    """
    from ariadne import make_executable_schema

    object_types = generate_resolvers(schema_dict, g)
    simple_schema = generate_schema_str(schema_dict)
    schema = make_executable_schema(simple_schema, object_types)
    return schema