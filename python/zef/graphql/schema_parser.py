# Copyright (c) 2022 Synchronous Technologies Pte Ltd
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from ..core import *
from ..ops import *
from graphql import parse
from graphql.language.ast import ScalarTypeDefinitionNode, ObjectTypeDefinitionNode, NamedTypeNode, ListTypeNode, NonNullTypeNode, InterfaceTypeDefinitionNode

# TODO add support for parsing enums

def generate_schema_dict(schema_str: str) -> dict:
    """
    Given a GraphQL schema string, generate a dictionary representation of the schema.
    """
    def resolve_interfaces(interfaces):
        if interfaces: return {'_interfaces': interfaces | map[lambda i: i.name.value] | collect}
        return {}

    def resolve_args(args):
        def resolve_arg(arg):
            def resolve_default_value(arg):
                if arg.default_value:
                    return {"default": arg.default_value.value}
                return {}

            return {
                arg.name.value: {
                    "type": resolve_type(arg.type),
                    **resolve_default_value(arg),
                }
            }
        
        if args: return {'args': args | map[resolve_arg] | merge | collect}
        return {}

    def resolve_type(type):
        return LazyValue(type) | match[
            (Is[lambda t: isinstance(t, NamedTypeNode)], lambda t: t.name.value),
            (Is[lambda t: isinstance(t, ListTypeNode)], lambda t: f"[{resolve_type(t.type)}]"),
            (Is[lambda t: isinstance(t, NonNullTypeNode)], lambda t: f"{resolve_type(t.type)}!"),
        ] | collect

    def resolve_field(field):
        return {
            field.name.value: {
                'type': resolve_type(field.type),
                'resolver': None,
                **resolve_args(list(field.arguments)),
            }
        }

    @func
    def dispatch_on_type_or_interface(type_node, is_interface = False):
        interface_resolver = {"_interface_resolver": None} if is_interface else {}
        return {
            type_node.name.value : {
                **(list(type_node.fields) | map[resolve_field] | merge | collect),
                **resolve_interfaces(list(type_node.interfaces)),
                **interface_resolver,
            }
        }

    def dispatch_on_scalar(type_node):
        return {
            type_node.name.value: {
                'parser': None,
                'serializer': None,
            }
        } 

    

    dispatch = match[
        (Is[lambda t: t[0] == ObjectTypeDefinitionNode], lambda g: {"_Types": g[1] | map[dispatch_on_type_or_interface] | merge | collect}),
        (Is[lambda t: t[0] == InterfaceTypeDefinitionNode], lambda g: {"_Interfaces": g[1] | map[dispatch_on_type_or_interface[True]] | merge | collect}),
        (Is[lambda t: t[0] == ScalarTypeDefinitionNode], lambda g: {"_Scalars": g[1] | map[dispatch_on_scalar] | merge | collect}),
    ]

    document = parse(schema_str, no_location=True)
    return (
        list(document.definitions)
        | group_by[type]
        | map[dispatch]
        | merge
        | insert["skip_generation_list"][[]]
        | collect
    )