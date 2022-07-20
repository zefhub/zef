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
from graphql.language.ast import ScalarTypeDefinitionNode, ObjectTypeDefinitionNode, NamedTypeNode, ListTypeNode, NonNullTypeNode

def schema_str_to_dict(schema_str):

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
        
        if args: return {'args': args | map[resolve_arg] | collect}
        return {}

    def resolve_type(type):
        return LazyValue(type) | match[
            (Is[lambda t: isinstance(t, NamedTypeNode)], lambda t: t.name.value),
            (Is[lambda t: isinstance(t, ListTypeNode)], lambda t: {"_type": "List", "value": resolve_type(t.type)}),
            (Is[lambda t: isinstance(t, NonNullTypeNode)], lambda t: {"_type": "Required", "value": resolve_type(t.type)}),
        ] | collect

    def resolve_field(field):
        return {
            field.name.value: {
                'return_type': resolve_type(field.type),
                'resolver': None,
                **resolve_args(list(field.arguments)),
            }
        }

    def dispatch_on_type(type_node):
        return {
            type_node.name.value : {
                "_gqltype": "Type",
                **(list(type_node.fields) | map[resolve_field] | merge | collect)
            }
        }

    def dispatch_on_scalar(type_node):
        return {
            type_node.name.value : {
                "_gqltype": "Scalar"
            }
        }

    dispatch = match[
        (Is[lambda t: isinstance(t, ObjectTypeDefinitionNode)], dispatch_on_type),
        (Is[lambda t: isinstance(t, ScalarTypeDefinitionNode)], dispatch_on_scalar),
    ]

    document = parse(schema_str, no_location=True)
    return (
        list(document.definitions)
        | map[dispatch]
        | merge
        | collect
    )