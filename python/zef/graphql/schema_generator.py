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
from ariadne import ObjectType,MutationType,QueryType,InterfaceType,SubscriptionType


#--------------------------Schema Generator-------------------------
def generate_schema_str(schema_dict: dict) -> str:
    """
    Generate a GraphQL schema string from a GraphQL schema dict.
    """

    def parse_enums(enums_list):
        def parse_enum(tup):
            enum, options = tup
            return (f"enum {enum}" + "{\n" 
                    + "\n".join(f"    {option}" for option in options.keys())
                    + "\n}"
            )
        
        return (
            enums_list
            | items
            | map[parse_enum]
            | join["\n"]
            | collect
        )

    def parse_scalars(scalars):
        return (
            scalars
            | items
            | map[lambda x: f"scalar {x[0]}"]
            | join["\n"]
            | collect
        )

    @func
    def parse_interfaces_or_types(d, prefix):
        def parse_implements(interfaces):
            if interfaces: return f"implements {''.join(interfaces)} "
            return ""

        def parse_interface_or_type(tup):
            name, fields_d = tup
            return (
                f"{prefix} {name} {parse_implements(fields_d.get('_interfaces', []))}" + "{\n"
                + parse_fields(fields_d)
                + "\n}"
            )

        return (
            d
            | items
            | map[parse_interface_or_type]
            | join["\n\n"]
            | collect
        )
    
    
    
    def parse_args(args):
        def parse_arg(tup):
            def parse_default_arg():
                if "default" in arg_dict: return f" = {arg_dict['default']}"
                return ""
            arg_name, arg_dict = tup
            return f"{arg_name}: {arg_dict['type']}{parse_default_arg()}"

        if args:
            args = (
            args
            | items
            | map[parse_arg]
            | join[", "]
            | collect
            )
            return f"({args})"
        
        return ""


    def parse_fields(fields_d):
        def parse_field(tup):
            field_name, field_dict = tup
            if field_name.startswith("_"): return ""
            return f"  {field_name}{parse_args(field_dict.get('args', []))}: {field_dict['type']}" 

        return (
            fields_d
            | items
            | map[parse_field]
            | join["\n"]
            | collect
        )
    
    def parse_inputs(input_d):
        def parse_input_field(fields_d):
            return (
                fields_d
                | items
                | map[lambda x: f"    {x[0]}: {x[1]}"]
                | join["\n"]
                | collect
            )
        def parse_input_type(tup):
            name, fields_d = tup
            return (
                f"input {name}" + "{\n"
                + parse_input_field(fields_d)
                + "\n}"
            )
        return (
            input_d
            | items
            | map[parse_input_type]
            | join["\n\n"]
            | collect
        )


    allowed_keys = ["_Interfaces", "_Types", "_Scalars", "_Enums", "_Inputs"]
    schema_dict = select_keys(schema_dict, *allowed_keys)
    dispatch = {
        "_Interfaces":  parse_interfaces_or_types["interface"],
        "_Types":       parse_interfaces_or_types["type"],
        "_Scalars":     parse_scalars,
        "_Enums":       parse_enums,
        "_Inputs":      parse_inputs,
    }

    return (
        schema_dict
        | items
        | map[lambda x: dispatch[x[0]](x[1])]
        | join["\n"]
        | collect
    )
