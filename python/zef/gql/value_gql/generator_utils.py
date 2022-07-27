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

from ...core import *
from ...ops import *
from ariadne import ObjectType,MutationType,QueryType,InterfaceType,SubscriptionType


#--------------------------Schema Generator-------------------------
def generate_schema_str(schema_dict: dict) -> str:
    def parse_scalars(scalars_list):
        return (
            scalars_list
            | map[lambda x: f"scalar {x}"]
            | join["\n"]
            | collect
        )

    @func
    def parse_interfaces_or_types(lst, prefix):
        def parse_implements(interfaces):
            if interfaces: return f"implements {''.join(interfaces)} "
            return ""

        def parse_interface_or_type(interface_or_type):
            name, fields_d = list(interface_or_type.items())[0]
            return (
                f"{prefix} {name} {parse_implements(fields_d.get('_interfaces', []))}" + "{\n"
                + parse_fields(fields_d)
                + "\n}"
            )
        return (
            lst
            | map[parse_interface_or_type]
            | join["\n\n"]
            | collect
        )
    
    
    
    def parse_args(args):
        def parse_arg(arg):
            def parse_default_arg():
                if "default" in arg_dict: return f" = {arg_dict['default']}"
                return ""
            arg_name, arg_dict = list(arg.items())[0]
            return f"{arg_name}: {arg_dict['type']}{parse_default_arg()}"

        if args:
            args = (args
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


    allowed_keys = ["_Interfaces", "_Subscriptions", "_Types", "_Scalars"]
    schema_dict = select_keys(schema_dict, *allowed_keys)
    dispatch = {
        "_Interfaces":  parse_interfaces_or_types["interface"],
        "_Types":       parse_interfaces_or_types["type"],
        "_Scalars":     parse_scalars,
    }

    return (
        schema_dict
        | items
        | map[lambda x: dispatch[x[0]](x[1])]
        | join["\n"]
        | collect
    )





#--------------------------Resolvers Generator-------------------------
def initialize_object_type(object_type):
    if "Mutation" == object_type:
        return MutationType()
    elif "Subscription" == object_type:
        return SubscriptionType()
    else:
        return ObjectType(object_type)

def resolve_args(args):
    def handle_arg_dict(d):
        arg, arg_d = list(d.items())[0]
        d = {}
        d[arg]  = arg_d.get('default', None)
        return d

    return (
        args
        | map[handle_arg_dict]
        | merge
        | collect
    )

def generate_fct(field_dict, g):
    kwargs = resolve_args(field_dict.get('args', []))
    resolver = field_dict["resolver"]

    def resolve_field(obj, info, **kwargs):
        args = [
            obj,
            g,
        ]
        kwargs_all = {
            "context": info,
            **kwargs,
        }

        if is_a(resolver, ZefOp):
            if peel(resolver)[0][0] == RT.Function:
                return resolver(*args, **kwargs_all)
            else:
                return resolver(obj)

        return resolver(*args,**kwargs_all)

    return resolve_field

def assign_field_resolver(object_type, field_name, field_dict, g):
    fct = generate_fct(field_dict, g)
    object_type.field(field_name)(fct)

def generate_resolvers(schema_dict, g):
    skip_generation_list = schema_dict.get("skip_generation_list", [])
    fallback_resolver = schema_dict.get("fallback_resolvers", [])

    types = schema_dict.get("_Types", [])
    interfaces = schema_dict.get("_Interfaces", [])
    object_types = []

    for t_dict in types:
        for object_type, fields_dict in t_dict.items():
        
            # Don't generate resolvers for function in this list
            if object_type in skip_generation_list: continue

            object_type = initialize_object_type(object_type)
            object_types.append(object_type)

            for field_name, field_dict in fields_dict.items():
                if field_name.startswith("_"): continue
                assign_field_resolver(object_type, field_name, field_dict, g)


    return object_types