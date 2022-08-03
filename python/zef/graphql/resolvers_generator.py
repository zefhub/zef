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
from ariadne import ObjectType,MutationType,QueryType,InterfaceType,SubscriptionType, EnumType


#--------------------------Resolvers Generator-------------------------
def initialize_object_type(object_type):
    if "Mutation" == object_type:
        return MutationType()
    elif "Subscription" == object_type:
        return SubscriptionType()
    else:
        return ObjectType(object_type)

def resolve_enum_type(object_type, options):
    return EnumType(object_type, options )

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

def get_zef_function_args(z_fct, g):
    from ..core.zef_functions import zef_function_args
    full_arg_spec = zef_function_args(g[peel(z_fct)[0][1][0][1]] | now  | collect)
    args, defaults =  full_arg_spec.args, full_arg_spec.defaults
    return args[:len(args) - len(defaults)]

def generate_fct(field_dict, g):
    kwargs = resolve_args(field_dict.get('args', []))
    resolver = field_dict["resolver"]

    def resolve_field(obj, info, **kwargs):
        context = {
            "obj": obj,
            "query_args": kwargs,
            "qraphql_info": info,
            # To be extended
        }
        if is_a(resolver, ZefOp):
            if peel(resolver)[0][0] == RT.Function:
                args = get_zef_function_args(resolver, g)
                arg_values = select_keys(context, *args).values()
                # Check if some args that are present in the Zef Function aren't present in context dict
                if len(arg_values) < len(args): raise ValueError("Some args present in the Zef Function aren't present in context dict")
                return resolver(*select_keys(context, *args).values())
            else:
                return resolver(obj)
        elif isinstance(resolver, LazyValue):
            return resolver()
        elif resolver is None:
            return None
        else:
            raise NotImplementedError(f"Cannot generate resolver using the passed object {resolver} of type {type(resolver)}")
    return resolve_field

def assign_field_resolver(object_type, field_name, field_dict, g):
    fct = generate_fct(field_dict, g)
    object_type.field(field_name)(fct)

def generate_resolvers(schema_dict, g):
    """
    Given a schema dict with definied resolvers fields, generates the ariadane resolvers.
    Returns back ariadne object types list.
    """
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
    
    enums = schema_dict.get("_Enums", [])
    for e_dict in enums:
        object_types.append(resolve_enum_type(*list(e_dict.items())[0]))

    return object_types