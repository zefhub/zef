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


#------------------------Utils---------------------------
def print_types(generated_types, cog):
    cog.out(f"object_types = [{','.join(generated_types)}]")


def print_imports(cog):
    cog.outl("from zef import *")
    cog.outl("from zef.ops import *")
    cog.outl("from zef.core.logger import log")
    cog.outl("from ariadne import ObjectType,MutationType,QueryType,InterfaceType,SubscriptionType")


#--------------------------Cogging-------------------------
def initialize_object_type(object_type, cog):
    if "Mutation" == object_type:
        cog.outl(f'{object_type} = MutationType()')
    elif "Subscription" == object_type:
        cog.outl(f'{object_type} = SubscriptionType()')
    else:
        cog.outl(f'{object_type} = ObjectType("{object_type}")')


def generate_fct_body(fct_body):
    if fct_body:
        return f"""
    op_or_func = {fct_body}
    return op_or_func('args')""" 
    
    return f"""
    return None""" 

def generate_field_resolver(object_type, field_name, fct_body, args, cog):
    custom_sort = lambda item: 1 if "=" in item else -1
    args = args | sort[custom_sort] | collect
    cog.outl(f'@{object_type}.field("{field_name}")')
    cog.outl(f'def resolve_{object_type}_{field_name}({", ".join(args)}):')
    # cog.outl(f'    print(obj);return "{args}"\n')
    cog.outl(f'{fct_body}\n')

def generate_args(args):
    def handle_arg_dict(d):
        arg, arg_d = list(d.items())[0]
        if "default" in arg_d: arg += f" = {arg_d['default']}"
        else: arg += f" = {None}"
        return arg

    default_args = ["obj", "info"]
    return (
        args
        | map[handle_arg_dict]
        | concat[default_args]
        | collect
    )


def create_field_resolver(object_type, field_name, field_dict):
    args = generate_args(field_dict.get('args', []))
    fct_body = generate_fct_body(field_dict["resolver"])
    return fct_body, args


def generate_resolvers(schema_dict, cog):
    skip_generation_list = schema_dict.get("skip_generation_list", [])
    fallback_resolvers = schema_dict.get("fallback_resolvers", [])

    types = schema_dict["_Types"]
    interfaces = schema_dict["_Interfaces"]
    object_types = []

    for t_dict in types:
        for object_type, fields_dict in t_dict.items():
        
            # Don't generate resolvers for function in this list
            if object_type in skip_generation_list: continue

            object_types.append(object_type)
            initialize_object_type(object_type, cog)

            for field_name, field_dict in fields_dict.items():
                if field_name.startswith("_"): continue
                fct_body, args = create_field_resolver(object_type, field_name, field_dict)
                generate_field_resolver(object_type, field_name, fct_body, args, cog)

    # for i in interfaces:
    #     object_types.append(generate_interface_resolver(i, cog))

    return object_types
