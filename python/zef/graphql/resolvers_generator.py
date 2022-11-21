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
from ariadne import ObjectType, MutationType, SubscriptionType, EnumType, ScalarType, InterfaceType


#--------------------------Resolvers Generator-------------------------
def fill_types_default_resolvers(schema_d, replace_policy=lambda field_name: field[RT(to_pascal_case(field_name))]):
    if "_Types" not in schema_d: return schema_d

    def generate_default_if_unset(type_name, field_name, field_dict):
        # If resolver is either unset or set None
        resolver = field_dict.get("resolver", None)
        if resolver: return None
        return (('_Types', type_name, field_name, 'resolver'), replace_policy(field_name))


    # Generate a list of Tuples[path, default_resolver] for fields where resolver is either unset or set to None
    paths_and_defaults = (
        schema_d['_Types']
        | items
        | filter[lambda x: x[0] not in {"Query", "Mutation"}]
        | map[lambda type_t: (type_t[1] 
                            | items 
                            | filter[lambda kv: not kv[0].startswith("_")]
                            | map[lambda field_t: generate_default_if_unset(type_t[0], *field_t)] 
                            | collect)
            ] 
        | concat
        | filter[Not[equals[None]]]
        | collect
    )

    # Apply each tuple on the resulting intermediate dict without mutations and return the last dict with every path set
    return (
        paths_and_defaults
        | reduce[lambda d, tup: d | insert_in[tup[0]][tup[1]] | collect][schema_d]
        | collect
    )

def initialize_object_type(object_type):
    if "Mutation" == object_type:
        return MutationType()
    elif "Subscription" == object_type:
        return SubscriptionType()
    else:
        return ObjectType(object_type)

def resolve_enum_type(object_type, options):
    return EnumType(object_type, options)

def resolve_scalar_type(object_type, options):
    # TODO: Should we check the passed ZefOp/Zef Functions?
    value_parser = options.get("parser", None)
    serializer = options.get("serializer", None)
    return ScalarType(object_type, serializer=serializer, value_parser=value_parser)

def resolve_interface_type(interface_name, interface_d, g):
    interface_resolver = interface_d.get("_interface_resolver", None)
    if interface_resolver is None: raise ValueError(f"{interface_name} Interface's type resolver must be definied")

    interface_type = InterfaceType(interface_name, interface_resolver)
    for field_name, field_dict in interface_d.items():
        if field_name.startswith("_"): continue
        assign_field_resolver(interface_type, field_name, field_dict, g, True)
    
    return interface_type

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
    zefref_or_func = peel(z_fct)[0][1][0][1]
    if is_a(zefref_or_func, Entity):
        full_arg_spec = zef_function_args(g[zefref_or_func] | now  | collect)
        args, defaults =  full_arg_spec.args, full_arg_spec.defaults
        if defaults: args = args[:len(args) - len(defaults)]
        return args
    else:
        import types
        assert type(zefref_or_func) == types.FunctionType
        import inspect
        args = inspect.getargs(zefref_or_func.__code__).args
        # Remove the already curried args
        curried = peel(z_fct)[0][1][1:]
        # args = args[:-len(curried)]
        args = args[0:1] + args[1+len(curried):]
        return args

def generate_fct(field_dict,field_name, g, allow_none):
    resolver = field_dict["resolver"]
    if resolver is None and not allow_none:
        raise ValueError("A type's field resolver shouldn't be set to None! \n\
            To use default values for your resolver, explicitly call fill_types_default_resolvers on your schema dictionary!")

    def resolve_field(obj, info, **kwargs):
        from ..core._error import ExceptionWrapper, zef_ui_err
        try:
            context = {
                "obj": obj,
                "query_args": kwargs,
                "graphql_info": info,
                "g": g,
                # To be extended
            }
            if isinstance(resolver, ZefFunction):
                args = get_zef_function_args(resolver, g)
                arg_values = select_keys(context, *args).values()
                # Check if some args that are present in the Zef Function aren't present in context dict
                if len(arg_values) < len(args): raise ValueError("Some args present in the Zef Function aren't present in context dict")
                arg_values = [context[x] for x in args]
                return resolver(*arg_values)
            elif isinstance(resolver, ZefOp):
                return resolver(obj)
            elif isinstance(resolver, LazyValue):
                return resolver()
            else:
                raise NotImplementedError(f"Cannot generate resolver using the passed object {resolver} of type {type(resolver)}")
        except ExceptionWrapper as exc:
            zef_ui_err(exc.wrapped)
            raise Exception("Internal error")
        except Exception as e:
            raise Exception(f"Error while resolving {field_name} with following arguments {obj}, {kwargs}") from e
    return resolve_field

def assign_field_resolver(object_type, field_name, field_dict, g, allow_none=False):
    fct = generate_fct(field_dict, field_name, g, allow_none)
    if fct is not None:
        object_type.field(field_name)(fct)

def generate_resolvers(schema_dict, g):
    """
    Given a schema dict with definied resolvers fields, generates the ariadane resolvers.
    Returns back ariadne object types list.
    """
    fallback_resolver = schema_dict.get("fallback_resolvers", [])
    object_types = []

    types = schema_dict.get("_Types", {})
    for object_type, fields_dict in types.items():
    
        object_type = initialize_object_type(object_type)
        object_types.append(object_type)

        for field_name, field_dict in fields_dict.items():
            if field_name.startswith("_"): continue
            assign_field_resolver(object_type, field_name, field_dict, g)

    enums = schema_dict.get("_Enums", {})
    for object_type, options in enums.items():
        object_types.append(resolve_enum_type(object_type, options))

    scalars = schema_dict.get("_Scalars", {})
    for object_type, options in scalars.items():
        object_types.append(resolve_scalar_type(object_type, options))

    
    interfaces = schema_dict.get("_Interfaces", {})
    for interface_name, interface_d in interfaces.items():
        object_types.append(resolve_interface_type(interface_name, interface_d, g))

    return object_types