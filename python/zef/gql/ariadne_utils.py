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


# printing utils
def print_types(generated_types, cog):
    cog.out(f"object_types = [{','.join(generated_types)}]")


def print_imports(additional_exec, cog):
    cog.outl("from ariadne import ObjectType,MutationType,QueryType,InterfaceType,SubscriptionType")
    cog.outl("from zef.gql.ariadne_utils import *")
    cog.outl("from zef.gql.resolvers_utils import *")
    cog.outl("from zef import *")
    cog.outl("from zef.ops import *")
    cog.outl(additional_exec)


# general utils
def de_gqlify(s, object):
    if len(s) < 5:
        raise Exception(f"String {s} is too small to be a GQL_x string.")
    if s[:4] != "GQL_":
        raise Exception(f"String {s} does not start with GQL_.")
    s = s[4:]
    if object:
        return s
    return s[0].lower() + s[1:]


def gqlify(s):
    s = s.strip('{ ')
    return "GQL_" + s[0].upper() + s[1:]

# data utils
def parse_list_et_gql(list_et):
    aet_values = []
    arguments = list_et | Outs[RT.argument] | collect
    for arg in arguments:
        if BT(arg) == BT.ENTITY_NODE:
            return ""
        else:
            aet_type = str(AET(arg | to_ezefref | collect))
            if arg | value | collect is not None:
                aet_values.append(arg | value | collect)
            else:
                return ""

    aet_values = "[" + ','.join([str(x) for x in aet_values]) + "]"
    return aet_values


# ariadne utils
def handle_params(rt, params):
    if length(rt | Outs[RT(gqlify("queryParams"))]) > 0:
        for param in rt | out_rels[RT(gqlify("queryParams"))]:
            param_name = de_gqlify(str(param | Out[ RT.Name] | value | collect), False)
            if BT(param | target | collect) == BT.ATTRIBUTE_ENTITY_NODE:
                aet_type = str(AET(param | target | collect))
                aet_value = param | target | value | collect
                if length(param | Outs[RT(gqlify("nonNullable"))]) > 0:
                    params.append(f"{param_name}")
                else:
                    params.append(f"{param_name} = {aet_value}")
            else:
                if de_gqlify(str(ET(param | target | collect)), False) == "list":
                    et_values = parse_list_et_gql(param | target | collect)
                    params.append(f"{param_name} = {et_values}")
                else:
                    params.append(param_name)
    return params


def resolve_with(rt, bt, ft):
    optional = single_or[None]
    d_rt = rt | Out[RT(gqlify("resolve_with"))] | collect
    d_rt_name = str(RT(d_rt))
    is_list = is_a(ft, ET(gqlify("list")))
    if is_list:
        bt = BT(ft | Out[RT.argument] | to_ezefref | collect)
    is_out = rt | out_rel[RT(gqlify("resolve_with"))] | Outs[RT.IsOut] | optional | value_or[True] | collect

    dir = "Outs" if is_out else "Ins"

    if bt == BT.ATTRIBUTE_ENTITY_NODE:
        if is_list:
            return f'return (z | {dir}[RT.{d_rt_name}]) | map[value] | collect'
        else:
            return f'return (z | {dir}[RT.{d_rt_name}]) | single_or[None] | maybe_value | collect'
    else:
        if is_list:
            # return f'return (z >> L[RT.{d_rt_name}] | collect) , (z > L[RT.{d_rt_name}])| map[uid] | collect'
            return f'return (z | {dir}[RT.{d_rt_name}] | collect)'
        else:
            # return f'return (z >> RT.{d_rt_name} | collect) , (z > RT.{d_rt_name})|uid | collect'
            return f'return (z | {dir}[RT.{d_rt_name}] | single_or[None] | collect)'


def resolve_with_script(rt):
    zef_script = rt | Out[ RT(gqlify("resolve_with_script"))]
    script = f'g["{zef_script | uid | collect}"] | now | collect'
    return f'zef_script = {script} ; return zef_execute(zef_script, z=z)'

def resolve_with_func(rt):
    z_func = rt | Out[ RT(gqlify("resolve_with_func"))]

    extra = rt | Outs[RT(gqlify("queryParams"))] | Out[ RT.Name] | value | map[lambda s: de_gqlify(s,False)] | collect
    param_names = {"z", "ctx", "g", *extra}

    dict_create = '{' + ', '.join(f"'{key}': {key}" for key in param_names) + '}'
    return f'''
        resolve_func = func[g["{uid(z_func)}"]] 
        pass_dict = {dict_create}
        return pass_dict | unpack[resolve_func] | collect
    '''

def resolve_with_intermediate(rt):
    d_rt = rt | Out[ RT(gqlify("resolve_with_intermediate"))]
    d_rt_name = str(RT(d_rt))
    return f'return (z | out_rel[RT.{d_rt_name})], (z | out_rel[RT.{d_rt_name})] |uid | collect'


def resolve_with_zef_function(rt):
    zef_function_uid = str(rt | Out[ RT(gqlify("resolve_with_zef_function"))] | uid | collect)
    params = rt | out_rels[RT(gqlify("queryParams"))] | map[Out[ RT.Name] | value] | map[lambda s: de_gqlify(s,False)] | collect
    defaults = ', '.join(f"{key} = {key}" for key in {"z", "ctx", "g"})
    params = ', '.join(i for i in params) + "," if len(params) != 0 else ""
    return f'return g["{zef_function_uid}"]({params}{defaults} )'


def resolve_with_wrapper(fn_body):
    return f"""
    try:
        {fn_body}
    """ + """
    except Exception as e:
        if "error traversing" not in str(e):
            log.warn(f"Handled exception while calling a resolve_with_*", exc_info=e)
        return None
    """


def create_function_body(ot, ft, bt, rt, fn, fallback_resolvers):
    default_params = ["z", "ctx"]
    params = handle_params(rt, default_params)
    # TODO (Cleanup) might consider making these as well customer specific
    # Resolve_With relations have highest priority
    if length(rt | Outs[RT(gqlify("resolve_with"))]) > 0:
        return resolve_with_wrapper(resolve_with(rt, bt, ft)), params

    elif length(rt | Outs[RT(gqlify("resolve_with_script"))]) > 0:
        return resolve_with_wrapper(resolve_with_script(rt)), params

    elif length(rt | Outs[RT(gqlify("resolve_with_zef_function"))]) > 0:
        return resolve_with_wrapper(resolve_with_zef_function(rt)), params

    elif length(rt | Outs[RT(gqlify("resolve_with_func"))]) > 0:
        return resolve_with_wrapper(resolve_with_func(rt)), params

    elif length(rt | Outs[RT(gqlify("resolve_with_intermediate"))]) > 0:
        return resolve_with_wrapper(resolve_with_intermediate(rt)), params

    elif length(rt | Outs[RT(gqlify("resolve_with_body"))]) > 0:
        # return (rt | Out[ RT(gqlify("resolve_with_body")) | value | collect), params
        return resolve_with_wrapper(rt | Out[ RT(gqlify("resolve_with_body"))] | value | collect), params

    return fallback_resolvers(ot, ft, bt, rt, fn), default_params

def initialize_object_type(object_type, cog):
    if "Mutation" == object_type:
        cog.outl(f'{object_type} = MutationType()')
    elif "Subscription" == object_type:
        cog.outl(f'{object_type} = SubscriptionType()')
    else:
        cog.outl(f'{object_type} = ObjectType("{object_type}")')


def generate_object_resolver(object_type, field_name, rt, fn_body, params, cog):
    def custom_sort(item):
        if "=" in item: return 1
        return -1
    params = params | sort[custom_sort] | collect
    cog.outl(f'@{object_type}.field("{field_name}")')
    cog.outl(f'def resolve_{object_type}_{uid(rt)}({", ".join(params)}):')
    cog.outl(f'     {fn_body}\n')


def generate_enum_resolver(e, cog):
    enum_name = de_gqlify(str(e | Out[ RT.Name ]| value | collect), True)
    cog.outl(f'{enum_name} = EnumType("{enum_name}", {"{"}')
    i = -1
    for f in (e | Outs[RT(gqlify("field"))]):
        i += 1
        en_value = (f | value).enum_value

        cog.outl(f' "{en_value}": {i},')
    cog.outl(f'{"}"},)\n')
    return enum_name


def generate_interface_resolver(i, cog):
    interface_name = de_gqlify(str(i | Out[ RT.Name ]| value | collect), True)
    cog.outl(f'{interface_name} = InterfaceType("{interface_name}")')
    cog.outl(f'@{interface_name}.type_resolver')
    cog.outl(f'def resolve_{interface_name}_type_resolver(obj, *_):')
    cog.outl(f'#TODO make sure that those names are consistent with your Data Graph type name')
    for t in (i |  Ins[RT(gqlify("implements"))]):
        object_name = de_gqlify(str(t | Out[ RT.Name ]| value | collect), True)
        cog.outl(f'    if str(ET(obj)) == "{object_name}":')
        cog.outl(f'        return "{object_name}"')
    cog.outl(f'    if str(ET(obj)) == "Step":')
    cog.outl(f'        return "ProcessOrder"')
    cog.outl(f'    return None')
    return interface_name


def generate_all(global_dict, cog):
    root = global_dict["schema_root"]
    default_resolvers_list = global_dict["default_resolvers_list"]
    fallback_resolvers = global_dict["fallback_resolvers"]
    object_types = []
    types = root | Outs[RT(gqlify("type"))]
    interfaces = root | Outs[RT(gqlify("interface"))]

    for t in types:
        object_type = de_gqlify(str(t | Out[ RT.Name ]| value | collect), True)
        # Don't generate resolvers for function in this list
        if object_type in default_resolvers_list:
            continue
        object_types.append(object_type)
        initialize_object_type(object_type, cog)

        for rt in (t | out_rels[RT(gqlify("field"))]):
            field_name = de_gqlify(str(rt | Out[ RT.Name] | value | collect), False)
            field_type = (rt | target | to_ezefref | collect)
            fn_body, params = create_function_body(object_type, field_type, BT(field_type), rt, field_name, fallback_resolvers)
            generate_object_resolver(object_type, field_name, rt, fn_body, params, cog)

    for i in interfaces:
        object_types.append(generate_interface_resolver(i, cog))

    return object_types
