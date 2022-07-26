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

from zef import *
from zef.ops import *
from .ariadne_utils import gqlify, de_gqlify


def parse_list_et(list_et):
    aet_values = []
    arguments = list_et | Outs[RT.argument] | collect
    at = ""
    for arg in arguments:
        if BT(arg) == BT.ENTITY_NODE:
            return "", de_gqlify(value(arg | Out[ RT.Name]), True)
        else:
            aet_type = str(AET(arg | to_ezefref | collect))
            at = aet_type
            if at == "Bool":
                at = "Boolean"
            if value(arg):
                aet_values.append(value(arg))
            else:
                return "", at

    aet_values = "[" + ','.join([str(x) for x in aet_values]) + "]"
    return aet_values, at


def check_nonnullable(param):
    nonnullable = ""
    if length(param | out_rels[RT(gqlify("nonNullable"))]) > 0:
        nonnullable = "!"
    return nonnullable


def check_implements(param):
    implements = ""
    if length(param | out_rels[RT(gqlify("implements"))]) > 0:
        interface_name = de_gqlify(value(param | Out[ RT(gqlify("implements"))] | Out[ RT.Name]), True)
        implements = f"implements {interface_name}"
    return implements


def parse_queryparams_et(params_et):
    returned_list = []
    if length(params_et | Outs[RT(gqlify("queryParams"))]) > 0:
        for param in params_et | out_rels[RT(gqlify("queryParams"))] | collect:
            param_name = de_gqlify(value(param | Out[ RT.Name]), False)
            nonnullable = check_nonnullable(param)
            if BT(param | target | collect) == BT.ATTRIBUTE_ENTITY_NODE:
                aet_type = str(AET(param | target | collect))
                aet_value = param | target | value | collect
                if aet_type == "Bool":
                    aet_type = "Boolean"
                if aet_value:
                    returned_list.append(f"{param_name}: {aet_type}{nonnullable} = {aet_value}")
                else:
                    returned_list.append(f"{param_name}: {aet_type}{nonnullable}")
            else:
                if de_gqlify(str(ET(param | target | collect)), False) == "list":
                    aet_values, et_type = parse_list_et(param | target | collect)
                    if aet_values:
                        aet_values = " = " + aet_values
                    returned_list.append(f"{param_name}: [{et_type}]{nonnullable}{aet_values}")
                else:
                    et_type = de_gqlify(value(((param | target) | Out[ RT.Name])), True)
                    returned_list.append(f"{param_name}: {et_type}{nonnullable}")

        return returned_list
    else:
        return None


def generate_fields(et, schema_out):
    for rt in (et | out_rels[RT(gqlify("field"))] | collect):
        nonnullable = check_nonnullable(rt)
        field_name = de_gqlify(value(rt | Out[ RT.Name]), False)
        field_return = rt | target | collect
        blob_type = BT(field_return | to_ezefref | collect)
        query_params = parse_queryparams_et(rt)

        def inject_params(qp):
            if qp:
                return f"({','.join(qp)})"
            else:
                return ""

        if blob_type == BT.ATTRIBUTE_ENTITY_NODE:
            aet_type = str(AET(field_return | to_ezefref | collect))
            aet_value = field_return | value | collect
            if aet_type == "Bool":
                aet_type = "Boolean"
            if aet_value:
                aet_value = f" = {aet_value}"
            else:
                aet_value = ""

            schema_out += f'  {field_name}{inject_params(query_params)}: {aet_type}{nonnullable}{aet_value}\n'
        else:
            if de_gqlify(str(ET(field_return)), False) == "list":
                aet_values, et_type = parse_list_et(field_return)
                if aet_values:
                    aet_values = " = " + aet_values
                schema_out += f'  {field_name}{inject_params(query_params)}: [{et_type}]{nonnullable}{aet_values}\n'
            else:
                schema_out += f'  {field_name}{inject_params(query_params)}: {de_gqlify(value(field_return | Out[ RT.Name]),True)}{nonnullable}\n'
    schema_out += "}\n"
    return schema_out


def generate_enum_values(en, schema_out):
    for field in (en | Outs[RT(gqlify("field"))] | collect):
        en_value = (field | value).enum_value
        schema_out += f'  {en_value}\n'
    schema_out += "}\n"
    return schema_out


def generate_api_schema_string(schema_root):
    types = schema_root | Outs[RT(gqlify("type"))] | collect
    inputs = schema_root | Outs[RT(gqlify("input"))] | collect
    interfaces = schema_root | Outs[RT(gqlify("interface"))] | collect
    scalars = schema_root | Outs[RT(gqlify("scalar"))] | collect
    enums = schema_root | Outs[RT(gqlify("enum"))] | collect

    schema_out = ""
    for et in types:
        implements = check_implements(et)
        object_type = de_gqlify(value(et | Out[ RT.Name]), True)
        schema_out += f'type {object_type} {implements} {"{"}\n'
        schema_out = generate_fields(et, schema_out)

    for et in inputs:
        object_type = de_gqlify(value(et | Out[ RT.Name]), True)
        schema_out += f'input {object_type} {"{"}\n'
        schema_out = generate_fields(et, schema_out)

    for et in interfaces:
        object_type = de_gqlify(value(et | Out[ RT.Name]), True)
        schema_out += f'interface {object_type} {"{"}\n'
        schema_out = generate_fields(et, schema_out)

    for et in scalars:
        object_type = de_gqlify(value(et | Out[ RT.Name]), True)
        schema_out += f'scalar {object_type}\n'

    for et in enums:
        object_type = de_gqlify(value(et | Out[ RT.Name]), True)
        schema_out += f'enum {object_type} {"{"}\n'
        schema_out = generate_enum_values(et, schema_out)

    return schema_out


def generate_api_schema_file(schema_root, destination):
    f = open(destination, "w+")
    f.write('generated_schema = """\n')
    schema_string = generate_api_schema_string(schema_root)
    f.write(schema_string)
    f.write('"""\n')
    f.close()
