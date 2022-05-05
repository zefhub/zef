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
from zef.ops import run
from .ariadne_utils import gqlify

#TODO types_dict will have collisions if  blocks of different types have same name i.e type Zeyad, enum Zeyad
def parse_schema(g, schema):
    """ Goes through the schema line by line initializing  entities based on their type.
        Those types are then connected to the root node.
        Depending on entity different handling takes place.

        During this process, a type_name to zefref is added to a dict which is then
        used by the connect_et_in_graph function to bind multiple instances of the same ET to
        one single zefref.
    """
    lines = schema.split('\n')
    root = instantiate(ET(gqlify("schema")), g)
    types_dict = {}

    for i in range(len(lines)):
        gql_type = ""
        if lines[i].lstrip(' ')[:4] == "type":  # i.e: type User {
            gql_type = "type"

        elif lines[i].lstrip(' ')[:5] == "input":  # i.e:  input CreateCar {
            gql_type = "input"

        elif lines[i].lstrip(' ')[:9] == "interface":  # i.e:   interface Node {
            gql_type = "interface"

        elif lines[i].lstrip(' ')[:6] == "scalar":  # i.e: scalar Date
            gql_type = "scalar"

        elif lines[i].lstrip(' ')[:4] == "enum":  # enum Availability {
            gql_type = "enum"

        if gql_type:
            name = gqlify(lines[i].strip().split(' ')[1])
            entity = initialize_named_entity(g, name, gql_type)
            instantiate(root, RT(gqlify(gql_type)), entity, g)
            types_dict[name] = entity

            if gql_type == "scalar":
                continue
            elif gql_type == "enum":
                i = handle_enum_type(g, name, entity, lines, i + 1)
            else:
                i = instantiate_type_fields(g, entity, lines, i + 1)

    connect_et_in_graph(g, schema, types_dict)
    return root, types_dict


def handle_enum_type(g, enum_type, entity, lines, i):
    line = lines[i]
    while "}" not in line:
        enum_val = line.strip(' ')
        EN(enum_type, enum_val)
        ae = instantiate(AET.Enum(enum_type), g)
        (ae <= EN(enum_type, enum_val)) | g | run
        r = instantiate(entity, RT(gqlify("field")), ae, g)
        i = i + 1
        line = lines[i]

    return i


def initialize_named_entity(g, name, gql_type):
    et_name = instantiate(AET.String, g)
    (et_name <= name) | g | run
    entity = instantiate(ET(gqlify(gql_type)), g)
    instantiate(entity, RT("Name"), et_name, g)
    return entity


def handle_list_type(g, field_type, aet_types):
    splitted = field_type.replace("]", "").replace("[", "").split("=")
    field_type = splitted[0].strip(" ")
    list_et = instantiate(ET(gqlify("list")), g)
    if field_type in aet_types:
        if len(splitted) > 1:
            default_values = splitted[1].split(",")
            for v in default_values:
                v = v.strip(" ")
                aet = instantiate(getattr(AET, field_type), g)
                aet <= type(aet_types[field_type])(v)
                instantiate(list_et, RT("argument"), aet, g)
        else:
            aet = instantiate(getattr(AET, field_type), g)
            instantiate(list_et, RT("argument"), aet, g)
    else:
        et_arg = instantiate(ET(gqlify(field_type)), g)
        instantiate(list_et, RT("argument"), et_arg, g)
    return list_et


def initialize_field_return(g, field_type):
    aet_types = {'String': "str", 'Int': 1, 'Float': 1.5, 'Boolean': True}
    #  i.e [String] or [String] = ["id"]
    if "[" in field_type:
        return handle_list_type(g, field_type, aet_types)

    #  i.e  Int = 2017 or Int
    splitted = field_type.split("=")
    field_type = splitted[0].strip(" ")

    if field_type in aet_types:
        if field_type == "Boolean":
            del(aet_types[field_type])
            field_type = "Bool"
            aet_types[field_type] = True
        aet = instantiate(getattr(AET, field_type), g)
        if len(splitted) > 1:
            (aet <= type(aet_types[field_type])(splitted[1].strip(" "))) | g | run
        return aet

    et = initialize_named_entity(g, field_type, "type")
    return et


def create_aet_relation(g, entity, relation_name, relation_target, relation_type):
    # relation between type and its field return
    stripped_target = relation_target.strip(' ').replace("!", "")
    field_return = initialize_field_return(g, stripped_target)
    r = instantiate(entity, RT(gqlify(relation_type)), field_return, g)
    if "!" in relation_target:
        instantiate(r, RT(gqlify("nonNullable")), instantiate(AET.Int, g), g)
    # relation between field relation and its field name
    name = instantiate(AET.String, g)
    (name <= relation_name) | g | run
    instantiate(r, RT("Name"), name, g)
    return r


def instantiate_type_fields(g, entity, lines, i):
    """ Instantiate GQL types as Entities on zef graph
    """
    line = lines[i]
    while "}" not in line:
        if "(" not in line:
            # i.e: username: String! or model: Int = 2017
            relation_name = gqlify(line[:line.index(':')])
            relation_target = line[line.index(':') + 1:]
            if not is_type(relation_target)[0]:
                create_aet_relation(g, entity, relation_name, relation_target, "field")

        i += 1
        line = lines[i]
    return i


def is_type(field_return):
    target_type = field_return.strip(' ').replace("]", "").replace("[", "").replace("!", "").replace(" ", "").split("=")
    if target_type[0].strip(' ') in ["String", "Int", "Float", "Boolean"]:
        return False, ""
    return True, target_type[0].strip(' ')


def create_et_relation(g, type_dict, et_name, entity, relation_target, relation_name, connection_type):
    field_entity = type_dict[gqlify(et_name)]
    if "[" in relation_target:
        list_et = instantiate(ET(gqlify("list")), g)
        instantiate(list_et, RT("argument"), field_entity, g)
        field_entity = list_et

    r = instantiate(entity, RT(gqlify(connection_type)), field_entity, g)
    if "!" in relation_target:
        instantiate(r, RT(gqlify("nonNullable")), instantiate(AET.Int, g), g)
    name = instantiate(AET.String, g)
    (name <= relation_name) | g | run
    instantiate(r, RT("Name"), name, g)
    return r


def connect_et_in_graph(g, schema, type_dict):
    """
        This function connects any non-AET field within a gql type, input, interface
        to an already instantiated zefref of that non-AET field.
    """
    lines = schema.split('\n')
    for i in range(len(lines)):
        if lines[i].lstrip(' ')[:4] == "type" \
                or lines[i].lstrip(' ')[:5] == "input" \
                or lines[i].lstrip(' ')[:9] == "interface":

            line = lines[i].strip().split(' ')
            name = gqlify(line[1])
            entity = type_dict[name]

            if "implements" in line:
                implement_entity = type_dict[gqlify(line[3].strip("{ "))]
                instantiate(entity, RT(gqlify("implements")), implement_entity, g)

            i = i + 1
            line = lines[i]
            while "}" not in line:
                if "(" not in line:
                    relation_name = gqlify(line[:line.index(':')])
                    relation_target = line[line.index(':') + 1:]
                    target_is_et, et_name = is_type(relation_target)
                    if target_is_et:
                        create_et_relation(g, type_dict, et_name, entity, relation_target, relation_name, "field")
                    # TODO: figure out why did we add this at some point!
                    # else:
                    #     create_aet_relation(g, entity, relation_name, relation_target, "field")
                else:
                    relation_name = gqlify(line[:line.index('(')])
                    relation_target = line[line.index('):') + 2:].strip(' ')
                    target_is_et, et_name = is_type(relation_target)
                    if target_is_et:
                        r = create_et_relation(g, type_dict, et_name, entity, relation_target, relation_name,
                                                    "field")
                    else:
                        r = create_aet_relation(g, entity, relation_name, relation_target, "field")
                    args = line[line.index('(') + 1: line.index(')')].split(',')
                    for arg in args:
                        relation_name = gqlify(arg[:arg.index(':')])  # i.e: GQL_ID
                        relation_target = arg[arg.index(':') + 1:]  # i.e: String
                        target_is_et, et_name = is_type(relation_target)
                        if target_is_et:
                            create_et_relation(g, type_dict, et_name, r, relation_target, relation_name,
                                                    "queryParams")
                        else:
                            create_aet_relation(g, r, relation_name, relation_target, "queryParams")

                i += 1
                line = lines[i]
