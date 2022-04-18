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

from ... import *
from ...ops import *

import graphql
def parse_partial_graphql(schema):
    """Goes through the schema line by line and creates a json file that represents
    the schema one-to-one. This should be an invertible process.

    """
    doc = graphql.parse(schema)

    json = {
        "types": {},
        "enums": {}
    }

    for definition in doc.definitions:
        if definition.kind == "object_type_definition":
            if definition.name.value == "_Auth":
                # This is the special node to grab the auth details
                assert len(definition.directives) == 1, "The _Auth type should have only a @details directive."
                assert definition.directives[0].name.value == "details", "The _Auth type should have only a @details directive."
                args = parse_arguments_as_dict(definition.directives[0].arguments)
                json["auth"] = {}
                for key,val in args.items():
                    if key not in ["JWKURL", "Audience", "Header", "Namespace"]:
                        raise Exception("Don't understand key of {key} in auth details.")
                    json["auth"][key] = val
                continue

            # Otherwise this is a normal type
            t_def = json["types"][definition.name.value] = {}

            for directive in definition.directives:
                if directive.name.value == "auth":
                    for arg in directive.arguments:
                        if arg.name.value == "query":
                            t_def["_AllowQuery"] = arg.value.value
                        elif arg.name.value == "add":
                            t_def["_AllowAdd"] = arg.value.value
                        elif arg.name.value == "update":
                            t_def["_AllowUpdate"] = arg.value.value
                        elif arg.name.value == "updatePost":
                            t_def["_AllowUpdatePost"] = arg.value.value
                        elif arg.name.value == "delete":
                            t_def["_AllowDelete"] = arg.value.value
                        else:
                            raise Exception(f"Don't know how to handle auth kind of {arg.name.value}")
                else:
                    raise Exception(f"Don't know how to handle type directive @{directive.name.value}")

            for field in definition.fields:
                f_def = t_def[field.name.value] = {}

                f_type = field.type
                if f_type.kind == "non_null_type":
                    f_def["required"] = True
                    f_type = f_type.type
                if f_type.kind == "list_type":
                    f_def["list"] = True
                    f_type = f_type.type
                if f_type.kind == "non_null_type":
                    f_def["listNonNullItem"] = True
                    f_type = f_type.type
                assert f_type.kind == "named_type", f"Don't know what this kind of field type is {f_type.kind}"
                f_def["type"] = f_type.name.value

                assert len(field.arguments) == 0, "Currently not allowing custom arguments to a field"
                for directive in field.directives:
                    v = directive.name.value
                    for bool_key in ["search", "unique", "incoming"]:
                        if v == bool_key:
                            f_def[bool_key] = True
                            assert len(directive.arguments) == 0, f"@{bool_key} cannot take arguments"
                            break
                    else:
                        if v == "relation":
                            args = parse_arguments_as_dict(directive.arguments)
                            assert args.keys() == {"rt"} or args.keys() == {"source", "rt", "target"}, "@relation must take either 'rt' or 'source', 'rt', 'target' arguments. Got {set(args.keys())}"
                            if "source" in args:
                                f_def["relation"] = (ET(args["source"]), RT(args["rt"]), ET(args["target"]))
                            else:
                                f_def["relation"] = RT(args["rt"])
                        elif v == "resolver":
                            raise Exception("@resolver not yet implemented")
                        else:
                            raise Exception(f"Don't know how to handle directive @{v}")
        elif definition.kind == "enum_type_definition":
            e_def = json["enums"][definition.name.value] = []
            assert len(definition.directives) == 0, "Don't accept directives on enums"

            for value in definition.values:
                e_def += [value.name.value]

                assert len(value.directives) == 0, "Don't accept directives on enum values"
        else:
            raise Exception(f"Don't know how to handle a top-level graphql schema object of kind {definition.kind}")

    return json

def string_to_RAET(s):
    if s.startswith("ET."):
        return ET(s[len("ET."):])
    elif s.startswith("RT."):
        return RT(s[len("RT."):])
    elif s.startswith("AET."):
        return AET(s[len("AET."):])
    elif s.startswith("EN."):
        return AET(s[len("EN."):])
    else:
        raise Exception(f"Can't parse string as a RAE type: {s}")

def parse_arguments_as_dict(args):
    d = {}
    for arg in args:
        d[arg.name.value] = arg.value.value
    return d
    
        
def simple_capitalize(s):
    # This is here to make things like firebaseID go to FirebaseID rather than
    # FirebaseId. This is more predicatable for users.
    assert len(s) > 0
    return s[0].upper() + s[1:]
    

def json_to_minimal_nodes(json):
    """This takes a json schema, such as one produced by the `parse_schema`
        function, and puts the minimal GQL schema nodes onto the graph.
        "Minimal" is the key word here, as the generated schema graph nodes will
        not define any extra resolvers, these will be done at server run-time.

    """

    core_types = {
        "String": "String",
        "Float": "Float",
        "DateTime": "Time",
        "Int": "Int",
        "Boolean": "Bool"
    }

    actions = []

    # The root node meta data
    actions += [ET.GQL_Root["root"]]
    if "auth" in json:
        actions += [(Z["root"], RT.AuthHeader, json["auth"]["Header"])]
        actions += [(Z["root"], RT.AuthJWKURL, json["auth"]["JWKURL"])]
        actions += [(Z["root"], RT.AuthAudience, json["auth"]["Audience"])]
        if "Namespace" in json["auth"]:
            actions += [(Z["root"], RT.AuthNamespace, json["auth"]["Namespace"])]

    for gql_name,typ in core_types.items():
        actions += [
            getattr(AET, typ)[gql_name],
            # Helpful hack to make the resolvers easier
            (Z[gql_name], RT.Name, gql_name),
            (Z["root"], RT.GQL_CoreScalarType, Z[gql_name])
        ]

    def name_to_raet(name):
        if name in core_types:
            return getattr(AET, core_types[name])
        if name in json["enums"]:
            return getattr(AET.Enum, name)
        return ET(name)
    
    for type_name,fields in json["types"].items():

        actions += [(ET.GQL_Type[type_name], RT.Name, type_name)]
        actions += [(Z["root"], RT.GQL_Type, Z[type_name])]

        et_name = fields.get("_ET", type_name)
        actions += [(Z[type_name], RT.GQL_Delegate, delegate_of(ET(et_name)))]

        for field_name,field in fields.items():
            assert field_name != "id", "id is an automatically generated field, do not explicitly include"

            if field_name.startswith("_"):
                # Special handling here
                if field_name == "_ET":
                    continue
                
                assert field_name in ["_AllowQuery", "_AllowAdd", "_AllowUpdate", "_AllowUpdatePost", "_AllowDelete"]
                # TODO: Turn into a zef function later on
                actions += [(Z[type_name], RT(field_name[1:]), field),]
            else:
                field = {**field}

                qual_name = type_name + "__" + field_name
                actions += [
                    (Z[type_name], RT.GQL_Field[qual_name], Z[field["type"]]),
                    (Z[qual_name], RT.Name, field_name)
                ]
                for bool_key in ["search", "unique", "incoming", "list", "required"]:
                    if bool_key in field:
                        actions += [(Z[qual_name], RT(simple_capitalize(bool_key)), field[bool_key])]
                        del field[bool_key]

                if "resolver" in field:
                    raise NotImplementedError("TODO")
                    del field["resolver"]
                else:
                    if "relation" in field:
                        resolve_with = delegate_of(field["relation"])
                        del field["relation"]
                    else:
                        this = ET(type_name)
                        rt = RT(simple_capitalize(field_name))
                        other = name_to_raet(field["type"])
                        if field.get("incoming", False):
                            resolve_with = delegate_of((other, rt, this))
                        else:
                            resolve_with = delegate_of((this, rt, other))

                    actions += [(Z[qual_name], RT.GQL_Resolve_With, resolve_with)]

                del field["type"]
                assert len(field) == 0, f"There are unhandled keys left in field object {field}"

    for enum_name,opts in json["enums"].items():
        actions += [(ET.GQL_Enum[enum_name], RT.Name, enum_name)]
        actions += [(Z["root"], RT.GQL_Enum, Z[enum_name])]

        for opt in opts:
            actions += [(Z[enum_name], RT.GQL_Field, EN(enum_name, opt))]

    # TODO: I want to validate that no inverse relation has not matched up correctly.

    return actions
