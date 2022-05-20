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

@func
def auto_generate_gql(g):
    if length(g | now | all[ET.GQL_Schema]) > 0:
        raise Exception("Graph already has a GQL schema, not going to auto generate a new one.")

    actions = [
        ET.GQL_Schema["schema_root"],

        (Z["schema_root"], RT.GQL_Type, ET.GQL_Type["query"]),
        (Z["query"], RT.Name, "GQL_Query"),

        (Z["schema_root"], RT.GQL_Scalar, ET.GQL_Scalar["scalar_enum"]),
        (Z["scalar_enum"], RT.Name, "GQL_Enum"),
        (Z["schema_root"], RT.GQL_Scalar, ET.GQL_Scalar["scalar_quantity_float"]),
        (Z["scalar_quantity_float"], RT.Name, "GQL_QuantityFloat"),
        (Z["schema_root"], RT.GQL_Scalar, ET.GQL_Scalar["scalar_quantity_int"]),
        (Z["scalar_quantity_int"], RT.Name, "GQL_QuantityInt"),
        (Z["schema_root"], RT.GQL_Scalar, ET.GQL_Scalar["scalar_time"]),
        (Z["scalar_time"], RT.Name, "GQL_Time"),
    ]

    @func
    def del_is_a(x, T):
        return x | And[is_a[Delegate]][abstract_type | is_a[T]] | collect
        
    for ent in g | all | filter[del_is_a[ET]] | collect:
        fields = ent > L[RT] | filter[lambda z: z | target | del_is_a[AET] | collect] | collect
        out_others = ent > L[RT] | filter[lambda z: z | target | del_is_a[ET] | collect] | collect
        in_others = ent < L[RT] | filter[lambda z: z | source | del_is_a[ET] | collect] | collect
        if len(fields) == 0 and len(out_others) == 0 and len(in_others) == 0:
            continue
        name = "GQL_" + str(ET(ent))
        actions += [
            (Z["schema_root"], RT.GQL_Type, ET.GQL_Type[name]),
            (Z[name], RT.Name, name),
        ]

        all_rts = (fields + out_others) | map[RT] | collect
        dup_rts = all_rts | filter[lambda x: all_rts.count(x) > 1] | collect
        if len(dup_rts) > 0:
            raise Exception(f"Can't auto-generate GQL, there are multiple RTs {dup_rts} which need to be disambiguated")

        for field in fields:
            f_name = "GQL_" + str(RT(field))
            full_name = name + f_name

            if del_is_a(target(field), AET.Enum):
                rt_s = f"{RT(field)!r}"
                actions += [
                    (Z[full_name], RT.GQL_Resolve_with_body, "v = maybe_value(z >> O[" + rt_s + "]); return None if v is None else {'value': v.enum_value, 'type': v.enum_type}"),
                ]
                aet = Z["scalar_enum"]
            elif del_is_a(target(field), AET.QuantityFloat):
                rt_s = f"{RT(field)!r}"
                actions += [
                    (Z[full_name], RT.GQL_Resolve_with_body, "q = maybe_value(z >> O[" + rt_s + "]); return None if q is None else {'value': q.value, 'unit': q.unit.enum_value}"),
                ]
                aet = Z["scalar_quantity_float"]
            elif del_is_a(target(field), AET.QuantityInt):
                rt_s = f"{RT(field)!r}"
                actions += [
                    (Z[full_name], RT.GQL_Resolve_with_body, "q = maybe_value(z >> O[" + rt_s + "]); return None if q is None else {'value': q.value, 'unit': q.unit.enum_value}"),
                ]
                aet = Z["scalar_quantity_int"]
            elif del_is_a(target(field), AET.Time):
                rt_s = f"{RT(field)!r}"
                actions += [
                    (Z[full_name], RT.GQL_Resolve_with_body, "t = maybe_value(z >> O[" + rt_s + "]); return None if t is None else str(t)"),
                ]
                aet = Z["scalar_time"]
            else:
                actions += [
                    # (Z[full_name], RT.GQL_Resolve_with, field),
                    (Z[full_name], RT.GQL_Resolve_with, delegate_of((ET(ent), RT(field), AET(target(field))))),
                ]
                aet = AET(target(field))

            actions += [
                (Z[name], RT.GQL_Field[full_name], aet),
                (Z[full_name], RT.Name, f_name),
                ]


        for other in out_others:
            f_name = "GQL_" + str(RT(other)) + "s"
            full_name = name + f_name
            full_name_rel = full_name + "_rel"
            other_name = "GQL_" + str(ET(target(other)))

            actions += [
                (Z[name], RT.GQL_Field[full_name_rel], ET.GQL_List[full_name]),
                (Z[full_name], RT.argument, Z[other_name]),
                (Z[full_name_rel], RT.Name, f_name),
                (Z[full_name_rel], RT.GQL_Resolve_with, delegate_of((ET(ent), RT(other), ET(target(other))))),
            ]

        for other in in_others:
            f_name = "GQL_" + "rev_" + str(RT(other)) + "s"
            full_name = name + f_name
            full_name_rel = full_name + "_rel"
            other_name = "GQL_" + str(ET(source(other)))

            actions += [
                (Z[name], RT.GQL_Field[full_name_rel], ET.GQL_List[full_name]),
                (Z[full_name], RT.argument, Z[other_name]),
                (Z[full_name_rel], RT.Name, f_name),
                (Z[full_name_rel], RT.GQL_Resolve_with[full_name_rel + "_with"], delegate_of((ET(target(other)), RT(other), ET(ent)))),
                (Z[full_name_rel + "_with"], RT.IsOut, False),
            ]

        # Always add a uid entry to ETs

        f_name = "GQL_Uid"
        full_name = name + f_name
        full_name_rel = full_name + "_rel"
        actions += [
            (Z[name], RT.GQL_Field[full_name_rel], AET.String),
            (Z[full_name_rel], RT.Name, f_name),
            (Z[full_name_rel], RT.GQL_Resolve_with_body, "return origin_uid(z)"),
        ]

            
        # Add an option to query for all of this type
        q_name = "query_" + name
        q_name_rel = q_name + "_rel"
        actions += [
            (Z["query"], RT.GQL_Field[q_name_rel], ET.GQL_List[q_name]),
            (Z[q_name], RT.argument, Z[name]),
            (Z[q_name_rel], RT.Name, name + "s"),
            (Z[q_name_rel], RT.GQL_Resolve_with_body, f"return g | now | all[{ET(ent)!r}] | collect"),
        ]

    return actions
