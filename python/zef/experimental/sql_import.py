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

import pandas
import math
from caseconverter import pascalcase

def is_nan(val):
    import pandas.core.dtypes.common as p_dtypes
    if p_dtypes.is_integer(val):
        return math.isnan(val)
    elif p_dtypes.is_float(val):
        return math.isnan(val)
    else:
        return False

def get_col_ind(cols, name):
    for i,col in enumerate(cols):
        if col["name"] == name:
            return i
    return None

def get_col(cols, name):
    return cols[get_col_ind(cols,name)]
        

def process_df_guess(df, tag):
    out = {
        "tag": tag,
        "cols": [],
    }

    cols = list(df)
    df2 = df.convert_dtypes()

    for col in cols:

        sw = df2[col].dtype
        import pandas.core.dtypes.common as p_dtypes
        if p_dtypes.is_string_dtype(sw):
            typ = 'String'
        elif p_dtypes.is_bool_dtype(sw):
            typ = 'Bool'
        elif p_dtypes.is_integer_dtype(sw):
            typ = 'Int'
        elif p_dtypes.is_float_dtype(sw):
            typ = 'Float'
        else:
            typ = 'Serialized'
        
        # TODO: We could put a guess here on cols which are ints and have "ID/Id" at the end of them.
        col_desc = {
            "name": col,
            "data_type": typ,
            # Everything starts off as a field
            "purpose": "field",
            "RT": pascalcase(col),
        }

        out["cols"] += [col_desc]

    col_names = out["cols"] | map[get["name"]] | collect
    if len(distinct(col_names)) != len(col_names):
        raise Exception("CSV has multiple columns with the same name")

    ID_col = None
    # The first column that is an integer is a good candidate for the ID
    for col in out["cols"]:
        if col["data_type"] == 'Int':
            ID_col = col["name"]
            break
    if ID_col is None:
        # Otherwise try the first string column
        for col in out["cols"]:
            if col["data_type"] == 'String':
                ID_col = col["name"]
                break

    if ID_col is None:
        raise Exception("Need an ID column!")

    if False:
        pass
    else:
        # Assume this table defines the fields of an entity
        out["kind"] = "entity"
        out["ET"] = pascalcase(tag)
        out["ID_col"] = ID_col

        col = get_col(out["cols"], ID_col)
        new_col = {
            "name": col["name"],
            "data_type": col["data_type"],
            "purpose": "id",
            "RT": "ID"
        }
        col.clear()
        col.update(new_col)

    return out


def guess_csvs(filenames):
    if isinstance(filenames, str):
        import glob
        filenames = glob.glob(filenames)

    defs = []
    for filename in filenames:
        tag = os.path.splitext(filename)[0]
        try:
            df = pandas.read_csv(filename)
        except pandas.errors.EmptyDataError:
            print(f"Pandas complained that file {filename!r} has no data - going to skip")
            continue
        except Exception as exc:
            raise Exception(f"Pandas couldn't parse file {filename!r}") from exc
        this = process_df_guess(df, tag)

        this["data_source"] = {
            "type": "csv",
            "filename": filename
        }

        defs += [this]
    
    decl = default_outer()
    decl["definitions"] = defs
    return decl

def default_outer():
    return {
        "default_ID": "ID"
    }

def et_id(et, val, decl):
    ID_col = get_ent_ID_col(et, decl)
    val = coerce_val(val, aet_str_to_aet(ID_col['data_type']))
    assert val is not None and not is_nan(val)
    return f"<ET_{et} {val}>"

def rt_id(src, trg, rt, val, data_type):
    val = coerce_val(val, aet_str_to_aet(data_type))
    assert val is not None and not is_nan(val)
    return f"<RT_{src}_{trg}_{rt} {val}>"

def aet_str_to_aet(aet_s):
    parts = aet_s.split('.')
    aet = AET
    for part in parts:
        aet = getattr(aet, part)
    return aet

def coerce_val(val, aet):
    if isinstance(val, list):
        raise Exception("Vals can't be lists")

    if is_a(aet, AET.QuantityFloat):
        return QuantityFloat(val, aet.__unit)
    elif is_a(aet, AET.QuantityInt):
        return QuantityFloat(val, aet.__unit)
    elif is_a(aet, AET.Enum):
        return EN(aet.__enum_type, to_pascal_case(val))
    elif is_a(aet, AET.Bool):
        if val in [0, False, 'False', 'false', 'FALSE', 'no']:
            return False
        elif val in [1, True, 'True', 'true', 'TRUE', 'yes']:
            return True
        else:
            raise Exception(f"Bool value can't be converted: {val}")
    elif is_a(aet, AET.Time):
        if isinstance(val, str):
            # If all numeric chars are zeros, this is a null time
            null = distinct([x for x in val if x.isdigit()]) == ['0']
            if null:
                return None
            return Time(val)
        else:
            raise Exception("Don't know how to convert to AET.Time")
    # elif is_a(aet, str):
    #     pass
    # else:
    #     assert not math.isnan(val)
    return val

def get_ent_ID_col(et, decl):
    # Try to find et in the definitions - if not there, then we'll just use ID
    # as the ID field
    for d in decl["definitions"]:
        if d.get("ET", None) == et:
            return get_col(d["cols"], d["ID_col"])
    fake_col = {
        "RT": decl["default_ID"],
        "data_type": "Int",
    }
    return fake_col

def make_ent(col, val, decl):
    et = col["ET"]

    z_id = et_id(et, val, decl)
    actions = [ET(et)[z_id]]

    ID_col = get_ent_ID_col(et, decl)
    actions += attach_field(Z[z_id], ID_col, val)

    return actions

def attach_field(z, col, val, rt=None):
    import math
    if val is None or isinstance(val, float) and math.isnan(val):
        return []
    
    if rt is None:
        rt = RT(col["RT"])
    aet = aet_str_to_aet(col['data_type'])
        
    ae_id = name_from_Z(z) + f" field {rt}"
    val = coerce_val(val, aet)
    if val is None:
        return []

    return [
        aet[ae_id],
        Z[ae_id] <= val,
        (z, rt, Z[ae_id]),
    ]

def name_from_Z(z):
    return LazyValue(z) | peel | first | second | first | collect

def import_actions_definition(definition, decl):
    assert validate_definition(definition, decl)
    actions = []
    
    if definition["data_source"]["type"] == "csv":
        df = pandas.read_csv(definition["data_source"]["filename"])
    else:
        raise Exception("Unknown data source type")

    groups = definition["cols"] | group_by[get["purpose"]][["entity","field", "field_on", "ignore", "id", "source", "target"]] | func[dict] | collect

    # col_ents = definition["cols"] | filter[
    #     get["purpose"] | equals["entity"]
    # ] | collect

    # col_fields = definition["cols"] | filter[
    #     get["purpose"] | equals["field"]
    # ] | collect
    col_ents = groups["entity"]
    col_fields = groups["field"]
    col_fieldons = groups["field_on"]

    if len(groups["id"]) == 0:
        col_ID = None
    elif len(groups["id"]) == 1:
        col_ID = only(groups["id"])

    # As this can be slow - let's warn about it
    next_print_time = now() + 5*seconds
    # Note: iterrows does implicit conversion of types. Use itertuples to maintain the dtypes
    # Also, itertuples can't allow python identifiers like "class".
    #
    # So they are both useless! Of course this is not recommended to do things
    # this way... but it doesn't help when I need to do things this way.
    #
    # Indices is the only way for now.
    for i_row in df.index:
        if now() > next_print_time:
            print(f"SQL import is taking some time. Currently up to row {i_row} of table tagged {definition['tag']}")
            next_print_time = now() + 5*seconds
            
        # First do everything that is not a relation
        for col_ent in col_ents:
            val = df[col_ent["name"]][i_row]
            val = coerce_val(val, aet_str_to_aet(col_ent["data_type"]))
            if val is not None and not is_nan(val):
                temp = make_ent(col_ent, val, decl)
                actions += temp

        # Next get the object which corresponds to this row
        if definition["kind"] == "entity":
            val = df[col_ID["name"]][i_row]
            this_id = et_id(definition["ET"], val, decl)
            actions += [ET(definition["ET"])[this_id]]
            z_this = Z[this_id]
        elif definition["kind"] == "relation":
            col_source = only(groups["source"])
            col_target = only(groups["target"])
            val_source = df[col_source["name"]][i_row]
            val_target = df[col_target["name"]][i_row]
            source_id = et_id(col_source["ET"], val_source, decl)
            target_id = et_id(col_target["ET"], val_target, decl)

            actions += [ET(col_source["ET"])[source_id]]
            actions += [ET(col_target["ET"])[target_id]]

            if col_ID is None:
                val = i_row
                this_id = rt_id(source_id, target_id, definition["RT"], val, "Int")
            else:
                val = df[col_ID["name"]][i_row]
                this_id = rt_id(source_id, target_id, definition["RT"], val, col_ID["data_type"])
            actions += [(Z[source_id], RT(definition["RT"])[this_id], Z[target_id])]
            z_this = Z[this_id]
        else:
            raise NotImplementedError()

        if col_ID is not None:
            val = df[col_ID["name"]][i_row]
            actions += attach_field(z_this, col_ID, val)

        # All fields for this object
        for col_field in col_fields:
            val = df[col_field["name"]][i_row]
            actions += attach_field(z_this, col_field, val)

        # All connections to the other entities
        for col_ent in col_ents:
            if col_ent["name"] == definition["ID_col"]:
                continue
            val = df[col_ent["name"]][i_row]
            val = coerce_val(val, aet_str_to_aet(col_ent["data_type"]))

            if val is not None and not is_nan(val):
                target_id = et_id(col_ent["ET"], val, decl)
                rel_id = f"{name_from_Z(z_this)} {col_ent['RT']} {target_id}"
                actions += [(z_this, RT(col_ent["RT"])[rel_id], Z[target_id])]

        # All fields on connections
        for col_fieldon in col_fieldons:
            col_target = get_col(col_ents, col_fieldon["target"])
            target_val = df[col_target["name"]][i_row]
            target_id = et_id(col_target["ET"], target_val, decl)
            rel_id = f"{name_from_Z(z_this)} {col_target['RT']} {target_id}"
            z_conn = Z[rel_id]
            val = df[col_fieldon["name"]][i_row]
            temp += attach_field(z_conn, col_fieldon, val)
            actions += temp

    actions = distinct(actions)
    return actions

def validate_definition(definition, decl):
    # if "ID_col" in definition:
    #     col_ID = get_col(definition["cols"], definition["ID_col"])
    #     if definition["kind"] == "entity":
    #         assert definition["ET"] == col_ID["ET"], f"The table definition of an entity needs to have the same ET set ('{definition['ET']}') for the column which is its identity ('{col_ID['ET']}')."

    return True

def import_actions(decl):
    actions = []
    for d in decl["definitions"]:
        actions += import_actions_definition(d, decl)
    return distinct(actions)