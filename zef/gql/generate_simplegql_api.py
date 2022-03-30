# Assuming this file is not imported during zefdb init.
from .. import *
from ..ops import *
from functools import partial as P
from .utils import *

from ariadne import ObjectType, QueryType, MutationType, EnumType, ScalarType

##############################
# * Utils
#----------------------------

@func
def is_core_scalar(z):
    return (
        is_a(z, AET.String) or
        is_a(z, AET.Int) or
        is_a(z, AET.Float) or
        is_a(z, AET.Bool)
    )
    
assert_type = Assert[Or[is_a[ET.GQL_Type]][is_a[ET.GQL_Enum]][is_a[AET]]][lambda z: f"{z} is not a GQL type"]
assert_field = Assert[is_a[RT.GQL_Field]][lambda z: f"{z} is not a GQL field"]

op_is_scalar = assert_type | Or[is_a[AET]][is_a[ET.GQL_Enum]]
op_is_orderable = assert_type | Or[is_a[AET.Float]][is_a[AET.Int]][is_a[AET.Time]]
op_is_summable = assert_type | Or[is_a[AET.Float]][is_a[AET.Int]]
op_is_list = assert_field >> O[RT.List] | value_or[False] | collect
op_is_required = assert_field >> O[RT.Required] | value_or[False] | collect
op_is_unique = assert_field >> O[RT.Unique] | value_or[False] | collect
op_is_searchable = assert_field >> O[RT.Search] | value_or[False] | collect
op_is_aggregable = assert_field | And[Not[op_is_list]][target | Or[op_is_orderable][op_is_summable]]
op_is_incoming = assert_field >> O[RT.Incoming] | value_or[False] | collect

########################################
# * Generating it all
#--------------------------------------

def generate_resolvers_fcts(schema_root):
    Query = QueryType()
    Mutation = MutationType()

    all_objects = [Query, Mutation]
    query_fields = []
    mutation_fields = []
    type_schemas = []
    enum_schemas = []

    extra_filters = {}


    TimeScalar = ScalarType("DateTime")
    all_objects += [TimeScalar]
    import datetime
    TimeScalar.set_serializer(lambda t: datetime.datetime.fromtimestamp(t.seconds_since_1970).isoformat())
    TimeScalar.set_value_parser(Time)

    for z_type in schema_root >> L[RT.GQL_Type]:
        name = value(z_type >> RT.Name)
        Type = ObjectType(name)
        all_objects += [Type]

        field_schemas = []
        ref_field_schemas = []
        add_field_schemas = []
        aggregate_outs = []

        for z_field in z_type > L[RT.GQL_Field]:
            field_name = z_field >> RT.Name | value | collect
            Type.set_field(field_name,
                            P(resolve_field, z_field=z_field))

            is_scalar = z_field | target | op_is_scalar | collect
            is_required = z_field | op_is_required | collect
            is_list = z_field | op_is_list | collect
            is_orderable = z_field | target | op_is_orderable | collect
            is_summable = z_field | target | op_is_orderable | collect
            is_aggregable = z_field | op_is_aggregable | collect

            base_field_type = z_field | target >> RT.Name | value | collect
            if is_scalar:
                base_ref_field_type = base_field_type
            else:
                base_ref_field_type = base_field_type + "Ref" 

            if is_required:
                maybe_required = "!"
            else:
                maybe_required = ""
            if is_list:
                maybe_params = "(" + schema_generate_list_params(target(z_field), extra_filters) + ")"
                ref_field_type = "[" + base_ref_field_type + "]"
                add_field_type = "[" + base_ref_field_type + maybe_required + "]"
                field_type = "[" + base_field_type + maybe_required + "]"
            else:
                maybe_params = ""
                ref_field_type = base_ref_field_type
                add_field_type = base_ref_field_type + maybe_required
                field_type = base_field_type + maybe_required
            field_schemas += [f"{field_name}{maybe_params}: {field_type}"]
            add_field_schemas += [f"{field_name}: {add_field_type}"]
            ref_field_schemas += [f"{field_name}: {ref_field_type}"]

            if is_aggregable:
                if is_orderable:
                    aggregate_outs += [
                        f"{field_name}Min: {ref_field_type}",
                        f"{field_name}Max: {ref_field_type}",
                    ]
                if is_summable:
                    aggregate_outs += [f"{field_name}Sum: {ref_field_type}"]
                    aggregate_outs += [f"{field_name}Avg: {ref_field_type}"]

        field_schemas += ["id: ID!"]
        add_field_schemas += ["id: ID"]
        ref_field_schemas += ["id: ID"]
        Type.set_field("id", resolve_id)

        # Add the 3 top-level queries
        Query.set_field(f"get{name}",
                        P(resolve_get, type_node=z_type))
        Query.set_field(f"query{name}",
                        P(resolve_query, type_node=z_type))
        Query.set_field(f"aggregate{name}",
                        P(resolve_aggregate, type_node=z_type))
        # Add the 3 top-level mutations
        Mutation.set_field(f"add{name}",
                            P(resolve_add, type_node=z_type))
        Mutation.set_field(f"update{name}",
                            P(resolve_update, type_node=z_type))
        Mutation.set_field(f"delete{name}",
                            P(resolve_delete, type_node=z_type))

        MutateResponse = ObjectType(f"Mutate{name}Response")
        all_objects += [MutateResponse]

        # MutateResponse.set_field(f"count", lambda x,*args,**kwds: x["count"])
        MutateResponse.set_field(to_camel_case(name), P(resolve_filter_response, type_node=z_type))

        query_params = schema_generate_list_params(z_type, extra_filters)
        field_schemas = '\n\t'.join(field_schemas)
        ref_field_schemas = '\n\t'.join(ref_field_schemas)
        add_field_schemas = '\n\t'.join(add_field_schemas)
        aggregate_outs = '\n\t'.join(aggregate_outs)

        type_schemas += [f"""type {name} {{
        {field_schemas}
}}

input {name}Ref {{
        {ref_field_schemas}
}}

input Add{name}Input {{
        {add_field_schemas}
}}
        
input Update{name}Input {{
        filter: {name}Filter!
        set: {name}Ref
        remove: {name}Ref
}}

type Mutate{name}Response {{
        {to_camel_case(name)}({query_params}): [{name}]
        count: Int
}}

type Aggregate{name}Response {{
        count: Int
        {aggregate_outs}
}}

"""]

        # id_fields = z_type > L[RT.GQL_Field] | filter[Z >> O[RT.IsID] | value_or[False]]

        # get_params = id_fields | map[lambda z: f"{value(z >> RT.Name)}: {value(z | target >> RT.Name)}"] | collect
        # get_params = ", ".join(get_params)
        get_params = "id: ID!"
        query_fields += [
            f"get{name}({get_params}): {name}",
            f"query{name}({query_params}): [{name}]",
            f"aggregate{name}({query_params}): Aggregate{name}Response",
        ]
        mutation_fields += [
            f"add{name}(input: [Add{name}Input!]!, upsert: Boolean): Mutate{name}Response",
            f"update{name}(input: Update{name}Input!): Mutate{name}Response",
            f"delete{name}(filter: {name}Filter!): Mutate{name}Response",
        ]


    for z_enum in schema_root >> L[RT.GQL_Enum]:
        name = value(z_enum >> RT.Name)

        opts = {}
        for z_opt in z_enum >> L[RT.GQL_Field]:
            assert is_a(z_opt, AET.Enum(name))
            opt_en = value(z_opt)
            opts[opt_en.enum_value] = opt_en

        Enum = EnumType(name, opts)

        opt_list = '\n\t'.join(opts.keys())

        enum_schemas += [f"""enum {name} {{
\t{opt_list}
}}"""]

        all_objects += [Enum]


    query_fields = '\n\t'.join(query_fields)
    mutation_fields = '\n\t'.join(mutation_fields)
    type_schemas = '\n\n'.join(type_schemas)
    enum_schemas = '\n\n'.join(enum_schemas)
    extra_filters = '\n\n'.join(x["schema"] for x in extra_filters.values())
    schema = f"""
{type_schemas}

{enum_schemas}

type Query {{
\t{query_fields}
}}

type Mutation {{
    {mutation_fields}
}}
    
scalar DateTime
    
{extra_filters}
    
"""

    return schema, all_objects

################################################
# * Schema specific parts
#----------------------------------------------

def schema_generate_list_params(z_type, extra_filters):
    name = value(z_type >> RT.Name)
    if z_type not in extra_filters:
        extra_filters[z_type] = None
        extra_filters[z_type] = schema_generate_type_filter(z_type, extra_filters)

    query_params = [
        f"filter: {name}Filter",
        # We probably want to change these to be proper cursors.
        "first: Int",
        "offset: Int",
    ]
    if extra_filters[z_type]["orderable"]:
        query_params += [f"order: {name}Order"]

    query_params = ", ".join(query_params)
    return query_params

def schema_generate_type_filter(z_type, extra_filters):
    name = value(z_type >> RT.Name)
    fil_name = f"{name}Filter"
    order_name = f"{name}Order"
    orderable_name = f"{name}Orderable"

    fields = [
        # TODO: I Think this is if the field is present?
        # "has: [{fil_name}]",
        f"and: [{fil_name}]",
        f"or: [{fil_name}]",
        f"not: {fil_name}",
    ]

    orderable_fields = []

    # Every type has an ID field, which is slightly custom - functions as an
    # automatically generated "in"
    fields += [f"id: [ID!]"]

    for field in z_type > L[RT.GQL_Field] | filter[op_is_searchable]:
        field_name = value(field >> RT.Name)
        field_type = target(field)
        field_type_name = value(field_type >> RT.Name)
        if is_a(field_type, AET.Bool):
            fields += [f"{field_name}: Boolean"]
        else:
            fields += [f"{field_name}: {field_type_name}Filter"]
            if field_type not in extra_filters:
                extra_filters[field_type] = None
                extra_filters[field_type] = schema_generate_scalar_filter(field_type)

    for field in z_type > L[RT.GQL_Field] | filter[target | op_is_orderable]:
        orderable_fields += [value(field >> RT.Name)]

    fields = "\n\t".join(fields)
    orderable_fields = "\n\t".join(orderable_fields)

    out = f"""input {fil_name} {{
\t{fields}
}}
"""
    if len(orderable_fields) > 0:
        out += f"""
input {order_name} {{
        asc: {orderable_name}
        desc: {orderable_name}
        then: {order_name}
}}
    
enum {orderable_name} {{
\t{orderable_fields}
}}
"""
    return {"schema": out,
            "orderable": len(orderable_fields) > 0}
        

def schema_generate_scalar_filter(z_node):
    type_name = z_node >> RT.Name | value | collect
    fil_name = f"{type_name}Filter"

    if is_a(z_node, AET.Bool):
        # Shoudln't need to do anything here, as it is true/false.
        return

    schema = f"""input {fil_name} {{
\teq: {type_name}
\tin: [{type_name}]
"""

    if op_is_orderable(z_node):
        schema += f"""\tle: {type_name}
\tlt: {type_name}
\tge: {type_name}
\tgt: {type_name}
\tbetween: {type_name}Range
"""

    schema += "}"
    
    if op_is_orderable(z_node):
        schema += f"""\ninput {type_name}Range {{
\tmin: {type_name}!
\tmax: {type_name}!
}}"""

    return {"schema": schema}

####################################
# * Query resolvers
#----------------------------------

def resolve_get(_, info, *, type_node, **params):
    # Look for something that fits exactly what has been given in the params, assuming
    # that ariadne has done its work and validated the query.
    return find_existing_entity(info, type_node, params["id"])

def resolve_query(_, info, *, type_node, **params):
    g = info.context["g"]
    ents = g | now | all[ET(type_node >> RT.GQL_Delegate | collect)]
    ents = ents | filter[pass_query_auth[type_node][info]]

    ents = handle_list_params(ents, type_node, params, info)

    return ents | collect

def resolve_aggregate(_, info, *, type_node, **params):
    # We can potentially defer the aggregation till later, by returning a kind
    # of lazy object here. However, for simplicity in the beginning, I will
    # aggregate everything, even if the query is only for a single field.

    ents = resolve_query(_, info, type_node=type_node, filter=params.get("filter", None))

    out = {"count": len(ents)}
    for z_field in type_node > L[RT.GQL_Field] | filter[op_is_aggregable]:
        vals = ents | map[lambda z: resolve_field(z, info, z_field=z_field)] | filter[Not[equals[None]]] | collect

        field_name = z_field >> RT.Name | value | collect

        if op_is_orderable(z_field | target):
            if len(vals) == 0:
                val_min = val_max = None
            else:
                val_min = min(vals)
                val_max = max(vals)

            # if isinstance(val_min, Time):
            #     val_min = str(val_min)
            #     val_max = str(val_max)

            out[f"{field_name}Min"] = val_min
            out[f"{field_name}Max"] = val_max

        if op_is_summable(z_field | target):
            if len(vals) == 0:
                val_sum = None
                val_avg = None
            else:
                val_sum = sum(vals)
                val_avg = val_sum / len(vals)

            out[f"{field_name}Sum"] = val_sum
            out[f"{field_name}Avg"] = val_avg

    return out

def resolve_field(z, info, *, z_field, **params):
    is_list = z_field | op_is_list | collect
    is_required = z_field | op_is_required | collect

    opts = internal_resolve_field(z, info, z_field)

    if is_list:
        opts = handle_list_params(opts, target(z_field), params, info)

    opts = collect(opts)

    if is_list:
        return opts
    else:
        if not is_required and len(opts) == 0:
            return None
        return single(opts)

def resolve_id(z, info):
    return str(origin_uid(z))


##############################
# * Mutations
#----------------------------
def resolve_add(_, info, *, type_node, **params):
    name_gen = NameGen()
    actions = []
    new_obj_names = []
    updated_objs = []
    
    # This is not optimal but simplest to understand for now.
    upsert = params.get("upsert", False)
    for item in params["input"]:
        # Check id fields to see if we already have this.
        if "id" in item:
            if not upsert:
                raise Exception("Can't update item with id without setting upsert")
            obj = find_existing_entity(info, type_node, item["id"])
            if obj is None:
                raise Exception("Item doesn't exist")
            set_d = {**item}
            set_d.pop("id")
            actions += update_entity(obj, info, type_node, set_d, {}, name_gen)
            updated_objs += [obj]
        else:
            obj_name,more_actions = add_new_entity(info, type_node, item, name_gen)
            actions += more_actions
            new_obj_names += [obj_name]

    g = info.context["g"]
    r = GraphDelta(actions) | g | run

    ents = updated_objs + [r[name] for name in new_obj_names]
    count = len(ents)

    return {"count": count, "ents": ents}
            
        
def resolve_update(_, info, *, type_node, **params):
    if "input" not in params or "filter" not in params["input"]:
        raise Exception("Not allowed to update everything!")
    ents = resolve_query(_, info, type_node=type_node, filter=params["input"]["filter"])

    actions = []
    name_gen = NameGen()
    for ent in ents:
        actions += update_entity(ent, info, type_node, params["input"].get("set", {}), params["input"].get("remove", {}), name_gen)

    g = info.context["g"]
    r = GraphDelta(actions) | g | run

    count = len(ents)

    # Note: we return the details after the update
    ents = ents | map[now] | collect

    return {"count": count, "ents": ents}

def resolve_delete(_, info, *, type_node, **params):
    g = info.context["g"]
    # Do the same thing as a resolve_query but delete the entities instead.
    if "filter" not in params:
        raise Exception("Not allowed to delete everything!")
    ents = resolve_query(_, info, type_node=type_node, **params)

    if not ents | map[pass_delete_auth[type_node][info]] | all | collect:
        raise Exception("Not allowed to delete")

    GraphDelta([terminate[ent] for ent in ents]) | g | run

    count = len(ents)

    return {"count": count, "ents": ents}

def resolve_filter_response(obj, info, *, type_node, **params):
    ents = obj["ents"]

    ents = handle_list_params(ents, type_node, params, info)

    return ents | collect




##############################################
# * Internal query parts
#--------------------------------------------

def handle_list_params(opts, z_node, params, info):
    opts = maybe_filter_result(opts, z_node, info, params.get("filter", None))
    opts = maybe_sort_result(opts, z_node, info, params.get("order", None))
    opts = maybe_paginate_result(opts, params.get("first", None), params.get("offset", None))
    return opts

@func
def field_resolver_by_name(z, type_node, info, name):
    # Note name goes last because it is curried last... this is weird
    if name == "id":
        return resolve_id(z, info=info)
    sub_field = get_field_rel_by_name(type_node, name)
    return resolve_field(z, info=info, z_field=sub_field)


# ** Filtering

def maybe_filter_result(opts, z_node, info, fil=None):
    if fil is None:
        return opts

    return opts | filter[build_filter_zefop(fil, field_resolver_by_name[z_node][info])]

def build_filter_zefop(fil, field_resolver):
    # top level is ands
    top = And
    for key,sub in fil.items():
        if key == "and":
            this = And
            for part in sub:
                this = this[build_filter_zefop(part, field_resolver)]
        elif key == "or":
            this = Or
            for part in sub:
                this = this[build_filter_zefop(part, field_resolver)]
        elif key == "not":
            this = Not[build_filter_zefop(sub, field_resolver)]
        elif key == "id":
            # This is handled specially - functions like an "in".
            val = field_resolver["id"]
            this = val | contained_in[sub]
        else:
            # This must be a field
            val = field_resolver[key]

            if isinstance(sub, bool):
                this = val | equals[sub]
            else:
                this = And[Not[equals[None]]]
                for key,sub in sub.items():
                    if key == "eq":
                        this = this[equals[sub]]
                    elif key == "in":
                        this = this[contained_in[sub]]
                    elif key == "le":
                        this = this[less_than_or_equal[sub]]
                    elif key == "lt":
                        this = this[less_than[sub]]
                    elif key == "ge":
                        this = this[greater_than_or_equal[sub]]
                    elif key == "gt":
                        this = this[greater_than[sub]]
                    elif key == "between":
                        this = this[greater_than_or_equal[sub["min"]]]
                        this = this[less_than_or_equal[sub["max"]]]
                    else:
                        raise Exception(f"Unknown comparison operator: {key}")
                this = val | this

        top = top[this]

    return top
                
def get_field_rel_by_name(z_type, name):
    return (z_type > L[RT.GQL_Field]
            | filter[Z >> RT.Name | value | equals[name]]
            | first
            | collect)

# ** Sorting

def maybe_sort_result(opts, z_node, info, sort_decl=None):
    if sort_decl is None:
        return opts

    field_resolver = field_resolver_by_name[z_node][info]

    # First, get the list of things to sort by, so that we can reverse it
    sort_list = []
    cur = sort_decl
    while cur is not None:
        if "asc" in cur:
            assert not "desc" in cur
            val = field_resolver[cur["asc"]]
            sort_list += [sort[val]]
        elif "desc" in cur:
            val = field_resolver[cur["desc"]]
            sort_list += [sort[val][True]]

        cur = cur.get("then", None)

    sort_list.reverse()

    for action in sort_list:
        opts = opts | action

    return opts

# ** Pagination

def maybe_paginate_result(opts, first=None, offset=None):
    if offset is not None:
        opts = opts | skip[offset]
    if first is not None:
        opts = opts | take[first]
    return opts


# ** Resolution

@func
def internal_resolve_field(z, info, z_field):
    # This returns a LazyValue so we can deal with whatever comes out without
    # instantiating the whole list.

    # Also *only* returns that list. The calling function needs to apply the single/list logic
    
    is_incoming = z_field | op_is_incoming | collect
    # This is a delegate
    relation = z_field >> RT.GQL_Resolve_With | collect
    is_triple = source(relation) != relation

    rt = RT(relation)

    if is_triple:
        if is_incoming:
            assert rae_type(z) == rae_type(target(relation)), f"The RAET of the object {z} is not the same as that of the delegate relation {target(relation)}"
        else:
            assert rae_type(z) == rae_type(source(relation)), f"The RAET of the object {z} is not the same as that of the delegate relation {source(relation)}"

    if is_incoming:
        opts = z << L[rt]
        if is_triple:
            opts = opts | filter[is_a[rae_type(source(relation))]]
    else:
        opts = z >> L[rt]
        if is_triple:
            opts = opts | filter[is_a[rae_type(target(relation))]]

    # Auth filtering todo
    opts = opts | filter[pass_query_auth[target(z_field)][info]]

    # We must convert final objects from AEs to python types.
    # if z_field | target | is_core_scalar | collect:
    if z_field | target | op_is_scalar | collect:
        opts = opts | map[value]

    return opts


# ** Handling adding

def NameGen():
    n = 0
    while True:
        n += 1
        yield n

def find_existing_entity(info, type_node, id):
    if id is None:
        return None
    the_uid = uid(id)
    if the_uid is None:
        raise Exception("An id of {id} cannot be converted to a uid.")
    
    g = info.context["g"]

    ent = g[uid(id)] | now | collect
    if not is_a(ent, ET(type_node >> RT.GQL_Delegate | collect)):
        return None
    if not ent | pass_query_auth[type_node][info] | collect:
        return None

    return ent

def add_new_entity(info, type_node, params, name_gen):
    if not pass_add_auth(type_node, info, params):
        raise Exception("Not allowed to add.")

    actions = []

    this = str(next(name_gen))

    et = ET(type_node >> RT.GQL_Delegate | collect)
    actions += [et[this]]

    # This should probably be cached
    field_mapping = {}
    for z_field in type_node > L[RT.GQL_Field]:
        field_mapping[value(z_field >> RT.Name)] = z_field

    # TODO: Validate that all required fields are present
    for key,val in params.items():
        # TODO: Validate that any unique field is not duplicated
        z_field = field_mapping[key]
        rt = RT(z_field >> RT.GQL_Resolve_With | collect)
        if z_field | op_is_list | collect:
            if not isinstance(val, list):
                raise Exception(f"Value should have been list but was {type(val)}")

        if z_field | target | op_is_scalar | collect:
            if z_field | op_is_list | collect:
                l = val
            else:
                l = [val]

            for item in l:
                # TODO: Maybe proper ordering here too? Or should this
                # always be considered to be a set, ordered only when the
                # client requests it?
                if z_field | op_is_incoming | collect:
                    actions += [item, rt, (Z[this])]
                else:
                    actions += [(Z[this], rt, item)]
        else:
            if z_field | op_is_list | collect:
                l = val
            else:
                l = [val]

            for item in l:
                obj,obj_actions = find_or_add_entity(item, info, target(z_field), params, name_gen)
                actions += obj_actions
                if z_field | op_is_incoming | collect:
                    actions += [(obj, rt, Z[this])]
                else:
                    actions += [(Z[this], rt, obj)]

    return this, actions

def find_or_add_entity(val, info, target_node, params, name_gen):
    if isinstance(val, dict) and val.get("id", None) is not None:
        obj = find_existing_entity(info, target_node, val["id"])
        return obj,[]
    else:
        obj_name,actions = add_new_entity(info, target_node, val, name_gen)
        return Z[obj_name], actions
    

def update_entity(z, info, type_node, set_d, remove_d, name_gen):
    # Refuse to set and remove the same thing. Just too confusing to deal with
    if not pass_update_auth(z, type_node, info, set_d, remove_d):
        raise Exception("Not allowed to add.")

    assert(len(set(set_d.keys()).intersection(remove_d.keys())) == 0), "Can't have the same set/remove keys"
    if len(set_d) + len(remove_d) == 0:
        raise Exception("No set or remove in update_entity!")

    actions = []

    # This should probably be cached
    field_mapping = {}
    for z_field in type_node > L[RT.GQL_Field]:
        field_mapping[value(z_field >> RT.Name)] = z_field

    for key,val in set_d.items():
        # TODO: Validate that any unique field is not duplicated
        z_field = field_mapping[key]
        # TODO: This should be able to distinguish based on the triple, not just the RT
        rt = RT(z_field >> RT.GQL_Resolve_With | collect)

        if op_is_list(z_field):
            raise Exception(f"Updating list things is a todo (for {z_field=})")
        else:
            if op_is_incoming(z_field):
                maybe_prior_rel = z < O[rt] | collect
            else:
                maybe_prior_rel = z > O[rt] | collect
            if z_field | target | op_is_scalar | collect:
                if maybe_prior_rel is None:
                    if op_is_incoming(z_field):
                        actions += [(val, rt, z)]
                    else:
                        actions += [(z, rt, val)]
                else:
                    actions += [(maybe_prior_rel | target | collect) <= val]
            else:
                raise Exception("Updating non-scalars is TODO")
    
    for key,val in remove_d.items():
        # TODO: Validate that required fields are not removed
        assert val is None, "Remove vals need to be nil"
        z_field = field_mapping[key]
        # TODO: This should be able to distinguish based on the triple, not just the RT
        rt = RT(z_field >> RT.GQL_Resolve_With | collect)
        if op_is_incoming(z_field):
            rels = z < L[rt]
        else:
            rels = z > L[rt]
        actions += [terminate[rel] for rel in rels]

        # Also delete scalars
        if z_field | target | op_is_scalar | collect:
            actions += [terminate[target(rel)] for rel in rels]

    return actions


##############################
# * Auth things
#----------------------------

@func
def pass_query_auth(z, schema_node, info):
    # TODO: Later on these will be zef functions so easier to call
    if not schema_node | has_out[RT.AllowQuery] | collect:
        return True
    return temporary__call_string_as_func(
        schema_node >> RT.AllowQuery | value | collect,
        z=z,
        info=info,
        type_node=schema_node
    )

@func
def pass_add_auth(schema_node, info, params):
    # TODO: Later on these will be zef functions so easier to call
    if not schema_node | has_out[RT.AllowAdd]| collect:
        return True
    return temporary__call_string_as_func(
        schema_node >> RT.AllowAdd | value | collect,
        info=info,
        type_node=schema_node,
        params=params
    )

@func
def pass_update_auth(z, schema_node, info, set_d, remove_d):
    # TODO: Later on these will be zef functions so easier to call
    if not schema_node | has_out[RT.AllowUpdate] | collect:
        return (pass_query_auth(z, schema_node, info)
                # TODO: This second part has to be handled properly through a PostCondition
                #and pass_add_auth(schema_node, info))
                )
    return temporary__call_string_as_func(
        schema_node >> RT.AllowUpdate | value | collect,
        z=z,
        info=info,
        type_node=schema_node
    )

@func
def pass_delete_auth(z, schema_node, info):
    # TODO: Later on these will be zef functions so easier to call
    if not schema_node | has_out[RT.AllowDelete]| collect:
        return pass_query_auth(z, schema_node, info)
    return temporary__call_string_as_func(
        root >> RT.AllowDelete | value | collect,
        z=z,
        info=info,
        type_node=schema_node
    )


def temporary__call_string_as_func(s, **kwds):
    from .. import core, ops
    try:
        out = eval(
            s, {
                **{name: getattr(core,name) for name in dir(core) if not name.startswith("_")},
                **{name: getattr(ops,name) for name in dir(ops) if not name.startswith("_")},
            },
            kwds
        )
    except Exception as exc:
        from ..core.logger import log
        log.error("Problem calling auth expression", expr=s, exc_info=exc, kwds=kwds)
        return False

    if isinstance(out, LazyValue):
        out = collect(out)
    elif isinstance(out, ZefOp):
        out = out(kwds.get("z", None))

    return out