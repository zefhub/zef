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

from .fx_types import Effect, FX
from .._ops import *
from ..zef_functions import func
from ..VT import *
from ..logger import log


# TODO:
# 3. Add a mutation for adding a new field


#-------------------------------------------------------------
#---------------------Resolvers-------------------------------
#-------------------------------------------------------------
# TODO: Add Better Error Reporting

@func
def graphs(query_args):
    from zef import pyzef
    uids = pyzef.internals.list_graph_manager_uids()
    return (
        uids
        | map[lambda uid: {'labels': [str(uid)], 'id': str(uid)}]
        | collect
    )
    return (
        globals() 
        | items     
        | filter[lambda kv: is_a(kv[1], Graph)]
        | map[lambda kv: {'labels': [kv[0]], 'id': str(uid(kv[1]))}]
        | collect
    )

@func
def entity_types(query_args):
    graph_id = query_args.get('graphID', None)
    if not graph_id: return None
    return list(Graph(graph_id) | now | all[ET] | map[rae_type] | func[set] | collect)


def make_cellet_value(obj):
    if has_out(obj, RT.Name):
        label = value(obj | Out[RT.Name])
    elif has_out(obj, RT.Label):
        label = value(obj | Out[RT.Label])
    else:
        label = None
        
    return {
            "type": "CellET",
            "id": uid(obj),
            "label": label,
            "etType": str(rae_type(obj)),
            "color": et_color(rae_type(obj)) #TODOOOO
        }

def make_individual_value(obj):
   if is_a(obj, Entity): return make_cellet_value(obj)

   types = {AET.String: "CellString", AET.Float: "CellFloat", AET.Bool: "CellBoolean", AET.Int: "CellInt"}
      
   return {
      "type": types.get(rae_type(obj), "CellZef"),
      "id": str(uid(obj)),
      "value": obj | attempt[value][str(rae_type(obj))] | collect ,
   }

def make_cell(obj, rt):
   trgts = obj | Outs[rt] | collect
   if not trgts: 
      return None
   elif len(trgts) == 1:
      return make_individual_value(trgts[0])
   else:
      return {
         "type": "CellList",
         "id": f"{uid(obj)}_{rt}_{len(trgts)}",
         "value": [make_individual_value(obj) for obj in trgts]
      }

def make_row(obj, connected_rels):
   return {
        "id": str(uid(obj)),
        "cells": [make_cell(obj, rt) for rt in connected_rels]
   }


@func
def make_column(column):
   return {
      "header": str(column)
   }

def make_columns(g, entity_type):
   # TODO Add sorting of columns
   z = delegate_of(entity_type, g)
   connected_rels = set(z | out_rels[RT] | map[rae_type] | collect)
   return connected_rels, connected_rels | map[make_column] | collect

def make_table_return(g, entity_type, entities):
   connected_rels, columns = make_columns(g, entity_type)
   return {
      # 'id': f"{graph_id}_{entity_type}",
      'columns': columns,
      'rows': [make_row(obj, connected_rels) for obj in entities],
   }

@func
def entity_table(query_args):
    graph_id = query_args.get('graphID', None)
    et_type  = query_args.get('entityType', None)
    if not graph_id or not et_type: return None

    limit  = query_args.get('limit', 10)
    try:
        et_type = ET(et_type)
        g = Graph(graph_id)
        return make_table_return(g, et_type, list(g | now |  all[et_type] | take[limit] | collect))
    except Exception as e:
        log.error(f"Error in entityTable: {e}")
        return None


@func
def cell_interface_resolver(obj, *_):
   return obj.get('type', "CellString")


@func
def assign_value_general(query_args, aet_type):
    graphID = query_args['ids']['graphID']
    aetID = query_args['ids']['aetID']

    g = Graph(graphID)
    aet = g[aetID]

    if not is_a(aet, AttributeEntity): 
      log.error(f"Cannot assign value to {aetID} as it is not an AET")
      return False 

    if not is_a(aet, aet_type): 
      log.error(f"Cannot assign value to {aetID} as it is not an {aet_type}")
      return False

    try:
      aet_value = query_args['value']
      aet | assign[aet_value] | g | run
      
      return True
    except Exception as e:
      log.error(f"Failed to assign value to {aetID}. {e}")
      return False

@func
def assign_value_string(query_args):
    return assign_value_general(query_args, AET.String)

@func
def assign_value_float(query_args):
    return assign_value_general(query_args, AET.Float)

@func
def assign_value_int(query_args):
    return assign_value_general(query_args, AET.Int)

@func
def assign_value_bool(query_args):
    return assign_value_general(query_args, AET.Bool)


def value_of_aet_at_tx(aet, tx) -> str:
    try:
        if tx is now:
            tx = Graph(aet) | now | collect
        if BT(aet) == BT.ATTRIBUTE_ENTITY_NODE:
            val = aet | value[tx] | collect 
            return '' if val is None else str(val)
        else:
            return ''
    except Exception:
        return ''

@func
def atom_events(zr):
    # graph_id = query_args.get('graphID', None)
    # g = Graph(graph_id)
    # rae_id  = query_args.get('raeID', None)
    # zr      = g[rae_id]

    @func
    def events_for_rt(rt):
        zr_or_uzr = target(rt)
        uzr = to_ezefref(zr_or_uzr)

        low_lvl_mapping = {
            BT.ATOMIC_VALUE_ASSIGNMENT_EDGE: "ValueAssignedEvent",
            BT.ATTRIBUTE_VALUE_ASSIGNMENT_EDGE: "ValueAssignedEvent",
            BT.INSTANTIATION_EDGE: "InstantiatedEvent",
            BT.TERMINATION_EDGE: "TerminatedEvent",
        }

        def construct_event(edge):
            value_maybe = ""
            if BT(edge) in {BT.ATOMIC_VALUE_ASSIGNMENT_EDGE, BT.ATTRIBUTE_VALUE_ASSIGNMENT_EDGE}: 
                value_maybe = value_of_aet_at_tx(uzr, edge | source | collect)
            
            uzr_tx = source(edge)
            transaction = create_tx(uzr_tx)
            return create_event(low_lvl_mapping[BT(edge)], str(rae_type(rt)), transaction, value_maybe)

        rel_ent_inst_edge = uzr | in_rel[BT.RAE_INSTANCE_EDGE]
        return rel_ent_inst_edge | in_rels[BT] | map[construct_event] | collect
    
    return zr | out_rels | map[events_for_rt] | concat | sort[lambda d: d['transaction']['txTimestamp']] | reverse | collect


def create_tx(tx):
    return {
        "id": str(uid(tx)),
        "txTimestamp": time(tx).seconds_since_1970,
        "txIndex": graph_slice_index(to_graph_slice(tx)),
    }

def create_event(event_type, field_name, transaction, assignedValue=""):
    return { 
        "type": event_type,
        "transaction": transaction,
        "fieldName": field_name,
        "value": assignedValue,
    }
@func
def event_interface_resolver(obj, *_):
   return obj.get('type', "InstantiatedEvent")


def create_field(atom, field_rt):
    return {
        # "id": str(uid(field_rt)),
        "name": str(field_rt),
        "value": make_cell(atom, field_rt),
        "isEditable": False,
    }

@func
def atom_fields(zr):
    unique_out_fields =  zr | out_rels[RT] | map[rae_type]  | func[set] | collect
    return unique_out_fields | map[lambda rt: create_field(zr, rt)] | collect


@func
def get_atom(query_args):
    atom_id  = query_args.get('atomID', None)
    if not atom_id: return None

    try:
        g_id = BaseUID(atom_id[-16:])
        g = Graph(g_id)
        atom = g[atom_id]
        atom_type = rae_type(atom)
    except Exception as e:
        log.error(f"Failed to get atom {atom_id}. {e}")
        return None

    return {
        "id": atom_id,
        "color": et_color(atom_type),
        "atomType": str(atom_type),
        "fields": atom_fields(atom),
        "events": atom_events(atom),
    }


def et_color(et):
    from zef.core.colors import colors
    idx =  hash(et) % len(colors)
    return list(colors.values())[idx].upper()


@func
def terminate_field(query_args):
    try:
        field_id  = query_args.get('fieldID')
        g_id = BaseUID(field_id[-16:])
        g = Graph(g_id)

        field = g[field_id]
        field | terminate | g | run

        return True
    except Exception as e:
        log.error(f"Failed to terminate field {query_args}. {e}")
        return False

@func
def add_field(query_args):
    try:
        source_id  = query_args.get('sourceID')
        relation_name  = query_args.get('relationName')
        value  = query_args.get('value')

        g_id = BaseUID(source_id[-16:])
        g = Graph(g_id)

        res = (g[source_id], RT(relation_name), value) | g | run
        return str(uid(res[-1]))
    except Exception as e:
        log.error(f"Failed to add field {query_args}. {e}")
        raise e

#-------------------------------------------------------------
#-------------------Schema String-------------------------------
#-------------------------------------------------------------
simple_schema = """
type Query {
    graphs: [Graph]
    entityTypes(graphID: ID!): [String]
    entityTable(graphID: ID!, entityType: String!, limit: Int): Table
    atom(atomID: ID!): Atom
}

type Mutation {
    assignValueString(ids: AssignValueIDs!, value: String!): Boolean
    assignValueFloat( ids: AssignValueIDs!, value: Float!): Boolean
    assignValueInt(   ids: AssignValueIDs!, value: Int!): Boolean
    assignValueBool(  ids: AssignValueIDs!, value: Boolean!): Boolean
    terminateField(fieldID: ID!): Boolean
    addFieldString(sourceID: ID!, relationName: String!, value: String!): ID
    addFieldFloat(sourceID: ID!, relationName: Float!, value: Float!): ID
    addFieldInt(sourceID: ID!, relationName: Int!, value: Int!): ID
    addFieldBool(sourceID: ID!, relationName: Boolean!, value: Boolean!): ID
}

input AssignValueIDs {
    graphID: ID!
    aetID: ID!
}

type Atom {
	id: ID!
    color: String
	atomType: String    
	fields:  [Field]
	events: [Event]
}

type Field {
	name: String
	value: Cell
	isEditable: Boolean
}

type Graph {
    id: ID!
    labels: [String]
}

type Table {
columns: [Column]
rows: [Row]
}

type Column {
header: String
}

type Row {
id: ID!
cells: [Cell]
}

interface Cell {
id: ID!
}

type CellInt implements Cell{
id: ID!
value: Int
}

type CellString implements Cell{
id: ID!
value: String
}

type CellFloat implements Cell{
id: ID!
value: Float
}

type CellBoolean implements Cell{
id: ID!
value: Boolean
}

type CellET implements Cell{
id: ID!
label: String
etType: String
color: String
}

type CellZef implements Cell{
id: ID!
value: String
}

type CellList implements Cell{
id: ID!
value: [Cell]
}

type Transaction {
    id: ID!
    txTimestamp: Float!
    txIndex: Int!
}

interface Event {
    transaction: Transaction!
    fieldName: String!
}

type InstantiatedEvent implements Event {
    transaction: Transaction!
    fieldName: String!
}

type TerminatedEvent implements Event {
    transaction: Transaction!
    fieldName: String!
}

type ValueAssignedEvent implements Event {
    transaction: Transaction!
    fieldName: String!
    value: String
}
"""

def create_schema_dict(simple_schema):
   from ...graphql import generate_schema_dict, fill_types_default_resolvers

    # Step 1: Create a Schema Dict "Data Structure" from the GraphQL Schema String
   schema_dict = generate_schema_dict(simple_schema)

   # Step 2 (Optional): Fill in the default resolvers for the types
   schema_dict = fill_types_default_resolvers(schema_dict, lambda field_name: get[field_name])

   # Step 3: Add the resolvers for the fields
   schema_dict = (
      schema_dict 
      | insert_in[('_Types', 'Query', 'graphs', 'resolver')][graphs] 
      | insert_in[('_Types', 'Query', 'entityTypes', 'resolver')][entity_types] 
      | insert_in[('_Types', 'Query', 'entityTable', 'resolver')][entity_table] 
      | insert_in[('_Types', 'Query', 'atom', 'resolver')][get_atom]
      | insert_in[('_Types', 'Mutation', 'assignValueString', 'resolver')][assign_value_string]
      | insert_in[('_Types', 'Mutation', 'assignValueFloat', 'resolver')][assign_value_float]
      | insert_in[('_Types', 'Mutation', 'assignValueInt', 'resolver')][assign_value_int]
      | insert_in[('_Types', 'Mutation', 'assignValueBool', 'resolver')][assign_value_bool]
      | insert_in[('_Types', 'Mutation', 'terminateField', 'resolver')][terminate_field]
      | insert_in[('_Types', 'Mutation', 'addFieldString', 'resolver')][add_field]
      | insert_in[('_Types', 'Mutation', 'addFieldFloat', 'resolver')][add_field]
      | insert_in[('_Types', 'Mutation', 'addFieldInt', 'resolver')][add_field]
      | insert_in[('_Types', 'Mutation', 'addFieldBool', 'resolver')][add_field]
      | insert_in[('_Interfaces', 'Cell', '_interface_resolver')][cell_interface_resolver] 
      | insert_in[('_Interfaces', 'Event', '_interface_resolver')][event_interface_resolver] 
      | collect
   )
   return schema_dict

def studio_start_handler(eff: Dict):
   """Example
   {
      "type": FX.Studio.Start,
      "report_errors": True, # Optional
   } | run
   """
   report_errors = eff.get("report_errors", False)
   report_errors = "&report_errors=true" if report_errors else ""

   def open_browser(port):
      studio_url = f"https://studio.zefhub.io/?endpoint=http://localhost:{port}/graphql{report_errors}"
      log.info(f"Started Zef Studio at {studio_url}")
      import webbrowser
      webbrowser.open(studio_url)

   import random
   from .. import internals
   g_process = internals.get_local_process_graph()

   schema_dict = create_schema_dict(simple_schema)

   trials = 5
   while trials:
      random_port = random.randint(10000, 30000)
      try:
         http_r = FX.GraphQL.StartServer(**{
            "schema_dict" : schema_dict,
            "db" :  Graph(),
            "port" :  random_port, 
             "path" :  "/graphql", 
         }) | run

         server_zr = now(g_process[http_r['server_uuid']])
         
         ET.ZefFXService(**{
            "type": Val(eff['type']),
            'owns': server_zr,
         }) | g_process | run # TODO Perform that in a flat way by directly calling into the FX handler

         open_browser(random_port)
         return http_r
      except Exception as e:
         trials -= 1
         if trials: continue
         raise e


def studio_stop_server_handler(eff: Dict):
    return run(FX.HTTP.StopServer(**eff))

