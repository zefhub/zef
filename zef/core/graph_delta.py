__all__ = [
    "GraphDelta",
]
from typing import Any, Tuple, Callable
from functools import partial as P

from .. import pyzef
from ._core import *
from . import internals
from .zef_functions import func
from ._ops import *
from .op_structs import ZefOp, LazyValue
from .graph_slice import GraphSlice
from .abstract_raes import Entity, Relation, AtomicEntity

from abc import ABC
class ListOrTuple(ABC):
    pass
for t in [list, tuple]:
    ListOrTuple.register(t)

scalar_types = {int, float, bool, str, Time, QuantityFloat, QuantityInt, ZefEnumValue}

def make_enum_aet(x):
    """ hacky work around function for now:
    e.g. given an enum value "EN.Color.White"
    we want to convert to the AET type expression: "AET.Enum.Color"
    """
    enum_typename: str = x.enum_type
    return getattr(AET.Enum, enum_typename)

def make_qf_aet(x):
    """ hacky work around function for now:
    e.g. given an enum value "QuantityFloat(2.1, EN.Unit.kilogram)"
    we want to convert to the AET type expression: "AET.QuantityFloat.kilogram"
    """
    quantity_unit: str = x.unit.enum_value
    return getattr(AET.QuantityFloat, quantity_unit)

def make_qi_aet(x):
    """ hacky work around function for now:
    e.g. given an enum value "QuantityInt(2.1, EN.Unit.kilogram)"
    we want to convert to the AET type expression: "AET.QuantityInt.kilogram"
    """
    quantity_unit: str = x.unit.enum_value
    return getattr(AET.QuantityInt, quantity_unit)

map_scalar_to_aet_type = {
    int:                lambda x: AET.Int,
    float:              lambda x: AET.Float,
    bool:               lambda x: AET.Bool,
    str:                lambda x: AET.String,
    Time:               lambda x: AET.Time,
    ZefEnumValue:       make_enum_aet,
    QuantityFloat:      make_qf_aet, 
    QuantityInt:        make_qi_aet, 
    }

def get_curried_arg(op, n, arg_indx=0):
    """ 
    utility function to get 
    e.g. get_curried_arg(Z['id1'][42], 0)       # => 'id1'
    e.g. get_curried_arg(Z['id1'][42], 1)       # => 42
    """
    return op.el_ops[arg_indx][1][n]



def most_recent_rae_on_graph(origin_uid: str, g: Graph)->ZefRef:
    """
    Will definitely not return a BT.ForeignInstance, always an instance.
    It could be that the node asked for has its origin on this graph 
    (the original rae may still be alive or it may be terminated)

    Args:
        origin_uid (str): the uid of the origin rae we are looking for
        g (Graph): on which graph are we looking?

    Returns:
        ZefRef: this graph knows about this: found instance
        None: this graph knows nothing about this RAE
    """
    if origin_uid not in g:
        return None     # this graph never knew about a RAE with this origin uid

    zz = g[origin_uid]
    if BT(zz) in {BT.FOREIGN_ENTITY_NODE, BT.FOREIGN_ATOMIC_ENTITY_NODE, BT.FOREIGN_RELATION_EDGE}:
        from .graph_slice import get_instance_rae
        return get_instance_rae(origin_uid, now(g))
        
    elif BT(zz) in {BT.ENTITY_NODE, BT.ATOMIC_ENTITY_NODE, BT.RELATION_EDGE}:
        if zz | exists_at[now(g)] | collect:
            return zz | in_frame[now(g)] | collect
        else:
            return None
    else:
        raise RuntimeError("Unexpected option in most_recent_rae_on_graph")
        









def perform_transaction_gdelta(g_delta, g: Graph):
    d_raes = {}  # keep track of instantiated RAEs with temp ids            
    try:
        with Transaction(g) as tx_now:
            # TODO: Have to change the behavior of Transaction(g) later I suspect
            frame_now = GraphSlice(tx_now)
            d_raes['tx'] = tx_now
            for i,cmd in enumerate(g_delta.commands):
                # print(f"{i}/{len(g_delta.commands)}: {g.graph_data.write_head * 16 / 1024 / 1024} MB")
                if cmd['cmd'] == 'instantiate' and type(cmd['rae_type']) in {EntityType, AtomicEntityType}:
                    if 'internal_id' in cmd:
                        d_raes[cmd['internal_id']] = instantiate(cmd['rae_type'], g) #| to_ezefref
                    else:
                        instantiate(cmd['rae_type'], g)
                
                elif cmd['cmd'] == 'instantiate' and type(cmd['rae_type']) in {RelationType}:
                    if 'internal_id' in cmd:
                        d_raes[cmd['internal_id']] = instantiate(to_ezefref(d_raes[cmd['source']]), cmd['rae_type'], to_ezefref(d_raes[cmd['target']]), g) | in_frame[frame_now] | collect
                    else:
                        instantiate(to_ezefref(d_raes[cmd['source']]), cmd['rae_type'], to_ezefref(d_raes[cmd['target']]), g) | in_frame[frame_now] | collect
                
                elif cmd['cmd'] == 'assign_value':
                    this_id = cmd['internal_id']
                    z = d_raes.get(this_id, None)
                    if z is None:
                        z = most_recent_rae_on_graph(this_id, g)
                    assert z is not None
                    if cmd['value'] is not  None:
                        if z | value | collect != cmd['value']:
                            # print("Assigning value of ", cmd['value'], "to a", AET(z))
                            internals.assign_value_imp(z, cmd['value'])
                    if 'internal_id' in cmd:
                        d_raes[cmd['internal_id']] = now(z)                            
                    
                elif cmd['cmd'] == 'merge':
                    # It is either an instance (with an 'origin_rae_uid' specified)
                    # or a delegate[...]
                    if is_a(cmd['origin_rae'], Delegate):
                        d = cmd['origin_rae']
                        z = internals.delegate_to_ezr(d, g, True, 0)
                        d_raes[d] = z
                        if 'internal_id' in cmd:
                            d_raes[cmd['internal_id']] = z
                    else:
                        candidate = most_recent_rae_on_graph(uid(cmd['origin_rae']), g)
                        if candidate is not None:
                            # this is already on the graph. Just assert and move on
                            assert rae_type(cmd['origin_rae']) == rae_type(candidate)
                            zz = candidate
                        else:
                            origin_rae_uid = uid(cmd['origin_rae'])
                            if isinstance(cmd['origin_rae'], Entity):
                                zz = internals.merge_entity_(
                                    g, 
                                    rae_type(cmd['origin_rae']), 
                                    origin_rae_uid.blob_uid,
                                    origin_rae_uid.graph_uid,
                                )
                            elif isinstance(cmd['origin_rae'], AtomicEntity):
                                zz = internals.merge_atomic_entity_(
                                    g, 
                                    rae_type(cmd['origin_rae']),
                                    origin_rae_uid.blob_uid,
                                    origin_rae_uid.graph_uid,
                                )
                            elif isinstance(cmd['origin_rae'], Relation):
                                src_origin_uid,_,trg_origin_uid = cmd['origin_rae'].d["uids"]
                                z_src = d_raes.get(src_origin_uid, most_recent_rae_on_graph(src_origin_uid, g))                                    
                                z_trg = d_raes.get(trg_origin_uid, most_recent_rae_on_graph(trg_origin_uid, g))                                    

                                assert z_src is not None
                                assert z_trg is not None                                    
                                zz = internals.merge_relation_(
                                    g, 
                                    rae_type(cmd['origin_rae']),
                                    to_ezefref(z_src),
                                    to_ezefref(z_trg),
                                    origin_rae_uid.blob_uid,
                                    origin_rae_uid.graph_uid,
                                )
                            else:
                                raise NotImplementedError
                        if 'internal_id' in cmd:
                            d_raes[cmd['internal_id']] = now(zz)                        
                        if 'origin_rae' in cmd:
                            d_raes[uid(cmd['origin_rae'])] = now(zz)
                
                elif cmd['cmd'] == 'terminate':
                    if uid(cmd['origin_rae']) not in g:
                        # raise KeyError(f"terminate {cmd['origin_rae']} called, but this RAE is not know to graph {uid(g)}")
                        # This is no longer an error, it should be ignored silently.
                        if 'internal_id' in cmd:
                            d_raes[cmd['internal_id']] = None
                    else:
                        zz = most_recent_rae_on_graph(uid(cmd['origin_rae']), g)
                        if zz is not None:
                            from ..pyzef import zefops as pyzefops
                            pyzefops.terminate(zz)
                        if 'internal_id' in cmd:
                            d_raes[cmd['internal_id']] = now(zz, allow_tombstone)
                elif cmd['cmd'] == 'tag':
                    if 'origin_rae' in cmd:
                        if uid(cmd['origin_rae']) not in g:
                            raise KeyError(f"tag {cmd['origin_rae']} called, but this RAE is not know to graph {g|uid}")
                        zz = most_recent_rae_on_graph(uid(cmd['origin_rae']), g)
                    else:
                        zz = d_raes[cmd['internal_id']]
                    # Go directly to the tagging instead of through another effect
                    pyzef.main.tag(zz, cmd['tag_name'], cmd['force'])
                else:
                    raise RuntimeError(f'---------Unexpected case in performing graph delta tx: {cmd}')

    except Exception as exc:        
        raise RuntimeError(f"Error executing graph delta transaction {exc=}") from exc
        
    return d_raes    # the transaction receipt






def perform_transaction(x, g: Graph):
    """
    x can be 
        1) a GraphDelta
        2) a list/tuple of GraphDeltas
        3) s horthand type for txs: single ET/AET/triple/ list thereof
        
    if a tuple of GraphDelta is piped in, perform all of them within the same transaction."""
    if isinstance(x, GraphDelta):
        return perform_transaction_gdelta(x, g)
    elif isinstance(x, list) or isinstance(x, tuple):
        with Transaction(g):
            res = tuple((perform_transaction_gdelta(el, g) for el in x))
        return res
    else:
        raise TypeError(f'Did not know how to perform tx for {type(x)}')







def verify_assign_values(assign_v, id_definitions):
    new_value = assign_v['new_value']

    # This function is called from 2 different locations. First inside the AssignValue_ constructor where we are able to cast a ZefRef to its rae_type
    # Secondly, inside the GraphDelta constructor where we check against uid inside the id_definitions dict.
    if "zr_type" in assign_v:
        type_of_assignedto = assign_v['zr_type']
    else:
        if assign_v['some_id'] in id_definitions: type_of_assignedto = id_definitions[assign_v['some_id']]
        else: type_of_assignedto = None

    # This comes first as _eq_ for ZefRef throws in the next if statement
    if isinstance(new_value, ZefRef) or isinstance(new_value, EZefRef) or isinstance(new_value, ZefRefs) or isinstance(new_value, EZefRefs):
        raise RuntimeError(f'The passed value can\'t be of type Zefref(s) or EZefRef(s)')

    # If no value was passed
    if new_value == None:
        raise RuntimeError(f'{assign_v} is missing a value to be assigned.')

    # If type of the value passed isn't part of this set of allowed types
    if type(new_value) in scalar_types:
        raise RuntimeError(f'The type of the value passed in {assign_v} isn\'t of the allowed types: str, int, float, bool, Time, QuantityFloat, QuantityInt, ZefEnumValue')

    if type_of_assignedto: 
        type_map = {
            str:           AET.String,
            ZefEnumValue:  AET.Enum,
            QuantityFloat: AET.QuantityFloat, 
            QuantityInt:   AET.QuantityInt, 
            Time:          AET.Time,
        }
        # If type of the rae that is being assigned isn't of an AET type i.e assigning a str to an ET.Cat
        if type_of_assignedto not in {AET.Int, AET.Float, AET.Bool, AET.String, AET.Enum, AET.QuantityFloat, AET.QuantityInt, AET.Time}:
            raise RuntimeError(f'The type_of_assignedto={type_of_assignedto} must be of the allowed types: AET.Int, AET.Float, AET.Bool, AET.String, AET.Enum, AET.QuantityFloat, AET.QuantityInt, AET.Time')

        # Verify cast-ability
        elif type(new_value) == int:
            if type_of_assignedto not in {AET.Int, AET.Float, AET.Bool}:
                raise RuntimeError(f'Can only assign values of type int to only AET.Int, AET.Float, AET.Bool')
            if type_of_assignedto == AET.Bool and new_value not in [0,1]:
                raise RuntimeError(f'Can\'t assign int value that isn\'t 0 or 1 to AET.Bool')
            
        elif type(new_value) == float:
            if type_of_assignedto not in {AET.Int, AET.Float}:
                raise RuntimeError(f'Can only assign values of type float to only AET.Int, AET.Float')
            import math
            if type_of_assignedto == AET.Int and math.fabs(new_value - round(new_value)) > 1e-8:
                raise RuntimeError(f'Can only assign values of type float to int if the double is numerically sufficiently close to make rounding safe.')

        elif type(new_value) == bool:    
            if type_of_assignedto not in {AET.Int, AET.Bool}:
                raise RuntimeError(f'Can only assign values of type bool to only AET.Int, AET.Bool')
            if type_of_assignedto == AET.Int and new_value not in [False, True]:
                raise RuntimeError(f'Can\'t assign bool value that isn\'t True or False to AET.Int')

        # These are only castable one-to-one
        elif type_map[type(new_value)] != type_of_assignedto:
            raise RuntimeError(f'Can\'t assign value of type {type(new_value)} to an AET of type {type_of_assignedto}')























def _on_single_node(x, merged_ids: set, gen_id):
    iid,actions = realise_single_node(x, gen_id)
    return actions, [], ()


def _on_instantiated(x: dict, merged_ids: set):
    internal_id = x.el_ops[0][1][1]
    raet = x.el_ops[0][1][0]
    cmds = []
    if isinstance(raet, QuantityFloat) or isinstance(raet, QuantityInt):
        # This duplicates the logic in realise_single_node a bit here
        val = raet
        if isinstance(val, QuantityFloat):
            raet = getattr(AET.QuantityFloat, val.unit.enum_value)
        else:
            raet = getattr(AET.QuantityInt, val.unit.enum_value)
        exprs += [Z[internal_id] <= val]#{'cmd': 'assign_value', 'value': val, 'explicit': True, 'internal_id': internal_id}]

    cmds += [{'cmd': 'instantiate', 'rae_type': raet, 'internal_id': internal_id}]
    return (), cmds, (internal_id, )

def _on_merged(x: dict, merged_ids: set):
    obj = x.el_ops[0][1][0]

    origin = origin_rae(obj)
    if uid(origin) in merged_ids:
        return (), (), ()     

    base_cmd = {"cmd": "merge",
                "origin_rae": origin}
    if len(x.el_ops[0][1]) == 2:
        base_cmd["internal_id"] = x.el_ops[0][1][1]
    else:
        assert len(x.el_ops[0][1]) == 1

    if is_a(obj, ZefRef) or is_a(obj, EZefRef):
        if BT(obj) == BT.ENTITY_NODE:
            return (), [base_cmd], (uid(origin), )

        elif BT(obj) == BT.ATOMIC_ENTITY_NODE:
            assign_val_cmds = [] if isinstance(obj, EZefRef) else [{
                'cmd': 'assign_value', 
                'value': obj | value | collect,
                'internal_id': uid(origin),
                'explicit': False,
                }]

            return (), [base_cmd] + assign_val_cmds, (uid(origin), )

        elif BT(obj) == BT.RELATION_EDGE:
            return (
                (obj | source | to_ezefref | collect, obj | target | to_ezefref | collect),
                [base_cmd],
                (uid(origin), )
            )
        else:
            raise NotImplementedError(f"Unknown ZefRef type for merging: {BT(obj)}")
    elif is_a(obj, Entity) or is_a(obj, AtomicEntity):
        return (), [base_cmd], (uid(origin), )
    elif is_a(obj, Relation):
                return (
                    (obj | source | collect, obj | target | collect),
                    [base_cmd],
                    (uid(origin), )
                )
    else:
        raise NotImplementedError(f"Unknown type for merged[]: {type(obj)}")
        
    
def _on_tag(x, merged_ids: set):
    target = x.el_ops[0][1][0]
    cmd = {'cmd': 'tag',
           'tag_name': x.el_ops[0][1][1],
           'force': x.el_ops[0][1][2] if len(x.el_ops[0][1]) >= 3 else False}
    if is_a(target, ZefOp) and is_a(target, Z):
        cmd['internal_id'] = target.el_ops[0][1][0]
    elif is_a(target, ZefRef) or is_a(target, EZefRef):
        cmd['origin_rae'] = origin_rae(target)
    return (), [cmd], ()
    


def _make_on_assign_value(generate_id: Callable)->Callable:
    """make this a higher order function that we can pass the id generator fct with state"""
    def _on_assign_value(x, merged_ids: set):
        # id_dict = {'uid': x.some_id} if zefdb.internals.is_any_UID(x.some_id) else {'internal_id': x.some_id}
        if isinstance(x, ZefOp):
            # This should only be possible if the first item is a `Z["..."]`
            assert x.zef_ops[0][0] == RT.Z

            # Convert this to a LazyValue by wrapping the Z part.
            lv = LazyValue(x.zef_ops[0])

            x = lv | ZefOp(x.zef_ops[1:])

        assert isinstance(x, LazyValue)

        # Get the evaluated LazyValue, this should return a LazyValue with only an assign_value in it.
        x = collect(x)
        assert x.el_ops.el_ops[0][0] == RT.AssignValue
        val = x.el_ops.el_ops[0][1][0]
        z_origin = x.initial_val
        iid,actions = realise_single_node(x.initial_val, generate_id)
            
        return actions, [{'cmd': 'assign_value', 'value': val, 'explicit': True, "internal_id": iid}], ()
    
    return _on_assign_value            
            
            
            
            
                

    

def _on_terminate(x, merged_ids: set):                            
    params = x.el_ops[0][1]
    z = params[0]
    cmd = {
        'cmd': 'terminate', 
        'origin_rae': origin_rae(z)
        }
    if len(params) >= 2:
        cmd['internal_id'] = params[1]
    if len(params) >= 3:
        raise Exception("Should have a maximum of two arguments to terminate")
    return (), [cmd], ()
    

def _on_delegate(x, merged_ids: set):                            
    return (), [{
        'cmd': 'merge', 
        'origin_rae': x,
        }], ()



def _make_on_tuple(generate_id: Callable)->Callable:
    
    
    def _on_tuple(x: tuple, merged_ids: set):
        new_exprs = []
        new_cmds = []
        new_ids = []

        if not is_valid_relation_template(x):
            raise Exception("Not allowing list/tuple nested in GraphDelta unless it belongs to a relation")

        if len(x) == 3:
            # NOTE: new_ids is only for things that have been handled by a
            # merge *CMD* not expr. The function realise_single_node will
            # turn an expr into one with an explicit id attached, so it is
            # not necessary to save the ids away in new_ids. Hence, all of
            # the commented out new_ids lines below.
            if isinstance(x[0], ListOrTuple) and isinstance(x[2], ListOrTuple):
                # Case 4 of is_valid_relation_template
                new_exprs += [(source, x[1], target) for source in x[0] for target in x[2]]
            elif isinstance(x[0], ListOrTuple):
                # Case 3 of is_valid_relation_template
                iid,actions = realise_single_node(x[2], generate_id)
                new_exprs += actions
                # new_ids += [iid]
                new_exprs += [(source, x[1], Z[iid]) for source in x[0]]
            elif isinstance(x[2], list):
                # Case 2 of is_valid_relation_template
                iid,actions = realise_single_node(x[0], generate_id)
                new_exprs += actions
                # new_ids += [iid]
                new_exprs += [(Z[iid], x[1], target) for target in x[2]]
            else:
                # Case 1 of is_valid_relation_template
                # it's a plain relation triple
                src_id, src_exprs = realise_single_node(x[0], generate_id)
                trg_id, trg_exprs = realise_single_node(x[2], generate_id)

                new_exprs += src_exprs
                new_exprs += trg_exprs
                # new_ids += [src_id, trg_id]

                # This is the only time the relation is created
                rel = x[1]
                if isinstance(rel, RelationType):
                    cmd1 = {'cmd': 'instantiate', 'rae_type': rel}
                elif isinstance(rel, ZefOp):
                    cmd1 = {'cmd': 'instantiate',
                            'rae_type': rel.el_ops[0][1][0],
                            'internal_id': rel.el_ops[0][1][1]}
                    new_ids += [cmd1['internal_id']]
                cmd1['source'] = src_id
                cmd1['target'] = trg_id

                new_cmds += [cmd1]

        elif len(x) == 2:
            # Case 5 of is_valid_relation_template
            iid,actions = realise_single_node(x[0], generate_id)
            new_exprs += actions
            new_ids += [iid]
            new_exprs += [(Z[iid], item[0], item[1]) for item in x[1]]
        else:
            raise Exception("Shouldn't get here")

        return new_exprs, new_cmds, new_ids

    return _on_tuple











def make_iteration_step(generate_id: Callable)->Callable:
    _on_assign_value = _make_on_assign_value(generate_id)

    def iteration_step(state: dict)->dict:
        """[summary]

        Args:
            state (dict): of form 
            {
                'user_expressions': elements,
                'commands': (),
                'ids_present': set(),       # keep track of uids merged in
            }

        Returns:
            dict: [description]
        """
        
        exprs = state['user_expressions']
        if len(exprs) == 0:
            return state
        # expr = exprs[0]
        if not isinstance(exprs, list):
            exprs = list(exprs)
        expr = exprs.pop()
        
        def get_handler(exprr):
            if isinstance(exprr, LazyValue):
                return _on_assign_value

            elif isinstance(exprr, ZefOp):
                if is_a(exprr, terminate):
                    return _on_terminate
                elif is_a(exprr, assign_value):
                    return _on_assign_value
                elif is_a(exprr, instantiated):
                    return _on_instantiated
                elif is_a(exprr, merged):
                    return _on_merged
                elif is_a(exprr, tag):
                    return _on_tag
                
                raise RuntimeError('We should not have landed here, with exprr={exprr}')
            if is_a(exprr, Delegate):
                return _on_delegate                
        
            _on_single_node_P = P(_on_single_node, gen_id=generate_id)
            d_dispatch = {
                EntityType: _on_single_node_P,
                AtomicEntityType: _on_single_node_P,
                ZefRef: _on_single_node_P,
                EZefRef: _on_single_node_P,
                Entity: _on_single_node_P,
                AtomicEntity: _on_single_node_P,
                Relation: _on_single_node_P,
                tuple: _make_on_tuple(generate_id),
                list: _make_on_tuple(generate_id),
            }
            if type(exprr) not in d_dispatch:
                raise TypeError(f"transform_to_commands was called for type {type(exprr)} value={exprr}, but no handler in dispatch dict")            
            return d_dispatch[type(exprr)]
        
        handler_fct = get_handler(expr)
        new_exprs, new_cmds, new_ids = handler_fct(expr, state['ids_present'])
        if False:
            print("Iter step============")
            print(expr)
            print(new_exprs)
            print(new_cmds)
            print(new_ids)
            print("======")

        # Here are three ways to accomplish the same thing, although the last
        # (and fastest) mutates.
        
        # return {
        #         'user_expressions': (*exprs[1:], *new_exprs),
        #         'commands': (*state['commands'], *new_cmds),                 # for now this will be a dict to allow quick lookup: the key is the uid / internal id
        #         'ids_present': {*state['ids_present'], *new_ids}
        #     }

        # return {
        #         'user_expressions': exprs[1:] + list(new_exprs),
        #         'commands': state['commands'] + list(new_cmds),
        #         'ids_present': state['ids_present'] | set(new_ids)
        #     }

        exprs.extend(new_exprs)
        cmds = state["commands"]
        cmds.extend(new_cmds)
        ids_present = state['ids_present']
        ids_present.update(new_ids)
        return {
                'user_expressions': exprs,
                'commands': cmds,
                'ids_present': ids_present,
        }
    
    return iteration_step




generated_prefix = "tmp_id_"
def make_generate_id():
    auto_id_counter = 0
    def generate_id():
        """count up"""
        nonlocal auto_id_counter
        auto_id_counter += 1
        return f"{generated_prefix}{auto_id_counter}"
    return generate_id

def is_generated_id(x):
    if not isinstance(x, str):
        return False
    return x.startswith(generated_prefix)






def command_ordering_by_type(d_raes: dict) -> int:
    """we want some standardized order of the output to simplify value-based
        comparisons and other operations for graph deltas"""
        
    if d_raes['cmd'] == 'merge':
        if isinstance(d_raes['origin_rae'], Relation): return 0.5
        else: return 0
    if d_raes['cmd'] == 'instantiate':
        if isinstance(d_raes['rae_type'], EntityType): return 1
        if isinstance(d_raes['rae_type'], AtomicEntityType): return 2
        if isinstance(d_raes['rae_type'], RelationType): return 3
        return 4                                            # there may be {'cmd': 'instantiate', 'rae_type': AET.Bool}
    if d_raes['cmd'] == 'assign_value': return 5
    if d_raes['cmd'] == 'terminate' and isinstance(d_raes['origin_rae'], Relation): return 6
    if d_raes['cmd'] == 'terminate' and isinstance(d_raes['origin_rae'], Entity): return 7
    if d_raes['cmd'] == 'terminate' and isinstance(d_raes['origin_rae'], AtomicEntity): return 8
    if d_raes['cmd'] == 'terminate' and is_a(d_raes['origin_rae'], Delegate): return 9
    if d_raes['cmd'] == 'tag': return 10
    else: raise NotImplementedError(f"In Sort fct for {d_raes}")

def get_id(cmd):
    if 'internal_id' in cmd:
        return cmd['internal_id']
    elif 'origin_rae' in cmd:
        if is_a(cmd['origin_rae'], Delegate):
            return cmd['origin_rae']
        return uid(cmd['origin_rae'])
    else:
        return None

def resolve_dag_ordering_step(arg: dict)->dict:
    # arg is "state" + "num_changed"
    state = arg["state"]
    ids = state['known_ids']        
    
    def can_be_executed(cmd):        
        if cmd['cmd'] == 'instantiate' and isinstance(cmd['rae_type'], RelationType) and (cmd['source'] not in ids or cmd['target'] not in ids):
            return False
        if cmd['cmd'] == 'merge' and isinstance(cmd['origin_rae'], Relation) and (cmd['origin_rae'].d["uids"][0] not in ids or cmd['origin_rae'].d["uids"][2] not in ids):
            return False
        if cmd['cmd'] == 'terminate' and any(get_id(cmd) == get_id(other) for other in state['input'] if other['cmd'] != 'terminate'):
            return False
        return True        
    (_,can), (_,cannot) = state['input'] | group_by[can_be_executed][[True, False]] | collect
    return {
        "state": {
            'input': tuple(cannot),
            'output': (*state['output'], *can),
            'known_ids': {*state['known_ids'], *(can | map[get_id])},
        },
        "num_changed": len(can) > 0
    }
    
        
def verify_and_compact_commands(cmds: tuple):                
    # print("Start of verify", now())
    @func
    def validate_and_compress_unique_assignment(cmds: list[dict]):
        values = cmds | map[get["value"]] | func[set] | collect
        if length(values) == 1:
            return cmds[0]
        raise ValueError(f'There may be at most one assignment commands for each AE. There were multiple for assignment to {get_id(cmds[0])!r} with values {values}')

    aes_with_explicit_assigns = (cmds
                                 | filter[lambda d: d["cmd"] == "assign_value"]
                                 | filter[lambda d: d["explicit"]]
                                 | map[lambda d: d["internal_id"]]
                                 | collect
                                 )
    def is_unnecessary_automatic_merge_assign(d):
        return (d["cmd"] == "assign_value"
                and not d["explicit"]
                and d["internal_id"] in aes_with_explicit_assigns)

    sorted_cmds = tuple(cmds 
                        | sort[command_ordering_by_type] 
                        | filter[Not[is_unnecessary_automatic_merge_assign]]
                        | collect
                        )
    # print("Did sort", now())

    sorted_cmds = (   # make sure if multiple assignment commands exist for the same AE, that their values agree
        sorted_cmds 
        | group_by[get['cmd']]
        | map[match_apply[
            # Do special things for assign_value
            (first | equals["assign_value"], second | group_by[get_id]
             | map[second | validate_and_compress_unique_assignment]),
            # Just pack things back in for other cmds
            (always[True], second)
            ]]
        | concat
        | collect
    )
    
    # print("Check multiple assign", now())

    def make_debug_output():
        num_done = 0
        next_print = now()+5*seconds
        def debug_output(x):
            nonlocal num_done, next_print
            num_done += 1
            if now() > next_print:
                print(f"Up to compacting {num_done} - {len(x['state']['input'])}, {len(x['state']['output'])}", now())
                next_print = now() + 5*seconds
        return debug_output

    state_initial = {
        'input': sorted_cmds,
        'output': (),
        'known_ids': set(),
    }
    state_final = (
        {"state": state_initial, "num_changed": -1}
        | iterate[resolve_dag_ordering_step] 
        # | tap[make_debug_output()]
        | take_until[lambda s: s["num_changed"] == 0]
        # | tap[lambda x: print("After take_until")]
        | last
        | get["state"]
        | collect
    )
    # print("Done state_final", now())
    if len(state_final['input']) > 0:
        import json
        print(json.dumps(state_final, indent=4, default=repr))
        raise NotImplementedError("Error constructing GraphDelta: instantiation order iteration did not converge. Probably there is a circular dependency in the required imperative instantiation order between inter-dependent relations. This is a valid GraphDelta in principle, but currently not implemented in zefDB.")
    ordered_cmds = state_final['output']
    return ordered_cmds




def is_a_RT(x):
    # This checks for RT.X and RT.X["x"]. It should return False for z where z is of type RT.
    if isinstance(x, ZefRef) or isinstance(x, EZefRef):
        return False
    return is_a(x, RT) or is_a(x, instantiated[RT])

def is_valid_single_node(x):
    if type(x) in scalar_types:
        return True
    if is_a(x, instantiated[QuantityFloat]) or is_a(x, instantiated[QuantityInt]):
        return True
    if isinstance(x, ZefRef) or isinstance(x, EZefRef):
        return True
    if is_a(x, Z):
        return True
    if isinstance(x, EntityType) or is_a(x, instantiated[ET]):
        return True
    if is_a_RT(x):
        return True
    if isinstance(x, AtomicEntityType) or is_a(x, instantiated[AET]):
        return True
    return False

def is_valid_relation_template(x):
    # Check for the relation templates:
    # Either:
    # 1. (a, RT.x, b)
    # 2. (a, RT.x, [b,c,d,e,...])
    # 3. ([a,b,c,...], RT.x, d)
    # 4. ([a,b,c,...], RT.x, [d,e,f,...])
    # 5. (a, [(RT.x, b), (RT.y, c)])
    # Note: tuple/list can be used interchangably
    # Note: 5. deliberately does not have its symmetric counterpart. This could be introduced later.
    if any(is_a_RT(item) for item in x):
        # Cases 1-4 above
        if len(x) != 3:
            raise Exception("A list has an RT but isn't 3 elements long")
        if not is_a_RT(x[1]):
            raise Exception("A list has an RT but it isn't in the second position")
        # Note: if there are any lists involved, we cannot have a given ID as it
        # would have to be given to multiple instantiated relations.
        if not is_a(x[1], RT) and (isinstance(x[0], ListOrTuple) or isinstance(x[2], ListOrTuple)):
            raise Exception("An RT with an internal name is not allowed with multiple sources or targets")
        return True
    elif len(x) == 2 and isinstance(x[1], ListOrTuple):
        return all(isinstance(item, ListOrTuple) and len(item) == 2 and is_a_RT(item[0]) and is_valid_single_node(item[1]) for item in x[1])
    return False


class GraphDelta:
    def __repr__(self):
        return ("GraphDelta([\n    "
            +',\n    '.join(self.commands | map[lambda d_raes: str(d_raes)])
            +"\n])")
        
    def __init__(self, elements: tuple):
        """
        create a simple serializable dict/list representation until we have FlatGraphs
        1) check that each command by itself is valid and transform into pure data: a dict
        2) check that each potential internal ID is assigned at most once
        3) check that each internal id that Z['...'] uses has been assigned as an internal id if it is not a uid
        
        ------------- FAQ -------------
        a) Why can't I specify an internal id for a merge operation?
                If you know the RAE that you want to merge, then you can just use that or its uid.
        """

        # print("Start GraphDelta init", now())
        from .internals import is_any_UID 
        id_definitions = {}   # key: str (internal id), value: type of RAE it defines
        
        def ensure_not_previously_defined(internal_id: str):
            if internal_id in id_definitions:
                raise KeyError(f"The internal id '{internal_id}' was already defined. Multiple definitions of an internal id are not allowed.")
            
        
        @func
        def verify_internal_id(x, id_definitions):
            # Note: we put this up front and early as an `isinstance` so that we
            # don't accidentally evaluate a LazyValue
            if isinstance(x, LazyValue):
                verify_internal_id(x.initial_val, id_definitions)
                return

            # ET.Person['Fred'] currently returns {'RAE': ET.Person, 'capture': 'Fred'}
            if isinstance(x, dict):
                assert 'RAE' in x
                assert 'capture' in x
                int_id = x['capture']
                assert isinstance(int_id, str)
                assert not is_any_UID(int_id)
                ensure_not_previously_defined(int_id)
                id_definitions[int_id] = x['RAE']
                return
            
            if isinstance(x, ZefOp):
                if len(x.el_ops) > 1:
                    # This could be something like AET.Int["asdf"] | assign_value[5]
                    if x | peel | last | is_a[assign_value]:
                        return

                if is_a(x, instantiated):
                    # is there an internal id curried in?
                    if len(x.el_ops[0][1]) > 1:
                        internal_id = get_curried_arg(x, 1)
                        rae_type = get_curried_arg(x, 0)
                        ensure_not_previously_defined(internal_id)
                        id_definitions[internal_id] = rae_type
                    return

                if is_a(x, merged):
                    # is there an internal id curried in?
                    if len(x.el_ops[0][1]) > 1:
                        internal_id = get_curried_arg(x, 1)
                        obj = get_curried_arg(x, 0)
                        ensure_not_previously_defined(internal_id)
                        id_definitions[internal_id] = obj
                    return

                if is_a(x, assign_value):
                    verify_internal_id(get_curried_arg(x, 0), id_definitions)
                    return

            if is_a(x, Delegate):
                id_definitions[x] = x
                return
                
            # this must be a triple, defining a relation
            if isinstance(x, ListOrTuple):
                x | for_each[verify_internal_id[id_definitions]]
                return


        @func    
        def verify_input_el(x, allow_rt, allow_scalar, id_definitions):
            # Note: we put this up front and early as an `isinstance` so that we
            # don't accidentally evaluate a LazyValue

            if isinstance(x, LazyValue):
                # This is checking for an (x <= value) format
                if not is_a(x.el_ops, assign_value):
                    raise Exception(f"A LazyValue must have come from (x <= value) only. Got {x}")
                return

            # Note: just is_a(x, Z) will also mean ZefRefs will be hit
            elif is_a(x, ZefOp) and is_a(x, Z):
                try:
                    some_id = get_curried_arg(x, 0)
                except:
                    raise KeyError(f"Invalid internal ID provided for Z[...] in GraphDelta. A String must be Curried in. Received: {x}")
                    
                if not (is_a(some_id, uid) or some_id in id_definitions):
                    raise KeyError(f"The id '{some_id}' used in Z refers neither to a uid, nor to an internal id defined in the GraphDelta init list")
                return
            
            elif isinstance(x, ZefRef) or isinstance(x, EZefRef):
                return
            
            elif isinstance(x, ListOrTuple):                         
                if is_valid_relation_template(x):
                    if len(x) == 3:
                        verify_input_el(x[0], False, True, id_definitions)
                        verify_input_el(x[1], True, False, id_definitions)
                        verify_input_el(x[2], False, True, id_definitions)
                        return
                    else:
                        return
                for item in x:
                    verify_input_el(item, False, False, id_definitions)
                return
            elif is_a_RT(x):
                if allow_rt:
                    return
                else:
                    raise ValueError(f"Bare RTs without source or target cannot be initialized. You tried to create a {x}.")                    
            elif type(x) in scalar_types:
                if not allow_scalar:
                    raise Exception("Direct values are not allowed at the top level of a GraphDelta, as this is likely to indicate a typo, e.g. (ET.Machine, ET.ShouldBeRTNotET, 'name'). Please use explicit value assignment via (AET.String <= 'name') if you really want this behaviour.")
                return
            elif is_valid_single_node(x):
                return
            
            elif isinstance(x, ZefOp):
                if len(x.el_ops) > 1:
                    # This could be something like AET.Int["asdf"] <= 5
                    if x | peel | last | is_a[assign_value]:
                        return
                if len(x.el_ops) != 1:
                    raise Exception(f"ZefOp has more than one op inside of it. {x}")
                if is_a(x, terminate):
                    return
                if is_a(x, merged):
                    return
                if is_a(x, assign_value):
                    return
                if is_a(x, tag):
                    return
                raise Exception(f"Not allowing ZefOps except for terminate, assign_value, delegate and tag. Got {x}")

            elif is_a(x, Delegate):
                return

            else:
                raise ValueError(f"Unexpected type passed to init list of GraphDelta: {x} of type {type(x)}")
            
        
        generate_id = make_generate_id()    # keeps an internal counter        
        iteration_step = make_iteration_step(generate_id)


        # print("Before extracting nested GraphDeltas", now())
        # First extract any nested GraphDeltas commands out
        # Note: using native python filtering as the evaluation engine is too slow
        # nested_cmds = elements | filter[is_a[GraphDelta]] | map[lambda x: x.commands] | concat | collect
        nested_cmds = [x for x in elements if type(x) == GraphDelta] | map[lambda x: x.commands] | concat | collect
        # print("Before filter not is_a GraphDeltas", now())
        # elements = elements | filter[Not[is_a[GraphDelta]]] | collect
        elements = [x for x in elements if not type(x) == GraphDelta]

        # The nested cmds will need to be readjusted for their auto-generated ids 
        
        # I want to redo this as pure functional style but will do it with side effects for now
        mapping_dict = {}
        def update_ids(cmd, mapping_dict):
            new_cmd = dict(**cmd)
            if "internal_id" in cmd:
                if is_generated_id(cmd["internal_id"]):
                    if cmd["internal_id"] not in mapping_dict:
                        mapping_dict[cmd["internal_id"]] = generate_id()
                    new_cmd["internal_id"] = mapping_dict[cmd["internal_id"]]
            for key in ["source", "target"]:
                if key in cmd:
                    if cmd[key] in mapping_dict:
                        new_cmd[key] = mapping_dict[cmd[key]]
            return new_cmd
                    
        nested_cmds = nested_cmds | map[P(update_ids, mapping_dict=mapping_dict)] | collect

        # print("Before verify_internal_id", now())
        elements | for_each[verify_internal_id[id_definitions]]     # all the ids are added to id_definitions. These are used in next step
        # print("Before verify_input_el", now())
        elements | for_each[verify_input_el[False][False][id_definitions]]
        
        state_initial = {
            'user_expressions': tuple(elements),
            'commands': nested_cmds,
            'ids_present': set(),       # keep track of uids merged in
        }

        def make_debug_output():
            num_done = 0
            next_print = now()+5*seconds
            def debug_output(x):
                nonlocal num_done, next_print
                num_done += 1
                if now() > next_print:
                    print(f"Up to iteration {num_done} - {len(x['user_expressions'])}, {len(x['commands'])}", now())
                    next_print = now() + 5*seconds
            return debug_output
            
        # iterate until all user expressions and generated expressions have become commands
        state_final = (state_initial
                       | iterate[iteration_step]
                       # | tap[make_debug_output()]
                       | take_until[lambda s: len(s['user_expressions'])==0]
                       | last
                       | collect
                       )
        self.commands = verify_and_compact_commands(state_final['commands'])
           
@func
def realise_single_node(x, gen_id):
    # Take something that should refer to a single node, i.e. a RAE or a scalar
    # to be turned into a RAE, or a reference to a RAE, and return a version
    # with an explicit ID and the ID itself.
    #
    # For example
    #
    # realise_single_node(ET.Machine["a"])
    # will return
    # ("a", ET.Machine["a"])
    #
    # realise_single_node(ET.Machine)
    # might return
    # ("12323425346", ET.Machine["12323425346"])


        #     5 different cases we need to deal with:
        # 4) A ZefRef / EZefRef to an existing RAE is specified. Generate the merge command if it is not present yet
        # 5) Z['my_temp_id4']       The temp id may not exist yet (could come from another edge creation). Make sure it is checked to exist at the end
        # 3) e.g. if a plain value was specified, create an AE and assign the value
        # 1) e.g. if ET.Dog is specified as the source: definitely create it
        # 2) e.g. if ET.Dog['Rufus'] was specified, that will be there as a dict. It is the command to create it and register the temp id

    # First case of removing lazy values
    if isinstance(x, LazyValue):
        x = collect(x)

    # Now this is a check for whether we are an assign_value
    if isinstance(x, LazyValue):
        # A bit of duplication here...
        assert x.el_ops.el_ops[0][0] == RT.AssignValue, "realise_single_node got a LazyValue that wasn't an assign_value"
        val = x.el_ops.el_ops[0][1][0]
        iid,actions = realise_single_node(x.initial_val, gen_id)
        actions = actions + [LazyValue(Z[iid]) | assign_value[val]]
    elif isinstance(x, EntityType) or isinstance(x, AtomicEntityType):
        iid = gen_id()
        actions = [x[iid]]
    elif isinstance(x, ZefRef) or isinstance(x, EZefRef):
        if internals.is_delegate(x):
            d = to_delegate(x)
            actions = [d]
            iid = d
        else:
            actions = [merged[x]]
            iid = origin_uid(x)
    elif type(x) in [Entity, AtomicEntity, Relation]:
        actions = [merged[x]]
        iid = origin_uid(x)
    elif type(x) in scalar_types:
        iid = gen_id()
        aet = map_scalar_to_aet_type[type(x)](x)
        actions = [aet[iid], LazyValue(Z[iid]) | assign_value[x]]
    elif isinstance(x, ZefOp):
        params = LazyValue(x) | peel | first | second
        if is_a(x, Z):
            iid = params | first | collect
            # No action to perform
            actions = []
        elif is_a(x, instantiated):
            if length(params) == 1:
                # We need to create a new id for this object
                x = x[gen_id()]
                params = LazyValue(x) | peel | first | second
            iid = params | second | collect
            actions = [x]
        elif is_a(x, merged):
            # Note: we always use the origin_uid here, even if there's an
            # internal id curried in. Simpler to later order operations.
            iid = origin_uid(params | first | collect)
            actions = [x]
        elif is_a(x, assign_value):
            iid = origin_uid(params | first | collect)
            actions = [x]
        elif is_a(x, terminate):
            iid = origin_uid(params | first | collect)
            actions = [x]
        else:
            raise NotImplementedError(f"Can't pass zefops to GraphDelta: for {x}")
    elif is_a(x, Delegate):
        iid = x
        actions = [x]
    else:
        raise TypeError(f'in GraphDelta encode step: for {type(x)=}')

    return iid, actions

def encode(xx):
    """
    This function is invoked if one writes "[ET.Dog, (ET.Person, ET.Aardvark)] | g"
    It returns a GraphDelta with ids assigned, as well as a schema
    with the same structure as the nested input arrays, but the ids as placeholders

    Args:
        xx ([type]): [description]

    Returns:
        [type]: Tuple, 
    """
    gd_actions = []
    
    gen_id = make_generate_id()

    def step(x, allow_scalar):
        if isinstance(x, ListOrTuple):
            if is_valid_relation_template(x):
                if len(x) == 2:
                    ent = step(x[0], True)
                    triples = [step((Z[ent], *tup), False) for tup in x[1]]
                    doubles = [x[1:] for x in triples]

                    return (ent, doubles)

                if len(x) == 3:
                    if isinstance(x[0], ListOrTuple):
                        assert is_valid_single_node(x[2])
                        item = step(x[2], True)
                        triples = [step((source, x[1], Z[item]), False) for source in x[0]]
                        sources = [x[0] for x in triples]
                        # Note: have to return None for the relation, as there are actually multiple relations
                        return (sources, None, item)

                    if isinstance(x[2], ListOrTuple):
                        assert is_valid_single_node(x[0])
                        item = step(x[0], True)
                        triples = [step((Z[item], x[1], target), False) for target in x[2]]
                        targets = [x[2] for x in triples]
                        # Note: have to return None for the relation, as there are actually multiple relations
                        return (item, None, targets)

                    # it's a relation triple
                    src_id = step(x[0], True)
                    rel_id = gen_id()
                    trg_id = step(x[2], True)
                    gd_actions.append( (Z[src_id], x[1][rel_id], Z[trg_id]) )

                    return (src_id, rel_id, trg_id)
                raise Exception("Shouldn't get here")
            else:
                # recurse: return isomorphic structure with IDs
                return tuple((step(el, False) for el in x))
        
        # These next few ifs are for checks on syntax only
        if type(x) in scalar_types:
            if not allow_scalar:
                raise Exception("Scalars are not allowed on their own to avoid accidental typos such as (ET.X, ET.Y, 'z') when (ET.X, RT.Y, 'z') is meant. If you want this behaviour, then create an explicit AET, i.e. (AET.String <= 'z').")
        
        if isinstance(x, ZefOp):
            if is_a(x, instantiated):
                raise Exception("Not allowed to pass internal names to RAEs in shorthand syntax")

        iid,actions = realise_single_node(x, gen_id)
        gd_actions.extend(actions)
        return iid

    step_res = step(xx, False)
    return step_res, GraphDelta(gd_actions)




def dispatch_ror_graph(g, x):
    from . import Effect, FX
    if isinstance(x, LazyValue):
        x = collect(x)
        # Note that this could produce a new LazyValue if the input was an
        # assign_value. This is fine.

    if isinstance(x, GraphDelta):
        return Effect({
                "type": FX.TX.Transact,
                "target_graph": g,
                "graph_delta": x
            })
    elif any(isinstance(x, T) for T in {list, tuple, EntityType, AtomicEntityType, ZefRef, EZefRef, ZefOp, QuantityFloat, QuantityInt, LazyValue, Entity, AtomicEntity, Relation}):
        unpacking_template, graph_delta = encode(x)
        # insert "internal_id" with uid here: the unpacking must get to the RAEs from the receipt
        def insert_id_maybe(cmd: dict):
            if 'origin_rae' in cmd:
                if is_a(cmd['origin_rae'], Delegate):
                    internal_id = cmd['origin_rae']
                else:
                    internal_id = uid(cmd['origin_rae'])
                return {**cmd, 'internal_id': internal_id}
            return cmd

        graph_delta_with_ids = GraphDelta([])
        graph_delta_with_ids.commands = [insert_id_maybe(c) for  c in graph_delta.commands]
        return Effect({
                "type": FX.TX.Transact,
                "target_graph": g,
                "graph_delta": graph_delta_with_ids,
                "unpacking_template": unpacking_template,
            })
    raise NotImplementedError(f"'x | g' for x of type {type(x)}")


from ..pyzef import main
main.Graph.__ror__ = dispatch_ror_graph


