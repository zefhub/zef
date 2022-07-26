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


#%%
"""
TODO: should we allow specifying "OnChange" on the relation leading to an AE?
Why? In case multiple fields refer to the same AE and the handler function needs to have access to the relation.
For now: Not sure where we would need this.

"""

from icecream import ic

import zefdb
from zefdb.zefops import (
    to_frame, 
    ins, 
    outs, 
    target, 
    source, tx, subscribe, 
    on_instantiation, on_termination, on_value_assignment, incoming, outgoing)
from importlib import reload
reload(zefdb.tools.zascii)
import zefdb
from zefdb import *
from zefdb import Graph, tools
from zefdb.zefops import *
from zefdb.zefops import instances, now, value, only, uid
from typing import *
from zefdb.tools import (
    make_graph, parse_zascii, 
    ops, 
    draw_zascii_segmentation,      
    pipeable,
    make_graph_delta,
    zefui_component_graph_from_zascii
    )
import zefdb.effects as fx
import inspect
import uuid
from dataclasses import dataclass




def add_to_nested_dict(d: dict, keys: list, value):
    """
    Small utility function that ads an element, given a list of keys.
    If any sub-dictionary does not exist, it is created.

    Args:
        d (dict): The dictionary that is added to (mutated!)
        keys (list): e.g. ['key1','key2']
        value ([type]): the value added at the very end
    """
    if len(keys) == 0: return
    k = keys[0]
    if len(keys) == 1:
        d[k] = value
        return
    if k not in d:
        d[k] = {}
    add_to_nested_dict(d[k], keys[1:], value)
    


def append_list_in_nested_dict(d: dict, keys: list, list_to_append: list) -> None:
    """ 
    appends to a list if present. Otherwise adds it.
    d = {'a': {'b': [1, 2, 3]}}
    append_list_in_nested_dict(d, ['a', 'b'], [42, 43])
    
    -> {'a': {'b': [1, 2, 3, 42, 43]}}    
    """ 
    if len(keys) == 0: return
    k = keys[0]
    if len(keys) == 1:
        assert isinstance(list_to_append, list) or isinstance(list_to_append, tuple)
        if k in d:
            existing_list = d[k]
            assert isinstance(existing_list, list) or isinstance(existing_list, tuple)
            d[k] = [*existing_list, *list_to_append]
        else:
            d[k] = list_to_append
        return
    if k not in d:
        d[k] = {}
    append_list_in_nested_dict(d[k], keys[1:], list_to_append)
    




def is_nested_key_contained(d: dict, keys: list) -> bool:
    """
    Check whether a nested set of keys exists.
    
    Examples:
    is_nested_key_contained({'a': 42}, ['a']) # -> True
    is_nested_key_contained({'a': {}}, ['a']) # -> True
    is_nested_key_contained({'a': 42}, ['b']) # -> False
    is_nested_key_contained({'a': {}, 'c': 0}, ['a', 'b']) # -> False
    is_nested_key_contained({'a': {'b': 42}, 'c': 0}, ['a', 'b']) # -> True
    is_nested_key_contained({'a': {'b': {'d': 5}}, 'c': 0}, ['a', 'b', 'd']) # -> True
    is_nested_key_contained({'a': {'b': {'d': 5}}, 'c': 0}, []) # -> throws

    Args:
        d (dict): [the dictionary to look inside]
        keys (list): [a list of keys ordered in nesting order]

    Returns:
        bool: 
    """
    assert isinstance(d,dict)
    assert len(keys) > 0
    k = keys[0]
    if k not in d: return False
    if len(keys) == 1: return k in d    
    if isinstance(d[k], dict):
        return is_nested_key_contained(d[k], keys[1:])
    return False
    



def is_valid_effect_list_type(x) -> bool:
    """
    as output from user provided callback functions we expect
    a tuple of dicts, each dict representing one effect.
    """
    if not (isinstance(x, list) or isinstance(x, tuple)):
        return False        
    if not all([isinstance(el, dict) for el in x]):
        return False
    return True
    



# keep an intermediate layer of state in the runtime: if the FE socketIO connection connects first, messages can be pushed through immediately.
# If the zefUI app is initialized before the FE is connected, still allow the graph crawler to do its full job and pipe messages
# to the runtime. Buffer them until the client connects.

_state = {
    'app uid 1': {
        'graph_delta_q': [],
        'msgs_for_fe_client_q': [],
    }
}





#--------------------------------------------------------------------------------------------
#------------------------------------- zefui state  -----------------------------------------
#--------------------------------------------------------------------------------------------
# ZefUI has to store the state somewhere e.g. all the context dictionaries that are passed 
# around at runtime if we want to deal with restarts of the kernel at any time.

# Where is e.g. the context dict initialized? The 'render_component' function?

# of form:
# _zefui_state = { 
#   app_uid: {
#      component_uid: { 
#         'z_template_root': ... 
#          'context': {},  # this is only saved here to have all state explicit and the option to serialize a given app at any point
#         ...
#      },
#   instances_fe_notification
# }}
_zefui_state = {}                # stores app root and context dicts. Try to keep everything in here to be plain old data
_zefui_subscriptions = {}


# subscriptions are kept separately: they don't need to be serialized when the process shuts down
# _zefui_subscriptions = {
#     app_instance_uid: {
#         z_instance_uzr: {
#             component_uzr_1: [sub34, sub98],      # which RAE owns the subscrition?
#             component_uzr_2: []                   # there may be no subscriptions, but we still want to register that the component knows about this RAE. This is required to know when the last component relying on it is terminated.
#         }
#     }
# }





@dataclass
class CrawlArgs:
    z_template: ZefRef              # remains pinned to the time slice of the template that one chose to run the component with
    z_component_root: EZefRef
    context: dict
    app_instance_uid: str
    came_from: EZefRef
    incoming_rts_to_not_monitor: set
    outgoing_rts_to_not_monitor: set



class Conj:
    def __init__(self, d: dict):
        self.d = d
        
    def __getitem__(self, x):
        if isinstance(x, dict):
            return Conj(x)
            
    def __ror__(self, other):
        if self.d is None:
            raise RuntimeError("... | conj used, but no dictionary was curried in.")
        
        if isinstance(other, CrawlArgs):
            try:
                new_d = self.d or {}            
                return CrawlArgs(**{k: (new_d[k] if k in new_d else getattr(other, k)) for k in other.__dataclass_fields__})
            except Exception as e:
                print(f"Error in conj | operator: e={e}")
        elif isinstance(other, dict):
            return {**other, **self.d}
        else:
            raise TypeError(f"Unexpected type passed into '... | conj[]': type(other)={type(other)}. other={other} ")
    
    def __call__(self, other):
        return self | other
            
conj = Conj(None)
        






def prepare_dict_of_args_used_by_zef_function(f: ZefRef, dict_of_all_args: dict):
    """ used to prepare the arguments in any OnChange function provided on the template.
    Only pass through the arguments asked for.
    """
    from zefdb.zef_functions import zef_function_signature
    fct_args = set(zef_function_signature(f).parameters.keys())
    allowed_args_for_on_change_fcts = {'new_value', 'z_rel', 'z_ae', 'z_rel_template', 'z_ae_template', 'z_component_root', 'context'}
    if not fct_args.issubset(allowed_args_for_on_change_fcts):
        raise SyntaxError(f"""A zef function triggered by 'on_change' 
        may only take arguments {{'new_value', 'z_rel', 'z_ae', 'z_rel_template', 'z_ae_template', 'z_component_root', 'context'}} 
        or a subset thereof. The function provided takes {fct_args}. Problematic arg is :{allowed_args_for_on_change_fcts-fct_args}""")
    return {k:v for k,v in dict_of_all_args.items() if k in fct_args}  # which arguments did the author of the on_change callback ask for in his function signature? Only pass these






def verify_schema_locally(z_template, z_instance, a: CrawlArgs, rel_type_and_direction=None, rel_and_direction_to_exclude=None):    
    """ 
    Verifies that the schema is correct locally in the vicinity of z_template.    
    
    rel_types = None means: verify all incoming and outgoing fields from schema
    if rel_type = (RT.FirstName, outgoing)    
    
    rel_and_direction_to_exclude = (RT.Name, incoming)
    used to not crawl back up the path we came from
    
    Also: check whether we should call OnChange handlers specified by the user with the default value given on the template graph.
    if
        - the field is not marked "RT.ZEFUI_IsRequired"  (if not present this is the default)
        - the field is not marked "RT.ZEFUI_AllowMultiple" (if not present this is the default)
        - the traversal target is an AET and has >0 OnChange handlers attached
        - the data instance part of the graph does not have this field (relation is absent), i.e. use the default value from the template graph
        
    """
    
    def make_IsRequired_or_AllowMultiple(rel_type):
        """factor out similarity between is_IsRequired and is_AllowMultiple functions"""
        assert rel_type in {RT.ZEFUI_IsRequired, RT.ZEFUI_AllowMultiple}
        def f(z_edge_template):
            zs = z_edge_template >> L[rel_type]
            if len(zs)>1:
                raise RuntimeError(f"Multiple 'RT.{rel_type}' attached to {z_edge_template}. This makes no sense: only zero (default: {rel_type}=False) or one instance are allowed.")
            if len(zs)==0: 
                return False
            else:            
                val = zs | only | value
                assert val in {True, False}
                return val        
        return f
    
    is_IsRequired = make_IsRequired_or_AllowMultiple(RT.ZEFUI_IsRequired)
    is_AllowMultiple = make_IsRequired_or_AllowMultiple(RT.ZEFUI_AllowMultiple)    
                
    def make_verify_and_maybe_set_default_func(ins_or_outs):
        assert ins_or_outs in {ins, outs}
        def verify_in_or_out_edge_and_maybe_set_default(edge):
            # Checks for requirement of this relation
            is_required = is_IsRequired(edge)
            multiple_allowed = is_AllowMultiple(edge)
            field_instances = z_instance > L[edge | RT] if ins_or_outs==outs else z_instance < L[edge | RT] 
            traversal_source, traversal_target = (source, target) if ins_or_outs==outs else (target, source)
            
            if is_required and multiple_allowed:
                raise RuntimeError(f"""Conflicting requirements specified on template graph for field {edge}. 
                                A field can't be both 'IsRequired' and 'AllowMultiple'""")
                            
            if is_required and len(field_instances) != 1:
                raise RuntimeError(f"""For RAE type '{repr(rae_type(edge|traversal_source))} > {repr(rae_type(edge))}' the schema has a 
                                IsRequired relation on template graph and exactly one instance of this field is required. 
                                However, {len(field_instances)} instances were found on the data subgraph!""")
            
            for rel_field_inst in field_instances:
                if rae_type(edge | traversal_target) != rae_type(rel_field_inst | traversal_target):
                    raise RuntimeError(f"""{rae_type(edge)} is connected to a {repr(rae_type(edge | traversal_target))} on template 
                                graph but to a {repr(rae_type(rel_field_inst|traversal_target))} on the data graph!""")
            
            if not multiple_allowed and len(field_instances) > 1:
                raise RuntimeError(f"Multiple relations of type {rae_type(edge)}  shouldn't be allowed as ZEFUI_AllowMultiple doesn't exist for this relation on the template graph!")
            
            if (not is_required and not multiple_allowed and len(field_instances) == 0):
                for f in (edge | traversal_target) >> L[RT.ZEFUI_OnChange]:
                    default_value = edge | traversal_target | value
                    dict_of_all_args = dict(
                        new_value=default_value,
                        z_rel=None,              # these don't exist if we're resorting to the default value
                        z_ae=None,             
                        z_rel_template=edge, 
                        z_ae_template=edge | traversal_target,
                        z_component_root=a.z_component_root, 
                        context=a.context
                    )
                    d_args_used = prepare_dict_of_args_used_by_zef_function(f, dict_of_all_args)
                    requested_effects = f(**d_args_used)     # only pass in the args that the function wants
                    # exit early and tell the user in which zef function the problem is
                    # if the returned output is not of expected type
                    if not is_valid_effect_list_type(requested_effects):
                        raise TypeError(f"""
                                        Expecting a tuple of Dicts as effects from the user 
                                        provided zef function with original name "{f >> RT.OriginalName | value}"
                                        {f}, but received type {type(requested_effects)}: 
                                        {requested_effects} """)
                    
                    # This should become 'fx.for_each[runtime]' once we have the lazy zefops cleaned up. 
                    # "for_each" indicates that computation / side effects are triggered. 'runtime' is 
                    # the placeholder for the local execution environment.
                    (requested_effects 
                    #  | ops.map[conj[{'app_instance_uid': a.app_instance_uid}]] 
                     | fx.for_each
                     )
                    
        return verify_in_or_out_edge_and_maybe_set_default      
            
    
    is_template_data = lambda z_ed: str(RT(z_ed))[:6] != 'ZEFUI_'
    
    specific_out_edge = (z_template > rel_type_and_direction[0], ) if (rel_type_and_direction is not None and rel_type_and_direction[1] == outgoing) else ()
    specific_in_edge = (z_template < rel_type_and_direction[0], ) if (rel_type_and_direction is not None and rel_type_and_direction[1] == incoming) else ()    
    
    outs_filter = ops.identity if (rel_and_direction_to_exclude is None or rel_and_direction_to_exclude[1] != outgoing) else filter[lambda z_ed: RT(z_ed) != rel_and_direction_to_exclude[0]]
    ins_filter  = ops.identity if (rel_and_direction_to_exclude is None or rel_and_direction_to_exclude[1] != incoming) else filter[lambda z_ed: RT(z_ed) != rel_and_direction_to_exclude[0]]
    
    out_edges_to_verify = z_template | outs | filter[is_template_data] | outs_filter if rel_type_and_direction is None else ()        
    in_edges_to_verify =  z_template | ins  | filter[is_template_data] | ins_filter  if rel_type_and_direction is None else ()
        
    (*out_edges_to_verify, *specific_out_edge) | ops.for_each[make_verify_and_maybe_set_default_func(outs)]
    (*in_edges_to_verify, *specific_in_edge) | ops.for_each[make_verify_and_maybe_set_default_func(ins)]
    
    
    
    
    



#                            ____                      _          ____                         _                                     
#                           / ___| _ __   __ _  _ __  | |__      / ___| _ __   __ _ __      __| |  ___  _ __                         
#   _____  _____  _____    | |  _ | '__| / _` || '_ \ | '_ \    | |    | '__| / _` |\ \ /\ / /| | / _ \| '__|    _____  _____  _____ 
#  |_____||_____||_____|   | |_| || |   | (_| || |_) || | | |   | |___ | |   | (_| | \ V  V / | ||  __/| |      |_____||_____||_____|
#                           \____||_|    \__,_|| .__/ |_| |_|    \____||_|    \__,_|  \_/\_/  |_| \___||_|                           
#                                              |_|                                                                                   



def execute_on_instantiation_actions(z_instance: ZefRef, a: CrawlArgs):    
    # ------------------------------------------------------ ZEFUI_OnInstantiation -----------------------------------------------------
    # -------------------------------------- manually execute component callbacks  ----------------------------------------------    
    
    # A) look at z_template: are there any RT.ZEFUI_OnInstantiation / RT.ZEFUI_OnTermination fcts attached to z_template?
    # Do this for if z_instance is {ET, RT, AET}
    for f in a.z_template >> L[RT.ZEFUI_OnInstantiation]:
        from zefdb.zef_functions import zef_function_signature
        fct_args = set(zef_function_signature(f).parameters.keys())
        allowed_args_for_on_instantiation_fcts = {'z_instance', 'z_template', 'z_component_root', 'context'}
        if not fct_args.issubset(allowed_args_for_on_instantiation_fcts):
            raise SyntaxError(f"""A zef function triggered by 'on_instantiation' 
            may only take arguments {{'z_instance', 'z_template', 'z_component_root', 'context'}} 
            or a subset thereof. The function provided takes {fct_args}""")
        d_args = dict(z_instance=z_instance, z_template=a.z_template, z_component_root=a.z_component_root, context=a.context)       
        try:   
            requested_effects = f(**{k:v for k,v in d_args.items() if k in fct_args})     # only pass in the args that the function wants
            # exit early and tell the user in which zef function the problem is
            # if the returned output is not of expected type
            if not is_valid_effect_list_type(requested_effects):
                raise TypeError(f"""
                                Expecting a tuple of Dicts as effects from the user 
                                provided zef function with original name "{f >> RT.OriginalName | value}"
                                {f}, but received type {type(requested_effects)}: 
                                {requested_effects} """)
            
            # This should become 'fx.for_each[runtime]' once we have the lazy zefops cleaned up. 
            # "for_each" indicates that computation / side effects are triggered. 'runtime' is 
            # the placeholder for the local execution environment.
        
            (requested_effects 
            #  | ops.map[conj[{'app_instance_uid': a.app_instance_uid}]]  
             | fx.for_each)
        except Exception as e:
            print(f'Error 1: exc={exc}')
    



def execute_on_termination_actions(z_instance: ZefRef, a: CrawlArgs):
    """
    Not a pure function. It executes the effects by forwarding them to the runtime.
    """    
    # A) look at z_template: are there any RT.ZEFUI_OnInstantiation / RT.ZEFUI_OnTermination fcts attached to z_template?
    # Do this for if z_instance is {ET, RT, AET}
    for f in a.z_template >> L[RT.ZEFUI_OnTermination]:
        from zefdb.zef_functions import zef_function_signature
        fct_args = set(zef_function_signature(f).parameters.keys())
        allowed_args_for_on_instantiation_fcts = {'z_instance', 'z_template', 'z_component_root', 'context'}
        if not fct_args.issubset(allowed_args_for_on_instantiation_fcts):
            raise SyntaxError(f"""A zef function triggered by 'on_instantiation' 
            may only take arguments {{'z_instance', 'z_template', 'z_component_root', 'context'}} 
            or a subset thereof. The function provided takes {fct_args}""")
        d_args = dict(z_instance=z_instance, z_template=a.z_template, z_component_root=a.z_component_root, context=a.context)
        try:
            requested_effects = f(**{k:v for k,v in d_args.items() if k in fct_args})     # only pass in the args that the function wants
            # exit early and tell the user in which zef function the problem is
            # if the returned output is not of expected type
            if not is_valid_effect_list_type(requested_effects):
                raise TypeError(f"""
                                Expecting a tuple of Dicts as effects from the user 
                                provided zef function with original name "{f >> RT.OriginalName | value}"
                                {f}, but received type {type(requested_effects)}: 
                                {requested_effects} """)
            # This should become 'fx.for_each[runtime]' once we have the lazy zefops cleaned up. 
            # "for_each" indicates that computation / side effects are triggered. 'runtime' is 
            # the placeholder for the local execution environment.
            requested_effects | fx.for_each    
        except Exception as e:
            print(f"Exception in execute_on_termination_actions: {e}")


    










def execute_on_change(f: ZefRef, new_val, z_instance, a: CrawlArgs):
    """factored out to actually perform the change. Can be used in """
    from zefdb.zef_functions import zef_function_signature
    fct_args = set(zef_function_signature(f).parameters.keys())
    allowed_args_for_on_instantiation_fcts = {'new_value', 'z_instance', 'z_template', 'z_component_root', 'context'}
    if not fct_args.issubset(allowed_args_for_on_instantiation_fcts):
        raise SyntaxError(f"""A zef function triggered by 'on_instantiation' 
        may only take arguments {{'new_value', 'z_instance', 'z_template', 'z_component_root', 'context'}} 
        or a subset thereof. The function provided takes {fct_args}""")
        
    
    d_args = dict(new_value=new_val, z_instance=z_instance, z_template=a.z_template, z_component_root=a.z_component_root, context=a.context)        
    d_args_used = {k:v for k,v in d_args.items() if k in fct_args}    
    try:
        requested_effects = f(**d_args_used)     # only pass in the args that the function wants
        # exit early and tell the user in which zef function the problem is
        # if the returned output is not of expected type
        if not is_valid_effect_list_type(requested_effects):
            raise TypeError(f"""
                            Expecting a tuple of Dicts as effects from the user 
                            provided zef function with original name "{f >> RT.OriginalName | value}"
                            {f}, but received type {type(requested_effects)}: 
                            {requested_effects} """)
        # This should become 'fx.for_each[runtime]' once we have the lazy zefops cleaned up. 
        # "for_each" indicates that computation / side effects are triggered. 'runtime' is 
        # the placeholder for the local execution environment.
        requested_effects | fx.for_each
    except Exception as e:
        print(f"Exception in execute_on_change: {e}")






def execute_on_change_actions_inst(z_rel: ZefRef, a: CrawlArgs):    
    """
    z_rel is the instance of the relation
    """
    # ------------------------------------------------------ ZEFUI_OnChange ------------------------------------------------------------
    # ------------------------ if the z_ae_template (away from the direction of traversal) 
    # is an AET that is marked by OnChange, execute that upon instantiation of this edge ----------------------------
    

    
    if BT(z_rel) == BT.RELATION_EDGE:
        traverse_dir = target if (a.came_from is None or to_ezefref(z_rel|to_ezefref|source) == a.came_from) else source        
        z_ae_template = a.z_template | traverse_dir
        z_ae = z_rel | traverse_dir
        
        for f in z_ae_template >> L[RT.ZEFUI_OnChange]:
            if BT(z_ae_template) != BT.ATTRIBUTE_ENTITY_NODE:
                raise TypeError(f'Expecting an AET if an ZEFUI_OnChange was connected, but got z_ae_template={z_ae_template}  z_rel={z_rel}   z_ae={z_ae}')

            # An OnChange function given on a component template graph may ask for any subset of arguments from the dict below.
            # Only pass in the args asked for
            dict_of_all_args = dict(
                new_value=(z_ae | value) or (z_ae_template | value), 
                z_rel=z_rel, z_ae=z_ae, 
                z_rel_template=a.z_template, 
                z_ae_template=z_ae_template,
                z_component_root=a.z_component_root, 
                context=a.context
                )
            d_args_used = prepare_dict_of_args_used_by_zef_function(f, dict_of_all_args)
            try:
                requested_effects = f(**d_args_used)     # only pass in the args that the function wants
                # exit early and tell the user in which zef function the problem is
                # if the returned output is not of expected type
                if not is_valid_effect_list_type(requested_effects):
                    raise TypeError(f"""
                                    Expecting a tuple of Dicts as effects from the user 
                                    provided zef function with original name "{f >> RT.OriginalName | value}"
                                    {f}, but received type {type(requested_effects)}: 
                                    {requested_effects} """)
                # This should become 'fx.for_each[runtime]' once we have the lazy zefops cleaned up. 
                # "for_each" indicates that computation / side effects are triggered. 'runtime' is 
                # the placeholder for the local execution environment.
                requested_effects | fx.for_each
            except Exception as e:
                print(f"Exception in execute_on_change_actions_inst: {e}")
                
            def update_val(key: str, val, z_aet: ZefRef):
                """z_aet is passed in to extract the reference frame"""
                if key in {'context', 'z_rel_template', 'z_ae_template'}:
                    return val
                elif key == 'new_value':
                    return z_aet | value
                elif key in {'z_ae', 'z_rel', 'z_component_root'}:
                    return val | to_frame[z_aet|tx]
                raise RuntimeError(f'unexpected key={key}: we should not have landed here')
            
            def make_aet_val_change_subscription_callback(f_user_provided_on_change_fct, args_in_old_frame: dict):
                """infer which arguments are used from the keys in args_in_old_frame. Possibly forward some of them to the newest frame"""                
                def f_callback(z_aet: ZefRef) -> None:
                    d_args_cb_frame = {k: update_val(k, v, z_aet) for k, v in args_in_old_frame.items()}
                    try:
                        requested_effects = f_user_provided_on_change_fct(**d_args_cb_frame)      # we may want to add more key value pairs into each effect.
                        (requested_effects 
                        #  | ops.map[lambda d: d | conj[{'app_instance_uid': a.app_instance_uid}]] 
                        #  | ops.tap[lambda x: print(f">>>>>>>>>>>>>{x}")] 
                         | fx.for_each)
                        # app event handler function may/should not add the target ui model graph or the app id.
                    except Exception as e:
                        print(f'Error executing f_callback: {e}')
                        raise e
                return f_callback
            
            # pass in the args at the time of instantiation: keys required will be 
            # extracted and possibly transformed to the reference frame at the time of callback
            aet_val_change_callback = make_aet_val_change_subscription_callback(f, d_args_used)
            sub = z_ae | subscribe[on_value_assignment][keep_alive[False]][aet_val_change_callback]            
            append_list_in_nested_dict(_zefui_subscriptions, [a.app_instance_uid, to_ezefref(z_rel), to_ezefref(a.z_component_root)], [sub])
            





#                                                  _                       _    _                                                    _                       
#                          ___   ___   _ __   ___ | |_  _ __  _   _   ___ | |_ (_)  ___   _ __           ___  _ __   __ _ __      __| |                      
#  _____  _____  _____    / __| / _ \ | '_ \ / __|| __|| '__|| | | | / __|| __|| | / _ \ | '_ \         / __|| '__| / _` |\ \ /\ / /| |  _____  _____  _____ 
# |_____||_____||_____|  | (__ | (_) || | | |\__ \| |_ | |   | |_| || (__ | |_ | || (_) || | | |       | (__ | |   | (_| | \ V  V / | | |_____||_____||_____|
#                         \___| \___/ |_| |_||___/ \__||_|    \__,_| \___| \__||_| \___/ |_| |_| _____  \___||_|    \__,_|  \_/\_/  |_|                      
#                                                                                               |_____|                                                      



def show_crawl_args(a: CrawlArgs):
    return f"<z_template: {repr(rae_type(a.z_template))}  indx={index(a.z_template)}  came from a {'nothing' if a.came_from is None else rae_type(a.came_from)} >"


# the following function returns a function to be used in 'subscribe'.
# It is only responsible for the self-replication of the graph crawling behavior,
# not for executing any of the user provided callback functions
def make_cb_instantiation(a: CrawlArgs, verify_upstream_rae: bool):
    from random import randint
    def cb_instantiation(z_ed):
            try:
                traverse_dir = target if (a.came_from is None or to_ezefref(z_ed|to_ezefref|source) == a.came_from) else source
                opposite_traverse_dir = source if traverse_dir == target else target
                # opposite = {target: source, source: target}
                a_this_ed = a | conj[{
                    'came_from': z_ed | to_ezefref | opposite_traverse_dir,
                    'incoming_rts_to_not_monitor': set(),
                    'outgoing_rts_to_not_monitor': set(),
                    }]
                
                # if this rel was added: it could violate the schema one step down, on the RAE where we came from while traversing
                # Run an extra check there
                if verify_upstream_rae:
                    verify_schema_locally(
                        a.z_template | opposite_traverse_dir, 
                        z_ed | opposite_traverse_dir,
                        a, 
                        rel_type_and_direction=(RT(z_ed), incoming if traverse_dir == source else outgoing)     # limit to checking the specific edge type of z_ed
                        )
                construction_crawl(z_instance=z_ed, a=a_this_ed)            
                # the direction of the edge may differ from our direction on the traversal path
                
                # call construction_crawl on whatever is at the other end of this rel: but pass 
                # on the info where we came from to prevent subscriptions being set up that take
                # us back opposite of the traversal direction
                a_new = a | conj[{
                    'z_template': a.z_template|traverse_dir,
                    'came_from': z_ed | to_ezefref,
                    'incoming_rts_to_not_monitor': {RT(z_ed)} if traverse_dir is target else set(),
                    'outgoing_rts_to_not_monitor': {RT(z_ed)} if traverse_dir is source else set(),
                    }]
                construction_crawl(z_ed | traverse_dir, a_new)                
            except Exception as e:
                print("\n"*10)
                print("!"*100)
                print(f"Error in cb_termination:: {e}\n\n\n\n\n")

    return cb_instantiation



def construction_crawl(z_instance: ZefRef, a: CrawlArgs) -> Optional['displayable']:
    """
    crawl along a graph, ui tree recursively.
    It may return a displayable (e.g. when using ipw/bq) if called on the root of a component. Otherwise return None.
    
    Arguments:
        app_uid: str - uuid generated within zefui (does not necessarily live on the the graph) to identify an app. Useful if multiple apps run from the same process
        # naaaaaah, we don't need this! Encode it in the 'came_from'. traversal_dir: int - +1 for moving along the edge direction, -1 going against.
        came_from: EZefRef - the edge that the crawler came from during this traversal step
    """
          
    if not(BT(z_instance) == BT(a.z_template)):
        raise RuntimeError('Error in zefUI graph crawling: instance and template graph type are out of sync')
    if not(rae_type(z_instance) == rae_type(a.z_template)):
        raise RuntimeError('Error in zefUI graph crawling: instance and template graph rae type are out of sync')
    # what if we are on a relation type we should traverse according to the template, but the target rae does not correspond to that on the template
    if BT(z_instance)==BT.RELATION_EDGE:
        traverse_dir = target if z_instance | to_ezefref | source == a.came_from else source
        if rae_type(z_instance | traverse_dir) != rae_type(a.z_template | traverse_dir):     #TODO: this may become source as well
            raise RuntimeError("target attached to monitored edge does not align with what was expected from the template. Exiting early in crawler")
    
    # Verify the schema locally with the template and instance
    rel_and_direction_to_exclude= (None if a.came_from is None or BT(a.came_from) != BT.RELATION_EDGE else
                                    (RT(a.came_from), outgoing if (a.came_from | source == z_instance|to_ezefref) else incoming)
                                )
    verify_schema_locally(
        a.z_template, 
        z_instance, 
        a,
        rel_and_direction_to_exclude=rel_and_direction_to_exclude,
        )
    # don't crawl back up the relation where we came from
    out_edge_types_to_monitor = set(a.z_template | outs | filter[lambda z_ed: str(RT(z_ed))[:6] != 'ZEFUI_'] | tools.ops.map[RT]) - a.outgoing_rts_to_not_monitor
    in_edge_types_to_monitor = set(a.z_template | ins | filter[lambda z_ed: str(RT(z_ed))[:6] != 'ZEFUI_'] | tools.ops.map[RT]) - a.incoming_rts_to_not_monitor
    
    inst_subs_out = [z_instance | subscribe[keep_alive[False]][on_instantiation[outgoing][edge_type]][make_cb_instantiation(a | conj[{'z_template': a.z_template > edge_type, 'came_from': to_ezefref(z_instance)}], verify_upstream_rae=True)] for edge_type in out_edge_types_to_monitor]    
    inst_subs_in  = [z_instance | subscribe[keep_alive[False]][on_instantiation[incoming][edge_type]][make_cb_instantiation(a | conj[{'z_template': a.z_template < edge_type, 'came_from': to_ezefref(z_instance)}], verify_upstream_rae=True)] for edge_type in in_edge_types_to_monitor]
    term_subs_out = [z_instance | subscribe[keep_alive[False]][on_termination[outgoing][edge_type]][make_cb_termination(a | conj[{'z_template': a.z_template > edge_type, 'came_from': to_ezefref(z_instance)}], verify_upstream_rae=True)] for edge_type in out_edge_types_to_monitor]    
    term_subs_in  = [z_instance | subscribe[keep_alive[False]][on_termination[incoming][edge_type]][make_cb_termination(a | conj[{'z_template': a.z_template < edge_type, 'came_from': to_ezefref(z_instance)}], verify_upstream_rae=True)] for edge_type in in_edge_types_to_monitor]
    
    # save the subscriptions in the _zefui_subscriptions dictionary under this app uid -> RAE uid -> component uid
    # to prevent it from being garbage-collected.
    # Delete it from there 
    all_subs = inst_subs_out + inst_subs_in + term_subs_out + term_subs_in
    
    # even if there are no subscriptions: still add an empty list into the dict to mark that this rae is in this component
    append_list_in_nested_dict(_zefui_subscriptions, [a.app_instance_uid, to_ezefref(z_instance), to_ezefref(a.z_component_root)], all_subs)
    execute_on_instantiation_actions(z_instance, a)
    execute_on_change_actions_inst(z_instance, a)   # will only do anything if z_instance is a relation for which, going along the traversal dir, an AE with OnChange is attached. Note: it may be surprising at first that we don't need to deal with the case of z_instance being an AE directly.
    
    # execute once at this point in time, after setting up the subscription for the future
    # Don't walk back down the edge that we came from. Also respect that we cannot compare a EZefRef with None (== overlaoded in C++)
    def is_uzefref_we_came_from(zz: ZefRef)-> bool:
        if a.came_from is None: return False
        return to_ezefref(zz) == a.came_from
    
    for ed in z_instance | outs | filter[lambda z: RT(z) in out_edge_types_to_monitor and not is_uzefref_we_came_from(z)]:
        d_update = {
            'z_template': a.z_template > RT(ed),
            'came_from': to_ezefref(z_instance),
            }
        # only crawl downstream if we are here
        make_cb_instantiation(a | conj[d_update], verify_upstream_rae=False)(ed)       # this will trigger the recursive crawl
    
    for ed in z_instance | ins | filter[lambda z: RT(z) in in_edge_types_to_monitor and not is_uzefref_we_came_from(z)]:
        d_update = {
            'z_template': a.z_template < RT(ed),
            'came_from': to_ezefref(z_instance),
            }
        # only crawl downstream if we are here
        make_cb_instantiation(a | conj[d_update], verify_upstream_rae=False)(ed)       # this will trigger the recursive crawl
        
    







#                             _             _                       _    _                                                    _                       
#                          __| |  ___  ___ | |_  _ __  _   _   ___ | |_ (_)  ___   _ __           ___  _ __   __ _ __      __| |                      
#  _____  _____  _____    / _` | / _ \/ __|| __|| '__|| | | | / __|| __|| | / _ \ | '_ \         / __|| '__| / _` |\ \ /\ / /| |  _____  _____  _____ 
# |_____||_____||_____|  | (_| ||  __/\__ \| |_ | |   | |_| || (__ | |_ | || (_) || | | |       | (__ | |   | (_| | \ V  V / | | |_____||_____||_____|
#                         \__,_| \___||___/ \__||_|    \__,_| \___| \__||_| \___/ |_| |_| _____  \___||_|    \__,_|  \_/\_/  |_|                      
#                                                                                        |_____|                                                      


# A) cb_termination is always called on a relation.
#        - it calls destruction_crawl twice:
#                 1) for the relation z_rel it was called on
#                 2) on the src/trg RAE that the crawler did NOT come from.
#
# B) destruction_crawl can be called on any RAE z_instance. This takes care
#    of relations coming out of z_instance (z_instance could be a relation as well). It looks at
#    z_instance and the corresponding template and figures out which
#    edges to call cb_termination for. It excludes the traversing source relation.
#    For each of these edges cb_termination is called.


def make_cb_termination(a: CrawlArgs, verify_upstream_rae: bool):
    def cb_termination(z_ed):
        # it will die silently in the zefDB subscription system if we don't catch the error here
        try:
            traverse_dir = target if (a.came_from is None or to_ezefref(z_ed|to_ezefref|source) == a.came_from) else source
            opposite_traverse_dir = source if traverse_dir == target else target
            a_this_ed = a | conj[{
                'came_from': z_ed | to_ezefref | opposite_traverse_dir,
                'incoming_rts_to_not_monitor': set(),
                'outgoing_rts_to_not_monitor': set(),
                }]            
            # if this rel was added: it could violate the schema one step down, on the RAE where we came from while traversing
            # Run an extra check there
            if verify_upstream_rae:
                verify_schema_locally(
                    a.z_template | opposite_traverse_dir, 
                    z_ed | opposite_traverse_dir,
                    a, 
                    rel_type_and_direction=(RT(z_ed), incoming if traverse_dir == source else outgoing)     # limit to checking the specific edge type of z_ed
                    )
            destruction_crawl(z_instance=z_ed, a=a_this_ed)            
            # the direction of the edge may differ from our direction on the traversal path
            
            # call construction_crawl on whatever is at the other end of this rel: but pass 
            # on the info where we came from to prevent subscriptions being set up that take
            # us back opposite of the traversal direction
            a_new = a | conj[{
                'z_template': a.z_template|traverse_dir,
                'came_from': z_ed | to_ezefref,
                'incoming_rts_to_not_monitor': {RT(z_ed)} if traverse_dir is target else set(),
                'outgoing_rts_to_not_monitor': {RT(z_ed)} if traverse_dir is source else set(),
                }]
            # even if this edge was terminated in the last tx, allow us to traverse to the source or target
            to_frame = to_frame[allow_terminated_relent_promotion][z_ed|tx]
            destruction_crawl(z_ed| to_ezefref | traverse_dir | to_frame, a_new)
        except Exception as e:
            print("\n"*10)
            print("!"*100)
            print(f"Error in cb_termination::: {e}\n\n\n\n\n")
        
    return cb_termination
        
        
        


def destruction_crawl(z_instance: ZefRef, a: CrawlArgs) -> None:    
    if not(BT(z_instance) == BT(a.z_template)):
        raise RuntimeError('Error in zefUI graph crawling: instance and template graph type are out of sync')
    # Only continue if this was not cleared before
    this_was_already_cleared = not is_nested_key_contained(_zefui_subscriptions, [a.app_instance_uid, to_ezefref(z_instance), to_ezefref(a.z_component_root)])
    if this_was_already_cleared:
        return  # exit early to avoid double visiting and firing of on_termination callbacks. 
    if not(rae_type(z_instance) == rae_type(a.z_template)):
        raise RuntimeError('Error in zefUI graph crawling: instance and template graph rae type are out of sync')
    # what if we are on a relation type we should traverse according to the template, but the target rae does not correspond to that on the template
    if BT(z_instance)==BT.RELATION_EDGE:
        traverse_dir = target if z_instance|to_ezefref|source == a.came_from else source
        if rae_type(z_instance | to_ezefref | traverse_dir) != rae_type(a.z_template | traverse_dir):     #TODO: this may become source as well
            raise RuntimeError("target attached to monitored edge does not align with what was expected from the template. Exiting early in crawler")

    # Verify the schema locally with the template and instance
    to_frame = to_frame[allow_terminated_relent_promotion][z_instance | tx]
    del _zefui_subscriptions[a.app_instance_uid][to_ezefref(z_instance)][to_ezefref(a.z_component_root)]

    out_edge_types_to_monitor = set(a.z_template | outs | filter[lambda z_ed: str(RT(z_ed))[:6] != 'ZEFUI_'] | tools.ops.map[RT]) - a.outgoing_rts_to_not_monitor
    in_edge_types_to_monitor = set(a.z_template | ins | filter[lambda z_ed: str(RT(z_ed))[:6] != 'ZEFUI_'] | tools.ops.map[RT]) - a.incoming_rts_to_not_monitor
    execute_on_termination_actions(z_instance, a)
    
    # execute once at this point in time, after setting up the subscription for the future
    # Don't walk back down the edge that we came from. Also respect that we cannot compare a EZefRef with None (== overlaoded in C++)
    def is_uzefref_we_came_from(zz: ZefRef) -> bool:
        if a.came_from is None: return False
        return to_ezefref(zz) == a.came_from
    
    # we actually need to traverse one time slice back to see all that was connected for the clearing. After that, move back to this frame
    to_frame = to_frame[allow_terminated_relent_promotion][z_instance | tx]
    
    for ed in z_instance | time_travel[-1] | outs | filter[lambda z: RT(z) in out_edge_types_to_monitor and not is_uzefref_we_came_from(z)] | to_frame:
        d_update = {
            'z_template': a.z_template > RT(ed),
            'came_from': to_ezefref(z_instance),
            }
        make_cb_termination(a | conj[d_update], verify_upstream_rae=False)(ed)       # this will trigger the recursive crawl
    
    for ed in z_instance | time_travel[-1] | ins | filter[lambda z: RT(z) in in_edge_types_to_monitor and not is_uzefref_we_came_from(z)] | to_frame:
        d_update = {
            'z_template': a.z_template < RT(ed),
            'came_from': to_ezefref(z_instance),
            }
        make_cb_termination(a | conj[d_update], verify_upstream_rae=False)(ed)       # this will trigger the recursive crawl
        
    


















def verify_component(z_component_root):
    """
    Run upon the verify first initialization call of a component
    from inside render_component
    """
    if length(z_component_root >> L[RT.ZEFUI_InstanceOf]) != 1:
        raise RuntimeError(f"No unique ZefUI component template specified via >> RT.ZEFUI_InstanceOf for {z_component_root}")



def render_component(z_component_root: ZefRef, app_uid: str=None):
    """crawl along a graph, ui tree recursively.
    Returns something displayable for now when working with bqplot or ipw
    
    z_component_root is optional. It is used for recursive crawls inside a component to be aware of its roots
    
    This function is called recursively on each component in an app. The app_uid 
    is passed downstream. If it is None, it implies that it is called the very first time for
    the app, i.e. it is the root of the app.
    
    Arguments:
        app_uid: str - actually the uid of the app instance. Note that this uid is not to be found on the zefDB graph
    """
    verify_component(z_component_root)
    z_template_root = z_component_root >> RT.ZEFUI_InstanceOf        
    # first time this is called for the app, register in zefUI state
    if app_uid is None:
        app_instance_uid = uuid.uuid4().hex
        context = {}        # this part is only reached the very first time the app is initialized
        _zefui_state[app_uid] = {
            'z_app_root': z_component_root,
            'context': context
            }

    context = _zefui_state[app_uid]['context']    
    a = CrawlArgs(
        z_template=z_template_root,
        z_component_root=z_component_root|to_ezefref,
        context=context,
        app_instance_uid=app_instance_uid, 
        came_from=None,
        incoming_rts_to_not_monitor=set(),
        outgoing_rts_to_not_monitor=set(),
    )
    displayable = construction_crawl(z_component_root, a=a)
    return displayable
    





""" 
Callback cases we need to deal with:
--------------- 3 types of zefUI edges trigger callbacks are treated: --------------------

A) ZEFUI_OnInstantiation:
    - can be attached to a AET, ET or RT: callback will be fired upon the instantiation of the RAE instance
    - in all cases, the ZefRef to the new instance will be passed in as the arg 'z_instance' if the cb function requests this
    - z_template may also be requested

B) ZEFUI_OnTermination:
    - fully analogous to ZEFUI_OnInstantiation
    - z_instance will also be handed the terminated ZefRef (!) in form of a tombstone

C) ZEFUI_OnChange:
    - The most complicated case: this can only be used on AETs
    - simplifies the user's life in the following way: it triggers the cb function if the value of the AET is the thing
      of interest and this changes in any way. There are two conceptually different ways how a field can change:
        1) The latest value assigned to the AET changes
        2) the relation leading to the AET is instantiated (the AET may or may not have been instantiated in the same tx)
    - We could simply add the usual callbacks for the three types of changes, so why add even more complication here?
      The behavior we often want to specify as a component author is how we want to react to a field taking on a new value.
      We don't care which of the two options above led to this change and simply want to provide an effective callback function
      with the signature my_callback(new_value, context, ...).
      ZEFUI provides the ability to provide a single callback function in this case, instead of providing two (or 3 to handle terminations 
      and setting the value to the default value) with the usual language. This often turns out to be all one needs and significantly 
      simplifies the creation of new components.

    






"""




#                                                             __                      _    _                                            
#                          __      __ _ __   __ _  _ __      / _| _   _  _ __    ___ | |_ (_)  ___   _ __   ___                         
#   _____  _____  _____    \ \ /\ / /| '__| / _` || '_ \    | |_ | | | || '_ \  / __|| __|| | / _ \ | '_ \ / __|    _____  _____  _____ 
#  |_____||_____||_____|    \ V  V / | |   | (_| || |_) |   |  _|| |_| || | | || (__ | |_ | || (_) || | | |\__ \   |_____||_____||_____|
#                            \_/\_/  |_|    \__,_|| .__/    |_|   \__,_||_| |_| \___| \__||_| \___/ |_| |_||___/                        
#                                                 |_|                                                                                   


def wrap_user_cb_function(user_provided_fct: Callable, resulting_fct_usage_context: str, component_root: EZefRef, context, parent_uid):
    """The user provides a callback function, e.g. 
    'on_x_change(component_root: ZefRef, context: dict, **kwargs)'
    and can request various arguments (from a list of available options) 
    to be provided during app runtime if the corresponding event occurs.

    How does this work below the hood? The function call is triggered by
    a subscription firing. The subscription has to take a function with one 
    argument as it always does. Hence at setup time of the component instance
    (also at runtime for dynamic fields), zefui needs to create a subscription 
    function and curry in / transform various other args by wrapping the user 
    provided function.

    Potential user provided arguments:
        z_instance: ZefRef
        z_template: ZefRef
        component_root: EZefRef
        context: dict
        parent_uid: Optional[str]    (or should we rather pass the EZefRef?). 
        new_value: str/float/int/bool/time/...  - some AET value type

        ------------ future maybes: --------------
        app_root: EZefRef       # Do we ever need this?

    Args:
        user_provided_fct: Callable[various args of from list described above]
        resulting_fct_usage_context: str

    Returns:
        Callable[ZefRef]: the function being use in the subscription
    """
    # should we even accept normal python functions which are not ef functions?
    # Let's try without this generalization. If we really find a reason that we need this, 
    # we can add it later
    import zefdb
    expected_kw_args = {'z_instance', 'z_template', 'component_root', 'context', 'parent_uid', 'new_value'}

    if not isinstance(user_provided_fct, ZefRef):
        raise TypeError(f"user_provided_fct user_provided_fct={user_provided_fct} passed in that was not a zef function. You cannot use raw python functions to provide as callbacks in zefUI: please decorate your function with '@zef_function(g...) '")

    if rae_type(user_provided_fct) != ET.ZEF_Function:
        raise TypeError(f"user_provided_fct user_provided_fct={user_provided_fct} passed in that was a ZefRef but not not a zef function.")

    # don't figure out the signature and transformation on each function call: do it here
    # once upon instantiation. We can't get it off the zefref: make sure it is compiled and
    # then grab it from the internal cache in zef_functions
    
    # if a zef function was passed, it may not have been run and compiled yet. 
    # Extract it's signature by recompiling it purely for the reason to extract its signature
    fct_compiled = zefdb.zef_functions._local_compiled_zef_functions[zefdb.zef_functions.time_resolved_hashable(user_provided_fct)]
    inspect.signature(fct_compiled)
    sig_param = inspect.signature(fct_compiled).parameters
    print(f"signature found in constructing subscription callback: sig_param={sig_param}")

    for key, val in sig_param.items():
        if val.default != inspect._empty:
            raise RuntimeError(f"Default argument for user specified callback function was provided for zef function {user_provided_fct} in zefUI. It does not make sense to provide defaults here, zefUI will have to inject values. Please modify your function signature and remove the default argument.")
        if val.annotation == inspect._empty:
            raise RuntimeError(f"\033[91m Type hinting MUST be provided for all arguments of callback functions passed to zefUI! This was not the case for the argument '{val}' of function {user_provided_fct} \033[00m")
        if key not in expected_kw_args:
            raise RuntimeError(f"Unexpected keyword argument '{key}' in zef function '{wrap_user_cb_function}' used in zefUI. A function in this context may only take keyword args 'expected_kw_args={expected_kw_args}'")
        print(key)
        print((val.annotation))
        print('----------------------------------------------------------------')

    given_args = {k for k in sig_param.keys()}
    all_args_prepared = {
        'component_root': component_root,
        'context': context,
        'parent_uid': parent_uid,
    }
    args_to_curry = {k: all_args_prepared[k] for k in sig_param.keys() if k not in {'new_value'}}  # some of these can be determined now and will never change
    print(f"args_to_curry={args_to_curry}")

    if 'new_value' in given_args:
        # the case of being used in a subscribe on_value_assignment context
        def fct_used_in_subscription(z: ZefRef):            
            return user_provided_fct(new_value=z|value, **args_to_curry)        # take the new value from the AET itself

    else:
        raise NotImplementedError       # depends on where the function is called from


    return fct_used_in_subscription



