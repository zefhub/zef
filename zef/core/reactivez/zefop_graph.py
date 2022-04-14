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




def set_up_zefop_graph():
    """
    Run this function to prepare the graph with all builtin zefops.
    
    For now: instantiate everything from scratch upon loading zefDB.
    Disadvantage: identical functions have different identity on 
    different processes.

    This is planned to be changed and have one distributed graph with builtin 
    zefops implementations.

    Instead of the zefops pointing to implementation functions vi RTs and 
    lookup tables, have ZefOps fundamentally refer to the identity of the elementary
    zefop.: They store the abstract entity for generality and an optional EZefRef
    for performance.
    """
    from zef.core.reactivez import _rz_state   # add the builtin_zefop_graph here
    from zef import Graph

    print(f">>>>> setting up zefop graph...!")
    g_zo = Graph()
    _rz_state['builtin_zefop_graph'] = g_zo     
    _rz_state['rz_zefop_dispatch_dict'] = add_zefop_imps_as_zef_funcs(g_zo)

    





def add_zefop_imps_as_zef_funcs(g_zo):
    
    from zef import ZefRef, ET, RT, GraphDelta, func
    from zef.ops import collect, run, execute, get, origin_rae, Z




    def generate_zefop(name: str, rz_imp, helper_function=None) -> ZefRef:
        """ 
        Given a single implementation function for streams,
        generate the structure for the zefop on the graph

        TODO: check if the zef function is not defined on the local process graph:
        gather it and merge it in.
        """
        return [
            (ET.ZEF_ElementaryOp['op'], RT.ZEF_StreamImplementation, ET.ZEF_DispatchPoint['d']),
            (Z['op'], RT.Name, name),
            (Z['d'], RT.ZEF_Dispatch, origin_rae(rz_imp)),
            *([(Z['d'], RT.ZEF_HelperFunction, origin_rae(helper_function))] if helper_function is not None else []),
            ] | func[GraphDelta] | collect | g_zo | run[execute] | get['op'] | collect
        




    # ---------------------- map ----------------------

    @func(g_zo)
    def map_imp_rz(items, f, zefop_state, z_self):
        """
        Implementation of the map operator for RZ streams / observables.
        This operator can directly work on batches / lists that are passed in
        at one time.
        """
        return (
            [f(el) for el in items],    # output items
            [],                         # future jobs
            [],                         # Effects to execute
            None,                       # internal op state
        )


    @func(g_zo)
    def filter_imp_rz(items, f_pred, zefop_state, z_self):
        """
        Implementation of the filter operator for RZ streams / observables.
        This operator can directly work on batches / lists that are passed in
        at one time.
        """
        return (
            [el for el in items if f_pred(el)],    # output items
            [],                         # future jobs
            [],                         # Effects to execute
            None,                       # internal op state
        )


    # ---------------------- delay: requires a helper function ----------------------

    @func(g_zo)
    def delay_imp_rz(items, duration, zefop_state, z_self):
        """
        Forwards all items with a fixed temporal delay.
        """
        return (
            [],                              # output items
            [(duration, (items,) )],         # future jobs
            [],                              # effects
            None
        )


    @func(g_zo)
    def delay_helper_rz(helper_args, zefop_state, z_self):
        items, = helper_args
        return (
            items,                  # output items
            [],                     # future jobs
            [],                     # effects
            None
        )






    # this dict will be used at the point when the zefop is given and translated / broken down
    # into the elementary operators on the graph. The zef function to hook up is determined
    # from this dict, given the RT
    return {
        RT.Map: generate_zefop(name='map', rz_imp=map_imp_rz),
        RT.Filter: generate_zefop(name='filter', rz_imp=filter_imp_rz),
        RT.Delay: generate_zefop(name='delay', rz_imp=delay_imp_rz, helper_function=delay_helper_rz),
    }



