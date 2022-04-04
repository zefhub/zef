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









    
#                                   ____                _                 _     _____  _                                                 
#                                  / ___|  ___   _ __  | |_  _ __   ___  | |   |  ___|| |  ___  __      __                               
#   _____  _____  _____  _____    | |     / _ \ | '_ \ | __|| '__| / _ \ | |   | |_   | | / _ \ \ \ /\ / /    _____  _____  _____  _____ 
#  |_____||_____||_____||_____|   | |___ | (_) || | | || |_ | |   | (_) || |   |  _|  | || (_) | \ V  V /    |_____||_____||_____||_____|
#                                  \____| \___/ |_| |_| \__||_|    \___/ |_|   |_|    |_| \___/   \_/\_/                                 
#                                                                                                                                        


# state kept: this state may become specific to a thread in future 
# immediate_jobs = [...]          
# future_jobs = [...]             # [(t_m, helper_args)]
# op_states = {...}               # keys: EZefRef


# immediate_jobs = [
#     (priority1: float, z_trafo1: EZefRef, items1: list),
#     (priority2: float, z_trafo2: EZefRef, items2: list),
#     ...
# ]

# future_jobs = [
#     (t1: time, z_trafo: EZefRef, args1: Any)
# ]




# 'hello' | push[my_pushable_stream] | run            # must be a ET.PushableStream wrapped


# # within RZ: do we also have a low level push?
# _push([1,2,42], z_stream)
# _push(42, z_awaitable)


# dictionary containing all RZ state in the process. Possibly from multiple threads.



from ..reactivez import _rz_state





def start_zef_runtime():
    """
    Each thread / process that wants its own process graph 
    can call this function to set everything up.
    
    The state is intentionally not exposed and created for
    each thread separately to avoid misuse and violating thread
    safety of this function.
    """
    import threading
    import time
    from typing import List
    from zef import ops, internals, Graph, ZefRef, EZefRef, Error, RT
    from zef.ops import single, target, collect, L, now



    def get_zefop(z_trafo_rel: ZefRef):
        ops = z_trafo_rel > L[RT.Operator] | collect        # TODO: in which time slice should we look at this?
        if len(ops) != 1:
            return Error(f'Could not find exactly one operator attached to the RT.Transformation {z_trafo_rel}')
        # args = ops | single | attempt[Z >> RT.ZEF_Args ]][0] | c
        return ops | single | target >> RT.ZEF_StreamImplementation >> RT.ZEF_Dispatch | collect          # FIXME: assumes that there is only a single overloaded function



    def get_helper_function(z_trafo: EZefRef):
        """
        Performs caching of helper function when run for the first time
        """        
        assert isinstance(z_trafo, EZefRef)
        helper_fct, curried_args = cached_helper_fcts_and_args.get(z_trafo, None)        
        # look up on graph and add to cache
        if helper_fct is None:
            helper_fct = get_helper_function(z_trafo)           #TODO: noooooooooo!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Fix this
            cached_helper_fcts_and_args[z_trafo] = helper_fct

        return helper_fct




    def get_curried_args(z_trafo_rel: ZefRef):
        """
        curried args for operators are stored on the process graph as a zef list.
        Given the edge representing the transformation, this
        function retrieves these args.    
        """
        try:
            return z_trafo_rel > RT.Operator >> RT.ZEF_Args | zef_list_all | collect
        except:
            return ()







    this_thread_id = threading.get_ident()

    if this_thread_id in _rz_state:
        print(f'Zef Runtime was already initialized for this thread with thread id {this_thread_id}. Exiting early from start_zef_runtime...')
        return
    print("not exiting early...")

    # specific to the thread
    g_process = (internals.get_local_process_graph()        
            if len(_rz_state) == 0 else               # this will be true if the very first main process is launched
            Graph()                                         # must be another thread started in this process. Give it a new graph
        )

    immediate_jobs = []          # (priority: float, z_trafo: EZefRef, items: list)     -   passed in as args into the rz implementation functions
    future_jobs = []             # [(t_m, z_trafo: EZefRef, helper_args: dict)]        -   passed in as args into the helper functions
    op_states = {}               # keys: (EZefRef, dict/None )                          -   the specific state for each specific op (pipeline part) is passed in to both the operator implementation AND the helper

    cached_ops_and_curried_args = {}    # keys: EZefRef of the rel instance. Values: (function, args: Tuple)
    cached_helper_fcts_and_args = {}    # keys: EZefRef of the rel instance. Values: (function, args: Tuple)



    def queue_future_jobs(new_jobs):
        pass                       # TODO!!!!!!!!

        

    def check_and_process_future_jobs():
        """ 
        This function is called at
        A) after all immediate jobs have been performed to check 
        """
        while True:
            if future_jobs != [] and ops.now() >= future_jobs[-1][0]:
                t_this_job, z_trafo, helper_args = future_jobs.pop()
                helper_fct = get_helper_function(z_trafo)
                op_state = op_states.get(z_trafo, None)
                # vals_in_output_stream, upcoming_jobs, effects_to_run, new_op_state = helper_fct(**helper_args, op_state=)  #TODO


                # TODO !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                
                process_immediate_jobs(call_check_and_process_future_jobs=False)        # maybe there are some more immediate jobs
            else:
                break
            




    def process_immediate_jobs(call_check_and_process_future_jobs=True):
        """ 
        Equivalent to a scheduler, but slightly different mental image.
        Keep track of jobs and within which time slice of the project 
        graph they should be handled.

        Essentially run a while loop synchronously until all immediate tasks
        are worked off. Then check if any future job lies in the past.
        """
        while immediate_jobs !=[]:
            # any sorting of jobs by priority can come here ...        
            immediate_jobs.sort(key=lambda v: v[0])     # sort by priority: highest priority at the end to be popped off
            job = immediate_jobs.pop()                  # so mutaty :(
            process_single_job_step(job)                # this function call may also add new jobs and future_jobs to the external state

        if call_check_and_process_future_jobs:
            check_and_process_future_jobs()     # check whether any of the future jobs can be done now

    
    

    def process_single_job_step(job):
        priority, z_trafo, items = job
        op_imp_fct = get_zefop(z_trafo)
        curried_args = get_curried_args(z_trafo)
        op_state = op_states.get(z_trafo, None)


        print(f"about to execute func: {op_state=}  {curried_args=}    {items=}")
        vals_in_output_stream, upcoming_jobs, effects_to_run, new_op_state = op_imp_fct(items, *curried_args, zefop_state=op_state, z_self=z_trafo)
        
        assert effects_to_run == []     # enable this later
        op_states[z_trafo] = new_op_state        
        queue_future_jobs(upcoming_jobs)
        _push(vals_in_output_stream, target(z_trafo))





    # def process_single_job_step(job):

    #     print(f"data {x} flowing through pipe {z_stream_or_awaitable}")
    #     for trafo in z_stream_or_awaitable > L[RT.Transformation]:
    #         # look up the actual operator: this may have been compiled already with all args curried in
    #         state_key = EZefRef(trafo)
    #         op_state = op_states[state_key]
    #         vals_in_output_stream, upcoming_jobs, effects_to_run, new_op_state = appropriate_zefop_rz_imp(item_batch=x, _rz_operator_state=op_state)

    #         op_states[state_key] = new_op_state
            
    #         # The output result from one trafo are the items in the target stream.
    #         # Multiple transformations may be attached to this stream.
    #         # Create an immediate job for each of these attached transformations.

    #         # each immediate job consists of (priority2: float, z_trafo2: EZefRef, items2: list)
    #         immediate_jobs.append()
    


    
    def _push(items: List, z_stream_or_awaitable: EZefRef):
        """
        low level function never to be used at the user level. 
        It may be used by effect handlers though.
        
        e.g. 'hello' | push[z_stream] | run
        would dispatch to a handler: If the 
        """
        print(f">>> in _push: items {items} flowing through {z_stream_or_awaitable} ")

        if Graph(z_stream_or_awaitable) != g_process:
            raise RuntimeError(f'stream passed to _push function was not contained in this threads process graph: {z_stream_or_awaitable=}')
        assert isinstance(items, list) or isinstance(items,tuple)

        if items == []: return      # exit early if there are no elements
        
        # always use the latest time slice at the time of push?
        for z_trafo in now(z_stream_or_awaitable) > L[RT.Transformation] | collect:
            priority = 0.0
            print(f"appending job {(priority, z_trafo, items)=}")
            immediate_jobs.append((priority, z_trafo, items))

        process_immediate_jobs()                                        # get the jobs done




    _rz_state[this_thread_id] = {
        'dataflow_graph': g_process,
	    'push_fct': _push,
    	# 'op_states': {...},
    	# 'effect_states': {effect_ezr: ...},		
    }
    
