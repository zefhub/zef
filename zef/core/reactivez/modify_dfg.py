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

from ..reactivez import get_runtime_state







def decompose_operators(a_left, a_right):
    """ 
    return the parsed data to give clear instructions what to
    do on the Process Graph.
    Return a dict with the relevant data that we need to 
    instantiate everything on the process graph broken down.

    TODO: also account for "run[my_func]" if this is attached at the end
    """
    from zef.core.op_structs import SubscribingOp
    from zef import RT

    concrete_awaitable = a_left.initial_stream          # TODO: in the future it will change that this is always an abstract RAE
    all_el_ops = (*a_left.el_ops.el_ops, *a_right.el_ops)

    if isinstance(a_right, SubscribingOp):
        evaluating_op_rel = RT.Subscribe
    elif isinstance(a_right, SubscribingOp):
        evaluating_op_rel = RT.Run
    else:
        raise TypeError()
    
    return (
        concrete_awaitable,
        all_el_ops,
        (evaluating_op_rel, (a_right.func, ))         # there may be more than one arg curried in the future: currently this is always stored as 'func'
    )





def find_existing_result_aw(z_stream, op):
    """
    Does the concrete awaitable from this specific
    transformation already exist? If yes, return it.
    """
    return None             # TODO!!!!!!!!!!!!



def pipeline_construction_step(txcontent__latest_awaitable, op):
    """
    latest_awaitable: either a ZefRef or a Z["s4"].
    Returns a tuple (txrequest_list, last_awaitable_indx)
    op: the next operator step. e.g. (RT.Map, (f1, ))
    There are two options: 
    
    A)  Either the next step leading to the next stream exists.
        This means the transformation step must be identical and
        already reified on the graph: just return the concrete 
        resulting stream

    B)  If the exact transformation does not exist yet: create a new
        list that will be the content of the GraphDelta and append
        the changes that adding this step would incur.

    - if the latest_awaitable is an int (indicating the awaitable 
      number to be used inside Z), there's no way that the stream exists.
      
    """
    import zef
    from zef import ZefRef, GraphDelta, func, RT, ET
    from zef.ops import gather, collect, run, execute, Z
    pg = get_runtime_state()['dataflow_graph']
    print(f"{op=}")
    prev_tx_content, latest_aw = txcontent__latest_awaitable
    
    if isinstance(latest_aw, ZefRef):
        existing_result_aw = find_existing_result_aw(latest_aw, op)
        # If the stream from this transformation already exists, exit early
        if existing_result_aw is not None:
            return prev_tx_content, existing_result_aw

    prev_aw = Z[f"aw{latest_aw}"] if isinstance(latest_aw, int) else latest_aw
    next_awaitable_indx = 0 if isinstance(latest_aw, ZefRef) else latest_aw + 1

    # merge in the full operator used in this step if it is not present     #TODO
    # ensure the data (zef function or other zef values) that are curried into the op for this step exist on this graph   #TODO
    # If it is a "Run" / Subscribe, the main relation should reflect that: don't be "RT.ZEF_Transform" in this case

    # ----------------------------- do we need to merge the operator implementation over? -----------------------------------
    op_rel, curried_args = op
    op_rel_str = repr(op_rel)
    if not op_rel_str in pg:
        dispatch_dict = zef.core.reactivez._rz_state['rz_zefop_dispatch_dict']
        if op_rel not in dispatch_dict:
            raise RuntimeError(f"The operator represented by {op_rel} was not found in the ReactiveZ Operator graph and dispatch dict")
        z_op = dispatch_dict[op_rel] 
        z_op | gather | func[GraphDelta] | collect | pg | run[execute]

        
    print(f"{curried_args=}")


    new_tx_content = (
        *prev_tx_content,
        (prev_aw, RT.ZEF_Transform[f"transform{next_awaitable_indx}"], ET.ZEF_Awaitable[f"aw{next_awaitable_indx}"]),
        (Z[f"transform{next_awaitable_indx}"], RT.ZEF_Operator[f"op_rel{next_awaitable_indx}"], z_op),        # TODO: this is a placeholder for a linked_op
        (Z[f"op_rel{next_awaitable_indx}"], RT.ZEF_Args, ET.ZEF_List)
    )
    return new_tx_content, next_awaitable_indx     # the result streams internal names count up by 1
        





def reify_operator_pipeline_on_dfg(a_left, a_right):
    """ 
    This function is called by the evaluation engine once
    an awaitable is fused with a subscribe / run.
    In contrast to lazy values, this changes the program state
    by instantiating the relevant data pipelines on the dataflow
    graph.

    It is also worth noting that this function is only Called when

    """

    from zef.ops import reduce, first, collect, run, execute
    from zef import func, EZefRef, GraphDelta

    pg = get_runtime_state()['dataflow_graph']
    concrete_awaitable, all_el_ops, terminating_op = decompose_operators(a_left, a_right)
    
    print("\n\n---------- after decomposition --------- \n\n")

    print(concrete_awaitable)
    print(all_el_ops)
    print(terminating_op)

    print('^^^^^^^^')


    assert isinstance(concrete_awaitable, EZefRef)
    s_ini = concrete_awaitable | pg | run[execute]                  # make sure that this is merged into the local graph


    # iterate over states of form ([], last_awaitable)
    all_el_ops | reduce[pipeline_construction_step][((), s_ini)] | first | func[GraphDelta] | collect | pg | run[execute]
    return "TODO: return the resulting stream here"



