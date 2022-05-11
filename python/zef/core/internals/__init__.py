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

# Note: This should never depend on core at the top level!

from ...pyzef.internals import (
    AtomicEntityTypeStruct,
    BaseUID,
    BlobType,
    BlobTypeStruct,
    EntityTypeStruct,
    EternalUID,
    FinishTransaction,
    GraphData,
    Graph_from_ptr,
    KW,
    MMAP_STYLE_ANONYMOUS,
    MMAP_STYLE_AUTO,
    MMAP_STYLE_FILE_BACKED,
    NamedHeadRange,
    ObservablesDictElement,
    RelationTypeStruct,
    StartTransactionReturnTx,
    Subscription,
    UpdateHeads,
    UpdatePayload,
    ZefEnumStruct,
    ZefEnumStructPartial,
    ZefRefUID,
    add_entity_type,
    add_enum_type,
    add_relation_type,
    add_keyword,
    all_entity_types,
    all_enum_types_and_values,
    all_relation_types,
    apply_update,
    blob_to_json,
    create_graph_from_bytes,
    create_update_heads,
    create_update_payload,
    created_token_list,
    current_zefdb_protocol_version,
    delete_graphdata,
    delegate_to_ezr,
    early_token_list,
    ezr_to_delegate,
    get_data_layout_version_info,
    get_enum_value_from_string,
    get_graph_revision_info,
    get_latest_complete_tx_node,
    get_local_process_graph,
    graph_as_UpdatePayload,
    gtd_info_str,
    has_delegate,
    heads_apply,
    initialise_butler,
    initialise_butler_as_master,
    is_BaseUID,
    is_EternalUID,
    is_ZefRefUID,
    is_aet_a_enum,
    is_aet_a_quantity_float,
    is_aet_a_quantity_int,
    is_any_UID,
    is_delegate,
    is_delegate_group,
    is_root,
    is_up_to_date,
    list_graph_manager_uids,
    login,
    logout,
    make_random_uid,
    max_zefdb_protocol_version,
    memory_details,
    merge_atomic_entity_,
    merge_entity_,
    merge_relation_,
    num_blob_indexes_to_move,
    pageout,
    parse_payload_update_heads,
    partial_hash,
    root_node_blob_index,
    set_data_layout_version_info,
    set_graph_revision_info,
    show_blob_details,
    size_of_blob,
    sort_uzr,
    sort_uzr_magnitude,
    sort_zr,
    sort_zr_magnitude,
    start_connection,
    stop_butler,
    stop_connection,
    to_uid,
    validate_message_version,
    wait_for_auth,
)

from ...pyzef.main import ZefRef

# ----------------------------- monkey patch to allow dispatching on enums ---------------------------------------
# For some reason we need to pass it through a lambda fct to get the bindings to work.
# BlobType.__rrshift__ = lambda bt, any_zr_type: fct__rrshift__(bt, any_zr_type)
# BlobType.__rlshift__ = lambda bt, any_zr_type: fct__rlshift__(bt, any_zr_type)
# BlobType.__gt__ = lambda bt, any_zr_type: fct__gt__(bt, any_zr_type)
# BlobType.__lt__ = lambda bt, any_zr_type: fct__lt__(bt, any_zr_type)

BlobType.__str__ = lambda self: repr(self).split(':')[0][10:]   # strip away the 'BlobType. ...' and out <>

from .rel_ent_classes import ET, RT, EN, AET
from .merges import register_merge_handler
register_merge_handler()

BT = BlobTypeStruct()

def safe_current_task():
    import asyncio
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None
    
global_transaction_task = {}
from contextlib import contextmanager
@contextmanager
def Transaction(g, wait=None):
    global global_transaction_task
    from ...pyzef.zefops import uid

    cur_task = safe_current_task()
    prev_val = global_transaction_task.get(uid(g), None)

    if prev_val is not None and prev_val != cur_task:
        raise Exception("Can't open a Transaction from a different task to the original Transaction. Note that ZefDB is not safe to create simultaneous transactions from different asyncio tasks. Ideally, there should be no asyncio task switch occuring during a transaction.")

    global_transaction_task[uid(g)] = cur_task
    current_tx = StartTransactionReturnTx(g)
    try:
        yield current_tx
    finally:
        if prev_val is None and uid(g) in global_transaction_task:
            del global_transaction_task[uid(g)]
        else:
            global_transaction_task[uid(g)] = prev_val
        if wait is None:
            FinishTransaction(g)
        else:
            FinishTransaction(g, wait)



def assign_value_imp(z, value):
    from .._ops import is_a
    from ...pyzef.zefops import SerializedValue, assign_value as c_assign_value
    from ..serialization import serialize, serialization_mapping

    assert isinstance(z, ZefRef) or isinstance(z, EZefRef)
    if is_a(z, AET.Serialized):
        if type(z) in serialization_mapping:
            from json import dumps
            value = SerializedValue("tools.serialize", dumps(serialize(value)))
        else:
            raise Exception(f"Don't know how to serialize a type of {val}")
    c_assign_value(z, value)
