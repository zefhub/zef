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

from ctypes import *

BT = {
    0: "_unspecified",
    1: "ROOT_NODE",
    2: "TX_EVENT_NODE",
    3: "RAE_INSTANCE_EDGE",
    4: "TO_DELEGATE_EDGE",
    5: "NEXT_TX_EDGE",
    6: "ENTITY_NODE",
    7: "ATOMIC_ENTITY_NODE",
    8: "ATOMIC_VALUE_NODE",
    9: "RELATION_EDGE",
    10: "DELEGATE_INSTANTIATION_EDGE",
    11: "DELEGATE_RETIREMENT_EDGE",
    12: "INSTANTIATION_EDGE",
    13: "TERMINATION_EDGE",
    14: "ATOMIC_VALUE_ASSIGNMENT_EDGE",
    15: "DEFERRED_EDGE_LIST_NODE",
    16: "ASSIGN_TAG_NAME_EDGE",
    17: "NEXT_TAG_NAME_ASSIGNMENT_EDGE",
    18: "FOREIGN_GRAPH_NODE",
    19: "ORIGIN_RAE_EDGE",
    20: "ORIGIN_GRAPH_EDGE",
    21: "FOREIGN_ENTITY_NODE",
    22: "FOREIGN_ATOMIC_ENTITY_NODE",
    23: "FOREIGN_RELATION_EDGE",
}

def has_source_target(bt):
    return False
def extract_source_target(data, ptr):
    return ptr,{}

def has_edges(bt):
    return False
def extract_edges(data, ptr):
    return ptr,{}

def has_data_buffer(bt):
    return False
def extract_data_buffer(data, ptr):
    return ptr,{}

c_blob_index = c_int
# Note: need BaseUID as an array of bytes, as apparently C uses that for the alignment.
# c_BaseUID = c_longlong
c_BaseUID = c_ubyte*8
c_Time = c_double
c_TimeSlice = c_int
c_token = c_uint

def convert_BaseUID(raw):
    return ''.join('{:02x}'.format(x) for x in raw)

def convert_Time(raw):
    return {
        "zef_type": "Time",
        "seconds_since_1970": raw
    }

def convert_TimeSlice(raw):
    return {
        "zef_type": "TimeSlice",
        "slice": raw
    }

def convert_EntityType(raw):
    return {
        "zef_type": "EntityType",
        "entity_type_indx": raw
    }

def convert_AtomicEntityType(raw):
    return {
        "zef_type": "AtomicEntityType",
        "value": raw
    }

def convert_RelationType(raw):
    return {
        "zef_type": "RelationType",
        "relation_type_indx": raw
    }

def parse_version_0_1_0(data: bytes):
    start_index = 42
    index_stride = 16
    
    ptr = 0

    blobs = {}

    while True:
        if (ptr // index_stride) * index_stride != ptr:
            ptr = (ptr // index_stride + 1) * index_stride
        if ptr >= len(data):
            break
        index = start_index + (ptr // index_stride)
        assert ptr == (index - start_index) * index_stride, "ptr is not aligned with blob_index"

        bt = BT[int(data[ptr])]
        obj = {"type": bt}

        if bt not in extract_func:
            raise Exception(f"Don't know how to handle blob type {BT[bt]}")
        ptr,details = extract_func[bt](data, ptr)
        obj.update(details)

        # Other versions
        if has_source_target(bt):
            ptr,source_target = extract_source_target(data, ptr)
            obj.update(source_target)
        if has_edges(bt):
            ptr,edge_info = extract_edges(data, ptr)
            obj.update(edge_info)
        if has_data_buffer(bt):
            ptr,data_buffer = extract_data_buffer(data, ptr)
            obj.update(data_buffer)

        blobs[index] = obj

    assert ptr == len(data)

    blobs = combine_deferred_edge_lists(blobs)

    return blobs



def combine_deferred_edge_lists(blobs):
    out = {}
    for index,item in blobs.items():
        if item["type"] == "DEFERRED_EDGE_LIST_NODE":
            continue

        if "subsequent_deferred_edge_list_index" not in item:
            out[index] = item
            continue

        edge_list = []

        deferred_item = item
        while True:
            edge_list += deferred_item["local_edges"]
            if deferred_item["subsequent_deferred_edge_list_index"] == 0:
                break
            
            deferred_item = blobs[deferred_item["subsequent_deferred_edge_list_index"]]
            assert deferred_item["type"] == "DEFERRED_EDGE_LIST_NODE"

        new_item = {**item}
        new_item.pop("local_edges")
        new_item.pop("subsequent_deferred_edge_list_index")
        new_item["edges"] = edge_list

        out[index] = new_item

    return out



def extract_manual_edges(data, ptr, raw):
    size = raw.local_edge_indexes_capacity
    local_edges = (c_blob_index*size).from_buffer_copy(data, ptr)
    local_edges = [x for x in local_edges if x != 0]
    ptr += sizeof(c_blob_index)*size

    edge_details = {
        "local_edges": local_edges,
        "subsequent_deferred_edge_list_index": raw.subsequent_deferred_edge_list_index,
    }
    return ptr,edge_details

def extract_manual_buffer(data, ptr, raw):
    size = raw.buffer_size
    buffer = (c_ubyte*size).from_buffer_copy(data, ptr)
    ptr += size

    buffer_details = {
        "data_buffer": bytes(buffer),
    }
    return ptr,buffer_details

def extract_ROOT_NODE(data, ptr):
    class ROOT_NODE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("actual_written_data_layout_version_info_size", c_short),
            ("actual_written_graph_revision_info_size", c_short),
            ("uid", c_BaseUID),
            ("data_layout_version_info", c_byte*62),
            ("graph_revision_info", c_byte*64),
            ("local_edge_indexes_capacity", c_int),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("edge_indexes", c_blob_index),
        ]

    raw = ROOT_NODE.from_buffer_copy(data, ptr)

    details = {
        "data_layout_version": raw.data_layout_version_info[:raw.actual_written_data_layout_version_info_size],
        "graph_revision": raw.graph_revision_info[:raw.actual_written_graph_revision_info_size],
        "uid": convert_BaseUID(raw.uid),
    }

    ptr += ROOT_NODE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details


def extract_TX_EVENT_NODE(data, ptr):
    class TX_EVENT_NODE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("time", c_Time),
            ("time_slice", c_TimeSlice),
            ("uid", c_BaseUID),
            ("edge_indexes", c_blob_index),
        ]

    raw = TX_EVENT_NODE.from_buffer_copy(data, ptr)

    details = {
        "uid": convert_BaseUID(raw.uid),
        "time": convert_Time(raw.time),
        "time_slice": convert_TimeSlice(raw.time_slice),
    }

    ptr += TX_EVENT_NODE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details


def extract_just_source_target(data, ptr):
    class NEXT_TX_EDGE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("source_node_index", c_blob_index),
            ("target_node_index", c_blob_index),
        ]

    raw = NEXT_TX_EDGE.from_buffer_copy(data, ptr)

    details = {
        "source_node_index": raw.source_node_index,
        "target_node_index": raw.target_node_index,
    }

    ptr += sizeof(NEXT_TX_EDGE)

    return ptr,details

def extract_RAE_INSTANCE_EDGE(data, ptr):
    class RAE_INSTANCE_EDGE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("source_node_index", c_blob_index),
            ("target_node_index", c_blob_index),
            ("edge_indexes", c_blob_index),
        ]

    raw = RAE_INSTANCE_EDGE.from_buffer_copy(data, ptr)

    details = {
        "source_node_index": raw.source_node_index,
        "target_node_index": raw.target_node_index,
    }

    ptr += RAE_INSTANCE_EDGE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details

def extract_TO_DELEGATE_EDGE(data, ptr):
    class TO_DELEGATE_EDGE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("source_node_index", c_blob_index),
            ("target_node_index", c_blob_index),
            ("edge_indexes", c_blob_index),
        ]

    raw = TO_DELEGATE_EDGE.from_buffer_copy(data, ptr)

    details = {
        "source_node_index": raw.source_node_index,
        "target_node_index": raw.target_node_index,
    }

    ptr += TO_DELEGATE_EDGE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details

def extract_ENTITY_NODE(data, ptr):
    class ENTITY_NODE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("entity_type", c_token),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("instantiation_time_slice", c_TimeSlice),
            ("termination_time_slice", c_TimeSlice),
            ("uid", c_BaseUID),
            ("edge_indexes", c_blob_index),
        ]

    raw = ENTITY_NODE.from_buffer_copy(data, ptr)

    details = {
        "entity_type": convert_EntityType(raw.entity_type),
        "instantiation_time_slice": convert_TimeSlice(raw.instantiation_time_slice),
        "termination_time_slice": convert_TimeSlice(raw.termination_time_slice),
        "uid": convert_BaseUID(raw.uid),
    }

    ptr += ENTITY_NODE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details


def extract_ATOMIC_ENTITY_NODE(data, ptr):
    class ATOMIC_ENTITY_NODE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("my_atomic_entity_type", c_token),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("instantiation_time_slice", c_TimeSlice),
            ("termination_time_slice", c_TimeSlice),
            ("uid", c_BaseUID),
            ("edge_indexes", c_blob_index),
        ]

    raw = ATOMIC_ENTITY_NODE.from_buffer_copy(data, ptr)

    details = {
        "my_atomic_entity_type": convert_AtomicEntityType(raw.my_atomic_entity_type),
        "instantiation_time_slice": convert_TimeSlice(raw.instantiation_time_slice),
        "termination_time_slice": convert_TimeSlice(raw.termination_time_slice),
        "uid": convert_BaseUID(raw.uid),
    }

    ptr += ATOMIC_ENTITY_NODE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details

def extract_ATOMIC_VALUE_NODE(data, ptr):
    class ATOMIC_VALUE_NODE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("my_atomic_entity_type", c_token),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("buffer_size", c_uint),
            ("edge_indexes", c_blob_index),
            ("data_buffer", c_byte),
        ]

    raw = ATOMIC_VALUE_NODE.from_buffer_copy(data, ptr)
    local_edges = [x for x in raw.edge_indexes if x != 0]

    details = {
        "my_atomic_entity_type": convert_AtomicEntityType(raw.my_atomic_entity_type),
        "local_edges": local_edges,
    }

    ptr += ATOMIC_VALUE_NODE.data_buffer.offset

    ptr,buffer_details = extract_manual_buffer(data, ptr, raw)
    details.update(buffer_details)

    return ptr,details

def extract_RELATION_EDGE(data, ptr):
    class RELATION_EDGE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("hostage_flags", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("relation_type", c_token),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("source_node_index", c_blob_index),
            ("target_node_index", c_blob_index),
            ("instantiation_time_slice", c_TimeSlice),
            ("termination_time_slice", c_TimeSlice),
            ("uid", c_BaseUID),
            ("edge_indexes", c_blob_index),
        ]

    raw = RELATION_EDGE.from_buffer_copy(data, ptr)

    details = {
        "hostage_flags": raw.hostage_flags,
        "relation_type": convert_RelationType(raw.relation_type),
        "instantiation_time_slice": convert_TimeSlice(raw.instantiation_time_slice),
        "termination_time_slice": convert_TimeSlice(raw.termination_time_slice),
        "source_node_index": raw.source_node_index,
        "target_node_index": raw.target_node_index,
        "uid": convert_BaseUID(raw.uid),
    }

    ptr += RELATION_EDGE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details

def extract_ATOMIC_VALUE_ASSIGNMENT_EDGE(data, ptr):
    class ATOMIC_VALUE_ASSIGNMENT_EDGE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("my_atomic_entity_type", c_token),
            ("buffer_size", c_uint),
            ("source_node_index", c_blob_index),
            ("target_node_index", c_blob_index),
            ("data_buffer", c_byte),
        ]

    raw = ATOMIC_VALUE_ASSIGNMENT_EDGE.from_buffer_copy(data, ptr)

    details = {
        "my_atomic_entity_type": convert_AtomicEntityType(raw.my_atomic_entity_type),
        "source_node_index": raw.source_node_index,
        "target_node_index": raw.target_node_index,
    }

    # ptr += ATOMIC_VALUE_ASSIGNMENT_EDGE.data_buffer.offset

    # There is a bug in the C code which means the size of the blob is bigger
    # than it actually is. Hence we need some custom logic here.
    buf_ptr = ptr + ATOMIC_VALUE_ASSIGNMENT_EDGE.data_buffer.offset

    _,buffer_details = extract_manual_buffer(data, buf_ptr, raw)
    details.update(buffer_details)

    ptr += sizeof(ATOMIC_VALUE_ASSIGNMENT_EDGE) - 1 + len(buffer_details["data_buffer"]) + 1

    return ptr,details

def extract_DEFERRED_EDGE_LIST_NODE(data, ptr):
    class DEFERRED_EDGE_LIST_NODE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("preceding_edge_list_index", c_blob_index),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("edge_indexes", c_blob_index),
        ]

    raw = DEFERRED_EDGE_LIST_NODE.from_buffer_copy(data, ptr)

    details = {}

    ptr += DEFERRED_EDGE_LIST_NODE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details

def extract_ASSIGN_TAG_NAME_EDGE(data, ptr):
    class ASSIGN_TAG_NAME_EDGE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("chars_used_in_buffer", c_short),
            ("local_edge_indexes_capacity", c_int),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("source_node_index", c_blob_index),
            ("target_node_index", c_blob_index),
            ("rel_ent_tag_name_buffer", (c_byte*50)),
            ("edge_indexes", c_blob_index),
        ]

    raw = ASSIGN_TAG_NAME_EDGE.from_buffer_copy(data, ptr)

    details = {
        "source_node_index": raw.source_node_index,
        "target_node_index": raw.target_node_index,
        "data_buffer": bytes(raw.rel_ent_tag_name_buffer[:raw.chars_used_in_buffer]),
    }

    ptr += ASSIGN_TAG_NAME_EDGE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details


def extract_FOREIGN_GRAPH_NODE(data, ptr):
    class FOREIGN_GRAPH_NODE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("internal_foreign_graph_index", c_int),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("uid", c_BaseUID),
            ("edge_indexes", c_blob_index),
        ]

    raw = FOREIGN_GRAPH_NODE.from_buffer_copy(data, ptr)

    details = {
        "internal_foreign_graph_index": raw.internal_foreign_graph_index,
        "uid": convert_BaseUID(raw.uid),
    }

    ptr += FOREIGN_GRAPH_NODE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details

def extract_FOREIGN_ENTITY_NODE(data, ptr):
    class FOREIGN_ENTITY_NODE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("entity_type", c_token),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("uid", c_BaseUID),
            ("edge_indexes", c_blob_index),
        ]

    raw = FOREIGN_ENTITY_NODE.from_buffer_copy(data, ptr)

    details = {
        "entity_type": convert_EntityType(raw.entity_type),
        "uid": convert_BaseUID(raw.uid),
    }

    ptr += FOREIGN_ENTITY_NODE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details

def extract_FOREIGN_ATOMIC_ENTITY_NODE(data, ptr):
    class FOREIGN_ATOMIC_ENTITY_NODE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("atomic_entity_type", c_token),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("uid", c_BaseUID),
            ("edge_indexes", c_blob_index),
        ]

    raw = FOREIGN_ATOMIC_ENTITY_NODE.from_buffer_copy(data, ptr)

    details = {
        "entity_type": convert_EntityType(raw.entity_type),
        "uid": convert_BaseUID(raw.uid),
    }

    ptr += FOREIGN_ATOMIC_ENTITY_NODE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details

def extract_FOREIGN_RELATION_EDGE(data, ptr):
    class FOREIGN_RELATION_EDGE(LittleEndianStructure):
        _fields_ = [
            ("this_BlobType", c_ubyte),
            ("local_edge_indexes_capacity", c_int),
            ("relation_type", c_token),
            ("source_node_index", c_blob_index),
            ("target_node_index", c_blob_index),
            ("subsequent_deferred_edge_list_index", c_blob_index),
            ("uid", c_BaseUID),
            ("edge_indexes", c_blob_index),
        ]

    raw = FOREIGN_RELATION_EDGE.from_buffer_copy(data, ptr)

    details = {
        "relation_type": convert_RelationType(raw.relation_type),
        "uid": convert_BaseUID(raw.uid),
        "source_node_index": raw.source_node_index,
        "target_node_index": raw.target_node_index,
    }

    ptr += FOREIGN_RELATION_EDGE.edge_indexes.offset

    ptr,edge_details = extract_manual_edges(data, ptr, raw)
    details.update(edge_details)

    return ptr,details


extract_func = {
    "ROOT_NODE": extract_ROOT_NODE,
    "TX_EVENT_NODE": extract_TX_EVENT_NODE,
    "NEXT_TX_EDGE": extract_just_source_target,
    "RAE_INSTANCE_EDGE": extract_RAE_INSTANCE_EDGE,
    "TO_DELEGATE_EDGE": extract_TO_DELEGATE_EDGE,
    "ENTITY_NODE": extract_ENTITY_NODE,
    "ATOMIC_ENTITY_NODE": extract_ATOMIC_ENTITY_NODE,
    "ATOMIC_VALUE_NODE": extract_ATOMIC_VALUE_NODE,
    "RELATION_EDGE": extract_RELATION_EDGE,
    "DELEGATE_INSTANTIATION_EDGE": extract_just_source_target,
    "DELEGATE_RETIREMENT_EDGE": extract_just_source_target,
    "INSTANTIATION_EDGE": extract_just_source_target,
    "TERMINATION_EDGE": extract_just_source_target,
    "ATOMIC_VALUE_ASSIGNMENT_EDGE": extract_ATOMIC_VALUE_ASSIGNMENT_EDGE,
    "DEFERRED_EDGE_LIST_NODE": extract_DEFERRED_EDGE_LIST_NODE,
    "ASSIGN_TAG_NAME_EDGE": extract_ASSIGN_TAG_NAME_EDGE,
    "FOREIGN_GRAPH_NODE": extract_FOREIGN_GRAPH_NODE,
    "ORIGIN_RAE_EDGE": extract_just_source_target,
    "ORIGIN_GRAPH_EDGE": extract_just_source_target,
    "FOREIGN_ENTITY_NODE": extract_FOREIGN_ENTITY_NODE,
    "FOREIGN_ATOMIC_ENTITY_NODE": extract_FOREIGN_ATOMIC_ENTITY_NODE,
    "FOREIGN_RELATION_EDGE": extract_FOREIGN_RELATION_EDGE,
}




