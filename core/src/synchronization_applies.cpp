// Copyright 2022 Synchronous Technologies Pte Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


// This function is dirty - just because the ones lower down have extra args we
// might not know when calling this one.
void apply_action_lookup(GraphData& gd, EZefRef uzr, bool fill_caches) {
    switch (get<BlobType>(uzr)) {
    case BlobType::ATTRIBUTE_ENTITY_NODE: {
        apply_action_ATTRIBUTE_ENTITY_NODE(gd, uzr, fill_caches); 
        break;
    }
    case BlobType::ENTITY_NODE: {
        apply_action_ENTITY_NODE(gd, uzr, fill_caches); 
        break;
    }											 
    case BlobType::RELATION_EDGE: {
        apply_action_RELATION_EDGE(gd, uzr, fill_caches); 
        break;
    }									 
    case BlobType::FOREIGN_ENTITY_NODE: {
        apply_action_FOREIGN_ENTITY_NODE(gd, uzr, fill_caches);
        break;
    }
    case BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE: {
        apply_action_FOREIGN_ATTRIBUTE_ENTITY_NODE(gd, uzr, fill_caches);
        break;
    }
    case BlobType::FOREIGN_RELATION_EDGE: {
        apply_action_FOREIGN_RELATION_EDGE(gd, uzr, fill_caches);
        break;
    }
    default:
        print_backtrace();
        throw std::runtime_error("Unexpected blob type in apply_action_lookup: " + to_str(get<BlobType>(uzr)));
    }
}

void insert_uid_lookup(GraphData & gd, BaseUID uid, blob_index indx) {
    // gd->key_dict.map[get_uid_as_hex_str(uzr)] = index(uzr);
    // (*gd.key_dict)[get_blob_uid(uzr)] = index(uzr);
    auto ptr = gd.uid_lookup->get_writer();
    ptr->append(uid, indx, ptr.ensure_func());
}
void insert_euid_lookup(GraphData & gd, EternalUID uid, blob_index indx) {
    auto ptr = gd.euid_lookup->get_writer();
    ptr->append(uid, indx, ptr.ensure_func());
}
void insert_tag_lookup(GraphData & gd, std::string tag, blob_index indx) {
    // (*gd.key_dict)[name] = index(uzr);
    auto ptr = gd.tag_lookup->get_writer();
    ptr->append(tag, indx, ptr.ensure_func());
}
    
void insert_av_hash_lookup(GraphData & gd, const value_variant_t & value, blob_index indx) {
    auto compare_func = internals::create_compare_func_for_value_node(gd, &value);
    auto ptr = gd.av_hash_lookup->get_writer();
    ptr->append(value_hash(value), indx, compare_func, ptr.ensure_func());
}

void pop_uid_lookup(GraphData & gd, BaseUID uid, blob_index indx) {
    if(uid == BaseUID()) {
        std::cerr << "We are trying to pop a UID which is empty. This should never happen!" << std::endl;
        std::cerr << "ezr: " << EZefRef{indx, gd} << std::endl;
    }
    auto ptr = gd.uid_lookup->get_writer();
    ptr->_pop(uid, indx, ptr.ensure_func(true));
}
void pop_euid_lookup(GraphData & gd, EternalUID uid, blob_index indx) {
    auto ptr = gd.euid_lookup->get_writer();
    ptr->_pop(uid, indx, ptr.ensure_func(true));
}
void remove_tag_lookup(GraphData & gd, std::string tag, blob_index indx) {
    // (*gd.key_dict)[name] = index(uzr);
    auto ptr = gd.tag_lookup->get_writer();
    ptr->_pop(tag, indx, ptr.ensure_func(true));
}
void pop_av_hash_lookup(GraphData & gd, const value_variant_t & value) {
    auto ptr = gd.av_hash_lookup->get_writer();
    auto compare_func = internals::create_compare_func_for_value_node(gd, &value);
    ptr->_pop(compare_func, ptr.ensure_func(true));
}

void apply_action_ROOT_NODE(GraphData& gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::ROOT_NODE);
    if(fill_caches) {
        if(is_delegate(uzr)) {
        } else {
            insert_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
        }
    }
}

void unapply_action_ROOT_NODE(GraphData& gd, EZefRef uzr, bool fill_caches) {
    if(is_delegate(uzr)) {
    } else {
        throw std::runtime_error("Should never be undoing the root node.");
    }
}

void apply_action_ATTRIBUTE_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::ATTRIBUTE_ENTITY_NODE);
    auto & node = get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(uzr);
    // we need to distinguish whether this is a delegate entity or an instance: these get different keys				
    if(fill_caches) {
        if (is_delegate(uzr)) {
            // std::string name = type_name(uzr);
            // insert_tag_lookup(gd, name, index(uzr));
            // insert_tag_lookup(gd, "TO_DELEGATE_EDGE_edge." + name, index(uzr < BT.TO_DELEGATE_EDGE));
        }
        else {
            // gd->key_dict.map[get_uid_as_hex_str(uzr)] = index(uzr);
            insert_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
        }
        if(is_delegate(uzr)) {
            if(is_zef_subtype(node.primitive_type, VRT.Enum) ||
               is_zef_subtype(node.primitive_type, VRT.QuantityFloat) ||
               is_zef_subtype(node.primitive_type, VRT.QuantityInt)) {
                auto v = node.primitive_type.value;
                enum_indx indx = v - v % 16;
                auto p = gd.ENs_used->get_writer();
                if(!p->contains(indx))
                    p->append(indx, p.ensure_func());
            }
        }
    }
}

void unapply_action_ATTRIBUTE_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    auto & node = get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(uzr);
    if (is_delegate(uzr)) {
    } else {
        if(fill_caches)
            pop_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
    }
    // TODO Going to ignore undoing from ENs_used
    // WARNING THIS COULD MAKE A CLIENT GET OUT OF SYNC IN WEIRD ORDERING OF TAKING TRANSACTOR ROLE.
}

void apply_action_VALUE_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::VALUE_NODE);
    auto & node = get<blobs_ns::VALUE_NODE>(uzr);

    if(fill_caches) {
        auto this_val = value_from_node<value_variant_t>(node);
        insert_av_hash_lookup(gd, this_val, index(uzr));
    }
}

void unapply_action_VALUE_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    auto & node = get<blobs_ns::VALUE_NODE>(uzr);
    if(fill_caches) {
        // TODO: Pop the hash from a lookup
        auto this_val = value_from_node<value_variant_t>(node);
        pop_av_hash_lookup(gd, this_val);
    }
}

void apply_action_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::ENTITY_NODE);
    auto & node = get<blobs_ns::ENTITY_NODE>(uzr);
    // we need to distinguish whether this is a delegate entity or an instance: these get different keys
    // auto first_edge = EZefRef(-*(AllEdgeIndexes(uzr).begin()), graph_data(uzr));  // the first edge is always incoming
    if(fill_caches) {
        if (is_delegate(uzr)) {
            // std::string name = type_name(uzr);
            // insert_tag_lookup(gd, name, index(uzr));
            // insert_tag_lookup(gd, "TO_DELEGATE_EDGE_edge." + name, index(uzr < BT.TO_DELEGATE_EDGE));
        }
        else {
            insert_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
        }
        if(is_delegate(uzr)) {
            auto p = gd.ETs_used->get_writer();
            auto et = node.entity_type.entity_type_indx;
            if(!p->contains(et))
                p->append(et, p.ensure_func());
        }
    }
}

void unapply_action_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    auto & node = get<blobs_ns::ENTITY_NODE>(uzr);
    if (is_delegate(uzr)) {}
    else {
        if(fill_caches)
            pop_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
    }
    // TODO Going to ignore undoing from ETs_used
    // WARNING THIS COULD MAKE A CLIENT GET OUT OF SYNC IN WEIRD ORDERING OF TAKING TRANSACTOR ROLE.
}

void apply_action_RELATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::RELATION_EDGE);
    auto & node = get<blobs_ns::RELATION_EDGE>(uzr);
    if(fill_caches) {
        if (is_delegate(uzr)) {
            // std::string name = type_name(uzr);
            // insert_tag_lookup(gd, name, index(uzr));
            // if(!is_delegate_group(uzr))
            //     insert_tag_lookup(gd, "TO_DELEGATE_EDGE_edge." + name, index(uzr < BT.TO_DELEGATE_EDGE));
        }
        else {
            insert_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
        }
        if(is_delegate(uzr)) {
            auto p = gd.RTs_used->get_writer();
            auto rt = node.relation_type.relation_type_indx;
            if(!p->contains(rt))
                p->append(rt, p.ensure_func());
        }
    }
}									 

void unapply_action_RELATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    auto & node = get<blobs_ns::RELATION_EDGE>(uzr);
    if (is_delegate(uzr)) {}
    else {
        if(fill_caches)
            pop_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
    }
    // TODO Going to ignore undoing from RTs_used
    // WARNING THIS COULD MAKE A CLIENT GET OUT OF SYNC IN WEIRD ORDERING OF TAKING TRANSACTOR ROLE.
}									 

void apply_action_TX_EVENT_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::TX_EVENT_NODE);
    if(fill_caches) {
        if (is_delegate(uzr)) {
        } else {
            insert_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
        }
    }
}

void unapply_action_TX_EVENT_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    if(fill_caches){
        if (is_delegate(uzr)) {
        } else {
            pop_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
        }
    }
}

void apply_action_DEFERRED_EDGE_LIST_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::DEFERRED_EDGE_LIST_NODE);
}

void unapply_action_DEFERRED_EDGE_LIST_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {}

void apply_action_ASSIGN_TAG_NAME_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::ASSIGN_TAG_NAME_EDGE);
    if(fill_caches) {
        auto& action_blob = get<blobs_ns::ASSIGN_TAG_NAME_EDGE>(uzr);
        if(length(uzr > L[BT.NEXT_TAG_NAME_ASSIGNMENT_EDGE])==0)  // only add the tag name to the key dict if there is no ASSIGN_TAG_NAME_EDGE with the same tag name that follows (oredered by tx's)
            insert_tag_lookup(gd, std::string(get_data_buffer(action_blob), action_blob.buffer_size_in_bytes), index(uzr | target | target));
    }
}

void unapply_action_ASSIGN_TAG_NAME_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    if(fill_caches) {
        auto& action_blob = get<blobs_ns::ASSIGN_TAG_NAME_EDGE>(uzr);
        // Only remove the tag if this was the first node to introduce it.
        if(length(uzr < L[BT.NEXT_TAG_NAME_ASSIGNMENT_EDGE])==0) 
            remove_tag_lookup(gd, std::string(get_data_buffer(action_blob), action_blob.buffer_size_in_bytes), index(uzr | target | target));
    }
}

void apply_action_FOREIGN_GRAPH_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::FOREIGN_GRAPH_NODE);
    if(fill_caches) {
        BaseUID this_uid = get_blob_uid(uzr);
        insert_uid_lookup(gd, this_uid, index(uzr));
    }
}

void unapply_action_FOREIGN_GRAPH_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    if(fill_caches) {
        BaseUID this_uid = get_blob_uid(uzr);
        pop_uid_lookup(gd, this_uid, index(uzr));
    }
}

void apply_action_FOREIGN_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::FOREIGN_ENTITY_NODE);
    if(fill_caches) {
        BaseUID uid = get_blob_uid(uzr);
        // insert_uid_lookup(gd, uid, index(uzr));

        BaseUID graph_uid = get_blob_uid(uzr >> BT.ORIGIN_GRAPH_EDGE);
        insert_euid_lookup(gd, EternalUID(uid, graph_uid), index(uzr));
    }
}

void unapply_action_FOREIGN_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    if(fill_caches) {
        BaseUID uid = get_blob_uid(uzr);
        // pop_uid_lookup(gd, uid, index(uzr));

        BaseUID graph_uid = get_blob_uid(uzr >> BT.ORIGIN_GRAPH_EDGE);
        pop_euid_lookup(gd, EternalUID(uid, graph_uid), index(uzr));
    }
}

void apply_action_FOREIGN_ATTRIBUTE_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE);
    if(fill_caches) {
        BaseUID uid = get_blob_uid(uzr);
        // insert_uid_lookup(gd, uid, index(uzr));

        BaseUID graph_uid = get_blob_uid(uzr >> BT.ORIGIN_GRAPH_EDGE);
        insert_euid_lookup(gd, EternalUID(uid, graph_uid), index(uzr));
    }
}

void unapply_action_FOREIGN_ATTRIBUTE_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    if(fill_caches) {
        BaseUID uid = get_blob_uid(uzr);
        // pop_uid_lookup(gd, uid, index(uzr));

        BaseUID graph_uid = get_blob_uid(uzr >> BT.ORIGIN_GRAPH_EDGE);
        pop_euid_lookup(gd, EternalUID(uid, graph_uid), index(uzr));
    }
}

void apply_action_FOREIGN_RELATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::FOREIGN_RELATION_EDGE);
    if(fill_caches) {
        BaseUID uid = get_blob_uid(uzr);
        // insert_uid_lookup(gd, uid, index(uzr));

        BaseUID graph_uid = get_blob_uid(uzr >> BT.ORIGIN_GRAPH_EDGE);
        insert_euid_lookup(gd, EternalUID(uid, graph_uid), index(uzr));
    }
}
void unapply_action_FOREIGN_RELATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    if(fill_caches) {
        BaseUID uid = get_blob_uid(uzr);
        // pop_uid_lookup(gd, uid, index(uzr));

        BaseUID graph_uid = get_blob_uid(uzr >> BT.ORIGIN_GRAPH_EDGE);
        pop_euid_lookup(gd, EternalUID(uid, graph_uid), index(uzr));
    }
}

void apply_action_TERMINATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::TERMINATION_EDGE);
    // if we encounter a termination edge, we also need to mutate the previous section of the graph: write the time slice index of the most_recent_tx_uzr to the actual rel_ent blob
    auto this_rel_ent_instance_edge = EZefRef(target_node_index(uzr), gd);
    auto rel_ent_that_was_terminated = EZefRef(target_node_index(this_rel_ent_instance_edge), gd);
    TimeSlice termination_ts = get<blobs_ns::TX_EVENT_NODE>(EZefRef(source_node_index(uzr), gd)).time_slice;
    switch (get<BlobType>(rel_ent_that_was_terminated)) {   // For an entity, relation, atomic entity, root_node, add the uid to the dict
    case BlobType::ATTRIBUTE_ENTITY_NODE: {
        get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(rel_ent_that_was_terminated).termination_time_slice = termination_ts;
        break;
    }
    case BlobType::ENTITY_NODE: {
        get<blobs_ns::ENTITY_NODE>(rel_ent_that_was_terminated).termination_time_slice = termination_ts;
        break;
    }
    case BlobType::RELATION_EDGE: {
        get<blobs_ns::RELATION_EDGE>(rel_ent_that_was_terminated).termination_time_slice = termination_ts;
        break;
    }
    default: {throw std::runtime_error("In 'apply_action_blob' for case TERMINATION_EDGE: attempting to write termination time slice to previous section of graph. Landed on a blob where we should never have landed."); }
    }
}

void unapply_action_TERMINATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    auto this_rel_ent_instance_edge = EZefRef(target_node_index(uzr), gd);
    auto rel_ent_that_was_terminated = EZefRef(target_node_index(this_rel_ent_instance_edge), gd);
    switch (get<BlobType>(rel_ent_that_was_terminated)) {
    case BlobType::ATTRIBUTE_ENTITY_NODE: {
        get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(rel_ent_that_was_terminated).termination_time_slice.value = 0;
        break;
    }
    case BlobType::ENTITY_NODE: {
        get<blobs_ns::ENTITY_NODE>(rel_ent_that_was_terminated).termination_time_slice.value = 0;
        break;
    }
    case BlobType::RELATION_EDGE: {
        get<blobs_ns::RELATION_EDGE>(rel_ent_that_was_terminated).termination_time_slice.value = 0;
        break;
    }
    default: {throw std::runtime_error("In 'unapply_action_blob' for case TERMINATION_EDGE: attempting to write termination time slice to previous section of graph. Landed on a blob where we should never have landed."); }
    }
}

void apply_action_ATOMIC_VALUE_ASSIGNMENT_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE);
    auto & node = get<blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE>(uzr);

    if(fill_caches) {
        if(is_zef_subtype(node.rep_type, VRT.Enum)) {
            ZefEnumValue & en = *(ZefEnumValue*)node.data_buffer;
            enum_indx indx = en.value;

            auto p = gd.ENs_used->get_writer();
            if(!p->contains(indx))
                p->append(indx, p.ensure_func());
        }
    }
}

void unapply_action_ATOMIC_VALUE_ASSIGNMENT_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {}

// These ones are here for future compatibility
void apply_action_TO_DELEGATE_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {}
void unapply_action_TO_DELEGATE_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {}
void apply_action_DELEGATE_INSTANTIATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {}
void unapply_action_DELEGATE_INSTANTIATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {}
