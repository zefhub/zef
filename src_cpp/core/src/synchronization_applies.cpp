
// This function is dirty - just because the ones lower down have extra args we
// might not know when calling this one.
void apply_action_lookup(GraphData& gd, EZefRef uzr, bool fill_caches) {
    switch (get<BlobType>(uzr)) {
    case BlobType::ATOMIC_ENTITY_NODE: {
        apply_action_ATOMIC_ENTITY_NODE(gd, uzr, fill_caches); 
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
    case BlobType::FOREIGN_ATOMIC_ENTITY_NODE: {
        apply_action_FOREIGN_ATOMIC_ENTITY_NODE(gd, uzr, fill_caches);
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

void apply_action_ROOT_NODE(GraphData& gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::ROOT_NODE);
    if(fill_caches) {
        insert_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
    }
}

void apply_action_ATOMIC_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::ATOMIC_ENTITY_NODE);
    auto & node = get<blobs_ns::ATOMIC_ENTITY_NODE>(uzr);
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
            if(node.my_atomic_entity_type <= AET.Enum ||
               node.my_atomic_entity_type <= AET.QuantityFloat ||
               node.my_atomic_entity_type <= AET.QuantityInt) {
                auto v = node.my_atomic_entity_type.value;
                enum_indx indx = v - v % 16;
                auto p = gd.ENs_used->get_writer();
                if(!p->contains(indx))
                    p->append(indx, p.ensure_func());
            }
        }
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
void apply_action_TX_EVENT_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::TX_EVENT_NODE);
    if(fill_caches) {
        // gd->key_dict.map[get_uid_as_hex_str(uzr)] = index(uzr);
        insert_uid_lookup(gd, get_blob_uid(uzr), index(uzr));
    }
}

void apply_action_DEFERRED_EDGE_LIST_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::DEFERRED_EDGE_LIST_NODE);
    blobs_ns::DEFERRED_EDGE_LIST_NODE & deferred = get<blobs_ns::DEFERRED_EDGE_LIST_NODE>(uzr);

    // // Start at the first blob and walk forwards until we find the one that was just before, or we run into ourselves.
    // blob_index prev_index = deferred.first_blob;
    // while(prev_index != index(uzr)) {
    //     EZefRef prev_blob{prev_index, *graph_data(uzr)};
    //     blob_index * subsequent = internals::subsequent_deferred_edge_list_index(prev_blob);
    //     if(*subsequent != blobs_ns::sentinel_subsequent_index) {
    //         prev_index = *subsequent;
    //         continue;
    //     }

    //     // If we get here, going to update
    //     *subsequent = index(uzr);

    //     // Sanity check
    //     visit_blob_with_edges([](auto & s) {
    //         if(s.edges.indices[s.edges.local_capacity-1] == 0) {
    //             std::cerr << "Blob edge list being updated to point to new list, when it isn't even full yet!" << std::endl;
    //             throw std::runtime_error("Blob edge list being updated to point to new list, when it isn't even full yet!");
    //         }
    //     }, prev_blob);

    //     // Also update the direct lookup for the original blob - first find what we have as the final used index.
    //     int i = 0;
    //     for(; i < deferred.edges.local_capacity ; i++)
    //         if(deferred.edges.indices[i] == 0)
    //             break;
    //     // If we hit the end, then that is okay as the final index should be the
    //     // subsequent index. If it is empty then that's fine, but if it's
    //     // occupied then we'll overwrite this value later on anyway.
    //     uintptr_t ptr = (uintptr_t)&deferred.edges.indices[i];
    //     void * last_blob = (void*)(ptr - (ptr % constants::blob_indx_step_in_bytes));

    //     EZefRef src_blob{deferred.first_blob, gd};
    //     *last_edge_holding_blob(src_blob) = index(EZefRef{last_blob});
    // }
}

void apply_action_ASSIGN_TAG_NAME_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::ASSIGN_TAG_NAME_EDGE);
    if(fill_caches) {
        auto& action_blob = get<blobs_ns::ASSIGN_TAG_NAME_EDGE>(uzr);
        if(length(uzr > L[BT.NEXT_TAG_NAME_ASSIGNMENT_EDGE])==0)  // only add the tag name to the key dict if there is no ASSIGN_TAG_NAME_EDGE with the same tag name that follows (oredered by tx's)
            insert_tag_lookup(gd, std::string(get_data_buffer(action_blob), action_blob.buffer_size_in_bytes), index(uzr | target | target));
    }
}
void apply_action_FOREIGN_GRAPH_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::FOREIGN_GRAPH_NODE);
    if(fill_caches) {
        BaseUID this_uid = get_blob_uid(uzr);
        insert_uid_lookup(gd, this_uid, index(uzr));
    }
}

void apply_action_FOREIGN_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::FOREIGN_ENTITY_NODE);
    if(fill_caches) {
        BaseUID uid = get_blob_uid(uzr);
        insert_uid_lookup(gd, uid, index(uzr));

        BaseUID graph_uid = get_blob_uid(uzr >> BT.ORIGIN_GRAPH_EDGE);
        insert_euid_lookup(gd, EternalUID(uid, graph_uid), index(uzr));
    }
}

void apply_action_FOREIGN_ATOMIC_ENTITY_NODE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::FOREIGN_ATOMIC_ENTITY_NODE);
    if(fill_caches) {
        BaseUID uid = get_blob_uid(uzr);
        insert_uid_lookup(gd, uid, index(uzr));

        BaseUID graph_uid = get_blob_uid(uzr >> BT.ORIGIN_GRAPH_EDGE);
        insert_euid_lookup(gd, EternalUID(uid, graph_uid), index(uzr));
    }
}

void apply_action_FOREIGN_RELATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::FOREIGN_RELATION_EDGE);
    if(fill_caches) {
        BaseUID uid = get_blob_uid(uzr);
        insert_uid_lookup(gd, uid, index(uzr));

        BaseUID graph_uid = get_blob_uid(uzr >> BT.ORIGIN_GRAPH_EDGE);
        insert_euid_lookup(gd, EternalUID(uid, graph_uid), index(uzr));
    }
}

void apply_action_TERMINATION_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::TERMINATION_EDGE);
    // if we encounter a termination edge, we also need to mutate the previous section of the graph: write the time slice index of the most_recent_tx_uzr to the actual rel_ent blob
    auto this_rel_ent_instance_edge = EZefRef(target_node_index(uzr), gd);
    auto rel_ent_that_was_terminated = EZefRef(target_node_index(this_rel_ent_instance_edge), gd);
    TimeSlice termination_ts = get<blobs_ns::TX_EVENT_NODE>(EZefRef(source_node_index(uzr), gd)).time_slice;
    switch (get<BlobType>(rel_ent_that_was_terminated)) {   // For an entity, relation, atomic entity, root_node, add the uid to the dict
    case BlobType::ATOMIC_ENTITY_NODE: {
        get<blobs_ns::ATOMIC_ENTITY_NODE>(rel_ent_that_was_terminated).termination_time_slice = termination_ts;
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

void apply_action_ATOMIC_VALUE_ASSIGNMENT_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches) {
    assert(get<BlobType>(uzr) == BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE);
    auto & node = get<blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE>(uzr);

    if(fill_caches) {
        if(node.my_atomic_entity_type <= AET.Enum) {
            ZefEnumValue & en = *(ZefEnumValue*)node.data_buffer;
            enum_indx indx = en.value;

            auto p = gd.ENs_used->get_writer();
            if(!p->contains(indx))
                p->append(indx, p.ensure_func());
        }
    }
}
