#include "synchronization.h"

#include "low_level_api.h"
#include "high_level_api.h"

namespace zefDB {
	namespace internals {
#include "synchronization_applies.cpp"

        void apply_action_blob(GraphData& gd, EZefRef uzr_to_blob, bool ensure_idempotency, blob_index latest_blob_to_double_link, bool fill_caches) { //, std::unordered_map<blob_index,blob_index> & latest_deferred) {
            // ----------- for any blob with source and node indexes (low level edge): add the index of this blob to the preceding source and target's edge_list  ----------
            if (latest_blob_to_double_link > 0) {
                const blob_index this_indx = index(uzr_to_blob);
                if(has_source_target_node(uzr_to_blob)) {
                    blob_index src_indx = source_node_index(uzr_to_blob);
                    // auto it = latest_deferred.find(*src_indx);
                    // blob_index latest_list_indx = (it == latest_deferred.end()) ? *src_indx : latest_list_indx = it->second;
                    // if(latest_list_indx < latest_blob_to_double_link) {
                        blob_index trg_indx = target_node_index(uzr_to_blob);   // this will only be set iff src_indx is set
                        if (ensure_idempotency) {
                            // if(src_indx < latest_blob_to_double_link) {
                            //     auto it = latest_deferred.find(src_indx);
                            //     auto hint = (it == latest_deferred.end()) ? src_indx : it->second;
                            //     if(hint < latest_blob_to_double_link)
                            //         latest_deferred[src_indx] = idempotent_append_edge_index(EZefRef(hint, gd), this_indx);
                            // }

                            // if(trg_indx < latest_blob_to_double_link) {
                            //     auto it = latest_deferred.find(trg_indx);
                            //     auto hint = (it == latest_deferred.end()) ? trg_indx : it->second;
                            //     if(hint < latest_blob_to_double_link)
                            //         latest_deferred[trg_indx] = idempotent_append_edge_index(EZefRef(hint, gd), -this_indx);
                            // }
                            idempotent_append_edge_index(EZefRef{src_indx, gd}, this_indx);
                            idempotent_append_edge_index(EZefRef(trg_indx, gd), -this_indx);
                        } else {
                            append_edge_index(EZefRef(src_indx, gd), this_indx);
                            append_edge_index(EZefRef(trg_indx, gd), -this_indx);
                        }
                    // }
                }
            }

            switch (get<BlobType>(uzr_to_blob)) {
                // For an entity, relation, atomic entity, root_node, add the uid to the dict
            case BlobType::ROOT_NODE: {
                apply_action_ROOT_NODE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::ATOMIC_ENTITY_NODE: {
               apply_action_ATOMIC_ENTITY_NODE(gd, uzr_to_blob, fill_caches); 
                break;
            }
            case BlobType::ENTITY_NODE: {
               apply_action_ENTITY_NODE(gd, uzr_to_blob, fill_caches); 
                break;
            }											 
            case BlobType::RELATION_EDGE: {
               apply_action_RELATION_EDGE(gd, uzr_to_blob, fill_caches); 
                break;
            }									 
            case BlobType::TX_EVENT_NODE: {
               apply_action_TX_EVENT_NODE(gd, uzr_to_blob, fill_caches); 
                break;
            }
            case BlobType::DEFERRED_EDGE_LIST_NODE: {
                apply_action_DEFERRED_EDGE_LIST_NODE(gd, uzr_to_blob, fill_caches, latest_blob_to_double_link); 
                break;
            }
            case BlobType::ASSIGN_TAG_NAME_EDGE: {
               apply_action_ASSIGN_TAG_NAME_EDGE(gd, uzr_to_blob, fill_caches); 
                break;
            }
            case BlobType::FOREIGN_GRAPH_NODE: {
               apply_action_FOREIGN_GRAPH_NODE(gd, uzr_to_blob, fill_caches); 
                break;
            }
            case BlobType::FOREIGN_ENTITY_NODE: {
               apply_action_FOREIGN_ENTITY_NODE(gd, uzr_to_blob, fill_caches); 
                break;
            }
            case BlobType::FOREIGN_ATOMIC_ENTITY_NODE: {
               apply_action_FOREIGN_ATOMIC_ENTITY_NODE(gd, uzr_to_blob, fill_caches); 
                break;
            }
            case BlobType::FOREIGN_RELATION_EDGE: {
               apply_action_FOREIGN_RELATION_EDGE(gd, uzr_to_blob, fill_caches); 
                break;
            }
            case BlobType::TERMINATION_EDGE: {
                apply_action_TERMINATION_EDGE(gd, uzr_to_blob, fill_caches);
				break;
			}
            case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE: {
                apply_action_ATOMIC_VALUE_ASSIGNMENT_EDGE(gd, uzr_to_blob, fill_caches);
				break;
			}
			default: return;
			};
		}



        // std::unordered_map<blob_index,blob_index> build_deferred_edge_list_hints(GraphData&gd, blob_index blob_index_lo, blob_index blob_index_hi) {
        //     // This goes through all new blobs, identifying the deferred edge
        //     // lists and their original RAE, so that apply_action can quickly
        //     // identify if it needs to update the double linking, instead of
        //     // traversing through all deferred edge lists for each link.

        //     std::unordered_map<blob_index,blob_index> latest_deferred;

		// 	auto current_uzr = EZefRef(blob_index_lo, gd);
		// 	while (index(current_uzr) < blob_index_hi) {
        //         // Be careful - only the newest range of blobs is fully updated. But we can traverse backwards safely.
        //         if(get<BlobType>(current_uzr) == BlobType::DEFERRED_EDGE_LIST_NODE) {
        //             blob_index last_blob_before_lo = 0;
        //             // Walk backwards to find the original RAE, recording the first deferred list that is outside of our range, and its link to the original RAE.
        //             // auto previous = current_uzr;
        //             // while(get<BlobType>(previous) == BlobType::DEFERRED_EDGE_LIST_NODE) {
        //             //     auto deferred = (blobs_ns::DEFERRED_EDGE_LIST_NODE*)previous.blob_ptr;
        //             //     // Could optimise this, if the previous is > blob_index_lo, we can just use the precalced value in latest_deferred.
        //             //     previous = EZefRef(deferred->preceding_blob, gd);
        //             //     if(last_blob_before_lo == 0 && index(previous) < blob_index_lo)
        //             //         last_blob_before_lo = index(previous);
        //             // }
        //             // latest_deferred[index(previous)] = last_blob_before_lo;

        //             // We can no longer walk backwards, so instead walk forwards
        //             auto deferred = (blobs_ns::DEFERRED_EDGE_LIST_NODE*)current_uzr.blob_ptr;
        //             EZefRef start_of_chain{deferred->first_blob, gd};
        //             EZefRef cur_in_chain = start_of_chain;
        //             while(true) {
        //                 if(cur_in_chain == current_uzr)
        //                     break;
        //                 if(index(cur_in_chain) > blob_index_lo)
        //                     break;
        //                 last_blob_before_lo = index(cur_in_chain);

        //                 blob_index next_ind = *subsequent_deferred_edge_list_index(cur_in_chain);
        //                 if(next_ind == blobs_ns::sentinel_subsequent_index)
        //                     break;
        //                 cur_in_chain = EZefRef{next_ind, gd};
        //             }
        //             latest_deferred[index(start_of_chain)] = last_blob_before_lo;
        //         }
        //      // TODO: IF THIS COMES BACK, get_next NEEDS TO BE FIXED UP!
		// 		current_uzr = get_next(current_uzr);
		// 	}

        //     return latest_deferred;
        // }

		// apply action of all blobs_ns (that actually trigger one) on the given interval. Moves forward automatically.
		// the specified blob range should not go beyond what is set in the mem pool
		void apply_actions_to_blob_range(GraphData& gd, blob_index blob_index_lo, blob_index blob_index_hi, bool ensure_idempotency, bool double_link_edges_to_previous_blobs, bool fill_caches) {
            // This function only makes sense as a low-level action. But because
            // it modifies the graph, we should make sure the caller grabbed
            // write access first.
            if(gd.open_tx_thread != std::this_thread::get_id())
                throw std::runtime_error("Need write lock to update blob actions!");

            // Trying to optimise locking - hold it open for the entire function
            // std::unique_lock lock(gd->key_dict.m);

            auto t_start = now();
            auto print_interval = 5*seconds;
            auto next_print_time = t_start + print_interval;
            int num_blobs = 0;

            // std::unordered_map<blob_index,blob_index> latest_deferred;
            blob_index latest_blob_to_double_link = 0;
            if(double_link_edges_to_previous_blobs) {
                // latest_deferred = build_deferred_edge_list_hints(gd, blob_index_lo, blob_index_hi);
                latest_blob_to_double_link = blob_index_lo;
            }
            
            // Note: we can't use EZefRefs here as we will eventually run off
            // the end and try to ensure memory which is not alloced. Need to
            // work with indices first.

			// auto current_uzr = EZefRef(blob_index_lo, gd);
			blob_index cur_index = blob_index_lo;
			while (cur_index < blob_index_hi) {
                EZefRef uzr(cur_index, gd);
				#ifdef ZEF_DEBUG
				if (get<unsigned char>(uzr) == (unsigned char)(0))
					throw std::runtime_error("encountered an unset blob while traversing blob range in apply_actions_to_blob_range. Are the limits set correctly?");
				#endif  //ZEF_DEBUG
				apply_action_blob(gd, uzr, ensure_idempotency, latest_blob_to_double_link, fill_caches);//, latest_deferred);
                num_blobs++;
                if(zwitch.developer_output()) {
                    if(now() > next_print_time) {
                        std::cerr << "Taking a long time to apply actions (blob_link=" << latest_blob_to_double_link << "). Up to blob " << blob_index_lo << " : " << index(uzr) << " : " << blob_index_hi << std::endl;
                        // std::cerr << "Size of latest_deferred: " << latest_deferred.size() << std::endl;
                        std::cerr << "Doing dict updates: " << fill_caches << std::endl;
                        next_print_time = next_print_time + print_interval;
                    }
                }
                    
				cur_index += blob_index_size(uzr);
			}

            auto duration = now() - t_start;
            auto per_each = duration / num_blobs;
            if(zwitch.developer_output())
                std::cerr << "Took " << duration << " to apply blobs internally for " << uid(gd) << " when double_link is " << double_link_edges_to_previous_blobs << " and latest_blob_to_double_link is " << latest_blob_to_double_link << ". This is " << per_each << " on average" << std::endl;

            // TODO: This might not be the right place. Probably to put it into a separate wrapper function is a good idea.
            auto & info = MMap::info_from_blobs(&gd);
            MMap::flush_mmap(info, gd.write_head);
		}

	}
}
