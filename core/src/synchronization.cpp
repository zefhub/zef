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

#include "synchronization.h"

#include "low_level_api.h"
#include "high_level_api.h"
#include "blobs.h"

#include <algorithm>

namespace zefDB {
	namespace internals {
#include "synchronization_applies.cpp"


        void apply_double_linking(GraphData& gd, blob_index start_index, blob_index end_index) {
            // Build new edges to add in unordered_map and build deferred edge
            // list chains (using first_blob as key)
            std::unordered_map<blob_index, std::vector<blob_index>> new_edges;
            std::unordered_map<blob_index, std::vector<std::pair<blob_index,blob_index>>> deferred_chains;

            blob_index cur_index = start_index;
			while (cur_index < end_index) {
                EZefRef uzr{cur_index,gd};
                if(get<BlobType>(uzr) == BlobType::DEFERRED_EDGE_LIST_NODE) {
                    auto node = (blobs_ns::DEFERRED_EDGE_LIST_NODE*)uzr.blob_ptr;
                    blob_index subsequent_index = *subsequent_deferred_edge_list_index(uzr);
                    auto & list = deferred_chains[node->first_blob];
                    list.emplace_back(cur_index, subsequent_index);
                }

                if(has_source_target_node(uzr)) {
                    blob_index src = source_node_index(uzr);
                    blob_index trg = target_node_index(uzr);

                    if(src < start_index)
                        new_edges[src].push_back(cur_index);
                    if(trg < start_index)
                        new_edges[trg].push_back(-cur_index);
                }
                cur_index += blob_index_size(uzr);
            }

            // Find the new subsequent indices from the chains
            std::unordered_map<blob_index, blob_index> new_deferred;
            for(auto & it : deferred_chains) {
                // Find the deferred item which must be the next one from the existing item.
                auto & list = it.second;
                blob_index next_item = 0;
                for(const auto & candidate : list) {
                    bool found = true;
                    for(const auto & check : list) {
                        if(candidate.first == check.second) {
                            found = false;
                            break;
                        }
                    }
                    if(found) {
                        if(next_item != 0)
                            throw std::runtime_error("Found multiple candidates! Should never get here!");
                        next_item = candidate.first;
                    }
                }

                if(next_item == 0)
                    throw std::runtime_error("Did not find a candidate! Should never get here!");

                // We'll record this and assign it later on, so that all changes are done together.
                new_deferred[it.first] = next_item;
            }

            // Now update the prior blobs
            for(auto & it : new_edges) {
                // We first check to see which edges already exist, which will
                // be checking in the new deferred lists. We don't check in the
                // prior blobs because (if they are valid) then they couldn't
                // possibly know about the new indices.

                auto this_index = it.first;
                EZefRef uzr(this_index, gd);
                auto this_new_edges = it.second;
                // Note: this is a sorted set, which is kind of necessary for
                // the existing hash-checking procedure. In the future, the
                // hashes shouldn't care about the ordering of edges.
                std::unordered_set<blob_index> edges_to_add(this_new_edges.begin(), this_new_edges.end());

                auto subseq = new_deferred.find(this_index);
                if(subseq == new_deferred.end()) {
                    // Nothing to do here - accept all edges
                } else {
                    // We are doing a setdiff here - but to do it economically
                    // we want to loop through the deferred edge lists only
                    // once.
                    EZefRef z_subseq(subseq->second, gd);
                    for(blob_index existing_edge : AllEdgeIndexes(z_subseq, true)) {
                        // This only removes if the element is present
                        edges_to_add.erase(existing_edge);
                        if(edges_to_add.size() == 0)
                            break;
                    }
                }

                // Now we add these edges in - making sure that there would be
                // sufficient amount of space (and in fact, the exactly right
                // amount of space if there was a new subsequent deferred edge
                // list)
                //
                // As we can't test for the space, and are only adding in from
                // the last_blob lookup, this is not as simple as it could be.
                // blob_index last_blob_index = *last_edge_holding_blob(uzr);
                // last_blob = (blob_index*)ptr_from_blob_index(last_blob_index, gd);
                // Butler::ensure_or_get_range(last_blob, constants::blob_indx_step_in_bytes);
                // static_assert(constants::blob_indx_step_in_bytes == 4*sizeof(blob_index));

                std::vector<blob_index> sorted_edges(edges_to_add.begin(), edges_to_add.end());
                auto custom_sort = [](blob_index a, blob_index b) {
                    if(std::abs(a) < std::abs(b))
                        return true;
                    if(std::abs(a) > std::abs(b))
                        return false;
                    // Put outgoing edges before incoming
                    return a > 0;
                };
                std::sort(sorted_edges.begin(), sorted_edges.end(),
                          custom_sort);
                for(blob_index edge : sorted_edges) {
                    if(!append_edge_index(uzr, edge, true)) {
                        // This logic path means that append_edge_index wanted
                        // to create a deferred list but was not allowed. This
                        // would mean we are trying to write too many edges into
                        // the previous blobs.
                        throw std::runtime_error("Issue with too many new edges for double linking in an update.");
                    }
                }

                // Now we assert that anything which had a new subsequent index
                // also got filled up, and assign the subsequent index at the
                // same time.
                if(subseq != new_deferred.end()) {
                    blob_index last_blob_index = *last_edge_holding_blob(uzr);
                    blob_index * last_blob = (blob_index*)ptr_from_blob_index(last_blob_index, gd);
                    int subindex = subindex_in_last_blob(last_blob);
                    if(subindex != 3 || last_blob[3] != blobs_ns::sentinel_subsequent_index)
                        throw std::runtime_error("Issue with not enough edges in double linking in an update.");
                        
                    // This should line up with the subsequent index location.
                    last_blob[3] = subseq->second;

                    // TODO: Need to upate the last_blob
                    // Also update the direct lookup for the original blob - first find what we have as the final used index.

                    blob_index final_deferred_index = subseq->second;
                    while(true) {
                        EZefRef deferred_uzr(final_deferred_index, gd);
                        blob_index * this_index = subsequent_deferred_edge_list_index(deferred_uzr);
                        if(*this_index == blobs_ns::sentinel_subsequent_index)
                            break;
                        final_deferred_index = *this_index;
                    }

                    auto deferred = (blobs_ns::DEFERRED_EDGE_LIST_NODE *)EZefRef(final_deferred_index, gd).blob_ptr;
                    int i = 0;
                    for(; i < deferred->edges.local_capacity ; i++)
                        if(deferred->edges.indices[i] == 0)
                            break;
                    uintptr_t ptr = (uintptr_t)&deferred->edges.indices[i];
                    last_blob = (blob_index*)(ptr - (ptr % constants::blob_indx_step_in_bytes));

                    *last_edge_holding_blob(uzr) = blob_index_from_ptr(last_blob);
                }
            }
        }

        void verify_double_linking_partial() {
        }

        void apply_action_blob(GraphData& gd, EZefRef uzr_to_blob, bool fill_caches) {
            switch (get<BlobType>(uzr_to_blob)) {
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
                apply_action_DEFERRED_EDGE_LIST_NODE(gd, uzr_to_blob, fill_caches); 
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

        void unapply_action_blob(GraphData& gd, EZefRef uzr_to_blob, bool fill_caches) {
            switch (get<BlobType>(uzr_to_blob)) {
            case BlobType::ROOT_NODE: {
                unapply_action_ROOT_NODE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::ATOMIC_ENTITY_NODE: {
                unapply_action_ATOMIC_ENTITY_NODE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::ENTITY_NODE: {
                unapply_action_ENTITY_NODE(gd, uzr_to_blob, fill_caches);
                break;
            }											 
            case BlobType::RELATION_EDGE: {
                unapply_action_RELATION_EDGE(gd, uzr_to_blob, fill_caches);
                break;
            }									 
            case BlobType::TX_EVENT_NODE: {
                unapply_action_TX_EVENT_NODE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::DEFERRED_EDGE_LIST_NODE: {
                unapply_action_DEFERRED_EDGE_LIST_NODE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::ASSIGN_TAG_NAME_EDGE: {
                unapply_action_ASSIGN_TAG_NAME_EDGE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::FOREIGN_GRAPH_NODE: {
                unapply_action_FOREIGN_GRAPH_NODE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::FOREIGN_ENTITY_NODE: {
                unapply_action_FOREIGN_ENTITY_NODE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::FOREIGN_ATOMIC_ENTITY_NODE: {
                unapply_action_FOREIGN_ATOMIC_ENTITY_NODE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::FOREIGN_RELATION_EDGE: {
                unapply_action_FOREIGN_RELATION_EDGE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::TERMINATION_EDGE: {
                unapply_action_TERMINATION_EDGE(gd, uzr_to_blob, fill_caches);
                break;
            }
            case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE: {
                unapply_action_ATOMIC_VALUE_ASSIGNMENT_EDGE(gd, uzr_to_blob, fill_caches);
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

	}
}
