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

#include "verification.h"

#include "high_level_api.h"
#include "zefops.h"

#include <doctest/doctest.h>

namespace zefDB {
	namespace verification {	

        using namespace internals;


		// make go to the both the src and trg blobs: there check that indx of start blob is in the edge_list exactly once. Else throw.
		bool verify_that_my_index_is_in_source_target_node_edge_list(EZefRef uzr) {
			using namespace ranges;
			blob_index my_indx = index(uzr);
			blob_index src_indx = source_node_index(uzr);
			blob_index trg_indx = target_node_index(uzr);
			
			// assert src and trg have edge_list
			EZefRef src_uzr(src_indx, *graph_data(uzr));
			EZefRef trg_uzr(trg_indx, *graph_data(uzr));
			if (!has_edges(src_uzr))
				throw std::runtime_error("expected source blob to have an edge_list, but it did not. Graph uid: " + to_str(Graph(uzr) | uid));
			if (!has_edges(trg_uzr))
				throw std::runtime_error("expected target blob to have an edge_list, but it did not. Graph uid: " + to_str(Graph(uzr) | uid));

			int counter = 0;
			for (auto ind : AllEdgeIndexes(src_uzr)) {
				counter += int(ind == my_indx);
			}		
			if (counter != 1) {
				throw std::runtime_error("In verify_that_my_index_is_in_source_target_node_edge_list: index of a source node did not appear exactly once in edge_list of blob it refers to. Graph uid: " 
					+ to_str(Graph(uzr) | uid)
					+ "\n problem with edge indexes on "
					+ to_str(src_uzr)
					+ "\nexpeted index "
					+ to_str(my_indx)
				);
			}
			counter = 0;
			for (auto ind : AllEdgeIndexes(trg_uzr)) {
				counter += int(ind == -my_indx);
			}
			if (counter != 1)
				throw std::runtime_error("In verify_that_my_index_is_in_source_target_node_edge_list: index of a target node did not appear exactly once in edge_list of blob it refers to. Graph uid: "
					+ to_str(Graph(uzr) | uid)
					+ "\n problem with edge indexes on "
					+ to_str(trg_uzr)
					+ "\nexpeted index "
					+ to_str(-my_indx)
				);			
			return true;
		}
		

		bool verify_that_all_uzrs_in_my_edgelist_refer_to_me(EZefRef uzr) {
			using namespace internals;
			using namespace ranges;
			blob_index my_indx = index(uzr);
			for (auto edge_indx : AllEdgeIndexes(uzr)) {
				if (edge_indx > 0) {
					EZefRef edge_uzr(edge_indx, *graph_data(uzr));
					if (source_node_index(edge_uzr) != my_indx) {
						throw std::runtime_error("In verify_that_all_uzrs_in_my_edgelist_refer_to_me: Went to edge blob looking whether its source_index agrees with the starting blob, but it did not. Graph uid: " 
							+ to_str(Graph(uzr) | uid) 
							+ "\nsource of "
							+ to_str(edge_uzr)
							+ "\ndoes not agree with " 
							+ to_str(my_indx)
						);
					}
				}
			}
			for (auto edge_indx : AllEdgeIndexes(uzr)) {
				if (edge_indx < 0) {
					EZefRef edge_uzr(-edge_indx, *graph_data(uzr));
					if (target_node_index(edge_uzr) != my_indx) {
						throw std::runtime_error("In verify_that_all_uzrs_in_my_edgelist_refer_to_me: Went to edge blob looking whether its tarindex agrees with the starting blob, but it did not. Graph uid: "
							+ to_str(Graph(uzr) | uid)
							+ "\target of "
							+ to_str(edge_uzr)
							+ "\ndoes not agree with "
							+ to_str(my_indx)
						);
					}
				}
			}
			return true;
		}


        bool verify_source_target_in_edge_lists(GraphData & gd) {
            // To make this fast enough, we build up the edge lists first and
            // then do a list-list compare. This makes it jump around less in
            // memory.

            std::unordered_map<blob_index,std::vector<blob_index>> new_map;
            std::unordered_map<blob_index,std::vector<blob_index>> edge_lists;

            // Walk through graph once to create the empty vectors
            blob_index cur_index = internals::root_node_blob_index();
			while (cur_index < gd.write_head) {
                EZefRef uzr{cur_index,gd};
				if (internals::has_edges(uzr)) {
                    std::vector<blob_index> vec;
                    for(auto indx : AllEdgeIndexes(uzr))
                        vec.push_back(indx);
                    edge_lists.emplace(cur_index, vec);
                    new_map.emplace(cur_index, std::vector<blob_index>());
                }
                cur_index += blob_index_size(uzr);
            }

            // Now populate lists
            cur_index = internals::root_node_blob_index();
			while (cur_index < gd.write_head) {
                EZefRef uzr{cur_index,gd};
				if (internals::has_source_target_node(uzr)) {
                    new_map[source_node_index(uzr)].push_back(cur_index);
                    new_map[target_node_index(uzr)].push_back(-cur_index);
                }
                cur_index += blob_index_size(uzr);
            }

            // Now compare contents of lists
            for(auto & it : new_map) {
                auto indx = it.first;
                auto & new_list = it.second;
                auto & old_list = edge_lists.at(indx);

                std::unordered_set<blob_index> old_set(old_list.begin(), old_list.end());
                std::unordered_set<blob_index> new_set(old_list.begin(), old_list.end());
                if(old_set != new_set)
                    throw std::runtime_error("Edge lists do not agree with source/target");
            }

            // Now make sure there were no keys missing from the new_map
            for(auto & it : edge_lists)
                new_map.at(it.first);

            return true;
        }


		// check low level graph that double linking of node / edges with indexes is consistent
		bool verify_graph_double_linking(Graph& g){
			GraphData& gd = g.my_graph_data();

            if(!verify_source_target_in_edge_lists(gd))
                return false;
            
            // Need to use indices to avoid running off the edge of memory.
            blob_index cur_index = internals::root_node_blob_index();
			while (cur_index < gd.write_head) {
                EZefRef uzr{cur_index,gd};
				// deferred edge lists are the exception here: blobs in indexes appearing here will point to the true source blob and not the deferred_edge_list
				if (internals::has_edges(uzr) && get<BlobType>(uzr) != BlobType::DEFERRED_EDGE_LIST_NODE) {
					verify_that_all_uzrs_in_my_edgelist_refer_to_me(uzr);
				}
                cur_index += blob_index_size(uzr);
			}
			return true;
		}


        bool verify_chronological_instantiation_order(Graph g) {
			GraphData& gd = g.my_graph_data();

            blob_index cur_index = internals::root_node_blob_index();
			while (cur_index < gd.write_head) {
                EZefRef uzr{cur_index,gd};

                if(get<BlobType>(uzr) == BlobType::RAE_INSTANCE_EDGE) {
                    TimeSlice last_ts(0);
                    auto all_edges = ins_and_outs(uzr);
                    for (auto edge_uzr : all_edges) {
                        if(get<BlobType>(edge_uzr) == BlobType::INSTANTIATION_EDGE ||
                           get<BlobType>(edge_uzr) == BlobType::TERMINATION_EDGE) {
                            EZefRef tx_uzr = source(edge_uzr);
                            auto & tx_node = get<blobs_ns::TX_EVENT_NODE>(tx_uzr);
                            if(tx_node.time_slice < last_ts) {
                                std::cerr << "Chronological order is bad for blob: " << cur_index << std::endl;
                                return false;
                            }

                            last_ts = tx_node.time_slice;
                        }
                    }
                } else if(get<BlobType>(uzr) == BlobType::TO_DELEGATE_EDGE) {
                    TimeSlice last_ts(0);
                    auto all_edges = ins_and_outs(uzr);
                    for (auto edge_uzr : all_edges) {
                        if(get<BlobType>(edge_uzr) == BlobType::DELEGATE_INSTANTIATION_EDGE ||
                           get<BlobType>(edge_uzr) == BlobType::DELEGATE_RETIREMENT_EDGE) {
                            EZefRef tx_uzr = source(edge_uzr);
                            auto & tx_node = get<blobs_ns::TX_EVENT_NODE>(tx_uzr);
                            if(tx_node.time_slice < last_ts) {
                                std::cerr << "Chronological order is bad for blob: " << cur_index << std::endl;
                                return false;
                            }

                            last_ts = tx_node.time_slice;
                        }
                    }
                }

                cur_index += blob_index_size(uzr);
            }
            
            return true;
        }


        // This was for some kind of testing

        void break_graph(Graph&g, blob_index index, int style) {
            EZefRef ezr = g[index];
            if(style == 1) {
                visit_blob_with_source_target([](auto & x) {
                    x.source_node_index++;
                }, ezr);
            } else if(style == 2) {
                visit_blob_with_source_target([](auto & x) {
                    x.target_node_index *= -1;
                }, ezr);
            } else if(style == 3) {
                visit_blob_with_edges([](auto & edges) {
                    edges.indices[0] = 42;
                }, ezr);
            } else if(style == 4) {
                // Find a RAE instance that has been terminated and flip it
                if(get<BlobType>(ezr) != BlobType::RAE_INSTANCE_EDGE)
                    throw std::runtime_error("Index needs to be for a RAE_INSTANCE_EDGE");

                auto all_edges = ins_and_outs(ezr);
                int num_inst_term = 0;
                for(auto edge_ezr : all_edges) {
                    if(get<BlobType>(edge_ezr) == BlobType::INSTANTIATION_EDGE ||
                        get<BlobType>(edge_ezr) == BlobType::TERMINATION_EDGE) {
                        num_inst_term++;
                    }
                }
                if(num_inst_term < 2)
                    throw std::runtime_error("RAE needs two inst/terms");

                visit_blob_with_edges([](auto & edges) {
                    blob_index temp = edges.indices[1];
                    edges.indices[1] = edges.indices[0];
                    edges.indices[0] = temp;
                }, ezr);

            } else {
                throw std::runtime_error("Don't know style");
            }
        }

	}
}
