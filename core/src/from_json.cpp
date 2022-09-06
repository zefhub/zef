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

#include "from_json.h"
#include "uids.h"
#include "low_level_api.h"
#include "high_level_api.h"
#include "synchronization.h"
#include "verification.h"

#include "zefops.h"



namespace zefDB {
    namespace internals {

        using map_t = std::unordered_map<blob_index,blob_index>;

        EZefRef blank_instantiate(BlobType bt,
                                  GraphData * gd,
                                  std::optional<std::vector<blob_index>> edges,
                                  blob_index source_node_index,
                                  blob_index target_node_index,
                                  std::string data_buffer
        );
        BlobType lookup_BlobType(std::string name);

        blob_index mapped_index(blob_index old, const std::unordered_map<blob_index,blob_index> & map) {
            if(old < 0)
                return -map.at(-old);
            else
                return map.at(old);
        }

        template<class T>
        void update_blob_indices_specific(T & blob, const map_t & index_map) {}

        void assign_blob_specific(blobs_ns::_unspecified & blob, const json & details) {
            throw std::runtime_error("Shouldn't never get here");
        }

        void assign_blob_specific(blobs_ns::ROOT_NODE & blob, const json & details) {
            throw std::runtime_error("Shouldn't never get here");
        }

        void assign_blob_specific(blobs_ns::TX_EVENT_NODE & blob, const json & details) {
            blob.time = details["time"];
            blob.time_slice = details["time_slice"];
        }

        void assign_blob_specific(blobs_ns::NEXT_TX_EDGE & blob, const json & details) {}

        void assign_blob_specific(blobs_ns::RAE_INSTANCE_EDGE & blob, const json & details) {}

        void assign_blob_specific(blobs_ns::TO_DELEGATE_EDGE & blob, const json & details) {}

        void assign_blob_specific(blobs_ns::ENTITY_NODE & blob, const json & details) {
            blob.entity_type = details["entity_type"];
            blob.instantiation_time_slice = details["instantiation_time_slice"];
            blob.termination_time_slice = details["termination_time_slice"];
        }
        
        void assign_blob_specific(blobs_ns::ATTRIBUTE_ENTITY_NODE & blob, const json & details) {
            blob.primitive_type = details["primitive_type"];
            blob.instantiation_time_slice = details["instantiation_time_slice"];
            blob.termination_time_slice = details["termination_time_slice"];
        }
        
        void assign_blob_specific(blobs_ns::VALUE_NODE & blob, const json & details) {
            blob.rep_type = details["rep_type"];
        }

        void assign_blob_specific(blobs_ns::RELATION_EDGE & blob, const json & details) {
            blob.hostage_flags = details["hostage_flags"];
            blob.relation_type = details["relation_type"];
            blob.instantiation_time_slice = details["instantiation_time_slice"];
            blob.termination_time_slice = details["termination_time_slice"];
        }
        
        // these edges are created together with any new entity node or domain edge
        void assign_blob_specific(blobs_ns::DELEGATE_INSTANTIATION_EDGE & blob, const json & details) {};		

        void assign_blob_specific(blobs_ns::DELEGATE_RETIREMENT_EDGE & blob, const json & details) {}

        // these edges are created together with any new entity node or domain edge
        void assign_blob_specific(blobs_ns::INSTANTIATION_EDGE & blob, const json & details) {}
        void assign_blob_specific(blobs_ns::TERMINATION_EDGE & blob, const json & details) {}

        void assign_blob_specific(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE & blob, const json & details) {
            blob.rep_type = details["rep_type"];
        }

        void assign_blob_specific(blobs_ns::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE & blob, const json & details) {
            blob.value_edge_index = details["edges"][0];
        }

        void assign_blob_specific(blobs_ns::DEFERRED_EDGE_LIST_NODE & blob, const json & details) {
            throw std::runtime_error("Shouldn't get here");
        };

        void assign_blob_specific(blobs_ns::ASSIGN_TAG_NAME_EDGE & blob, const json & details) {}

        void assign_blob_specific(blobs_ns::NEXT_TAG_NAME_ASSIGNMENT_EDGE & blob, const json & details) {}

        void assign_blob_specific(blobs_ns::FOREIGN_GRAPH_NODE & blob, const json & details) {
            blob.internal_foreign_graph_index = details["internal_foreign_graph_index"];
        }

        // template<>
        // void update_blob_indices_specific(blobs_ns::FOREIGN_GRAPH_NODE & blob, const map_t & index_map) {
        //     blob.internal_foreign_graph_index = mapped_index(blob.internal_foreign_graph_index, index_map);
        // }

        void assign_blob_specific(blobs_ns::ORIGIN_RAE_EDGE & blob, const json & details) {}

        void assign_blob_specific(blobs_ns::ORIGIN_GRAPH_EDGE & blob, const json & details) {}

        void assign_blob_specific(blobs_ns::FOREIGN_ENTITY_NODE & blob, const json & details) {
            blob.entity_type = details["entity_type"];
        }

        void assign_blob_specific(blobs_ns::FOREIGN_ATTRIBUTE_ENTITY_NODE & blob, const json & details) {
            blob.primitive_type = details["primitive_type"];
        }

        void assign_blob_specific(blobs_ns::FOREIGN_RELATION_EDGE & blob, const json & details) {
            blob.relation_type = details["relation_type"];
        }

        void assign_blob_specific(blobs_ns::VALUE_TYPE_EDGE & blob, const json & details) {}

        void assign_blob_specific(blobs_ns::VALUE_EDGE & blob, const json & details) {}


        Graph create_from_json(std::unordered_map<blob_index,json> blobs) {
            // Lookup the UID first
            auto root_item = blobs[42];
            std::string uid_s = root_item["uid"];

            BaseUID uid = BaseUID::from_hex(uid_s);
    
            // We are going to work with a GraphData that's not managed by the
            // butler. This is a bit weird but should avoid any hassle.
            GraphData * gd = create_GraphData(MMap::MMAP_STYLE_ANONYMOUS, nullptr, uid, false);
            
            std::unordered_map<blob_index,blob_index> index_map_to_new;
            index_map_to_new.insert({constants::ROOT_NODE_blob_index, constants::ROOT_NODE_blob_index});

            // Do the root node immediately before anything else
            auto root_uzr = blank_instantiate(
                BT.ROOT_NODE,
                gd,
                root_item["edges"].get<std::vector<blob_index>>(),
                0,
                0,
                ""
            );
            internals::assign_uid(root_uzr, uid);
            internals::set_data_layout_version_info(data_layout_version, *gd);
            internals::set_graph_revision_info("0", *gd);

            // Insert everything in first and then we will go back and reassign the indices.
            auto t_start = now();
            auto print_interval = 5*seconds;
            auto next_print_time = t_start + print_interval;

            for(auto item : blobs) {
                blob_index cur_index = gd->write_head;
                blob_index old_index = item.first;
                auto details = item.second;
                std::string type_s = details["type"];

                BlobType type = lookup_BlobType(type_s);
                if(type == BlobType::ROOT_NODE)
                    continue;

                if(now() > next_print_time) {
                    std::cerr << "Taking a long time to create blobs. Up to blob " << cur_index << std::endl;
                    next_print_time = next_print_time + print_interval;
                }

                index_map_to_new.insert({old_index, cur_index});

                std::optional<std::vector<blob_index>> edges;
                if(details.contains("edges"))
                    edges = details["edges"].get<std::vector<blob_index>>();

                blob_index source_node_index = 0;
                blob_index target_node_index = 0;
                if(details.contains("source_node_index")) {
                    source_node_index = details["source_node_index"];
                    target_node_index = details["target_node_index"];
                }

                std::string data_buffer = "";
                if(details.contains("data_buffer")) {
                    std::vector<char> temp = details["data_buffer"];
                    data_buffer = std::string(temp.begin(), temp.end());
                }

                auto ezr = blank_instantiate(type,
                                             gd,
                                             edges,
                                             source_node_index,
                                             target_node_index,
                                             data_buffer);

                if(has_uid(ezr)) {
                    BaseUID & blob_uid = blob_uid_ref(ezr);
                    blob_uid = BaseUID::from_hex(details["uid"].get<std::string>());
                }
                    

                // Everything else that specific to a blob
                visit_blob([&](auto & blob) {
                    assign_blob_specific(blob, details);
                }, ezr);
            }

            next_print_time = now() + print_interval;
            // Now fix up the indices
            blob_index cur_index = constants::ROOT_NODE_blob_index;
            while(cur_index < gd->write_head) {
                if(now() > next_print_time) {
                    std::cerr << "Taking a long time to reassign indices. Up to blob " << cur_index << "/" << gd->write_head.load() << std::endl;
                    next_print_time = next_print_time + print_interval;
                }

                EZefRef ezr{cur_index, *gd};

                if(has_edge_list(ezr)) {
                    blob_index * blob_edges = edge_indexes(ezr);
                    for(blob_index * this_edge = blob_edges; this_edge != blob_edges + local_edge_indexes_capacity(ezr); this_edge++) {
                        if(*this_edge == 0  || *this_edge == blobs_ns::sentinel_subsequent_index)
                            continue;
                        *this_edge = mapped_index(*this_edge, index_map_to_new);
                    }
                }

                if(has_source_target_node(ezr)) {
                    visit_blob_with_source_target([&](auto & blob) {
                        blob.source_node_index = mapped_index(blob.source_node_index, index_map_to_new);
                        blob.target_node_index = mapped_index(blob.target_node_index, index_map_to_new);
                    }, ezr);
                }

                visit_blob([&](auto & blob) {
                    update_blob_indices_specific(blob, index_map_to_new);
                }, ezr);

                cur_index += num_blob_indexes_to_move(size_of_blob(ezr));
            }

            // We do apply_blobs here ourselves as this is the one way to ensure
            // double linking updates are done (although this might not be necessary).
            // create_from_bytes does not do this.
            {
                // We have to cheat a little here by taking a reference without letting the butler know this graph exists...
                Graph g(*gd);
                LockGraphData lock{gd};
                blob_index cur_index = constants::ROOT_NODE_blob_index;
                while (cur_index < gd->write_head) {
                    EZefRef uzr(cur_index, *gd);
                    apply_action_blob(*gd, uzr, true);
                    cur_index += blob_index_size(uzr);
                }

                if(!verification::verify_graph_double_linking(g)
                   || !verification::verify_chronological_instantiation_order(g)) {
                    throw std::runtime_error("Verificaiton failed after rebuilding graph");
                }
            }
            // Now use this byte data to construct a new graph which will do
            // apply blobs, create the caches and make it managed by the butler.
            gd->read_head = gd->write_head.load();
            return Graph::create_from_bytes(graph_as_UpdatePayload(*gd));
        }


        BlobType lookup_BlobType(std::string name) {
            if(name == "_unspecified") return BlobType::_unspecified;
            if(name == "ROOT_NODE") return BlobType::ROOT_NODE;
            if(name == "TX_EVENT_NODE") return BlobType::TX_EVENT_NODE;
            if(name == "RAE_INSTANCE_EDGE") return BlobType::RAE_INSTANCE_EDGE;
            if(name == "TO_DELEGATE_EDGE") return BlobType::TO_DELEGATE_EDGE;
            if(name == "NEXT_TX_EDGE") return BlobType::NEXT_TX_EDGE;
            if(name == "ENTITY_NODE") return BlobType::ENTITY_NODE;
            if(name == "ATTRIBUTE_ENTITY_NODE") return BlobType::ATTRIBUTE_ENTITY_NODE;
            if(name == "VALUE_NODE") return BlobType::VALUE_NODE;
            if(name == "RELATION_EDGE") return BlobType::RELATION_EDGE;
            if(name == "DELEGATE_INSTANTIATION_EDGE") return BlobType::DELEGATE_INSTANTIATION_EDGE;
            if(name == "DELEGATE_RETIREMENT_EDGE") return BlobType::DELEGATE_RETIREMENT_EDGE;
            if(name == "INSTANTIATION_EDGE") return BlobType::INSTANTIATION_EDGE;
            if(name == "TERMINATION_EDGE") return BlobType::TERMINATION_EDGE;
            if(name == "ATOMIC_VALUE_ASSIGNMENT_EDGE") return BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE;
            if(name == "DEFERRED_EDGE_LIST_NODE") return BlobType::DEFERRED_EDGE_LIST_NODE;
            if(name == "ASSIGN_TAG_NAME_EDGE") return BlobType::ASSIGN_TAG_NAME_EDGE;
            if(name == "NEXT_TAG_NAME_ASSIGNMENT_EDGE") return BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE;
            if(name == "FOREIGN_GRAPH_NODE") return BlobType::FOREIGN_GRAPH_NODE;
            if(name == "ORIGIN_RAE_EDGE") return BlobType::ORIGIN_RAE_EDGE;
            if(name == "ORIGIN_GRAPH_EDGE") return BlobType::ORIGIN_GRAPH_EDGE;
            if(name == "FOREIGN_ENTITY_NODE") return BlobType::FOREIGN_ENTITY_NODE;
            if(name == "FOREIGN_ATTRIBUTE_ENTITY_NODE") return BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE;
            if(name == "FOREIGN_RELATION_EDGE") return BlobType::FOREIGN_RELATION_EDGE;
            if(name == "VALUE_TYPE_EDGE") return BlobType::VALUE_TYPE_EDGE;
            if(name == "VALUE_EDGE") return BlobType::VALUE_EDGE;
            throw std::runtime_error("Unknown blob type: " + name);
        }

        EZefRef blank_instantiate(BlobType bt,
                                  GraphData * gd,
                                  std::optional<std::vector<blob_index>> edges,
                                  blob_index source_node_index,
                                  blob_index target_node_index,
                                  std::string data_buffer
        ) {
            // This instantiates the blob as far as everything to do with size
            // is concerned. It won't do the blob-specific details.
			using namespace blobs_ns;
            void * new_ptr = (void*)(std::uintptr_t(gd) + gd->write_head * constants::blob_indx_step_in_bytes);
            MMap::ensure_or_alloc_range(new_ptr,
                                        blobs_ns::max_basic_blob_size + (edges ? edges->size()*sizeof(blob_index) : 0));
            *(BlobType*)new_ptr = bt;
			auto this_new_blob = EZefRef(new_ptr);

            if(has_edge_list(this_new_blob)) {
                visit_blob_with_edges(overloaded {
                        [&](edge_info & blob_edges) {
                            int calced_size = edges->size()+1;
                            static_assert(constants::blob_indx_step_in_bytes == 4*sizeof(blob_index));
                            size_t start_offset = (char*)&blob_edges - (char*)this_new_blob.blob_ptr + offsetof(edge_info, indices);
                            // This will overcompensate by an extra blob if it was already perfectly aligned... no big deal.
                            calced_size += 4 - (start_offset/sizeof(blob_index) + calced_size + 1) % 4;

                            new (&blob_edges) edge_info(calced_size);
                        },
                        [&](DEFERRED_EDGE_LIST_NODE::deferred_edge_info &) {
                            throw std::runtime_error("Shouldn't get here");
                        }
                    },
                    this_new_blob);

                // Assign edges
                blob_index * blob_edges = edge_indexes(this_new_blob);
                for(auto edge : *edges) {
                    *blob_edges = edge;
                    blob_edges++;
                }

                // Need to set the final_blob now otherwise it won't get done
                // (not even in apply_actions_to_blobs)
                blob_index * last_blob = (blob_index*)((uintptr_t)blob_edges - ((uintptr_t)blob_edges % constants::blob_indx_step_in_bytes));
                *last_edge_holding_blob(this_new_blob) = blob_index_from_ptr(last_blob);
            }


            if(has_source_target_node(this_new_blob)) {
                visit_blob_with_source_target([&](auto & blob) {
                    blob.source_node_index = source_node_index;
                    blob.target_node_index = target_node_index;
                },
                    this_new_blob);
            }

            if(has_data_buffer(this_new_blob)) {
                visit_blob_with_data_buffer([&](auto & blob) {
                    internals::copy_to_buffer(get_data_buffer(blob),
                                              blob.buffer_size_in_bytes,
                                              data_buffer);
                },
                    this_new_blob);
            }
			
			move_head_forward(*gd);
			return this_new_blob;
		}
    }
}