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

#include "export_statement.h"

#include "constants.h"
#include "conversions.h"
#include "low_level_api.h"
#include "xxhash64.h"
#include "butler/butler.h"

namespace zefDB {
    namespace conversions {
        bool can_convert_0_3_0_to_0_2_0(void * start, size_t len) {
            char * ptr = (char*)start;
            char * end = ptr + len;

            while(ptr < end) {
                bool allowed = _visit_blob(overloaded {
                        [](blobs_ns::VALUE_NODE & x) {
                            return false;
                        },
                        [](blobs_ns::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE & x) {
                            return false;
                        },
                        [](auto & x) {
                            return true;
                        },
                    },
                    ptr);
                if(!allowed)
                    return false;
                ptr += _blob_index_size(ptr) * constants::blob_indx_step_in_bytes;
            }
            return true;
        }

        bool can_represent_graph_as_payload(const GraphData & gd, std::string target_layout) {
            if(target_layout == "")
                return true;

            // Assuming the graph is of data layout 0.3.0
            if(target_layout == "0.2.0") {
                void * ptr = ptr_from_blob_index(constants::ROOT_NODE_blob_index, gd);
                void * end_ptr = ptr_from_blob_index(gd.read_head, gd);
                size_t len = (char*)end_ptr - (char*)ptr;
                Butler::ensure_or_get_range(ptr, len);
                return can_convert_0_3_0_to_0_2_0(ptr, len);
            }

            if(target_layout == "0.3.0")
                return true;

            throw std::runtime_error("Don't know target layout: " + target_layout);
        }

        uint64_t hash_with_only_layout_version_different(std::string new_layout, void * start, size_t len, uint64_t seed) {
            // XXH64_state_t* const state = XXH64_createState();
            // if(state == NULL) throw std::runtime_error("XXH64 failure");
            // if(XXH64_reset(state, seed) == XXH_ERROR) throw std::runtime_error("XXH64 failure");

            // // Do the root node part
            // auto & root = *(blobs_ns::ROOT_NODE*)start;
            // memcpy(root.data_layout_version_info, new_layout.data(), new_layout.size());
            // root.actual_written_data_layout_version_info_size = new_layout.size();

            // size_t root_size = sizeof(blobs_ns::ROOT_NODE);
            // if(XXH64_update(state, &root, root_size) == XXH_ERROR) std::runtime_error("XXH64 failure");
            // if(XXH64_update(state, (char*)start + root_size, len - root_size) == XXH_ERROR) std::runtime_error("XXH64 failure");

            // uint64_t hash = XXH64_digest(state);
            // XXH64_freeState(state);
            // return hash;

            XXHash64 xxh(seed);

            // Do the root node part
            const size_t root_size = sizeof(blobs_ns::ROOT_NODE);

            if(len == 0)
                return 0;
            if(len < root_size)
                throw std::runtime_error("Trying to hash a graph with a different layout version, but we don't have enough data to replace the root blob");
            // We can't create a blobs_ns::ROOT_NODE object here as the
            // overflowing indices causes lots of problems. Instead we work with
            // the raw buffer size.
            char buf[root_size];
            memcpy(buf, start, root_size);
            auto root_ptr = (blobs_ns::ROOT_NODE *)buf;
            memcpy(&root_ptr->data_layout_version_info, new_layout.data(), new_layout.size());
            root_ptr->actual_written_data_layout_version_info_size = new_layout.size();

            xxh.add(buf, root_size);
            xxh.add((char*)start + root_size, len - root_size);

            return xxh.hash();
        }

        uint64_t hash_0_3_0_as_if_0_2_0(void * start, size_t len, uint64_t seed) {
            return hash_with_only_layout_version_different("0.2.0", start, len, seed);
        }

        std::string convert_blobs_0_3_0_to_0_2_0(std::string && blobs) {
            // We go through each blob and update:
            // a) the root node blob needs to contain a later layout version
            // b) assert for the lack of ATOMIC_VALUE_NODEs (no change)

            char * start = blobs.data();
            char * ptr = start;
            char * end = start + blobs.size();

            while(ptr < end) {
                _visit_blob(overloaded {
                        [](blobs_ns::VALUE_NODE & x) {
                            throw std::runtime_error("We can't convert a graph to 0.2.0 when there are atomic value nodes!");
                            // Going to need a lot more to do...
                            // So much that it's not really worth it.
                        },
                        [start,ptr](blobs_ns::ROOT_NODE & x) {
                            // If this one is a delegate then it'll have no data layout version
                            // Note: because we are looking at raw pointers, we
                            // can't use the usually methods to travesre the
                            // graph. So instead of calling is_delegate, we see
                            // if this is the first blob in the graph.
                            if(start == ptr) {
                                memcpy(x.data_layout_version_info, "0.2.0", strlen("0.2.0"));
                                force_assert(x.actual_written_data_layout_version_info_size == strlen("0.2.0"));
                            }
                        },
                        [](auto & x) {},
                    },
                    ptr);

                ptr += _blob_index_size(ptr) * constants::blob_indx_step_in_bytes;
            }

            return blobs;
        }

        UpdatePayload create_update_payload_as_if_0_2_0(GraphData & gd, UpdateHeads update_heads) {
            UpdatePayload payload = create_update_payload_current(gd, update_heads);

            payload.rest[0] = convert_blobs_0_3_0_to_0_2_0(std::move(payload.rest[0]));

            payload.j["hash_full_graph"] = gd.hash(constants::ROOT_NODE_blob_index, update_heads.blobs.to, 0, "0.2.0");
            payload.j["data_layout_version"] = "0.2.0";

            // We remove the av_hash cache but leave the rest
            int cache_index = -1;
            int iter_i = 0;
            for(auto & cache : payload.j["caches"]) {
                if(cache["name"] == "_av_hash_lookup") {
                    if(payload.rest[iter_i+1].size() != 0)
                        throw std::runtime_error("The rest size for the atomic value hash lookup is nonzero! Shouldn't have got here.");

                    cache_index = iter_i;
                    break;
                }
                iter_i++;
            }
            // Test as we might not have anything to do here
            if(cache_index != -1) {
                auto caches = payload.j["caches"].get<std::vector<std::string>>();
                caches.erase(caches.begin()+cache_index);
                payload.j["caches"] = caches;

                // +1 here as the blobs are in index 0.
                payload.rest.erase(payload.rest.begin()+cache_index + 1);
            }

            return payload;
        }

        std::string convert_blobs_0_2_0_to_0_3_0(std::string && blobs) {
            char * start = blobs.data();
            char * ptr = start;
            char * end = start + blobs.size();

            while(ptr < end) {
                _visit_blob(overloaded {
                        [](blobs_ns::VALUE_NODE & x) {
                            throw std::runtime_error("No value nodes should exist on a 0.2.0 graph.");
                        },
                        [start,ptr](blobs_ns::ROOT_NODE & x) {
                            // If this one is a delegate then it'll have no data layout version
                            // Note: because we are looking at raw pointers, we
                            // can't use the usually methods to travesre the
                            // graph. So instead of calling is_delegate, we see
                            // if this is the first blob in the graph.
                            if(start == ptr) {
                                memcpy(x.data_layout_version_info, "0.3.0", strlen("0.3.0"));
                                force_assert(x.actual_written_data_layout_version_info_size == strlen("0.3.0"));
                            }
                        },
                        [](auto & x) {},
                    },
                    ptr);

                ptr += _blob_index_size(ptr) * constants::blob_indx_step_in_bytes;
            }

            return blobs;
        }

        UpdatePayload convert_payload_0_2_0_to_0_3_0(const UpdatePayload & payload) {
            if(payload.j["data_layout_version"] != "0.2.0")
                throw std::runtime_error("Wrong data layout version");

            UpdatePayload out;

            // We go through each blob and update:
            // a) the root node blob needs to contain a later layout version
            std::string blobs_in = payload.rest[0];
            std::string blobs = convert_blobs_0_2_0_to_0_3_0(std::move(blobs_in));

            out.j = json{
                {"blob_index_lo", payload.j["blob_index_lo"]},
                {"blob_index_hi", payload.j["blob_index_hi"]},
                {"graph_uid", payload.j["graph_uid"]},
                {"index_of_latest_complete_tx_node", payload.j["index_of_latest_complete_tx_node"]},
                // Unfortunately we can't copy the full hash because with the
                // change of the data layout it affects the full graph hash.
                // {"hash_full_graph", ...},
                {"data_layout_version", "0.3.0"}
            };

            // We are able to do a full hash in this case.
            if(payload.j["blob_index_lo"] == constants::ROOT_NODE_blob_index)
                out.j["hash_full_graph"] = internals::hash_memory_range(blobs.data(), blobs.size());

            out.j["caches"] = payload.j["caches"];

            out.rest.push_back(std::move(blobs));
            for(auto it = payload.rest.cbegin()+1 ; it != payload.rest.cend() ; it++) {
                out.rest.push_back(*it);
            }

            return out;
        }


        void modify_update_heads(json & cache_heads, std::string working_layout) {
            if(working_layout == "0.2.0") {
                cache_heads.erase("_av_hash_lookup");
            }
        }


        std::string version_layout(int version) {
            if(version <= 6)
                return "0.2.0";
            if(version == 7)
                return "0.3.0";
            throw std::runtime_error("Did not have a way to say what the upstream layout was for zefdb protocol version: " + to_str(version));
        }
    }
}