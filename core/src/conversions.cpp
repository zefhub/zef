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

namespace zefDB {
    namespace conversions {
        UpdatePayload convert_payload_0_3_0_to_0_2_0(const UpdatePayload & payload) {
            if(payload.j["data_layout_version"] != "0.3.0")
                throw std::runtime_error("Wrong data layout version");

            UpdatePayload out;

            // We go through each blob and update:
            // a) the root node blob needs to contain a later layout version
            // b) assert for the lack of ATOMIC_VALUE_NODEs (no change)
            std::string blobs = payload.rest[0];
            char * start = blobs.data();
            char * ptr = start;
            char * end = start + blobs.size();

            while(ptr < end) {
                _visit_blob(overloaded {
                        [](blobs_ns::ATOMIC_VALUE_NODE & x) {
                            throw std::runtime_error("We can't convert a graph to 0.2.0 when there are atomic value nodes!");
                        },
                        [](blobs_ns::ROOT_NODE & x) {
                            memcpy(x.data_layout_version_info, "0.2.0", strlen("0.2.0"));
                            force_assert(x.actual_written_data_layout_version_info_size != strlen("0.2.0"));
                        },
                        [](auto & x) {},
                    },
                    ptr);
            }

            out.rest.push_back(std::move(blobs));

            out.j = json{
                {"blob_index_lo", payload.j["blob_index_lo"]},
                {"blob_index_hi", payload.j["blob_index_hi"]},
                {"graph_uid", payload.j["graph_uid"]},
                {"index_of_latest_complete_tx_node", payload.j["index_of_latest_complete_tx_node"]},
                // Unfortunately we can't copy the full hash because with the
                // change of the data layout it affects the full graph hash.
                // {"hash_full_graph", ...},
                {"data_layout_version", "0.2.0"}
            };

            // We remove the av_hash cache but leave the rest

            std::vector<json> new_caches;
            int rest_index = 1;
            for(auto & cache : payload.j["caches"]) {
                if(cache["name"] == "_av_hash_lookup") {
                    if(payload.rest[rest_index].size() != 0)
                        throw std::runtime_error("The rest size for the atomic value hash lookup is nonzero! Shouldn't have got here.");
                    continue;
                }
                out.rest.push_back(payload.rest[rest_index]);

                rest_index++;
            }
            out.j["caches"] = new_caches;

            return out;
        }
    }
}