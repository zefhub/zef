/*
 * Copyright 2022 Synchronous Technologies Pte Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "export_statement.h"
#include "butler/messages.h"
#include "butler/butler.h"

namespace zefDB {
    namespace conversions {

        using Messages::UpdatePayload;
        using Butler::UpdateHeads;

        bool can_convert_0_3_0_to_0_2_0(void * start, size_t len);
        uint64_t hash_0_3_0_as_if_0_2_0(void * start, size_t len, uint64_t seed);
        std::string convert_blobs_0_3_0_to_0_2_0(std::string && blobs);
        UpdatePayload create_update_payload_as_if_0_2_0(GraphData & gd, UpdateHeads update_heads);

        std::string convert_blobs_0_2_0_to_0_3_0(std::string && blobs);
        UpdatePayload convert_payload_0_2_0_to_0_3_0(const UpdatePayload & payload);

        void modify_update_heads(json & cache_heads, std::string working_layout);

        LIBZEF_DLL_EXPORTED std::string version_layout(int version);

        LIBZEF_DLL_EXPORTED bool can_represent_graph_as_payload(const GraphData & gd, std::string target_layout);
    }
}