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

#include "butler/butler_zefhub.h"

namespace zefDB {
    namespace Butler {
        
        void add_entity_type(const token_value_t & indx, const std::string & name) {
            if (!butler_is_master)
                throw std::runtime_error("Shouldn't be calling this function in normal execution.");

            update_bidirection_name_map(global_token_store().ETs, indx, name);
        }

        void add_relation_type(const token_value_t & indx, const std::string & name) {
            if (!butler_is_master)
                throw std::runtime_error("Shouldn't be calling this function in normal execution.");

            update_bidirection_name_map(global_token_store().RTs, indx, name);
        }

        void add_enum_type(const enum_indx & indx, const std::string& name) {
            if (!butler_is_master)
                throw std::runtime_error("Shouldn't be calling this function in normal execution.");

            update_zef_enum_bidirectional_map(global_token_store().ENs, indx, name);
        }

        void add_keyword(const token_value_t & indx, const std::string & name) {
            if (!butler_is_master)
                throw std::runtime_error("Shouldn't be calling this function in normal execution.");

            update_bidirection_name_map(global_token_store().KWs, indx, name);
        }
    }
}
