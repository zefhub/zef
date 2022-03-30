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

#include "butler/butler.h"
 
// These are low-level functions necessary for the functionality of zefhub.

namespace zefDB {

    namespace Butler {
        LIBZEF_DLL_EXPORTED void add_entity_type(const token_value_t & indx, const std::string & name);
        LIBZEF_DLL_EXPORTED void add_relation_type(const token_value_t & indx, const std::string & name);
        LIBZEF_DLL_EXPORTED void add_enum_type(const enum_indx & indx, const std::string& name);
        LIBZEF_DLL_EXPORTED void add_keyword(const token_value_t & indx, const std::string & name);
    }
}
