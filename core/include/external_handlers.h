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

#include "zefref.h"
#include "graph.h"
#include "scalars.h"
#include "high_level_api.h"

namespace zefDB {

    namespace internals {
        // ** Merge handler
        typedef json (merge_handler_t)(Graph, const json &);
        LIBZEF_DLL_EXPORTED void register_merge_handler(std::function<merge_handler_t> func);
        LIBZEF_DLL_EXPORTED void remove_merge_handler();
        LIBZEF_DLL_EXPORTED json pass_to_merge_handler(Graph g, const json & payload);

        // ** Schema validator
        typedef void (schema_validator_t)(ZefRef);
        LIBZEF_DLL_EXPORTED void register_schema_validator(std::function<schema_validator_t> func);
        LIBZEF_DLL_EXPORTED void remove_schema_validator();
        LIBZEF_DLL_EXPORTED void pass_to_schema_validator(ZefRef tx);


        // ** Type checking
        typedef bool (value_type_check_t)(value_variant_t val, SerializedValue type);
        LIBZEF_DLL_EXPORTED void register_value_type_check(std::function<value_type_check_t> func);
        LIBZEF_DLL_EXPORTED void remove_value_type_check();
        LIBZEF_DLL_EXPORTED bool pass_to_value_type_check(value_variant_t val, SerializedValue type);

        // ** Primitive type determination
        typedef ValueRepType (determine_primitive_type_t)(AttributeEntityType aet);
        LIBZEF_DLL_EXPORTED void register_determine_primitive_type(std::function<determine_primitive_type_t> func);
        LIBZEF_DLL_EXPORTED void remove_determine_primitive_type();
        LIBZEF_DLL_EXPORTED ValueRepType pass_to_determine_primitive_type(AttributeEntityType aet);

    }
}
