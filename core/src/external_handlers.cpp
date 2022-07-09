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

#include "external_handlers.h"

namespace zefDB {
    namespace internals {
        // ** Merge handler
        std::optional<std::function<merge_handler_t>> merge_handler;
        json pass_to_merge_handler(Graph g, const json & payload) {
            if(!merge_handler)
                throw std::runtime_error("Merge handler has not been assigned.");

            return (*merge_handler)(g, payload);
        }

        void register_merge_handler(std::function<merge_handler_t> func) {
            if(merge_handler)
                throw std::runtime_error("Merge handler has already been registered.");
            merge_handler = func;
        }

        void remove_merge_handler() {
            if(!merge_handler)
                std::cerr << "Warning, no merge_handler registered to be removed." << std::endl;
            merge_handler.reset();
        }

        // ** Schema validator
        std::optional<std::function<schema_validator_t>> schema_validator;
        void pass_to_schema_validator(ZefRef tx) {
            if(schema_validator)
                (*schema_validator)(tx);
        }

        void register_schema_validator(std::function<schema_validator_t> func) {
            if(schema_validator)
                throw std::runtime_error("schema_validator has already been registered.");
            schema_validator = func;
        }

        void remove_schema_validator() {
            if(!schema_validator)
                std::cerr << "Warning, no schema_validator registered to be removed." << std::endl;
            schema_validator.reset();
        }

        // ** Value type check
        std::optional<std::function<value_type_check_t>> value_type_check;
        bool pass_to_value_type_check(value_variant_t val, SerializedValue type) {
            if(!value_type_check)
                throw std::runtime_error("Value type check handler has not been assigned.");
            return (*value_type_check)(val, type);
        }

        void register_value_type_check(std::function<value_type_check_t> func) {
            if(value_type_check)
                throw std::runtime_error("value_type_check has already been registered.");
            value_type_check = func;
        }

        void remove_value_type_check() {
            if(!value_type_check)
                std::cerr << "Warning, no value_type_check registered to be removed." << std::endl;
            value_type_check.reset();
        }
    }
}