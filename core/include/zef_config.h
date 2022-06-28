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

#include <iostream>
#include <variant>
#include <vector>
#include <array>
#include <algorithm>
#include "include_fs.h"

namespace zefDB {

    // Note: the order needs bool then int then double as pybind will otherwise
    // cast inputs to a different type.
    using config_var_t = std::variant<std::string, bool, int, double>;

    struct ConfigItem {
        std::string path;

        // The default also sets the allowed type of the variable
        config_var_t default_value;

        // If filled, these are the only allowed options.
        std::vector<config_var_t> options;

        // Environment variable to override config variable
        std::string env_var;
        std::string doc;

        // Need this really specific constructor to avoid issues with const char * converting to bool instead of std::string
        ConfigItem(const char * path, const char * default_value, std::vector<const char *> options, const char * env_var, const char * doc) :
            path(path),
            default_value(std::string(default_value)),
            env_var(env_var),
            doc(doc) {
            std::transform(options.begin(), options.end(), std::back_inserter(this->options), [](const char * opt) { return std::string(opt); });
        }

        ConfigItem(std::string path, config_var_t default_value, std::vector<config_var_t> options, std::string env_var, std::string doc) :
            path(path),
            default_value(default_value),
            options(options),
            env_var(env_var),
            doc(doc) {}
    };

       
    LIBZEF_DLL_EXPORTED bool validate_config_file();

    LIBZEF_DLL_EXPORTED config_var_t get_config_var(std::string key);
    LIBZEF_DLL_EXPORTED void set_config_var(std::string key, config_var_t val);
    LIBZEF_DLL_EXPORTED std::vector<std::pair<std::string,config_var_t>> list_config(std::string filter="");

    LIBZEF_DLL_EXPORTED std::filesystem::path zefdb_config_path();
}

