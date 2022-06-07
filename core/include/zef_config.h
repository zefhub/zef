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
    };

    const ConfigItem config_spec[] = {
        {"login.autoConnect", "auto", {"false", "auto", "always"}, "ZEFDB_LOGIN_AUTOCONNECT",
         "Whether zefdb should automatically connect to ZefHub on start of the butler."},
        {"butler.autoStart", true, {}, "ZEFDB_BUTLER_AUTOSTART",
         "Whether the butler will automatically be started on import of the zefdb module."},
    };
    const int num_config_spec = sizeof(config_spec) / sizeof(ConfigItem);

       
    LIBZEF_DLL_EXPORTED bool validate_config_file();

    LIBZEF_DLL_EXPORTED config_var_t get_config_var(std::string key);
    LIBZEF_DLL_EXPORTED void set_config_var(std::string key, config_var_t val);
    // LIBZEF_DLL_EXPORTED SOMETHING list_config(std::string filter="");
}

