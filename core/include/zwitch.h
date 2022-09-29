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

#include "fwd_declarations.h"
#include "zefDB_utils.h"
#include <chrono>
#include <iomanip>

namespace zefDB {
    struct Graph;

#define DEFINE_FLAG(x, default_val)             \
    bool flag_##x = default_val;                \
    bool x() const { return flag_##x; }         \
    Zwitch x(bool new_value) {                  \
        flag_##x = new_value;                   \
        return *this;                           \
    }
    // The above generates code similar to below:
    // bool flag_display_zefhub_communication_msgs = false;
    // bool display_zefhub_communication_msgs() const { return flag_display_zefhub_communication_msgs; };
    // Zwitch display_zefhub_communication_msgs(bool new_value) {
    //     flag_display_zefhub_communication_msgs = new_value;
    //     return *this;
    // };

    struct LIBZEF_DLL_EXPORTED Zwitch {
        DEFINE_FLAG(allow_dynamic_entity_type_definitions, true);
        DEFINE_FLAG(allow_dynamic_relation_type_definitions, true);
        DEFINE_FLAG(allow_dynamic_enum_type_definitions, true);
        DEFINE_FLAG(allow_dynamic_keyword_definitions, true);

        DEFINE_FLAG(short_output, true);

        DEFINE_FLAG(zefhub_communication_output, false);
        DEFINE_FLAG(graph_event_output, true);
        DEFINE_FLAG(developer_output, false);
        DEFINE_FLAG(debug_zefhub_json_output, false);
        DEFINE_FLAG(debug_times, false);
        DEFINE_FLAG(debug_allow_unknown_tokens, false);
        DEFINE_FLAG(extra_quiet, false);

        DEFINE_FLAG(throw_on_zefrefs_no_tx, false);
        DEFINE_FLAG(default_wait_for_tx_finish, true);
        DEFINE_FLAG(default_rollback_empty_tx, true);

        Zwitch allow_dynamic_type_definitions(bool new_value) {
            allow_dynamic_entity_type_definitions(new_value);
            allow_dynamic_relation_type_definitions(new_value);
            allow_dynamic_enum_type_definitions(new_value);
            allow_dynamic_keyword_definitions(new_value);
            return *this;
        };

        std::unordered_map<str, str> as_dict() { std::unordered_map<str, str> res;
            auto str_to_bool = [](bool f)->std::string { return f ? "yes" : "no"; };
            res["allow_dynamic_entity_type_definitions"] = str_to_bool(flag_allow_dynamic_entity_type_definitions);
            res["allow_dynamic_relation_type_definitions"] = str_to_bool(flag_allow_dynamic_relation_type_definitions);
            res["allow_dynamic_enum_type_definitions"] = str_to_bool(flag_allow_dynamic_enum_type_definitions);
            res["allow_dynamic_keyword_definitions"] = str_to_bool(flag_allow_dynamic_keyword_definitions);
            res["short_output"] = str_to_bool(flag_short_output);
            res["zefhub_communication_output"] = str_to_bool(flag_zefhub_communication_output);
            res["extra_quiet"] = str_to_bool(flag_extra_quiet);
            res["graph_event_output"] = str_to_bool(flag_graph_event_output);
            res["developer_output"] = str_to_bool(flag_developer_output);
            res["debug_zefhub_json_output"] = str_to_bool(flag_debug_zefhub_json_output);
            res["debug_times"] = str_to_bool(flag_debug_times);
            res["throw_on_zefrefs_no_tx"] = str_to_bool(flag_throw_on_zefrefs_no_tx);
            res["default_wait_for_tx_finish"] = str_to_bool(flag_default_wait_for_tx_finish);
            res["default_rollback_empty_tx"] = str_to_bool(flag_default_rollback_empty_tx);
            return res;
        }

        Zwitch() {
            if(check_env_bool("ZEFDB_QUIET")) {
                zefhub_communication_output(false);
                extra_quiet(true);
                graph_event_output(false);
            }
            if(check_env_bool("ZEFDB_VERBOSE")) {
                zefhub_communication_output(true);
                extra_quiet(false);
                graph_event_output(true);
            }
            if(check_env_bool("ZEFDB_DEVELOPER_OUTPUT")) {
                zefhub_communication_output(true);
                graph_event_output(true);
                developer_output(true);
                debug_times(true);
                // debug_zefhub_json_output(true);
            }
            if(check_env_bool("ZEFDB_DEVELOPER_ZEFHUB_JSON")) {
                debug_zefhub_json_output(true);
            }
        }
    };
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& o, Zwitch zw);

    LIBZEF_DLL_EXPORTED extern Zwitch zwitch;

    extern std::chrono::steady_clock::time_point time_start_of_process;
    inline void debug_time_print(std::string msg) {
        if(!zwitch.debug_times())
            return;
        
        std::chrono::duration<double> elapsed = std::chrono::steady_clock::now() - time_start_of_process;
        std::cerr << elapsed.count() << " seconds: " << msg << std::endl;
    }

    inline void developer_output(std::string s) {
        if(!zwitch.developer_output())
            return;

        std::time_t t = std::time(nullptr);
        std::tm tm = *std::localtime(&t);

        std::cerr << "DEV-" << std::put_time(&tm, "%T") << ": ";
        std::cerr << s << std::endl;
    }
}
