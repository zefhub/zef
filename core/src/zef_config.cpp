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

#include "zef_config.h"
#include "butler/butler.h"

#include "yaml-cpp/yaml.h"

namespace zefDB {

    // We force config_spec to be loaded from within a function, rather than a
    // global, as this allows others globals to make use of it when they
    // themselves are being initialised.
    //
    // This used to be a const array but keeping it as a vector is fine.
    const std::vector<ConfigItem>& all_config_spec() {
        static const std::vector<ConfigItem> config_spec{
            {"login.autoConnect", "auto", {"no", "auto", "always"}, "ZEFDB_LOGIN_AUTOCONNECT",
            "Whether zefdb should automatically connect to ZefHub on start of the butler."},
            {"butler.autoStart", true, {}, "ZEFDB_BUTLER_AUTOSTART",
            "Whether the butler will automatically be started on import of the zefdb module."},
            {"login.zefhubURL", "wss://hub.zefhub.io", {}, "ZEFHUB_URL",
            "Which URL to connect to ZefHub."},
            {"tokens.cachePath", "$CONFIG/tokens_cache.json", {}, "ZEFDB_TOKENS_CACHE_PATH",
            "Where are the tokens cached between sessions"},
            {"tokens.pullOnConnect", true, {}, "ZEFDB_TOKENS_PULL_ON_CONNECT",
            "When connecting, get latest set for user."},
        };
        return config_spec;
    }

    std::unordered_map<std::string, config_var_t> session_overrides;

    std::filesystem::path zefdb_config_path() {
        char * env = std::getenv("ZEFDB_SESSION_PATH");
        if (env != nullptr)
            return std::string(env);

#ifdef ZEF_WIN32
        env = std::getenv("LOCALAPPDATA");
#else
        env = std::getenv("HOME");
#endif
        if (env == nullptr)
            throw std::runtime_error("No HOME env set!");

        std::filesystem::path path(env);
        path /= ".zef";
        if(!std::filesystem::exists(path))
            std::filesystem::create_directories(path);
        return path;
    }

    std::filesystem::path config_file_path() {
        return zefdb_config_path() / "config.yaml";
    }

    bool config_file_exists() {
        return std::filesystem::exists(config_file_path());
    }

    void ensure_config_file() {
        if(config_file_exists())
            return;

        std::filesystem::create_directories(config_file_path().parent_path());
        std::ofstream touch(config_file_path());
    }

    const ConfigItem & get_spec(std::string key) {
        for(auto & item : all_config_spec()) {
            if(item.path == key)
                return item;
        }
        throw std::runtime_error("Don't recognise path '" + key + "' in config");
    }

    std::vector<std::string> split_path(std::string key) {
        std::vector<std::string> out;
        int p = -1;
        while(true) {
            int last_p = p+1;
            p = key.find('.', last_p);
            if(p == std::string::npos) {
                out.push_back(key.substr(last_p));
                break;
            }
            out.push_back(key.substr(last_p, p));
        }
        return out;
    }

    config_var_t get_config_var(std::string key) {
        FileLock lock(zefdb_config_path());
        auto search = session_overrides.find(key);
        if(search != session_overrides.cend()) {
            return search->second;
        }

        auto & spec = get_spec(key);
        char * env = getenv(spec.env_var.c_str());
        if(env != NULL) {
            return std::visit(overloaded {
                    [&env](const bool&) -> config_var_t { return parse_string_bool(std::string(env)); },
                    [&env](const int&) -> config_var_t { return std::stoi(std::string(env)); },
                    [&env](const double&) -> config_var_t { return std::stod(std::string(env)); },
                    [&env](const std::string&) -> config_var_t { return std::string(env); },
                }, spec.default_value);
        }

        if(config_file_exists()) {
            YAML::Node config = YAML::LoadFile(config_file_path().string());
            // TODO: What happens with a bad config file?

            for(auto section : split_path(key)) {
                config = config[section];
            }

            if(config.IsDefined()) {
                auto value = std::visit(overloaded {
                        [&config](const bool&) -> config_var_t { return config.as<bool>(); },
                        [&config](const int&) -> config_var_t { return config.as<int>(); },
                        [&config](const double&) -> config_var_t { return config.as<double>(); },
                        [&config](const std::string&) -> config_var_t { return config.as<std::string>(); },
                    }, spec.default_value);

                if(spec.options.size() == 0)
                    return value;

                for(auto & opt : spec.options) {
                    if(opt == value)
                        return value;
                }

                throw std::runtime_error("Config option '" + key + "' has a value which is not allowed by the options.");
            }
        }

        return spec.default_value;
    }

    // x get_full_config()
    // TODO: Has to lookup every key for defaults/overrides and indicate this too in the return
    
    void set_config_var(std::string key, config_var_t val) {
        FileLock lock(zefdb_config_path());
        ensure_config_file();

        auto & spec = get_spec(key);

        if(spec.default_value.index() != val.index())
            throw std::runtime_error("Trying to set config variable '" + key + "' to a type (" + to_str(val.index()) + ") which is not the same as the default (" + to_str(spec.default_value.index()) + ")");

        if(spec.options.size() > 0) {
            bool found = false;
            for(auto & opt : spec.options) {
                if(opt == val) {
                    found = true;
                    break;
                }
            }
            if(!found)
                throw std::runtime_error("Trying to set config variable '" + key + "' to a value which is not in the allowed options.");
        }

        try {
            YAML::Node config = YAML::LoadFile(config_file_path().string());

            auto sections = split_path(key);
            auto last = sections.end() - 1;
            YAML::Node config_part;
            for(auto it = sections.begin() ; it != last ; it++) {
                auto section = *it;
                // This is the weird part
                if(it == sections.begin()) {
                    config_part = config[section];
                } else {
                    config_part = config_part[section];
                }

            }

            std::visit([&config_part,&last](auto & _val) {
                config_part[*last] = _val;
            }, val);

            std::ofstream fout(config_file_path());

            fout << config;

        } catch(...) {
            std::cerr << "Some kind of error when saving config file. Still assign varaible for session." << std::endl;
        }

        session_overrides[key] = val;
    }

    std::vector<std::pair<std::string,config_var_t>> list_config(std::string filter) {
        std::vector<std::pair<std::string,config_var_t>> out;

        for(auto & item : all_config_spec()) {
            if(filter != "" && item.path.find(filter) == std::string::npos)
                continue;

            out.emplace_back(item.path, get_config_var(item.path));
        }

        return out;
    }

    void recurse_validate(std::string parent_path, YAML::Node self) {
        if(self.IsMap()) {
            for(auto item : self) {
                std::string new_path = parent_path;
                if(parent_path != "")
                    new_path += ".";
                new_path += item.first.as<std::string>();
                recurse_validate(new_path, item.second);
            }
        } else {
            // If the path is "" then this is likely that the config file is empty.
            if(parent_path != "")
                // We only care about the path, not the value in it.
                get_spec(parent_path);
        }
    };

    bool validate_config_file() {
        try {
            // Get all variables so that everything is checked.
            list_config();

            // Also check every entry in the file corresponds to something in the spec.
            if(config_file_exists()) {
                YAML::Node config = YAML::LoadFile(config_file_path().string());
                recurse_validate("", config);
            }
        } catch(const std::exception & exc) {
            std::cerr << "validate_config_file got an exception: " << exc.what() << std::endl;
            return false;
        }

        return true;
    }
}