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

////////////////////////////////////////////////
// * Connection management

bool Butler::want_upstream_connection() {
    if(check_env_bool("ZEFDB_OFFLINE_MODE"))
        return false;

    if(network.uri == "")
        return false;

    // If we are running, then we want to continue.
    if(network.is_running())
        return true;

    std::string auto_connect = std::get<std::string>(get_config_var("login.autoConnect"));

    if(auto_connect == "auto" && have_auth_credentials())
        return true;
    if(auto_connect == "always")
        return true;

    return false;
}

bool Butler::wait_for_auth(std::chrono::duration<double> timeout) {
    if(butler_is_master)
        throw std::runtime_error("Butler started as master - should never have got to this point of requesting auth.");
    if(network.uri == "")
        throw std::runtime_error("Not connecting to ZefHub without a URL");
    if(!want_upstream_connection())
        throw std::runtime_error("Not connecting to ZefHub until we have credentials. Either login using `login | run` to store your credentials or `login_as_guest | run` for a temporary guest login.");
    if(!network.managing_thread) {
        // throw std::runtime_error("Network is not trying to connect, can't wait for auth.");
        debug_time_print("before start connection");
        if(should_stop) {
            return false;
        }
        start_connection();
    }
    auto done_auth = [this]() { return should_stop || connection_authed || fatal_connection_error; };
    if(timeout < std::chrono::duration<double>(0)) {
        // wait_pred(auth_locker, done_auth);
        // We have a special spam message in here to let users know what's going on. 
        wait_pred(auth_locker, done_auth, std::chrono::seconds(3));
        if(!done_auth()) {
            std::cerr << "Warning: waiting for connection with ZefHub is taking a long time.\n\nIf you would like to see more information enable debug messages through `zwitch.zefhub_communication_output(True)` or setting the environment variable `ZEFDB_VERBOSE=true`.\n\nIf you would like to run Zef in offline mode, then start a new python session with the environment variable `ZEFDB_OFFLINE_MODE=TRUE`." << std::endl;
            wait_pred(auth_locker, done_auth);
        }
    }
    else {
        if(!wait_pred(auth_locker, done_auth, timeout))
            return false;
    }

    if(no_credentials_warning)
        throw std::runtime_error("No credentials present to allow auth to take place.");
    if(fatal_connection_error)
        throw std::runtime_error("Fatal connection error while trying to auth with ZefHub.\n\nIf you would like to run in offline mode, please restart your python session with the environment variable `ZEFDB_OFFLINE_MODE=TRUE`.");
    debug_time_print("finish wait_for_auth");
    return true;
}

void Butler::determine_login_token() {
    std::optional<std::string> key_string = load_forced_zefhub_key();
    if(key_string) {
        refresh_token = "";
        if(*key_string == constants::zefhub_guest_key) {
            std::cerr << "Connecting as guest user" << std::endl;
            have_logged_in_as_guest = true;
        } else  {
            api_key = *key_string;
        }
        return;
    } else if(!have_auth_credentials()) {
        no_credentials_warning = true;
        throw std::runtime_error("Have no existing credentials to determine auth token. You must login first.");
    } else {
        ensure_auth_credentials();
        if(have_logged_in_as_guest) {
            refresh_token = "";
        } else {
            auto credentials_file = credentials_path();
            std::ifstream file(credentials_file);
            json j = json::parse(file);
            refresh_token = j["refresh_token"].get<std::string>();
        }
    }
}

std::string Butler::who_am_i() {
    std::string name;

    std::optional<std::string> key_string = load_forced_zefhub_key();
    if(key_string) {
        if(*key_string == constants::zefhub_guest_key) {
            name = "GUEST via forced key";
        } else {
            name = "API key beginning with " + key_string->substr(0, 4) + "...";
        }
    } else if(!have_auth_credentials()) {
        name = "";
    } else {
        if(have_logged_in_as_guest) {
            name = "GUEST via login prompt";
        } else {
            auto credentials_file = credentials_path();
            if(!std::filesystem::exists(credentials_file)) {
                name = "";
            } else {
                std::ifstream file(credentials_file);
                json j = json::parse(file);
                refresh_token = j["refresh_token"].get<std::string>();
                std::string token = get_firebase_token_refresh_token(refresh_token);
                using traits = jwt::traits::nlohmann_json;
                auto decoded = jwt::decode<traits>(token);
                name = decoded.get_payload_claim("email").to_json();

                auto firebase_field = decoded.get_payload_claim("firebase").to_json();
                if(firebase_field.contains("sign_in_provider"))
                    name += " through firebase provider: " + firebase_field["sign_in_provider"].get<std::string>();
            }
        }
    }

    if(connection_authed)
        name += " (CONNECTED)";
    else if(fatal_connection_error)
        name += " (CONNECITON ERROR)";
    else
        name += " (DISCONNECTED)";
    return name;
}

Butler::header_list_t Butler::prepare_send_headers(void) {
    determine_login_token();
    decltype(network)::header_list_t headers;
    if(api_key != "") {
        headers.emplace_back("X-API-Key", api_key);
    } else {
        std::string auth_token = get_firebase_token_refresh_token(refresh_token);
        headers.emplace_back("X-Auth-Token", auth_token);
    }
    headers.emplace_back("X-Requested-Version", to_str(zefdb_protocol_version_max));
    return headers;
}

void Butler::start_connection() {
    if(should_stop) {
        print_backtrace();
        std::cerr << "What the hell is getting here all the time?" << std::endl;
        return;
    }
    // This is to prevent multiple threads attacking this function at
    // the same time.
    static std::atomic<bool> _starting = false;
    bool was_starting = _starting.exchange(true);
    if(was_starting)
        return;

    if(network.is_running())
        return;

    std::string auto_connect = std::get<std::string>(get_config_var("login.autoConnect"));
    if(have_auth_credentials() || auto_connect == "always")
        // Note: in the case of already having credentials,
        // ensure_auth_credentials, will make sure the credentials are
        // up to date
        ensure_auth_credentials();

    // if(want_upstream_connection()) {
    network.prepare_headers_func = std::bind(&Butler::prepare_send_headers, this);
    network.start_running();
    // }
    _starting = false;
}

void Butler::stop_connection() {
    if(!network.is_running())
        return;

    network.stop_running();
    if(zwitch.zefhub_communication_output())
        std::cerr << "Disconnecting from ZefHub" << std::endl;
}

void wait_for_token_errors(std::vector<Butler::task_ptr> tasks) {
    bool failed = false;
    try {
    for(auto & task : tasks) {
        auto resp = std::get<TokenQueryResponse>(task->future.get());
        if(!resp.generic.success) {
            failed = true;
            break;
        }
    }
    } catch(...) {
        failed = true;
    }
    if(failed) {
        std::cerr << "=============================================" << std::endl;
        std::cerr << "WARNING: problem verifying cached tokens!!!" << std::endl;
        std::cerr << "WARNING: problem verifying cached tokens!!!" << std::endl;
        std::cerr << std::endl;
        std::cerr << "This is probably due to an invalid token cache, saved at $HOME/.zef/tokens_cache.json. You should exit all zef sessions, remove this file, and then start your zef session again." << std::endl;
        std::cerr << std::endl;
        std::cerr << "WARNING: problem verifying cached tokens!!!" << std::endl;
        std::cerr << "WARNING: problem verifying cached tokens!!!" << std::endl;
        std::cerr << "=============================================" << std::endl;
    }
}

void Butler::handle_successful_auth() {
    if(zwitch.zefhub_communication_output())
        std::cerr << "Authenticated with ZefHub" << std::endl;

    if(have_logged_in_as_guest && !zwitch.extra_quiet()) {
        std::cerr << std::endl;
        std::cerr << "=================================" << std::endl;
        std::cerr << "You are logged in as a guest user, which allows you to view public graphs but" << std::endl;
        std::cerr << "does not allow for synchronising new graphs with ZefHub." << std::endl;
        std::cerr << std::endl;
        std::cerr << "Disclaimer: any ETs, RTs, ENs, and KWs that you query will be stored with ZefHub." << std::endl;
        std::cerr << "=================================" << std::endl;
        std::cerr << std::endl;
    }
    debug_time_print("successful auth");
    update(auth_locker, connection_authed, true);

    // Get hostname with C function. Have to make sure it is null terminated ourselves.
    char hostname[80];
    gethostname(hostname, 80);
    hostname[79] = 0;
    send_ZH_message({
            {"msg_type", "register_metadata"},
            {"hostname", hostname},
            {"client_version", zefdb_protocol_version.load()}
        });

    // Request our tokens
    task_ptr task = add_task(true, 0);
    send_ZH_message({
            {"msg_type", "token"},
            {"msg_version", 1},
            {"action", "list"},
            {"task_uid", task->task_uid}
        });

    // If we have any tokens loaded from a local cache, check that these are the
    // valid indices.
    task_ptr task_validate_ET = add_task(true, 0);
    send_ZH_message({
            {"msg_type", "token"},
            {"msg_version", 1},
            {"action", "query"},
            {"task_uid", task_validate_ET->task_uid},
            {"group", "ET"},
            {"indices", global_token_store().ETs.all_indices()}
        });
    task_ptr task_validate_RT = add_task(true, 0);
    send_ZH_message({
            {"msg_type", "token"},
            {"msg_version", 1},
            {"action", "query"},
            {"task_uid", task_validate_RT->task_uid},
            {"group", "RT"},
            {"indices", global_token_store().RTs.all_indices()}
        });
    task_ptr task_validate_EN = add_task(true, 0);
    send_ZH_message({
            {"msg_type", "token"},
            {"msg_version", 1},
            {"action", "query"},
            {"task_uid", task_validate_EN->task_uid},
            {"group", "EN"},
            {"indices", global_token_store().ENs.all_indices()}
        });
    task_ptr task_validate_KW = add_task(true, 0);
    send_ZH_message({
            {"msg_type", "token"},
            {"msg_version", 1},
            {"action", "query"},
            {"task_uid", task_validate_KW->task_uid},
            {"group", "KW"},
            {"indices", global_token_store().KWs.all_indices()}
        });

    std::vector<task_ptr> vec;
    vec.push_back(task_validate_ET);
    vec.push_back(task_validate_RT);
    vec.push_back(task_validate_EN);
    vec.push_back(task_validate_KW);
    auto t = std::thread(&wait_for_token_errors, vec);
    t.detach();

    // To tell the managers about the reconnection, we should jump out
    // of the network thread and into the butler thread, which requires
    // a msg push here.
    msg_push(Reconnected{}, false, true);
    debug_time_print("finish successful auth");
}

std::future<Response> Butler::msg_push_internal(Request && content, bool ignore_closed) {
    // TODO: with this style, we no longer need shared pointers... I
    // don't think? I'm less confident about this now...
    auto msg = std::make_shared<RequestWrapper>(std::move(content));
    auto future = msg->promise.get_future();
    msgqueue.push(std::move(msg), ignore_closed);
    return future;
}

void Butler::msg_push(Request && content, bool wait, bool ignore_closed) {
    auto future = msg_push_internal(std::move(content), ignore_closed);
    if(wait)
        future.get();
}

std::string Butler::upstream_name() {
    if(network.uri == "")
        return "LOCAL";

    std::string name = network.uri;
    // Remove any "wss://" at the front
    int after_protocol = name.find("//");
    if(after_protocol != std::string::npos)
        name = name.substr(after_protocol+2);

    // Remove anypart with a "/".
    int maybe_slash = name.find("/");
    if(maybe_slash != std::string::npos)
        name = name.substr(0, maybe_slash);

    return name;
}

std::filesystem::path Butler::credentials_path() {
    return zefdb_config_path() / upstream_name() / "credentials";
}

//////////////////////////////
// * Credentials


std::optional<std::string> Butler::load_forced_zefhub_key() {
    if(session_auth_key)
        return session_auth_key;

    char * env = std::getenv("ZEFHUB_AUTH_KEY");
    if (env != nullptr && env[0] != '\0')
        return std::string(env);

    auto path = zefdb_config_path();
    path /= "zefhub.key";
    if (std::filesystem::exists(path)) {
        std::ifstream file(path);
        std::string output;
        std::getline(file, output);
        return output;
    }

    return {};
}

bool Butler::have_auth_credentials() {
    // TODO: This should become more sophisticated in the future
    if(api_key != "" || refresh_token != "")
        return true;
    
    if(load_forced_zefhub_key())
        return true;

    if(is_credentials_file_valid())
        return true;

    if(have_logged_in_as_guest)
        return true;

    return false;
}

bool Butler::is_credentials_file_valid() {
    auto credentials_file = credentials_path();
    if(!std::filesystem::exists(credentials_file))
        return false;

    std::ifstream file(credentials_file);
    if(!json::accept(file)) {
        if(!zwitch.extra_quiet())
            std::cerr << "Credentials file is not in json format" << std::endl;
        return false;
    }

    file.seekg(0, file.beg);
    json j = json::parse(file);
    if(!j.contains("refresh_token")) {
        if(!zwitch.extra_quiet())
            std::cerr << "Credentials file does not have a refresh_token field" << std::endl;
        return false;
    }

    // TODO: Future auth methods may have expiry times here.
    return true;
}

void Butler::ensure_auth_credentials() {
    // TODO: mutex here
            
    std::optional<std::string> forced_zefhub_key = load_forced_zefhub_key(); 
    if(forced_zefhub_key) {
        if(*forced_zefhub_key == constants::zefhub_guest_key)
            have_logged_in_as_guest = true;
        else
            api_key = *forced_zefhub_key;
    } else {
        auto credentials_file = credentials_path();
        if(is_credentials_file_valid())
            return;
        if(have_logged_in_as_guest)
            return;
        // TODO: Get from config
        port_t port_start = 7000;
        port_t port_end = 9000;
        auto auth_server = manage_local_auth_server(port_start, port_end);
        if(!auth_server->wait_with_timeout()) {
            throw std::runtime_error("Unable to obtain credentials");
        }

        if (!auth_server->reply) {
            throw std::runtime_error("Someting went wrong with the auth server");
        }
        if(auth_server->reply == "GUEST") {
            have_logged_in_as_guest = true;
            if(zwitch.zefhub_communication_output())
                std::cerr << "Logging in as guest" << std::endl;
        } else {
            have_logged_in_as_guest = false;
            {
                std::filesystem::create_directories(credentials_file.parent_path());
                std::ofstream file(credentials_file);
                file << *(auth_server->reply);
                if(file.fail())
                    throw std::runtime_error("Problem saving credentials to file!");
            }
            if(zwitch.zefhub_communication_output())
                std::cerr << "Successful obtained credentials" << std::endl;
            if(!zwitch.extra_quiet()) {
                std::cerr << std::endl;
                std::cerr << "=================================" << std::endl;
                std::cerr << "You are now logged in to ZefHub. You can synchronize graphs which will enable them to be stored on" << std::endl;
                std::cerr << "ZefHub. Any ETs, RT, ENs and KWs you create will also be synchronized with ZefHub." << std::endl;
                std::cerr << std::endl;
                std::cerr << "Note: your credentials have been stored at '" + credentials_file.string() + "'." << std::endl;
                std::cerr << "By default these will be used to automatically connect to ZefHub on zef import." << std::endl;
                std::cerr << "If you would like to change this behavior, please see the `config` zefop for more information."  << std::endl;
                std::cerr << "=================================" << std::endl;
                std::cerr << std::endl;
            }
        }
    }
}

void Butler::user_login() {
    // Always remove this. It can't hurt and can only be confusing if we leave it set.
    auto butler = get_butler();
    butler->session_auth_key.reset();

    if(load_forced_zefhub_key())
        throw std::runtime_error("Can't login when an explicit key is given in ZEFHUB_AUTH_KEY or zefhub.key");
    if(have_auth_credentials())
        throw std::runtime_error("Can't login when credentials already present.");
    ensure_auth_credentials();

    // Immediately try and connect - but flag that we have just authed,
    // so it could be that there is a delay for firebase to update
    // zefhub about the new account.
    network.allowed_silent_failures = 5;
    start_connection();
}

void Butler::user_logout() {
    auto butler = get_butler();
    butler->session_auth_key.reset();

    // Now remove credentials
    if(api_key != "" || load_forced_zefhub_key())
        throw std::runtime_error("Can't logout when an explicit key is given in ZEFHUB_AUTH_KEY or zefhub.key");
    if(have_auth_credentials()) {
        auto credentials_file = credentials_path();
        if(std::filesystem::exists(credentials_file))
            std::filesystem::remove(credentials_file);
    } else if(have_logged_in_as_guest) {
    } else {
        std::cerr << "Warning: no credentials, so logout did not remove any." << std::endl;
    }

    // Always just in case
    have_logged_in_as_guest = false;

    // Always disconnect
    stop_connection();

}

