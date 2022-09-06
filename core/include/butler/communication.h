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
#include "butler/locking.h"

#define ASIO_STANDALONE
#define _WEBSOCKETPP_CPP11_STL_
#include <websocketpp/config/asio_client.hpp>
#ifdef ZEFDB_ALLOW_NO_TLS
#include <websocketpp/config/asio_no_tls_client.hpp>
#endif

#include <websocketpp/client.hpp>

#include <nlohmann/json.hpp>
 
#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <fstream>
#include <optional>


#include <sstream>
#include <variant>

#include <zstd.h>

namespace zefDB {
    namespace Communication {
        struct disconnected_exception : public std::runtime_error {
            disconnected_exception() : std::runtime_error("Disconnected from upstream. Please connect to ZefHub and make sure you have either provided login credentials using `login | run` or chosen a guest login via `login_as_guest | run`") {}
        };

        using json = nlohmann::json;

        LIBZEF_DLL_EXPORTED std::string decompress_zstd(std::string input);

        // Arbitrarily chosen compression level (apparently range is 1-22)
        LIBZEF_DLL_EXPORTED std::string compress_zstd(std::string input, int compression_level = 10);

        // Convert a compressed string into the json + extras part.
        // example: [27,5]|{...}xzxzx
        LIBZEF_DLL_EXPORTED std::tuple<json,std::vector<std::string>> parse_ZH_message(std::string input); 

        LIBZEF_DLL_EXPORTED std::string prepare_ZH_message(const json & main_json, const std::vector<std::string> & vec = {}); 

        typedef websocketpp::lib::shared_ptr<websocketpp::lib::asio::ssl::context> ssl_context_ptr;

        typedef websocketpp::client<websocketpp::config::asio_client> base_client_notls_t;
        typedef websocketpp::client<websocketpp::config::asio_tls_client> base_client_tls_t;
        typedef std::shared_ptr<base_client_notls_t> client_notls_t;
        typedef std::shared_ptr<base_client_tls_t> client_tls_t;

        // Note: the whole point of this is because clang seems to be stricter on variants and these types.
#if ZEFDB_ALLOW_NO_TLS
        typedef std::variant<client_tls_t, client_notls_t> client_t;
        typedef std::variant<base_client_tls_t::connection_ptr, base_client_notls_t::connection_ptr> conn_t;
#else
        typedef base_client_tls_t base_client_t;
        typedef std::shared_ptr<base_client_t> client_t;
        typedef base_client_t::connection_ptr conn_t;
#endif

        struct LIBZEF_DLL_EXPORTED PersistentConnection {
            // Args: message
            using msg_handler_func = std::function<void(std::string)>;
            // Args: is_failure
            using close_handler_func = std::function<void(bool)>;
            using open_handler_func = std::function<void(void)>;
            using fatal_handler_func = std::function<void(std::string)>;

            ////////
            // Externally useful variables
            // std::string uri = "http://localhost:3000";
            std::string uri = "";
            using header_list_t = std::vector<std::pair<std::string,std::string>>;
            std::optional<std::function<header_list_t()>> prepare_headers_func = {};
            std::chrono::duration<double> retry_wait = std::chrono::milliseconds(3000);
            msg_handler_func outside_message_handler;
            close_handler_func outside_close_handler;
            open_handler_func outside_open_handler;
            fatal_handler_func outside_fatal_handler;

            std::unique_ptr<std::thread> ws_thread;
            client_t endpoint;
            // Note that con can be modified from the main thread or the ws
            // thread. Hence it should be locked behind atomic accesses. But
            // since this is a variant<shared_ptr> nesting, we are not going to
            // atomically lock the variant mutation. The variant mutation should
            // happen only once, when the endpoint is created. Thenafter it'll
            // be only shared_ptr accesses to be locked behind atomic accesses.
            //
            // The utility func visit_con is handy for this
            conn_t _con;

            template<class FUNC>
            auto visit_con(const FUNC & f) {
#if ZEFDB_ALLOW_NO_TLS
                std::visit([this, &f](auto & _con) {
#endif
                    auto con = atomic_load(&_con);
                    return f(con);
#if ZEFDB_ALLOW_NO_TLS
                }, _con);
#endif
            }

            template<class FUNC>
            auto visit_endpoint(const FUNC & f) {
#if ZEFDB_ALLOW_NO_TLS
                std::visit([this, &f](auto & endpoint) {
#endif
                    return f(endpoint);
#if ZEFDB_ALLOW_NO_TLS
                }, endpoint);
#endif
            }
            
            ////////
            // Managing vars
            std::unique_ptr<std::thread> managing_thread;
            std::atomic_bool connected = false;
            std::atomic_bool wspp_in_control = false;
            bool last_was_failure = false;
            std::chrono::time_point<std::chrono::steady_clock> last_connect_time;
            std::atomic_bool should_stop = false;
            std::chrono::duration<double> ping_interval = std::chrono::seconds(15);
            int allowed_silent_failures = 0;

            int ping_counts = 0;
            double ping_accum = 0;
            const int max_pings = 5;

            // TODO change to atomic wait struct
            AtomicLockWrapper locker;

            PersistentConnection() {
                // create_endpoint();
            };
            ~PersistentConnection() {
                stop_running();
            }

            //////////////////////////////////
            // * WSPP functions

            void fail_handler(websocketpp::connection_hdl hdl);
            void pong_timeout_handler(websocketpp::connection_hdl hdl, std::string s);
            void pong_handler(websocketpp::connection_hdl hdl, std::string s);
            void open_handler(websocketpp::connection_hdl hdl);
            void close_handler(websocketpp::connection_hdl hdl);
            void message_handler_tls(websocketpp::connection_hdl hdl, base_client_tls_t::message_ptr msg);
            void message_handler_notls(websocketpp::connection_hdl hdl, base_client_notls_t::message_ptr msg);

            void create_endpoint();

            void start_connection();

            void close(bool failure=false);
            void restart(bool failure);

            void send_ping();

            //////////////////////////////////////////
            // * Utility extensions


            bool wait_for_connected_predicate();
            void wait_for_connected();
            void wait_for_connected(std::chrono::duration<double> timeout);

            void send(std::string msg, websocketpp::frame::opcode::value opcode = websocketpp::frame::opcode::binary);

            bool is_running() { return bool(managing_thread); }
            void start_running();

            void stop_running();

            void manager_runner();
        };
    }
}
