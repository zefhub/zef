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

#include <map>
#include <optional>

#define ASIO_STANDALONE
#include <asio.hpp>

namespace zefDB {
    using port_t = unsigned short;

    void open_url_in_browser(const std::string & url);
    struct LIBZEF_DLL_EXPORTED AuthServer {
        AuthServer(port_t port_start, port_t port_end);
        ~AuthServer();

        port_t port_start;
        port_t port_end;
        void start_running();
        void do_accept();
        void stop_server();
        void interact(asio::ip::tcp::socket * socket);
        bool wait_with_timeout(std::chrono::duration<double> timeout = std::chrono::minutes(15));

        std::string auth_reply();
        std::string callback_reply(std::string url);
        std::string guest_reply();
        std::string exit_reply();

        bool stopped = false;
        asio::io_service io_service_;
        asio::ip::tcp::acceptor acceptor_;
        std::shared_ptr<std::thread> thread;

        std::atomic<bool> should_stop = false;
        std::atomic<bool> received_query = false;
        // TODO: Change to all details
        std::optional<std::string> reply = {};
        AtomicLockWrapper locker;

        struct Connection {
            Connection(AuthServer * auth_server)
                : auth_server(auth_server),
                  socket(auth_server->io_service_) {}
            // ~Connection() { std::cerr << "Deconstructing a Connection" << std::endl; }
            
            AuthServer * auth_server;
            asio::ip::tcp::socket socket;
            asio::streambuf buff;
            struct HTTPHeaders {
                std::string method;
                std::string url;
                std::string version;

                std::map<std::string, std::string> headers;
            } http_headers;
        };
        typedef std::shared_ptr<Connection> Session;
    };

    std::ostream& operator<<(std::ostream& os, const AuthServer::Connection::HTTPHeaders & http_headers);

    LIBZEF_DLL_EXPORTED std::shared_ptr<AuthServer> manage_local_auth_server(port_t port_start, port_t port_end);
    LIBZEF_DLL_EXPORTED void stop_local_auth_server();

}

extern "C" {
    // This is a nothing function just so we can use dlsym to find a symbol in our library... SO DIRTY!
    LIBZEF_DLL_EXPORTED void __zefDB__just_for_locating_library();
}
