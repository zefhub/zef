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

#include "butler/auth_server.h"

#include <iostream>
#include <fstream>
#include <chrono>

#include <curl/curl.h>

#include "nlohmann/json.hpp"
using json = nlohmann::json;

#include "zefDB_utils.h"
#include "zwitch.h"

#ifdef LIBZEF_STATIC
std::filesystem::path find_auth_html_dir() {
    char* path = std::getenv("LIBZEF_AUTH_HTML_PATH");
    if (zefDB::zwitch.developer_output())
        std::cerr << "getenv returned (" << path << ") for LIBZEF_AUTH_HTML_PATH" << std::endl;
    if (path == NULL)
        throw std::runtime_error("Path for auth html was not set by controlling process!");
    return std::filesystem::path(path);
}

#elif defined(ZEF_WIN32)
#error "Windows code doesn't know how to find auth html dir if not built as static code."
    std::filesystem::path find_auth_html_dir() {
        throw std::runtime_error("Windows code doesn't know how to find the auth.html directory yet.")
    }
#elif __APPLE__
#include <dlfcn.h>
    std::filesystem::path find_auth_html_dir() {
        void * handle = NULL;
        handle = dlopen("libzef.dylib", RTLD_NOW);
        if(handle == NULL)
            throw std::runtime_error("Can't find libzef.dylib");
        void * address = dlsym(handle, "__zefDB__just_for_locating_library");
        Dl_info info;
        if(!dladdr(address, &info))
            throw std::runtime_error("Couldn't call dladdr");

        return std::filesystem::path(info.dli_fname).parent_path();
    }
#elif __linux__
#include <dlfcn.h>
#include <link.h>
    std::filesystem::path find_auth_html_dir() {
        void * handle = NULL;
        handle = dlopen("libzef.so", RTLD_NOW);
        if(handle == NULL)
            throw std::runtime_error("Can't find libzef.so");
        struct link_map * info;
        dlinfo(handle, RTLD_DI_LINKMAP, (void*)&info);
        char * filename = info->l_name;

        return std::filesystem::path(filename).parent_path();
    }
#else
#error "Unknown platform"
#endif

namespace zefDB {

    std::shared_ptr<AuthServer> global_auth_server;

    std::ostream& operator<<(std::ostream& os, const AuthServer::Connection::HTTPHeaders & http_headers) {
        os << "HTTPHeaders: " << http_headers.method << " " << http_headers.url << " " << http_headers.version;
        for(auto it : http_headers.headers) {
            os << " ++ " << it.first << " : " << it.second;
        }
        return os;
    }

    void Session_read_next_line(AuthServer::Session sesh);
    void Session_interact(AuthServer::Session sesh);

    AuthServer::AuthServer(port_t port_start, port_t port_end)
        : port_start(port_start),
          port_end(port_end),
          acceptor_(io_service_) {

        if(zwitch.developer_output())
            std::cerr << "About to setup ASIO resolver and endpoint" << std::endl;
        asio::ip::tcp::resolver resolver(io_service_);
        asio::ip::tcp::endpoint endpoint{asio::ip::tcp::v4(), port_start};
        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        std::error_code ec;
        port_t cur_port = port_start;
        while(cur_port <= port_end) {
            endpoint.port(cur_port);
            acceptor_.bind(endpoint, ec);
            if(ec) {
                // TODO: Test if only port error
                cur_port++;
                continue;
            }
            break;
        }
        if(cur_port > port_end)
            throw std::runtime_error("Unable to find an unoccupied port in the port range " + to_str(port_start) + ":" + to_str(port_end));

        if(zwitch.developer_output())
            std::cerr << "About to call acceptor_.listen" << std::endl;
        acceptor_.listen();

        do_accept();

        if(zwitch.developer_output())
            std::cerr << "About to start io_server thread" << std::endl;
        thread = std::make_shared<std::thread>([this]() {
            try {
                asio::error_code ec;
                io_service_.run(ec);
                if(ec)
                    std::cerr << "Auth server crashed with exception: " << ec << std::endl;
            } catch(const std::exception & exc) {
                std::cerr << "Auth server caught an unhandled exception: " << exc.what() << std::endl;
            }
            update(locker, should_stop, true);
        });
        // TODO: Trigger opening of browser
        std::string url("http://localhost:" + std::to_string(cur_port) + "/auth");
        url += "?redirectUrl=callback";
        if(!zwitch.extra_quiet())
            std::cerr << "Opened auth browser session at " << url << std::endl;
        open_url_in_browser(url);
    }

    void open_url_in_browser(const std::string & url) {
#if defined(WIN32) || defined(_WIN32) || defined(__WIN32__) || defined(__NT__)
        std::string command = "start " + url;
#elif __APPLE__
        std::string command = "open " + url;
#elif __linux__
        std::string command = "xdg-open " + url;
#elif __unix__ // all unices not caught above
        std::string command = "xdg-open " + url;
#else
#   error "Unknown compiler"
#endif
        if(zwitch.developer_output())
            std::cerr << "About to run system command to open browser." << std::endl;
        system(command.c_str());
    }

    void AuthServer::do_accept() {
        if(zwitch.developer_output())
            std::cerr << "Start of do_accept" << std::endl;
        Session sesh = std::make_shared<Connection>(this);
        acceptor_.async_accept(sesh->socket,
                               [sesh, this](std::error_code ec)
                               {
                                   if (zwitch.developer_output())
                                       std::cerr << "Start of async_accept callback" << std::endl;
                                   if (!acceptor_.is_open())
                                       return;
                                   if (!ec)
                                   {
                                       update(locker, received_query, true);
                                       Session_interact(sesh);
                                   }

                                   do_accept();
                               });
    }

    void AuthServer::stop_server() {
        if (stopped)
            return;

        io_service_.stop();
        acceptor_.close();
        if(thread && thread->joinable())
            thread->join();
        thread.reset();

        // Note this is rather dogdy - could imagine a case where a new object
        // is being instantiated and then this comes in a clobbers it. Will have
        // to assume that everything is done correctly through the global-access
        // functions only.
        
        global_auth_server.reset();

        stopped = true;
    }

    AuthServer::~AuthServer() {
        stop_server();
    }

    bool AuthServer::wait_with_timeout(std::chrono::duration<double> timeout) {
        // First wait for the initial connection. We have this as a short
        // timeout so that things like being started with jupyter doesn't sit
        // there waiting forever.
        wait_pred(locker, [&]() { return should_stop || received_query; }, std::chrono::seconds(5));
        if(!received_query) {
            stop_server();
            throw std::runtime_error("Did not receive any browser connection in 5 secs, aborting auth server.");
            return false;
        }
        bool res = wait_same(locker, should_stop, true, timeout);
        stop_server();
        if(!res)
            return false;
        if(!reply)
            return false;
        return true;
    }

    void Session_interact(AuthServer::Session sesh) {
        if (zwitch.developer_output())
            std::cerr << "Start of Session_interact" << std::endl;
        // Read first line
        asio::async_read_until(sesh->socket, sesh->buff, '\r',
                               [sesh](const std::error_code& e, std::size_t s)
                               {
                                   std::string line, ignore;
                                   std::istream stream {&sesh->buff};
                                   std::getline(stream, line, '\r');
                                   std::getline(stream, ignore, '\n');
                                   std::stringstream ss(line);
                                   ss >> sesh->http_headers.method;
                                   ss >> sesh->http_headers.url;
                                   ss >> sesh->http_headers.version;
                                   if (zwitch.developer_output())
                                       std::cerr << "Session URL: " << sesh->http_headers.url << std::endl;
                                   Session_read_next_line(sesh);
                               });
    }

    void Session_read_next_line(AuthServer::Session sesh) {
        asio::async_read_until(sesh->socket, sesh->buff, '\r',
                               [sesh](const std::error_code& e, std::size_t s)
                               {
                                   std::string line, ignore;
                                   std::istream stream {&sesh->buff};
                                   std::getline(stream, line, '\r');
                                   std::getline(stream, ignore, '\n');

                                   std::stringstream ss(line);
                                   std::string header_name;
                                   std::getline(ss, header_name, ':');
                                   std::string value;
                                   std::getline(ss, value);
                                   sesh->http_headers.headers[header_name] = value;

                                   if(line.length() == 0) {
                                       // std::cerr << sesh->http_headers << std::endl;
                                       std::string response;
                                       if(sesh->auth_server->should_stop.load())
                                           response = "HTTP/1.1 200 OK\n\nAlready finished";
                                       else if( (sesh->http_headers.url.find("/auth/guest") == 0) ||
                                                (sesh->http_headers.url.find("/callback/guest") == 0) ||
                                                (sesh->http_headers.url.find("/guest") == 0))
                                           response = sesh->auth_server->guest_reply();
                                       else if(sesh->http_headers.url.find("/auth") == 0)
                                           response = sesh->auth_server->auth_reply();
                                       else if(sesh->http_headers.url.find("/callback") == 0)
                                           response = sesh->auth_server->callback_reply(sesh->http_headers.url);
                                       else if(sesh->http_headers.url.find("/exit") == 0)
                                           response = sesh->auth_server->exit_reply();
                                       else
                                           response = "HTTP/1.1 404 Not Found\n\n";
                                       auto ptr_response = std::make_shared<std::string>(response);
                                       asio::async_write(sesh->socket,
                                                         asio::buffer(*ptr_response),
                                                         [sesh,ptr_response](const std::error_code& e, std::size_t s) {
                                                             // std::cout << "done" << std::endl;
                                                         });
                                       return;
                                   }

                                   Session_read_next_line(sesh);
                               });
    }


    // std::shared_ptr<server> s;

    std::string AuthServer::auth_reply() {
        if (zwitch.developer_output())
            std::cerr << "Going to reply with auth.html" << std::endl;
        auto p = find_auth_html_dir();
        if (zwitch.developer_output())
            std::cerr << "auth.html dir is: " << p.string() << std::endl;
        p /= "auth.html";
        if (zwitch.developer_output())
            std::cerr << "auth.html path is: " << p.string() << std::endl;

        // std::cerr << p << std::endl;

        if(!std::filesystem::exists(p)) {
            std::cerr << "Failed check for existence of auth.html" << p.string() << std::endl;
            throw std::runtime_error("Can't find the template for the auth server.");
        }

        if (zwitch.developer_output())
            std::cerr << "Reading auth.html" << std::endl;
        std::ifstream t(p);
        std::stringstream ss;
        ss << t.rdbuf();
        if (zwitch.developer_output())
            std::cerr << "Going to send file contents to client" << std::endl;
        std::string response = "HTTP/1.1 200 OK\n\n";
        response += ss.str();
        return response;
    }

    std::string AuthServer::exit_reply() {
        std::string response = "HTTP/1.1 200 OK\n\n";
        update(locker, should_stop, true);
        return response;
    }

    std::string AuthServer::guest_reply() {
        std::string response = "HTTP/1.1 302 Found\nLocation: https://www.zefhub.io/auth/cli/success\n\n";
        reply = std::string("GUEST");
        update(locker, should_stop, true);
        return response;
    }

    std::string AuthServer::callback_reply(std::string url) {
        std::string response = "HTTP/1.1 302 Found\nLocation: https://www.zefhub.io/auth/cli/success\n\n";
        // Identify the refreshToken
        int params_start = url.find("?");
        if(params_start == std::string::npos)
            throw std::runtime_error("Unable to find refreshToken in callback");
        int cur_pos = params_start + 1;
        std::string refresh_token;
        // std::cerr << url << std::endl;
        while(true) {
            if(url.find("refreshToken=", cur_pos) == cur_pos) {
                cur_pos += std::strlen("refreshToken=");
                int next_pos = url.find("&", cur_pos);
                if(next_pos == std::string::npos)
                    next_pos = url.size();
                refresh_token = url.substr(cur_pos, next_pos-cur_pos);
                break;
            } else {
                int next_pos = url.find("&", cur_pos);
                if(next_pos == std::string::npos)
                    throw std::runtime_error("Unable to find refreshToken in callback (2)");
                cur_pos = next_pos + 1;
            }
        }
                
        json j{{"refresh_token", refresh_token}};
        reply = j.dump();
        update(locker, should_stop, true);
        return response;
    }

    std::shared_ptr<AuthServer> manage_local_auth_server(port_t port_start, port_t port_end) {
        if(zwitch.developer_output())
            std::cerr << "Start of manage_local_auth_server" << std::endl;
        if(global_auth_server)
            throw std::runtime_error("There is already an auth server running");
        global_auth_server = std::make_shared<AuthServer>(port_start, port_end);
        return global_auth_server;
    }

    void stop_local_auth_server() {
        if(!global_auth_server)
            return;
        update(global_auth_server->locker, global_auth_server->should_stop, true);
        global_auth_server.reset();
    }

}

extern "C" {
    void __zefDB__just_for_locating_library(){};
}

