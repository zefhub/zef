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

#include "butler/communication.h"
#include "zwitch.h"

#ifdef ZEF_WIN32
#include <wincrypt.h>
#endif

namespace zefDB {
    namespace Communication {

        using json = nlohmann::json;

        std::string decompress_zstd(std::string input) {
            size_t r_size = ZSTD_getFrameContentSize(input.c_str(), input.length());
            if(r_size == ZSTD_CONTENTSIZE_ERROR)
                throw std::runtime_error("Not a zstd compressed string.");
            if(r_size == ZSTD_CONTENTSIZE_UNKNOWN)
                throw std::runtime_error("Unable to determine length of zstd content.");

            std::string output;
            output.resize(r_size);
            // TODO: Setup and reuse the context from ZSTD_decompressDCtx.

            size_t d_size = ZSTD_decompress(output.data(), r_size, input.c_str(), input.length());
            if (d_size != r_size) {
                std::string zstd_err = ZSTD_getErrorName(d_size);
                throw std::runtime_error("Problem decompressing zstd string. zstd error: " + zstd_err);
            }

            return output;
        }

        std::string compress_zstd(std::string input, int compression_level) {
            size_t const max_size = ZSTD_compressBound(input.length());

            std::string output;
            output.resize(max_size);

            // TODO: Setup and reuse the context from ZSTD_compressCCtx.
            size_t const c_size = ZSTD_compress(output.data(), max_size, input.c_str(), input.length(), compression_level);
            if (ZSTD_isError(c_size)) {
                std::string zstd_err = ZSTD_getErrorName(c_size);
                throw std::runtime_error("Problem compressing zstd string. zstd error: " + zstd_err);
            }

            output.resize(c_size);

            return output;
        }

        std::tuple<json, std::vector<std::string>> parse_ZH_message(std::string input) {
            auto raw_message = decompress_zstd(input);
            auto prefix_length = raw_message.find('|');
            if (prefix_length == std::string::npos)
                throw std::runtime_error("Message doesn't not contain prefix");
            auto prefix = raw_message.substr(0,prefix_length);
            if (!json::accept(prefix))
                throw std::runtime_error("Message prefix is not valid json");
            auto j = json::parse(prefix);
            if (j.type() != json::value_t::array)
                throw std::runtime_error("Message prefix is not an array");

            if(j.size() == 0)
                throw std::runtime_error("Message had no content.");

            std::string main_msg;
            std::vector<std::string> strings;
            strings.resize(j.size()-1);
            int cur_loc = prefix_length+1;
            int n = 0;
            for(auto it : j) {
                if (it.type() != json::value_t::number_unsigned)
                    throw std::runtime_error("Message prefix contains an item which is not a number");
                int length = it.get<int>();

                std::string this_string = raw_message.substr(cur_loc, length);
                if (n == 0)
                    main_msg = this_string;
                else
                    strings[n-1] = this_string;
                n++;
                cur_loc += length;
            }

            if (!json::accept(main_msg)) {
                std::cerr << main_msg << std::endl;
                throw std::runtime_error("Couldn't parse main message into json.");
            }
            json main_j = json::parse(main_msg);
            return std::make_tuple(main_j, strings);
        }

        std::string prepare_ZH_message(const json & main_json, const std::vector<std::string> & vec) {
            std::string main_msg = main_json.dump();

            std::stringstream ss;
            ss << "[" << main_msg.length();
            for (auto it : vec)
                ss << "," << it.length();
            ss << "]|";
            std::string prefix = ss.str();

            std::string full_msg = prefix + main_msg;
            for (auto it : vec)
                full_msg += it;

            return compress_zstd(full_msg);
        }




        void PersistentConnection::fail_handler(websocketpp::connection_hdl hdl) {
            developer_output("Fail handler");
            visit_endpoint([this,&hdl](auto & endpoint) {
                auto con = endpoint->get_con_from_hdl(hdl);
                if(!con)
                    return;
                auto ec = con->get_ec();
                int response_code = con->get_response_code();
                if(response_code == 401) {
                    std::cerr << "Upstream rejected connection: " << response_code << " \"" << con->get_response_msg() << "\"." << std::endl;
                    std::cerr << "Please logout and login again." << std::endl;
                    auto & response = con->get_response();
                    if(response.get_header("Content-Length") != "") {
                        int supposed_length = std::stoi(response.get_header("Content-Length"));
                        while(!response.ready()) {}
                        std::string raw;
                        raw = response.get_body();
                        if(raw.size() != supposed_length)
                            std::cerr << "Response is not long enough" << std::endl;
                        std::cerr << "Response was: " << response.raw() << std::endl;
                    }
                    close();
                } else {
                    if(zwitch.zefhub_communication_output())
                        if(allowed_silent_failures <= 0) {
                            std::cerr << "Failure in WS: " << ec.message() << " : " << con->get_response_code() << " : " << con->get_response_msg() << std::endl;
                        }
                }
                // TODO: Should also provide a way to change URI if this has been directed to a load balancer.
                update(locker, [&]() {
                    connected = false;
                    wspp_in_control = false;
                    last_was_failure = true;
                });
                if (outside_close_handler)
                    outside_close_handler(true);
            });
        }
        void PersistentConnection::pong_timeout_handler(websocketpp::connection_hdl hdl, std::string s) {
            if(zwitch.zefhub_communication_output()) {
                std::cerr << "Pong timeout" << std::endl;
            }
            // Probably need to assert or forcibly close here.
            // While zefhub doesn't support it yet, we will not fail just yet.
            // fail_handler(hdl);
            restart(true);
        };
        void PersistentConnection::pong_handler(websocketpp::connection_hdl hdl, std::string s) {
            long long ping_start = std::stoll(s);
            auto duration = std::chrono::steady_clock::now().time_since_epoch();
            long long now = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            auto ping_time = now - ping_start;
            ping_counts++;
            ping_accum += ping_time;
            if(ping_counts > max_pings) {
                ping_accum *= double(max_pings) / double(ping_counts);
                ping_counts = max_pings;
            }
            developer_output("Received pong in " + to_str(ping_time) + "ms. Average ping time is now: " + to_str(ping_accum / ping_counts) + "ms.");
        };
        void PersistentConnection::open_handler(websocketpp::connection_hdl hdl) {
            debug_time_print("start of open_handler");
            update(locker, connected, true);
            last_connect_time = std::chrono::steady_clock::now();
            if(should_stop) {
                visit_endpoint([this,&hdl](auto & endpoint) {
                    auto con = endpoint->get_con_from_hdl(hdl);
                    if(con) {
                        std::error_code ec;
                        con->close(websocketpp::close::status::going_away, "", ec);
                        if(ec) 
                            std::cout << "> Closing connection error (in open_handler): " << ec.message() << std::endl;
                    }
                });
                return;
            }
            send_ping();

            if (outside_open_handler) {
                outside_open_handler();
            }
        };
        void PersistentConnection::close_handler(websocketpp::connection_hdl hdl) {
            developer_output("Close handler");
            visit_endpoint([this,&hdl](auto & endpoint) {
                auto con = endpoint->get_con_from_hdl(hdl);
                if(con) {
                    auto ec = con->get_ec();
                    if(ec || con->get_remote_close_code() != 1000) {
                        developer_output("Remote close (" + to_str(con->get_remote_close_code()) + ") reason: " + con->get_remote_close_reason());
                        developer_output("Local close reason: " + con->get_local_close_reason());
                        if(con->get_remote_close_code() == 4000) {
                            std::cerr << "Upstream told us to stop connecting: " << to_str(con->get_remote_close_code()) << " \"" << con->get_remote_close_reason() << "\"." << std::endl;
                            close();
                        }
                        last_was_failure = true;
                    }
                }
                update(locker, [&]() {
                    connected = false;
                    wspp_in_control = false;
                    // This is getting the shared pointer on our object, not
                    // that of websocketpp. This might not be necessary if we
                    // make sure to use connected instead of _con to determine
                    // connection state.
                    std::visit([&](auto & con) {
                        con.reset();
                    }, _con);
                });
                if (outside_close_handler)
                    outside_close_handler(false);
            });
        }
        void PersistentConnection::message_handler_tls(websocketpp::connection_hdl hdl, base_client_tls_t::message_ptr msg) {
                outside_message_handler(msg->get_payload());
            }

        void PersistentConnection::message_handler_notls(websocketpp::connection_hdl hdl, base_client_notls_t::message_ptr msg) {
                outside_message_handler(msg->get_payload());
            }

#ifdef ZEF_WIN32
        void add_windows_root_certs(ssl_context_ptr & ctx) {
            HCERTSTORE hStore = CertOpenSystemStore(0, "ROOT");
            if(hStore == NULL)
                throw std::runtime_error("Unable to get Windows root certificate store.");

            X509_STORE * store = X509_STORE_new();
            PCCERT_CONTEXT pContext = NULL;
            while((pContext = CertEnumCertificatesInStore(hStore, pContext)) != NULL) {
                X509 *x509 = d2i_X509(NULL,
                                      (const unsigned char **)&pContext->pbCertEncoded,
                                      pContext->cbCertEncoded);
                if(x509 != NULL) {
                    X509_STORE_add_cert(store, x509);
                    X509_free(x509);
                }
            }

            CertFreeCertificateContext(pContext);
            CertCloseStore(hStore, 0);

            SSL_CTX_set_cert_store(ctx->native_handle(), store);
        }
#endif
        
        // ssl_context_ptr on_tls_init(const char * hostname, websocketpp::connection_hdl) {
        ssl_context_ptr on_tls_init(websocketpp::connection_hdl x) {
            ssl_context_ptr ctx = websocketpp::lib::make_shared<asio::ssl::context>(asio::ssl::context::sslv23);

            try {
                ctx->set_options(asio::ssl::context::default_workarounds |
                                 asio::ssl::context::no_sslv2 |
                                 asio::ssl::context::no_sslv3 |
                                 asio::ssl::context::single_dh_use);


#ifdef ZEF_WIN32
                add_windows_root_certs(ctx);
#else
                ctx->set_default_verify_paths();
#endif

                // We add in the certificates that may have been set by python,
                // as the default openssl paths could be baked into a bundled
                // libssl.
                char * env = std::getenv("LIBZEF_CA_BUNDLE");
                if(env != nullptr && env[0] != '\0')
                    ctx->load_verify_file(env);
                env = std::getenv("LIBZEF_CA_PATH");
                if(env != nullptr && env[0] != '\0')
                    ctx->add_verify_path(env);

                ctx->set_verify_mode(asio::ssl::verify_peer);
                // ctx->set_verify_mode(asio::ssl::verify_none);
                // ctx->set_verify_callback(std::bind(&verify_certificate, hostname, std::placeholders::_1, std::placeholders::_2));

            } catch (std::exception& e) {
                std::cout << e.what() << std::endl;
            }
            return ctx;
        }

        void PersistentConnection::create_endpoint() {
            // We can't double create, so bail if we have already.
            if(ws_thread)
                return;

            try {
#if ZEFDB_ALLOW_NO_TLS
                if(uri.find("ws://") == 0) {
                    // endpoint.emplace<client_notls_t>();
                    endpoint = std::make_shared<base_client_notls_t>();
                    _con = base_client_notls_t::connection_ptr(nullptr);
                    if(zwitch.zefhub_communication_output())
                        std::cerr << "Using no TLS" << std::endl;
                } else if(uri.find("wss://") == 0) {
                    // endpoint.emplace<client_tls_t>();
                    endpoint = std::make_shared<base_client_tls_t>();
                    _con = base_client_tls_t::connection_ptr(nullptr);
                    if(zwitch.zefhub_communication_output())
                        std::cerr << "Using TLS" << std::endl;
                } else {
                    throw std::runtime_error("Unknown protocol for uri: " + uri);
                }
                
#else
                endpoint = std::make_shared<base_client_t>();
#endif

                visit_endpoint([this](auto & endpoint) {
                    endpoint->clear_access_channels(websocketpp::log::alevel::all);
                    endpoint->clear_error_channels(websocketpp::log::elevel::all);
                    endpoint->init_asio();
                    endpoint->start_perpetual();
                    endpoint->set_fail_handler(std::bind(&PersistentConnection::fail_handler, this, std::placeholders::_1));
                    endpoint->set_pong_timeout_handler(std::bind(&PersistentConnection::pong_timeout_handler, this, std::placeholders::_1, std::placeholders::_2));
                    endpoint->set_pong_handler(std::bind(&PersistentConnection::pong_handler, this, std::placeholders::_1, std::placeholders::_2));
                    endpoint->set_open_handler(std::bind(&PersistentConnection::open_handler, this, std::placeholders::_1));
                    endpoint->set_close_handler(std::bind(&PersistentConnection::close_handler, this, std::placeholders::_1));
                });

#if ZEFDB_ALLOW_NO_TLS
                std::visit(overloaded{
                        [this](client_tls_t & endpoint) {
#endif
                            endpoint->set_tls_init_handler(&on_tls_init);
                            endpoint->set_message_handler(std::bind(&PersistentConnection::message_handler_tls, this, std::placeholders::_1, std::placeholders::_2));
#if ZEFDB_ALLOW_NO_TLS
                        },
                        [this](client_notls_t & endpoint) {
                            endpoint->set_message_handler(std::bind(&PersistentConnection::message_handler_notls, this, std::placeholders::_1, std::placeholders::_2));
                        }
                    }, endpoint);
#endif

                visit_endpoint([this](auto & endpoint) {
                    ws_thread = std::make_unique<std::thread>(&std::remove_reference<decltype(endpoint)>::type::element_type::run, endpoint.get());
                });
            } catch(const std::exception & exc) {
                std::cerr << "Bad error while trying to setup the websocket++ endpoint:" << exc.what() << std::endl;
                return;
            }
        }

        void PersistentConnection::start_connection() {
            create_endpoint();

            connected = false;
            visit_endpoint([this](auto & endpoint) {
                std::error_code ec;
                auto con = endpoint->get_connection(uri, ec);
                if (ec) {
                    std::cout << "> Connect initialization error: " << ec.message() << std::endl;
                    throw std::runtime_error("Couldn't create connection for websocket++"); 
                }
                if(prepare_headers_func)
                    for(auto & item : (*prepare_headers_func)())
                        con->append_header(item.first, item.second);

                debug_time_print("before endpoint connect");
                endpoint->connect(con);
                // This is a little tricky - we should probably have put the
                // endpoint and the connection in the same variant to be able to
                // access this with type guarantees. As it is, nesting std::visits
                // will form the 2x2 product of all type combinations.
#if ZEFDB_ALLOW_NO_TLS
                // This type manipulation needs an example. If the
                // visit_endpoint above is called with auto&=client_tls_t&, then
                // this type manipulation will get us
                // base_client_tls_t::connection_ptr
                auto & ptr_con = std::get<typename std::remove_reference_t<decltype(endpoint)>::element_type::connection_ptr>(_con);
#else
                auto & ptr_con = _con;
#endif
                update(locker, [&]() {
                    if(should_stop) {
                        con->close(websocketpp::close::status::going_away, "", ec);
                        if(ec) 
                            std::cout << "> Closing connection error (in start_connection): " << ec.message() << std::endl;
                    } else {
                        atomic_store(&ptr_con, con);
                    }
                });
            });
        }

        void PersistentConnection::close(bool failure) {
            update(locker, [&]() {
                should_stop = true;
                std::visit([&](auto & con) {
                    if(con) {
                        std::error_code ec;
                        con->close(websocketpp::close::status::going_away, "", ec);
                        if(ec)
                            std::cout << "> Closing connection error: " << ec.message() << std::endl;
                        con.reset();
                    }
                }, _con);

                connected = false;
                wspp_in_control = false;
                // Note: don't reset this to false, because the user might call
                // this after a real failure that they didn't know about.
                if (failure)
                    last_was_failure = failure;
            });
        }
        void PersistentConnection::restart(bool failure) {
            visit_con([&](auto & con) {
                if(!con)
                    return;

                std::error_code ec;
                con->close(websocketpp::close::status::going_away, "", ec);
                if(ec)
                    std::cout << "> Closing connection error (in restart): " << ec.message() << std::endl;
                con.reset();
                update(locker, [&]() {
                    connected = false;
                    wspp_in_control = false;
                    last_was_failure = failure;
                });
            });
        }

            //////////////////////////////////////////
            // * Utility extensions


        bool PersistentConnection::wait_for_connected_predicate() {
                return connected || should_stop;
            }
        void PersistentConnection::wait_for_connected() {
            wait_pred(locker, [&]() { return wait_for_connected_predicate(); });
        }
        void PersistentConnection::wait_for_connected(std::chrono::duration<double> timeout) {
            wait_pred(locker, [&]() { return wait_for_connected_predicate(); }, timeout);
        }

        void PersistentConnection::send(std::string msg, websocketpp::frame::opcode::value opcode) {
                wait_for_connected();
                if(should_stop)
                    return;

                visit_con([this,&msg,&opcode](auto & con) {
                if(con) {
                            std::error_code ec = con->send(msg, opcode);
                            if (ec) {
                                if(ec == websocketpp::error::invalid_state) {
                                    throw disconnected_exception();
                                } else {
                                    std::cerr << "Error sending message: " << ec.message() << std::endl;
                                    // return;
                                    throw std::runtime_error("Error sending message: " + ec.message());
                                }
                            }
                }
                });
            }

        void PersistentConnection::start_running() {
                if (managing_thread)
                    throw std::runtime_error("Trying to start a persistent connection again");
                should_stop = false;
                managing_thread = std::make_unique<std::thread>(&PersistentConnection::manager_runner, this);
            }

        void PersistentConnection::stop_running() {
                close();
                if(managing_thread) {
                    managing_thread->join();
                    managing_thread.reset();
                }
            }


        void PersistentConnection::send_ping() {
            developer_output("Sending ping");
            visit_con([this](auto & con) {
                if(!con)
                    return;
                auto duration = std::chrono::steady_clock::now().time_since_epoch();
                long long now = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                std::error_code ec;
                con->ping(to_str(now), ec);
                if (ec) {
                    std::cerr << "Error sending ping: " << ec.message() << std::endl;
                    // throw std::runtime_error("Error sending ping: " + ec.message());
                }
            });
        }

        void PersistentConnection::manager_runner() {
            try {
                while(true) {
                    wait_pred(locker, [this]() { return !wspp_in_control || should_stop; }, ping_interval);

                    if(should_stop)
                        break;

                    // This is how we test whether we should ping, or if we've
                    // actually lost the connection and need to reconnect.
                    if(wspp_in_control) {
                        send_ping();
                        continue;
                    }

                    // Try and connect - or reconnect if this is a second time.
                    if(last_was_failure) {
                        if(allowed_silent_failures > 0)
                            allowed_silent_failures--;
                        else if(zwitch.zefhub_communication_output())
                            std::cerr << "Sleeping for retry due to failure" << std::endl;
                        std::this_thread::sleep_for(retry_wait);
                    }
                    auto duration_since_last =  std::chrono::steady_clock::now() - last_connect_time;
                    if(duration_since_last < std::chrono::seconds(1)) {
                        if(zwitch.zefhub_communication_output())
                            std::cerr << "Sleeping for retry due to rapid reconnection time (" << duration_since_last / std::chrono::milliseconds(1) / 1000.0 << " s)" << std::endl;
                        std::this_thread::sleep_for(retry_wait);
                    }
                    // std::cerr << "Trying to connect" << std::endl;

                    update(locker, wspp_in_control, true);
                    start_connection();
                }

                visit_endpoint([this](auto & endpoint) {
                    if(endpoint)
                        endpoint->stop_perpetual();
                });
                // We may have created a new connection in the intermediate time
                // between this and the last close. So do it again now. (the
                // last time, con may have been empty, if start_connection was
                // halfway through its thing)
                close();

                ws_thread->join();
                ws_thread.reset();
            }
            catch(const std::exception & exc) {
                if(outside_fatal_handler) {
                    outside_fatal_handler(exc.what());
                }

                should_stop = true;
                visit_endpoint([this](auto & endpoint) {
                    endpoint->stop_perpetual();
                });
                ws_thread->join();
                ws_thread.reset();
            }
        }
    }
}
