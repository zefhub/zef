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

#include "butler/butler.h"
#include "butler/auth_server.h"
#include <memory>
#include <algorithm>
#include <chrono>
#include <fstream>

#include "nlohmann/json.hpp"
using json = nlohmann::json;

#include <jwt-cpp/jwt.h>
#include "jwt-cpp/traits/nlohmann-json/traits.h"

// I want to not have to use this:
#include "high_level_api.h"
#include "zefops.h"
#include "synchronization.h"
#include "zef_config.h"
#include "external_handlers.h"
#include "conversions.h"
#include "tar_file.h"

namespace zefDB {
    bool initialised_python_core = false;
    namespace Butler {

        // These define template options, so must come first
#include "butler_handlers_graph_manager.cpp"
#include "butler_handlers_main.cpp"
#include "butler_handlers_ws.cpp"

#include "butler_connection.cpp"
#include "butler_tasks.cpp"
#include "butler_update_payloads.cpp"

        //////////////////////////////////////////////////////////
        // * Butler lifecycle management

        std::shared_ptr<Butler> butler;
        bool butler_is_master = false;
        bool butler_allow_auto_start = true;
        bool butler_registered_thread_exiter = false;

        Butler::Butler(std::string uri) {
            msgqueue.who_am_i = "butler main msg queue";
            network.outside_message_handler = std::bind(&Butler::ws_message_handler, this, std::placeholders::_1);
            network.outside_close_handler = std::bind(&Butler::ws_close_handler, this);
            network.outside_fatal_handler = std::bind(&Butler::ws_fatal_handler, this, std::placeholders::_1);
            network.outside_open_handler = std::bind(&Butler::ws_open_handler, this);
            network.uri = uri;

            // Note: defaults for this are set in the class.
            const char * env = std::getenv("ZEFDB_TRANSFER_CHUNK_SIZE");
            if (env != nullptr) {
                throw std::runtime_error("Not implemented");
            }
        }

        bool is_butler_running() {
            return (bool)butler;
        }

        std::shared_ptr<Butler> get_butler() {
            if(!butler) {
                if(butler_allow_auto_start) {
                    if(!zwitch.extra_quiet())
                        std::cerr << "Starting butler automatically. Call initialise_butler if you want more control." << std::endl;
                    butler_allow_auto_start = false;
                    initialise_butler();
                } else
                    throw std::runtime_error("This action needs the zefDB butler running, yet its autostart has been disabled. Note: autostart is disabled after the butler has been started once.");
            }
            return butler;
        }

        void terminate_handler() {
#ifndef ZEF_WIN32
            void *trace_elems[20];
            int trace_elem_count(backtrace( trace_elems, 20 ));
            char **stack_syms(backtrace_symbols( trace_elems, trace_elem_count ));
            for ( int i = 0 ; i < trace_elem_count ; ++i ) {
                std::cerr << stack_syms[i] << "\n";
            }
            free( stack_syms );
#endif

            if( auto exc = std::current_exception() ) { 
                try {
                    rethrow_exception(exc);
                } catch( std::exception const& e ) {
                    std::cerr << "Terminate had an exception: " << e.what() << std::endl;
                }
            } else {
                std::cerr << "No exception in terminate" << std::endl;
            }

            std::abort();
        }   

        void initialise_butler() {
            initialise_butler(std::get<std::string>(get_config_var("login.zefhubURL")));
        }
        void initialise_butler(std::string zefhub_uri) {
#ifdef DEBUG
            internals::static_checks();
#endif
            if(!validate_config_file()) {
                std::cerr << "WARNING: config options are invalid..." << std::endl;
            }
            
            if(zwitch.zefhub_communication_output())
                std::cerr << "Will use uri=" << zefhub_uri << " when later connecting to ZefHub" << std::endl;
            if (butler) {
                std::cerr << "Butler already initialised" << std::endl;
                return;
            }

            debug_time_print("initialise butler");

            if(!butler_is_master && check_env_bool("ZEFDB_OFFLINE_MODE")) {
                initialise_butler_as_master();
                return;
            }

            // Going to try some weird debugging
            std::set_terminate( terminate_handler );

            // std::cerr << "Before making butler" << std::endl;
            butler = std::make_unique<Butler>(zefhub_uri);

            // std::cerr << "Before making butler thread" << std::endl;
            butler->thread = std::make_unique<std::thread>(&Butler::msgqueue_listener, &(*butler));
#if __linux__
            auto native = butler->thread->native_handle();
            pthread_setname_np(native, "Butler");
#endif

            if(!butler_registered_thread_exiter) {
                on_thread_exit("stop_butler", &stop_butler);
                butler_registered_thread_exiter = true;
            }

            if(butler->want_upstream_connection()) {
                // Lock this behind the if check, as we can get into trouble
                // with python circular deps.
                std::string auto_connect = std::get<std::string>(get_config_var("login.autoConnect"));
                if(auto_connect == "always" ||
                   (auto_connect == "auto" && butler->have_auth_credentials()))
                    butler->start_connection();
            }
        }
        void initialise_butler_as_master() {
            if (butler && !butler_is_master)
                throw std::runtime_error("Butler already initialised as non-master");
            butler_is_master = true;

            std::cerr << "Warning: starting the Zef butler in offline mode. This means that you can create graphs and arbitrary ET/RT/EN/KW tokens. However, note that you can't persist graphs beyond your session." << std::endl;
            
            initialise_butler("");
        }

        void long_wait_or_kill(std::thread & thread, std::promise<bool> & return_value, std::string name, std::optional<std::function<void()>> extra_print = {}) {
            try {
                int wait_seconds = 60;
                    auto future = return_value.get_future();
                    auto status = future.wait_for(std::chrono::seconds(wait_seconds));
                    if (status == std::future_status::timeout) {
                        std::cerr << "Thread taking a long time to shutdown... " << name << ". Going to wait for " << wait_seconds << " s." << std::endl;
                        if(extra_print)
                            (*extra_print)();
                        if(check_env_bool("ZEFDB_DEVELOPER_THREAD_LONGWAIT"))
                            status = future.wait_for(std::chrono::seconds(10000));
                        else
                            status = future.wait_for(std::chrono::seconds(10));
                        if (status == std::future_status::timeout) {
                            std::cerr << "Gave up on waiting for thread after " << wait_seconds << " s: " << name << std::endl;
                            thread.detach();
                            return;
                        }
                    }
                    bool val = future.get();
                    if(!val)
                        std::cerr << "Thread " << name << " returned false." << std::endl;
                    if (thread.joinable())
                        thread.join();
            } catch(const std::exception & e) {
                std::cerr << "Exception while joining " << name << " thread: " << e.what() << std::endl;
            }
        }
        void stop_butler() {
            // This is to prevent multiple threads attacking this function at
            // the same time.
            static std::atomic<bool> _running = false;
            bool was_running = _running.exchange(true);
            if(was_running)
                return;

            if(zwitch.developer_output()) {
                std::cerr << "stop_butler was called" << std::endl;
            }

            if (!butler) {
                if(zwitch.developer_output())
                    std::cerr << "Butler wasn't running." << std::endl;
                _running = false;
                return;
            }

            update(butler->auth_locker, butler->should_stop, true);
            butler->msgqueue.set_closed();

            if(zwitch.developer_output())
                std::cerr << "Going to close all graph manager queues" << std::endl;
            {
                std::lock_guard lock(butler->graph_manager_list_mutex);
                for(auto data : butler->graph_manager_list) {
                    try {
                        data->queue.set_closed(false);
                    } catch(const std::exception & e) {
                        std::cerr << "Exception while trying to close queue for graph manager thread: " << e.what() << std::endl;
                    }
                }
            }
            if(zwitch.developer_output())
                std::cerr << "Going to wait on graph manager threads" << std::endl;
            std::vector<std::shared_ptr<Butler::GraphTrackingData>> saved_list;
            {
                std::lock_guard lock(butler->graph_manager_list_mutex);
                saved_list = butler->graph_manager_list;
            }

            if(zwitch.developer_output())
                std::cerr << "Number of graphs to wait for: " << saved_list.size() << std::endl;
            for(auto data : saved_list) {
                auto extra_print = [&data]() {
                    std::cerr << "Last action was: " << data->debug_last_action << std::endl;
                    std::cerr << "Number of msgs queue: " << data->queue.num_messages.load() << std::endl;
                    std::cerr << "please_stop: " << data->please_stop << std::endl;
                    std::cerr << "Last msg (or current msg) processed: " << data->queue.last_popped << std::endl;
                    std::cerr << "Items are: [";
                    for (auto & slot : data->queue.slots) {
                        // std::shared_ptr<RequestWrapper> * ptr = slot.load();
                        std::shared_ptr<RequestWrapper> ptr = slot;
                        // This is scary! We can easily segfault in here... but I'm counting on two things:
                        // 1) We've stalled.
                        // 2) We're likely shutting down.
                        // But there's still a chance this is going to crash your program.
                        if(ptr != nullptr) {
                            // std::string this_thing = msgqueue_to_str(*(*ptr));
                            std::string this_thing = msgqueue_to_str(*ptr);
                            std::cerr << "'" << this_thing << "', ";
                        }
                    }
                    std::cerr << "]" << std::endl;
                };
                long_wait_or_kill(*data->managing_thread, data->return_value, data->uid, extra_print);
            }
            // Lock and keep locked for the remainder now
            {
                std::lock_guard lock(butler->graph_manager_list_mutex);
                butler->graph_manager_list.clear();
            }
            if(zwitch.developer_output())
                std::cerr << "Removing local process graph" << std::endl;
            butler->local_process_graph.reset();

            if(zwitch.developer_output())
                std::cerr << "Stopping network" << std::endl;
            butler->network.stop_running();
            if(zwitch.developer_output())
                std::cerr << "Clear waiting tasks" << std::endl;
            butler->waiting_tasks.clear();
            if(zwitch.developer_output())
                std::cerr << "Save tokens to cache" << std::endl;
            global_token_store().save_cached_tokens();

            if(zwitch.developer_output())
                std::cerr << "Joining main butler thread" << std::endl;
            long_wait_or_kill(*butler->thread, butler->return_value, "butler");
            // This seems to be necessary if there's someone else holding onto
            // the butler shared_ptr. I'm not sure why this is the case though,
            // because we have already joined the thread.
            butler->thread.reset();
            if(zwitch.developer_output())
                std::cerr << "Finished stopping butler" << std::endl;
            butler.reset();

            _running = false;
        }


        bool before_first_graph = true;
        std::vector<std::string> early_tokens;
        std::vector<std::string> created_tokens;
        void maybe_show_early_tokens() {
            if(!before_first_graph)
                return;

            if(check_env_bool("ZEFDB_DEVELOPER_EARLY_TOKENS")) {
                std::cerr << "Early token count: " << early_tokens.size() << std::endl;
                for(auto & it : early_tokens)
                    std::cerr << it << std::endl;
                std::cerr << "=====" << std::endl;
            }

            before_first_graph = false;
        }

        void add_to_early_tokens(TokenQuery::Group group, const std::string & name) {
            std::string group_s;
            switch(group) {
            case TokenQuery::ET:
                group_s = "ET";
                break;
            case TokenQuery::RT:
                group_s = "RT";
                break;
            case TokenQuery::EN:
                group_s = "EN";
                break;
            case TokenQuery::KW:
                group_s = "KW";
                break;
            default:
                throw std::runtime_error("This hasn't been implemented!");
            }

            std::string s = group_s + "." + name;

            created_tokens.push_back(s);

            if(!before_first_graph)
                return;
            if(!check_env_bool("ZEFDB_DEVELOPER_EARLY_TOKENS"))
                return;

            early_tokens.push_back(s);
        }

        std::vector<std::string> early_token_list() {
            return early_tokens;
        }
        std::vector<std::string> created_token_list() {
            return created_tokens;
        }


        ////////////////////////////////////////////////////////////////
        // * Butler main msgqueue handling

        void Butler::msgqueue_listener(void) {
            try {
                Butler::msg_ptr msg;
                while(msgqueue.pop_loop(msg)) {
                    try {
                        std::visit(overloaded {
                                [&](DoneWithGraph & v) { handle_guest_message_passthrough(msg, v.gd); },
                                [&](LoadPage & v) { handle_guest_message_passthrough(msg, &v.g.my_graph_data()); },
                                [&](NotifySync & v) { handle_guest_message_passthrough(msg, &v.g.my_graph_data()); },
                                [&](NewTransactionCreated  & v) { handle_guest_message_passthrough(msg, &v.g.my_graph_data()); },
                                [&](TagGraph  & v) { handle_guest_message_passthrough(msg, &v.g.my_graph_data()); },
                                [&](MakePrimary  & v) { handle_guest_message_passthrough(msg, &v.g.my_graph_data()); },
                                [&](SetKeepAlive  & v) { handle_guest_message_passthrough(msg, &v.g.my_graph_data()); },
                                [&](auto & v) { handle_guest_message(v, msg); },
                            },
                            msg->content);
                    } catch(const std::exception & e) {
                        // Failsafe
                        // std::cerr << "Got exception inside of msgqueue processing, passing back: " << e.what() << std::endl;
                        // std::visit([&](auto & v) { std::cerr << "Message type: " << typeid(v).name() << std::endl; },
                        //            msg->content);
                        // The promise may have been std::moved. Handle this here, but it should be an error.
                        try {
                            msg->promise.set_exception(std::current_exception());
                        } catch(const std::exception & exc) {
                            if(zwitch.developer_output())
                                std::cerr << "Unable to set exception on promise in msgqueue: " << exc.what() << std::endl;
                        }
                    }

                    // After handling a message we should release our ownership on this msg_ptr to free stuff.
                    msg.reset();
                }
                // TODO: cancel all remaining msgs on the queue
                while(msgqueue.pop_any(msg)) {
                    // msg->promise.set_exception(...);
                    // We don't need to set promises, just make sure the promises are destructed, which is done through popping.
                }
            } catch(const std::exception & e) {
                std::cerr << "*** MAJOR FAILURE OF BUTLER MAIN MSG QUEUE LISTENER!!!! ***" << std::endl;
                std::cerr << "*** MAJOR FAILURE OF BUTLER MAIN MSG QUEUE LISTENER!!!! ***" << std::endl;
                std::cerr << e.what() << std::endl;
                std::cerr << "*** MAJOR FAILURE OF BUTLER MAIN MSG QUEUE LISTENER!!!! ***" << std::endl;
                std::cerr << "*** MAJOR FAILURE OF BUTLER MAIN MSG QUEUE LISTENER!!!! ***" << std::endl;
                return_value.set_exception(std::current_exception());
                throw;
            }
            return_value.set_value(true);
        }


        void Butler::load_graph_from_uid(Butler::msg_ptr & msg, BaseUID uid) {
            auto data = find_graph_manager(uid);
            if(data) {
                data->queue.push(std::move(msg));
            } else {
                // Create a new manager
                data = spawn_graph_manager(uid);
                data->queue.push(std::move(msg));
            }
        }

        void Butler::load_graph_from_tag_worker(Butler::msg_ptr msg) {
            try {
                auto content = std::get<LoadGraph>(msg->content);

                if(content.callback)
                    (*content.callback)("Looking up UID for '" + content.tag_or_uid + "'");

                auto response = wait_on_zefhub_message({
                        {"msg_type", "lookup_uid"},
                        {"tag", content.tag_or_uid},
                    });
                if(!response.generic.success) {
                    msg->promise.set_value(GraphLoaded{"Unable to lookup graph uid: " + response.generic.reason});
                    return;
                }
                if(!response.j.contains("graph_uid")) {
                    msg->promise.set_value(GraphLoaded{"Upstream doesn't know about graph tag."});
                    return;
                }

                auto uid = BaseUID::from_hex(response.j["graph_uid"].get<std::string>());

                load_graph_from_uid(msg, uid);
            } catch(const std::exception & e) {
                try {
                    std::cerr << "Setting exception on promise: " << e.what() << std::endl;
                    msg->promise.set_exception(std::current_exception());
                } catch(...) {
                    std::cerr << "Threw an error while trying to set a promise exception" << std::endl;
                }
            }
        }

        void Butler::load_graph_from_file(Butler::msg_ptr & msg, std::filesystem::path path) {
            developer_output("Loading local graph with path: " + path.string());
            // In this version we unpack the graph into an anonymous mmap
            if(std::filesystem::exists(path) && std::filesystem::is_regular_file(path)) {
                // auto file_group = load_tar_into_memory(path);
                BaseUID uid;
                try {
                    auto file = load_specific_file_from_tar(path, "graph.uid");
                    if(!file)
                        throw std::runtime_error("Tar doesn't have uid file.");
                    auto maybe_uid = to_uid(file->contents);
                    if(!std::holds_alternative<BaseUID>(maybe_uid))
                        throw std::runtime_error("UID in file is not a valid UID.");
                    uid = std::get<BaseUID>(maybe_uid);
                } catch(...) {
                    throw;
                }
                auto data = find_graph_manager(uid);
                if(!data)
                    data = spawn_graph_manager(uid);
                data->queue.push(std::make_shared<RequestWrapper>(std::move(msg->promise), LocalGraph{path, false}));
                return;
            }

            auto data = spawn_graph_manager(make_random_uid());
            data->queue.push(std::make_shared<RequestWrapper>(std::move(msg->promise), LocalGraph{path, true}));
        }



        ////////////////////////////////////////////////////
        // * Graph manager management


        void Butler::manage_graph_worker(std::shared_ptr<Butler::GraphTrackingData> me) {
            // This is a little weird, we don't want to start the sync_thread
            // here becuase gd is not yet assigned, and gd.heads_locker is how
            // we communicate with the sync_thread.
            // sync_thread = std::make_unique<std::thread>(&Butler::manage_graph_sync_worker, this, me);
            try { 
                // It is a requirement that there is no possible case to block
                // in these messages. Every handler must return in a maximum
                // time.
                Butler::msg_ptr msg;
                while(me->queue.pop_loop(msg)) {
                    if(me->please_stop)
                        break;
                    if(me->queue._closed)
                        break;

                    try {
                        std::visit([&](auto & v) { graph_worker_handle_message(*me, v, msg); }, msg->content);
                    } catch(const std::exception & e) {
                        std::cerr << "THROW IN GRAPH MANAGER: " << e.what() << std::endl;
                        std::cerr << "While handling msg variant type: " << msg->content.index() << std::endl;
                        if(me->gd) {
                            std::cerr << "Setting graph into error state" << std::endl;
                            set_into_invalid_state(*me);
                        } else {
                            // Since no graphdata exists, no reason to hang around
                            me->please_stop = true;
                        }
                        // Failsafe
                        msg->promise.set_exception(std::current_exception());
                    }
                    // After handling a message we should release our ownership on this msg_ptr to free stuff.
                    msg.reset();

                    // Don't go back into the wait if we need to exit now
                    if(me->please_stop)
                        break;
                }

                // std::cerr << "Starting graph manager shutdown" << std::endl;
                remove_graph_manager(me);

                // Now pop all messages - the only ones we act upon are the
                // LoadGraphs, which we feed back onto the main butler
                // again.
                //
                // The do-while version is because we may have popped a msg in
                // the loop above which has yet to be processed. However, if we
                // didn't, the msg will be a nullptr and we can skip over it.
                do {
                    if(msg == nullptr)
                        continue;
                    if(std::holds_alternative<LoadGraph>(msg->content))
                        msg_push(std::move(msg->content), false, true);
                    else {
                        // We don't need to set promises, just make sure the promises are destructed, which is done through popping.
                    }
                } while(me->queue.pop_any(msg));

                me->return_value.set_value(true);
            } catch(const std::exception & e) {
                std::cerr << "*** MAJOR FAILURE OF GRAPH WORKER THREAD!!!! ***" << std::endl;
                std::cerr << "*** MAJOR FAILURE OF GRAPH WORKER THREAD!!!! ***" << std::endl;
                std::cerr << e.what() << std::endl;
                std::cerr << "*** MAJOR FAILURE OF GRAPH WORKER THREAD!!!! ***" << std::endl;
                std::cerr << "*** MAJOR FAILURE OF GRAPH WORKER THREAD!!!! ***" << std::endl;
                set_into_invalid_state(*me);
                me->return_value.set_exception(std::current_exception());
            }

            // Because we are the last thing holding onto the shared_ptr to our
            // GraphTrackingData, we need to detach it from the main execution,
            // so that we don't self-combust when trying to call ~thread on the
            // thread while inside the thread.
            me->managing_thread->detach();
            // std::cerr << "Final shutdown of manage graph thread for: " << me->uid << std::endl;
        }

        // void Butler::manage_graph_sync_worker(Butler::GraphTrackingData & me, std::unique_lock<std::shared_mutex> && key_lock) {
        void Butler::manage_graph_sync_worker(Butler::GraphTrackingData & me) {
            try {
                me.gd->sync_thread_id = std::this_thread::get_id();

                // We use asynchronous send_updates so that we can allow
                // multiple transactions to continue, without waiting for
                // updates to finish going to zefhub.
                //
                // An alternative to this would be to have another thread that
                // just deals with this, this would be less complicated...
                std::unique_ptr<std::future<void>> send_update_future;

                while(true) {
                    wait_pred(me.gd->heads_locker,
                              [&]() {
                                  return me.please_stop
                                      || (network.connected && me.gd->should_sync
                                          && (!send_update_future || send_update_future->wait_for(std::chrono::seconds(0)) == std::future_status::ready)
                                          && (me.gd->sync_head == 0 || (me.gd->sync_head < me.gd->read_head.load())))
                                      || (me.gd->latest_complete_tx > me.gd->manager_tx_head.load());
                              });
                                  
                    if(me.please_stop)
                        break;

                    // Only run subs if we are a reader or the write_thread flag is
                    // not set. TODO: In the future, probably indicate the
                    // difference between "anytime" subs and "in reaction to
                    // updates" subs.
                    blob_index target_complete_tx = me.gd->latest_complete_tx;
                    while(!me.please_stop
                            && target_complete_tx > me.gd->manager_tx_head) {
                        // Do one tx at a time until we have nothing left to do.

                        // First check for subscriptions
                        EZefRef last_tx{me.gd->manager_tx_head, *me.gd};
                        if(!(last_tx | has_out[BT.NEXT_TX_EDGE])) {
                            std::cerr << "guid: " << me.uid << std::endl;
                            std::cerr << "write_head: " << me.gd->write_head.load() << std::endl;
                            std::cerr << "read_head: " << me.gd->read_head.load() << std::endl;
                            std::cerr << "latest_complete_tx: " << me.gd->latest_complete_tx.load() << std::endl;
                            std::cerr << "manager_tx_head: " << me.gd->manager_tx_head.load() << std::endl;
                            throw std::runtime_error("Big problem in sync thread, could not find BT.NEXT_TX_EDGE from last_tx");
                        }
                        EZefRef this_tx = last_tx >> BT.NEXT_TX_EDGE;

                        try {
                            run_subscriptions(*me.gd, this_tx);
                        } catch(...) {
                            std::cerr << "There was a failure in the run_subscriptions part of the sync thread." << std::endl;
                            throw;
                        }

                        // TODO:
                        // TODO:
                        // TODO:
                        // TODO: We probably need to save off this
                        // manager_tx_head, so that premature exits can be
                        // picked up from later. This is especially relevant
                        // for the tokens.
                        // TODO:
                        // TODO:
                        // TODO:
                        update(me.gd->heads_locker, me.gd->manager_tx_head, index(this_tx));
                    }
                    internals::execute_queued_fcts(*me.gd);

                    if(me.gd->error_state != GraphData::ErrorState::OK)
                        throw std::runtime_error("Sync worker for graph detected invalid state and is aborting");

                    // After doing everything, now is a good time to send out updates.
                    // TODO: Maybe this is better left to the main thread, and we just trigger it from here by placing a message on the queue.

                    // First check if a previous send_updates has finished.
                    if(send_update_future && send_update_future->wait_for(std::chrono::seconds(0)) == std::future_status::ready)
                        send_update_future.release();

                    if(me.gd->should_sync
                       && me.gd->is_primary_instance
                       && me.gd->sync_head < me.gd->read_head.load()
                       && want_upstream_connection()
                       && !send_update_future) {
                        // send_update(me);
                        send_update_future = std::make_unique<std::future<void>>(std::async(&Butler::send_update, this, std::ref(me)));
                    }
                }

                if(send_update_future)
                    send_update_future->get();

                // This is the last hail mary before we shut down. Required for the logic of remove_graph_manager.
                if(me.gd->should_sync &&
                   me.gd->is_primary_instance &&
                   me.gd->sync_head < me.gd->read_head.load()
                ) {
                    developer_output("The last hail mary send_update has been triggered. sync_head=" + to_str(me.gd->sync_head.load()) + " and read_head=" + to_str(me.gd->read_head.load()));
                    send_update(me);
                }

                me.sync_return_value.set_value(true);
            } catch(const std::exception & e) {
                std::cerr << "*** MAJOR FAILURE OF GRAPH SYNC THREAD!!!! ***" << std::endl;
                std::cerr << "*** MAJOR FAILURE OF GRAPH SYNC THREAD!!!! ***" << std::endl;
                std::cerr << "Some kind of exception occurred in the sync thread at the highest level: " << e.what() << std::endl;
                std::cerr << "Setting graph into error state" << std::endl;
                update(me.gd->heads_locker, [this,&me]() {
                        set_into_invalid_state(me);
                    });
                std::cerr << "*** MAJOR FAILURE OF GRAPH SYNC THREAD!!!! ***" << std::endl;
                std::cerr << "*** MAJOR FAILURE OF GRAPH SYNC THREAD!!!! ***" << std::endl;
                me.sync_return_value.set_exception(std::current_exception());
            }
        }

        std::shared_ptr<Butler::GraphTrackingData> Butler::spawn_graph_manager(BaseUID uid) {
            std::unique_lock lock(graph_manager_list_mutex);

            // Because of race conditions, we should check if one has already been created
            for(auto & data : graph_manager_list) {
                if(data->uid == uid)
                    return data;
            }

            auto data = graph_manager_list.emplace_back(std::make_shared<GraphTrackingData>());
            data->uid = uid;
            data->managing_thread = std::make_unique<std::thread>(&Butler::manage_graph_worker, this, data);
#if __linux__
            auto native = data->managing_thread->native_handle();
            std::string temp = "GM" + str(data->uid).substr(0,8);
            auto rc = pthread_setname_np(native, temp.c_str());
#endif
            data->queue.who_am_i = "graph manager for " + str(uid);

            return data;
        }

        void Butler::spawn_graph_sync_thread(GraphTrackingData & me) {
            // EXPLANATION TIME! It is assumed that the key_dict generation has
            // been deferred until this point, and also that there has been no
            // chance for any other thread to access the graph. It will be the
            // sync-thread's job to handle the key-dict as its first job. We
            // need to make sure it acquires the locks on both the key_dict and
            // the graph itself to do this.
            //
            // My first attempt was to pass the locks through (the key_dict lock
            // as a moved std::unique_lock) however this exposed a flaw in the
            // move logic of the STL - the "owning thread" of the unique_lock
            // is not moved to the new thread at the same time.
            //
            // Hence this annoying fallback style - pass a promise, which is
            // resolved by the sync-thread once it has acquired the locks. Then
            // this thread can continue, which will allow others to try and
            // access the graph, which won't be allowed until the sync-thread is
            // done with the key-dict.

            me.sync_thread = std::make_unique<std::thread>(&Butler::manage_graph_sync_worker, this, std::ref(me));
            // Need to make sure we give up the graph lock to the sync thread -
            // only if we already have the lock.
            if(me.gd->open_tx_thread == std::this_thread::get_id())
                update_when_ready(me.gd->open_tx_thread_locker,
                                  me.gd->open_tx_thread,
                                  std::this_thread::get_id(),
                                  me.sync_thread->get_id());
#if __linux__
            auto native = me.sync_thread->native_handle();
            std::string temp = "GS" + str(me.uid).substr(0,8);
            pthread_setname_np(native, temp.c_str());
#endif
        }

        std::shared_ptr<Butler::GraphTrackingData> Butler::find_graph_manager(GraphData * gd) {
            std::shared_lock lock(graph_manager_list_mutex);

            for(auto & data : graph_manager_list) {
                if(data->gd == gd) {
                    return data;
                }
            }
            return nullptr;
        }
        std::shared_ptr<Butler::GraphTrackingData> Butler::find_graph_manager(BaseUID uid) {
            std::shared_lock lock(graph_manager_list_mutex);

            for(auto & data : graph_manager_list) {
                if(data->uid == uid) {
                    return data;
                }
            }
            return nullptr;
        }

        void Butler::remove_graph_manager(std::shared_ptr<Butler::GraphTrackingData> gtd) {
            if(gtd->gd != nullptr) {
                // This doesn't need to be synchronised, as only this thread cares about it. It is only used to "disable" ~Graph inside of this function.
                // If keep_alive, then get rid of that now
                if(zwitch.developer_output())
                    std::cerr << "About to reset the keep alive" << std::endl;
                gtd->keep_alive_g.reset();
                if(zwitch.developer_output())
                    std::cerr << "Reset the keep alive" << std::endl;
                gtd->gd->started_destructing = true;
                if(!gtd->queue._closed)
                    gtd->queue.set_closed();
                gtd->debug_last_action = "Closed queue";

                // This order of cleanup is important. First, make sure we send
                // out any updates, so that any new manager will not conflict
                // with this one.

                // Note: TODO there is an edge case that is not being handled
                // here - while we haven't yet cleaned up, the manager is in the
                // list, but its queue is closed... so it will be identified,
                // and the main butler will try and push messages onto it. Need
                // to think of how to handle this...
                
                update(gtd->gd->heads_locker, gtd->please_stop, true);
                // By applying "please_stop" the sync worker should complete one last send
                if(gtd->sync_thread && gtd->sync_thread->joinable()) {
                    gtd->debug_last_action = "Joining sync thread";
                    gtd->sync_thread->join();
                }

                // if(gtd->gd->should_sync && gtd->gd->is_primary_instance)
                //     send_update(*gtd);
                // And send out the unsubscribe of course
                gtd->debug_last_action = "Going to send out unsubscribe";
                if(gtd->gd->currently_subscribed) {
                    // Note: we wait on the response here as otherwise we can
                    // get into all kinds of trouble if upstream is a little
                    // busy with other messages and we try and resubscribe again
                    // soon.
                    json unsub_msg = {
                            {"msg_type", "unsubscribe_from_graph"},
                            {"graph_uid", str(gtd->uid)},
                    };
                    if(should_stop)
                        // If we are stopping, then the network will never return, so don't wait.
                        send_ZH_message(unsub_msg);
                    else
                        wait_on_zefhub_message(unsub_msg);
                }
                gtd->debug_last_action = "Sent out unsubscribe";

                // Before removing the manager we have to clean up memory, just
                // in case we are using a filegraph and we'd end up with two
                // threads doing things to the prefix.
                if(zwitch.developer_output()) {
                    std::cerr << "Unloading graph: " << gtd->uid << std::endl;
                    // std::cerr << "Heads: sync: " << gtd->gd->sync_head.load() << ", read: " << gtd->gd->read_head.load() << ", write: " << gtd->gd->write_head.load() << std::endl;
                }
                gtd->debug_last_action = "Before destructing GraphData";
                gtd->gd->~GraphData();
                gtd->debug_last_action = "Destructed GraphData";
                MMap::destroy_mmap((void*)gtd->gd);
                gtd->debug_last_action = "Cleaned up mmap";
            }
                
            // Next, remove this manager from the list.
            std::unique_lock lock(graph_manager_list_mutex);
            bool did_not_find = true;
            for(auto it = graph_manager_list.begin() ; it != graph_manager_list.end() ; it++) {
                if(*it == gtd) {
                    graph_manager_list.erase(it);
                    did_not_find = false;
                    break;
                }
            }
            gtd->debug_last_action = "Removed from graph manager list";
            if(did_not_find)
                throw std::runtime_error("Graph manager disappeared from list!");
        }

        std::vector<BaseUID> Butler::list_graph_manager_uids() {
            // Next, remove this manager from the list.
            std::unique_lock lock(graph_manager_list_mutex);
            std::vector<BaseUID> list;
            for(auto it = graph_manager_list.begin() ; it != graph_manager_list.end() ; it++)
                list.push_back((*it)->uid);
            return list;
        }


        ////////////////////////////////////////
        // * Memory management

        void ensure_or_get_range(const void * ptr, size_t size) {
#ifndef ZEFDB_TEST_NO_MMAP_CHECKS

            if(!MMap::is_range_alloced(ptr, size)) {
                GraphData* gd = (GraphData*)MMap::blobs_ptr_from_blob(ptr);

                if(gd->managing_thread_id == std::this_thread::get_id()) {
                    // std::cerr << "@@@####**** Did a dodgy internal load - which will be replaced" << std::endl;
                    // TODO: Actually this won't be replaced, it is now better
                    // with the seperate managing threads. This should be able
                    // to now query upstream, as it can synchronise with itself
                    // and the network thread.
                    MMap::ensure_or_alloc_range(ptr, size);
                    return;
                }

                // Note: we won't try and use the managing_thread_id stored in
                // gd, just in case there is something funky
                // (destruction/etc...) going on with the managing thread.
                // Instead, put everything on the main msg queue.
                auto response = butler->msg_push<GenericResponse>(LoadPage{ptr,size});
                // Note: we don't set a timeout here, as we will rely on the
                // butler to throw an exception if it thinks things are taking
                // too long.
                if(response.success)
                    return;
                throw std::runtime_error("Page is not accessible: " + response.reason);
            }
#endif
        }

        std::filesystem::path file_graph_folder(std::string upstream_name) {
            const char * env = std::getenv("ZEFDB_FILEGRAPH_PATH");
            if (env != nullptr)
                return std::filesystem::path(std::string(env)) / upstream_name;

            std::filesystem::path path = zefdb_config_path();
            path /= upstream_name;
            path /= "graphs";
            return path;
        }

        std::filesystem::path file_graph_prefix(BaseUID uid, std::string upstream_name) {
            std::filesystem::path path = file_graph_folder(upstream_name);
            std::filesystem::create_directories(path);
            path /= str(uid);
            return path;
        }

        std::filesystem::path local_graph_prefix(std::filesystem::path dir) {
            return dir / "graph";
        }
        std::filesystem::path local_graph_uid_path(std::filesystem::path dir) {
            return dir / "graph.uid";
        }



        //////////////////////////////
        // * Misc

        Graph Butler::get_local_process_graph() {
            if(!local_process_graph) {
                Graph g;
                set_keep_alive(g);
                local_process_graph = g;
            }
            return *local_process_graph;
        }

        std::string Butler::upstream_layout() {
            if(butler_is_master)
                return data_layout_version;
            if(zefdb_protocol_version == -1)
                throw std::runtime_error("Shouldn't be asking for upstream layout when we haven't connected and done a handshake.");

            return conversions::version_layout(zefdb_protocol_version);
        }
    }
}
