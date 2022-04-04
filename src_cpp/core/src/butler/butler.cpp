#include "butler/butler.h"
#include "butler/auth_server.h"
#include <memory>
#include <algorithm>
#include <chrono>
#include <fstream>
// I want to not have to use this:
#include "high_level_api.h"
#include "synchronization.h"
#include <execinfo.h>

namespace zefDB {
    bool initialised_python_core = false;
    namespace Butler {

        // These define template options, so must come first
#include "butler_handlers_graph_manager.cpp"
#include "butler_handlers_main.cpp"
#include "butler_handlers_ws.cpp"

        //////////////////////////////////////////////////////////
        // * Butler lifecycle managment

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
            void *trace_elems[20];
            int trace_elem_count(backtrace( trace_elems, 20 ));
            char **stack_syms(backtrace_symbols( trace_elems, trace_elem_count ));
            for ( int i = 0 ; i < trace_elem_count ; ++i ) {
                std::cerr << stack_syms[i] << "\n";
            }
            free( stack_syms );

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
            initialise_butler(get_default_zefhub_uri());
        }
        void initialise_butler(std::string zefhub_uri) {
#ifdef DEBUG
            internals::static_checks();
#endif
            
            if(zwitch.zefhub_communication_output())
                std::cerr << "Will use uri=" << zefhub_uri << " when later connecting to ZefHub" << std::endl;
            if (butler) {
                std::cerr << "Butler already initialised" << std::endl;
                return;
            }

            debug_time_print("initialise butler");

            if(zefhub_uri == "MASTER") {
                initialise_butler_as_master();
                return;
            }

            // Going to try some weird debugging
            std::set_terminate( terminate_handler );

            // std::cerr << "Before making butler" << std::endl;
            butler = std::make_unique<Butler>(zefhub_uri);

            // // We wait for the first connection to go through, so as to better
            // // return error messages.
            // if(zefhub_uri != "") {
            //     // std::cerr << "Going to wait for connection: " << zefhub_uri << std::endl;
            //     butler->wait_for_auth();
            //     if (!butler->connection_authed) {
            //         // TODO: Error handling
            //         std::cerr << "When initialising butler, wasn't able to connect." << std::endl;
            //     }
            //     // Wait a tiny bit to give time for the ET/RT/EN updates to come through...
            //     // TODO: Remove this once we have proper ZH communication happening
            //     // std::this_thread::sleep_for(std::chrono::seconds(1));
            //     // auto& tracker = get_all_active_graph_data_tracker();
            //     // while(!tracker.received_initial_ET_list) {
            //     //     std::this_thread::sleep_for(std::chrono::seconds(1));
            //     // }
            // }

            // std::cerr << "Before making butler thread" << std::endl;
            butler->thread = std::make_unique<std::thread>(&Butler::msgqueue_listener, &(*butler));
#if __linux__
            auto native = butler->thread->native_handle();
            pthread_setname_np(native, "Butler");
#endif

            if(!butler_registered_thread_exiter) {
                on_thread_exit(&stop_butler);
                butler_registered_thread_exiter = true;
            }

            if(butler->want_upstream_connection()) {
                // Lock this behind the if check, as we can get into trouble
                // with python circular deps.
                auto auto_connect = get_auto_connect();
                if(auto_connect == "always" ||
                   (auto_connect == "auto" && butler->have_auth_credentials()))
                    butler->start_connection();
            }
        }
        void initialise_butler_as_master() {
            if (butler && !butler_is_master)
                throw std::runtime_error("Butler already initialised as non-master");
            butler_is_master = true;
            // We have no upstream in this case, as we are the top.
            initialise_butler("");
        }

        void long_wait_or_kill(std::thread & thread, std::promise<bool> & return_value, std::string name, std::optional<std::function<void()>> extra_print = {}) {
            try {
                    auto future = return_value.get_future();
                    auto status = future.wait_for(std::chrono::seconds(1));
                    if (status == std::future_status::timeout) {
                        std::cerr << "Thread taking a long time to shutdown... " << name << std::endl;
                        if(extra_print)
                            (*extra_print)();
                        status = future.wait_for(std::chrono::seconds(10));
                        if (status == std::future_status::timeout) {
                            std::cerr << "Gave up on waiting for thread: " << name << std::endl;
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
            if(zwitch.developer_output()) {
                std::cerr << "stop_butler was called" << std::endl;
            }

            if (!butler) {
                std::cerr << "Butler wasn't running." << std::endl;
                return;
            }

            update(butler->auth_locker, butler->should_stop, true);
            butler->msgqueue.set_closed();

            if(zwitch.developer_output())
                std::cerr << "Going to close all graph manager queues" << std::endl;
            for(auto data : butler->graph_manager_list) {
                try {
                    data->queue.set_closed(false);
                } catch(const std::exception & e) {
                    std::cerr << "Exception while trying to close queue for graph manager thread: " << e.what() << std::endl;
                }
            }
            // std::cerr << "Going to wait on graph manager threads" << std::endl;
            std::vector<std::shared_ptr<Butler::GraphTrackingData>> saved_list;
            {
                std::lock_guard lock(butler->graph_manager_list_mutex);
                saved_list = butler->graph_manager_list;
            }

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
            butler->graph_manager_list.clear();
            if(zwitch.developer_output())
                std::cerr << "Removing local process graph" << std::endl;
            butler->local_process_graph.reset();

            if(zwitch.developer_output())
                std::cerr << "Stopping network" << std::endl;
            butler->network.stop_running();
            butler->waiting_tasks.clear();

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
            return early_tokens;
        }


        ////////////////////////////////
        // * Task handling and WS msgs

        Butler::task_ptr Butler::add_task(bool is_online, QuantityFloat timeout) {
            return add_task(is_online, timeout, std::promise<Response>(), true);
        };
        Butler::task_ptr Butler::add_task(bool is_online, QuantityFloat timeout, std::promise<Response> && promise, bool acquire_future) {
            std::lock_guard lock(waiting_tasks_mutex);
#ifdef ZEF_DEBUG
            if(waiting_tasks.size() > 100)
                std::cerr << "Warning, there are a lot of tasks building up in the task_list! These should probably be removed at some point." << std::endl;
#endif
            task_ptr task = std::make_shared<Task>(now(), is_online, timeout, promise, acquire_future);
            waiting_tasks.emplace_back(std::make_shared<TaskPromise>(task, std::move(promise)));
            return task;
        };
        // Search and pop a task from the waiting tasks. Return true/false if found.
        // Note the iteration style is due to the forward_list type.
        Butler::task_promise_ptr Butler::find_task(std::string task_uid, bool forget) {
            std::lock_guard lock(waiting_tasks_mutex);
            for(auto it = waiting_tasks.begin() ; it != waiting_tasks.end() ; it++) {
                if ((*it)->task->task_uid == task_uid) {
                    if(forget)
                        waiting_tasks.erase(it);
                    return *it;
                }
            }
            return {};
        }
        bool Butler::forget_task(std::string task_uid) {
            return (bool)find_task(task_uid, true);
        }
        void Butler::cancel_online_tasks() {
            std::lock_guard lock(waiting_tasks_mutex);

            waiting_tasks.erase(
                std::remove_if(waiting_tasks.begin(),
                               waiting_tasks.end(),
                               [](task_promise_ptr & task_promise) {
                                   if(task_promise->task->is_online) {
                                       task_promise->promise.set_exception(std::make_exception_ptr(disconnected_exception()));
                                       return true;
                                   }
                                   return false;
                               }),
                waiting_tasks.end()
            );
        }

        Response wait_future(Butler::task_ptr task) {
            if(task->timeout.value > 0) {
                while(true) {
                    assert(task->timeout.unit == EN.Unit.seconds);
                    // TODO: We might have to make last_activity an atomic to make sure updates come through.
                    auto wait_time = task->last_activity + task->timeout - now();
                    auto status = task->future.wait_for(std::chrono::duration<double>(wait_time.value));
                    if(status == std::future_status::ready) {
                        if(zwitch.developer_output())
                            std::cerr << "Zefhub message took " << (now() - task->started_time) << std::endl;
                        break;
                    }

                    if(now() > task->last_activity + task->timeout) {
                        std::cerr << "Throwing timeout exception because last_activity was " << task->last_activity << " and now is " << now() << std::endl;
                        throw Butler::timeout_exception();
                    }
                }
            }
            return task->future.get();
        }
        Response Butler::wait_on_zefhub_message_any(json & j, const std::vector<std::string> & rest, QuantityFloat timeout, bool throw_on_failure, bool chunked) {
            std::string msg_type = j["msg_type"].get<std::string>();
            task_ptr task = add_task(true, timeout);
            j["task_uid"] = task->task_uid;

            if(chunked) {
                send_chunked_ZH_message(task->task_uid, j, rest);
            }
            else
                // Note: send returns before the data is actually sent.
                send_ZH_message(j, rest);

            task->last_activity = now();

            // Make sure we clean up the task from the task_list in all cases
            RAII_CallAtEnd call_at_end([this,&task]() {
                forget_task(task->task_uid);
            });
            
            // The timeout is an inactivity timeout
            auto result = wait_future(task);

            if(throw_on_failure) {
                GenericResponse generic = std::visit(overloaded {
                        [](auto & v)->GenericResponse { return v.generic; },
                        [](GenericResponse & v)->GenericResponse { return v; },
                        [](std::monostate & v)->GenericResponse { throw std::runtime_error("Invalid response state."); }
                    }, result);
                if(!generic.success)
                    throw std::runtime_error("ZefHub replied with failure: " + generic.reason);
            }

            return result;
        }

        void Butler::fill_out_ZH_message(json & j) { //, bool add_hints) {
            j["protocol_type"] = "ZEFDB";
            j["protocol_version"] = zefdb_protocol_version.load();
            j["who"] = get_firebase_token_refresh_token(refresh_token);
            // if(add_hints) {
                if(estimated_transfer_speed_accum_time > 0) {
                    double speed = estimated_transfer_speed_accum_bytes / estimated_transfer_speed_accum_time;
                    double goal_time = chunk_timeout / 5;
                    double est_chunk_size = goal_time * speed;
                    j["hint_chunk_size"] = est_chunk_size;
                }
            // }
        }

        void Butler::send_ZH_message(json & j, const std::vector<std::string> & rest) {
            if(!want_upstream_connection()) {
                throw std::runtime_error("Not connecting to ZefHub, can't send messages.");
            }

            fill_out_ZH_message(j);

            if(zwitch.debug_zefhub_json_output())
                std::cerr << "About to send out: " << j << std::endl;

            auto zh_msg = Communication::prepare_ZH_message(j, rest);

            // We do our own wait here, so that this is a proper timeout.
            // network.wait_for_connected(constants::zefhub_reconnect_timeout);
            // wait_for_auth(constants::zefhub_reconnect_timeout);
            wait_for_auth();

            if(!network.connected)
                throw std::runtime_error("Network did not connect in time to send message.");
            network.send(zh_msg);
        }

        void Butler::send_chunked_ZH_message(std::string main_task_uid, json & j, const std::vector<std::string> & rest) {
            if(zefdb_protocol_version <= 4) {
                send_ZH_message(j, rest);
                return;
            }

            Butler::task_promise_ptr main_task = find_task(main_task_uid);

            // Currently hardcoded but we need a way to extract this from config.
            int chunk_size = this->chunked_transfer_size;
            // This uid links all chunks in the transfer together.
            std::string chunk_uid = generate_random_task_uid();

            std::vector<int> rest_sizes(rest.size());
            for(int i = 0; i < rest.size(); i++)
                rest_sizes[i] = rest[i].size();

            fill_out_ZH_message(j);
            task_ptr task = add_task(true, chunk_timeout*seconds);
            send_ZH_message(json{
                    {"msg_type", "chunked"},
                    {"task_uid", task->task_uid},
                    {"chunk_type", "new"},
                    {"chunk_uid", chunk_uid},
                    {"rest_sizes", rest_sizes},
                    {"msg", j}
                });
            std::deque<task_ptr> futures;
            futures.emplace_back(task);

            auto do_wait = [&](task_ptr & task) -> double {
                try {
                    auto start_time = now();
                    auto result = wait_future(task);
                    double this_waited_time = (now() - start_time).value;
                    
                    auto resp = std::visit(overloaded {
                            [](const GenericZefHubResponse & response) { return response; },
                            [](const auto & response) -> GenericZefHubResponse {
                                throw std::runtime_error("Response from ZefHub is not of the right type in send_chunked_ZH_message.");
                                }
                            }, result);
                    if(!resp.generic.success)
                        throw std::runtime_error("Failure in chunked send: " + resp.generic.reason);
                    main_task->task->last_activity = now();
                    std::cerr << "Got back one response to a chunk after waiting: " << this_waited_time << std::endl;
                    return this_waited_time;
                } catch(const timeout_exception & exc) {
                    // Reduce the chunk size. Unfortunately we can't restart the
                    // transfer (without making a new chunk uid) as it could be
                    // that the packets are still en route. This needs to be
                    // improved.
                    //
                    // What could happen is a query back to ask "where are you
                    // up to?" and then continuing from that point. This would
                    // require removing the for loop over rest_index below, and
                    // reacting to variables.
                    if(chunked_transfer_auto_adjust)
                        // Big adjustment down, smaller adjustments up.
                        chunked_transfer_size /= 10;
                    std::rethrow_exception(std::current_exception());
                }
            };

            auto start_time = now();
            int transfer_total_size = 0;

            // Split rest into chunks
            for(int rest_index = 0;rest_index < rest.size();rest_index++) {
                // The chunk size can change over the message, so we do this chunking in a kind of weird way.
                int rest_size = rest[rest_index].size();
                transfer_total_size += rest_size;
                int cur_start = 0;
                while(cur_start < rest_size) {
                    int this_chunk_size = std::min(chunk_size, rest_size - cur_start);
                    std::string this_bytes = rest[rest_index].substr(cur_start, this_chunk_size);
                    std::cerr << "Sending a chunk, size: " << this_bytes.size() << ", start: " << cur_start << "/" << rest_size << ", rest_index: " << rest_index << std::endl;
                    task = add_task(true, chunk_timeout*seconds);
                    send_ZH_message(json{
                            {"task_uid", task->task_uid},
                            {"msg_type", "chunked"},
                            {"chunk_type", "payload"},
                            {"chunk_uid", chunk_uid},
                            {"rest_index", rest_index},
                            {"bytes_start", cur_start}
                        }, {this_bytes});
                    futures.emplace_back(task);

                    if(futures.size() >= this->chunked_transfer_queued) {
                        // Wait on first future
                        auto & it = futures.front();
                        double first_wait_time = do_wait(it);
                        futures.pop_front();

                        if(chunked_transfer_auto_adjust) {
                            // Now wait on more until the queue is half-empty, this
                            // gives us a way to check the response times. If they
                            // are ping-dominated then the spacing between these
                            // times will be heavily bimodal (i.e. one slow and the
                            // rest nearly instant afterwards).
                            double max_wait_time = first_wait_time;
                            double second_wait_time = 0;
                            while(futures.size() > this->chunked_transfer_queued / 2) {
                                auto & it = futures.front();
                                double this_time = do_wait(it);
                                if(this_time > max_wait_time) {
                                    second_wait_time = max_wait_time;
                                    max_wait_time = this_time;
                                } else if (this_time > second_wait_time) {
                                    second_wait_time = this_time;
                                }
                                futures.pop_front();
                            }

                            if(max_wait_time < chunk_timeout/100) {
                                std::cerr << "Increasing chunk size as the wait time is very small: " << max_wait_time << std::endl;
                                // Because we are so much smaller, we can grow a lot in here.
                                chunk_size *= 10.0;
                            } else if(second_wait_time < max_wait_time/5) {
                                // Note: /5 because we don't want to get too greedy.
                                std::cerr << "Increasing chunk size as the wait times are very noisy" << std::endl;
                                std::cerr << "max_wait_time: " << max_wait_time << std::endl;
                                std::cerr << "second_wait_time: " << second_wait_time << std::endl;
                                // Although this is not much, it grows fast.
                                chunk_size *= 1.5;
                            } else {
                                std::cerr << "Leaving chunk size as is because the ratio is " << second_wait_time / max_wait_time << std::endl;
                            }
                        }
                    }

                    cur_start += this_chunk_size;
                }
            }

            // Wait on remaining futures
            for(auto & it : futures) {
                do_wait(it);
            }

            // Note: the do_wait sets last_activity on the main task, so there's
            // nothing special to do at the end.

            // Update estimated transfer speed
            auto transfer_duration = now() - start_time;
            estimated_transfer_speed_accum_bytes += transfer_total_size;
            estimated_transfer_speed_accum_time += transfer_duration.value;
            double ratio = estimated_transfer_speed_accum_time / limit_estimated_transfer_speed_accum_time;
            if(ratio > 1) {
                estimated_transfer_speed_accum_time /= ratio;
                estimated_transfer_speed_accum_bytes /= ratio;
            }

            if(chunked_transfer_auto_adjust) {
                double speed = estimated_transfer_speed_accum_bytes / estimated_transfer_speed_accum_time;
                double goal_time = chunk_timeout / 5;
                double est_chunk_size = goal_time * speed;
                // if(est_chunk_size > chunk_size)
                std::cerr << "est_chunk_size: " << est_chunk_size << std::endl;
                std::cerr << "est_speed: " << speed << std::endl;
                std::cerr << "previous chunk_size: " << chunked_transfer_size << std::endl;
                if(est_chunk_size < chunked_transfer_size)
                    chunked_transfer_size = est_chunk_size;
                else
                    chunked_transfer_size = (chunked_transfer_size + est_chunk_size) / 2;
                std::cerr << "Adjusting chunk size to " << chunked_transfer_size << std::endl;
            }
        }


        GenericResponse generic_from_json(json j) {
            GenericResponse generic;
            if(j.contains("success")) {
                if(j["success"].is_boolean())
                    generic.success = j["success"].get<bool>();
                else {
                    std::string temp = j["success"].get<std::string>();
                    if(temp == "0")
                        generic.success = false;
                    else if(temp == "1")
                        generic.success = true;
                    else
                        throw std::runtime_error("Unknown value for success in GenericZefHubResponse");
                }
                if(j.contains("reason"))
                    generic.reason = json_string_default(j, "reason");
                else if(j.contains("response"))
                    generic.reason = json_string_default(j, "response");
            } else {
                generic.success = true;
                generic.reason = "";
            }
            return generic;
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

        void Butler::load_graph_from_file(Butler::msg_ptr & msg, std::filesystem::path dir) {
            if(std::filesystem::exists(dir) && std::filesystem::is_directory(dir)) {
                auto uid_file = local_graph_uid_path(dir);
                if(!MMap::filegraph_exists(local_graph_prefix(dir)) || !std::filesystem::exists(uid_file))
                    throw std::runtime_error("Directory exists but no local zefgraph found inside. Aborting graph load.");

                std::ifstream file(uid_file);
                std::string output;
                std::getline(file, output);
                auto maybe_uid = to_uid(output);
                if(!std::holds_alternative<BaseUID>(maybe_uid))
                    throw std::runtime_error("UID at location '" + uid_file.string() + "' is not a valid UID.");

                BaseUID uid = std::get<BaseUID>(maybe_uid);
                auto data = find_graph_manager(uid);
                if(!data)
                    data = spawn_graph_manager(uid);
                data->queue.push(std::make_shared<RequestWrapper>(std::move(msg->promise), LocalGraph{dir, false}));
                return;
            }

            // Need to create
            if(std::filesystem::exists(dir))
                throw std::runtime_error("Can't create local graph at '" + dir.string() + "' as it is already a file.");

            std::filesystem::create_directory(dir);

            auto data = spawn_graph_manager(make_random_uid());
            data->queue.push(std::make_shared<RequestWrapper>(std::move(msg->promise), LocalGraph{dir, true}));
        }


        ////////////////////////////////////////////////////////
        // * Updates to upstream

        bool is_up_to_date(const UpdateHeads & update_heads) {
            // Note we use < instead of != here to ease checks on zefhub where clients can be ahead.
            if(update_heads.blobs.from < update_heads.blobs.to)
                return false;

            for(auto & cache : update_heads.caches) {
                if(cache.from < cache.to)
                    return false;
            }

            return true;
        }

        UpdatePayload create_update_payload(const GraphData & gd, const UpdateHeads & update_heads) {
            if(update_heads.blobs.from > update_heads.blobs.to)
                throw std::runtime_error("Somehow upstream is ahead of us and we're primary!");

            blob_index last_tx = gd.latest_complete_tx;

            // This here is a dodgy internal hack to grab the whole graph so we can send the blobs/uids out:
            char * blobs_ptr = (char*)(&gd) + update_heads.blobs.from * constants::blob_indx_step_in_bytes;
            size_t len = (update_heads.blobs.to - update_heads.blobs.from) * constants::blob_indx_step_in_bytes;
            ensure_or_get_range(blobs_ptr, len);
            // TODO: Turn these back into string_views once everything is safe
            std::string blobs(blobs_ptr, len);

            UpdatePayload p;
            p.j = json{
                {"blob_index_lo", update_heads.blobs.from},
                {"blob_index_hi", update_heads.blobs.to},
                {"graph_uid", str(internals::get_graph_uid(gd))},
                {"index_of_latest_complete_tx_node", last_tx},
                {"hash_full_graph", gd.hash(constants::ROOT_NODE_blob_index, update_heads.blobs.to)},
                {"data_layout_version", internals::get_data_layout_version_info(gd)}
            };
            p.rest = {blobs};
            std::vector<json> caches;

            for(auto & cache : update_heads.caches) {
#define GEN_CACHE(x, y) else if(cache.name == x) { \
                    if(cache.from != cache.to) { \
                        caches.push_back({ \
                                {"name", x}, \
                                {"index_lo", cache.from}, \
                                {"index_hi", cache.to}, \
                                {"revision", cache.revision}, \
                            }); \
                        auto ptr = gd.y->get(); \
                        p.rest.push_back(ptr->create_diff(cache.from, cache.to)); \
                    } \
                }

                if(false) {}
                GEN_CACHE("_ETs_used", ETs_used)
                GEN_CACHE("_RTs_used", RTs_used)
                GEN_CACHE("_ENs_used", ENs_used)
                GEN_CACHE("_uid_lookup", uid_lookup)
                GEN_CACHE("_euid_lookup", euid_lookup)
                GEN_CACHE("_tag_lookup", tag_lookup)
                else {
                    throw std::runtime_error("Unknown cache");
                }
#undef GEN_CACHE

            }

            p.j["caches"] = caches;

            return p;
        }

        json create_heads_json_from_sync_head(const GraphData & gd, const UpdateHeads & update_heads) {
            json j{
                {"blobs_head", update_heads.blobs.from},
            };
            json caches;

            for(auto & cache : update_heads.caches) {
#define GEN_CACHE(x, y) else if(cache.name == x) { \
                    caches[x] = {                       \
                        {"name", x},                    \
                        {"head", cache.from},   \
                        {"revision", cache.revision},   \
                    };                                  \
                }                                       \

                if(false) {}
                GEN_CACHE("_ETs_used", ETs_used)
                GEN_CACHE("_RTs_used", RTs_used)
                GEN_CACHE("_ENs_used", ENs_used)
                GEN_CACHE("_uid_lookup", uid_lookup)
                GEN_CACHE("_euid_lookup", euid_lookup)
                GEN_CACHE("_tag_lookup", tag_lookup)
                else {
                    throw std::runtime_error("Unknown cache");
                }
#undef GEN_CACHE

            }

            j["cache_heads"] = caches;

            return j;
        }

        UpdateHeads client_create_update_heads(const GraphData & gd) {
            if(gd.open_tx_thread != std::this_thread::get_id())
                throw std::runtime_error("Need write lock to carefully read update heads.");

            blob_index update_from = gd.sync_head;
            if(update_from == 0)
                update_from = constants::ROOT_NODE_blob_index;
            UpdateHeads update_heads{ {update_from, gd.read_head} };

#define GEN_CACHE(x, y) { \
                auto ptr = gd.y->get(); \
                update_heads.caches.push_back({x, ptr->upstream_size(), ptr->size(), ptr->revision()}); \
            }
            GEN_CACHE("_ETs_used", ETs_used)
            GEN_CACHE("_RTs_used", RTs_used)
            GEN_CACHE("_ENs_used", ENs_used)
            GEN_CACHE("_uid_lookup", uid_lookup)
            GEN_CACHE("_euid_lookup", euid_lookup)
            GEN_CACHE("_tag_lookup", tag_lookup)
#undef GEN_CACHE
            return update_heads;
        }

        void parse_filegraph_update_heads(MMap::FileGraph & fg, json & j) {
            j["blobs_head"] = fg.get_latest_blob_index();

            json cache_heads;
            auto prefix = fg.get_prefix();
            {
                decltype(GraphData::ETs_used)::element_type mapping(fg, prefix->tokens_ET);
                cache_heads["_ETs_used"] = json{
                    {"head", mapping.get()->size()},
                    {"revision", mapping.get()->revision()},
                };
            }
            {
                decltype(GraphData::RTs_used)::element_type mapping(fg, prefix->tokens_RT);
                cache_heads["_RTs_used"] = json{
                    {"head", mapping.get()->size()},
                    {"revision", mapping.get()->revision()},
                };
            }
            {
                decltype(GraphData::ENs_used)::element_type mapping(fg, prefix->tokens_EN);
                cache_heads["_ENs_used"] = json{
                    {"head", mapping.get()->size()},
                    {"revision", mapping.get()->revision()},
                };
            }
            {
                decltype(GraphData::uid_lookup)::element_type mapping(fg, prefix->uid_lookup);
                cache_heads["_uid_lookup"] = json{
                    {"head", mapping.get()->size()},
                    {"revision", mapping.get()->revision()},
                };
            }
            {
                decltype(GraphData::euid_lookup)::element_type mapping(fg, prefix->euid_lookup);
                cache_heads["_euid_lookup"] = json{
                    {"head", mapping.get()->size()},
                    {"revision", mapping.get()->revision()},
                };
            }
            {
                decltype(GraphData::tag_lookup)::element_type mapping(fg, prefix->tag_lookup);
                cache_heads["_tag_lookup"] = json{
                    {"head", mapping.get()->size()},
                    {"revision", mapping.get()->revision()},
                };
            }
            j["cache_heads"] = cache_heads;
        }

        void apply_sync_heads(GraphData & gd, const UpdateHeads & update_heads) {
            update(gd.heads_locker, [&]() {
                gd.sync_head = update_heads.blobs.to;
                gd.currently_subscribed = true;

                for(auto & cache : update_heads.caches) {
#define GEN_CACHE(x, y) else if(cache.name == x) { \
                        auto ptr = gd.y->get_writer(); \
                        ptr->upstream_size() = cache.to; \
                        ptr->revision() = cache.revision; \
                    }

                    if(false) {}
                    GEN_CACHE("_ETs_used", ETs_used)
                    GEN_CACHE("_RTs_used", RTs_used)
                    GEN_CACHE("_ENs_used", ENs_used)
                    GEN_CACHE("_uid_lookup", uid_lookup)
                    GEN_CACHE("_euid_lookup", euid_lookup)
                    GEN_CACHE("_tag_lookup", tag_lookup)
                    else
                        throw std::runtime_error("Don't understand cache: " + cache.name);
#undef GEN_CACHE
                }
            });
        }

        UpdateHeads parse_message_update_heads(const json & j) {
            UpdateHeads update_heads{
                {0, j["blobs_head"].get<blob_index>()}
            };

            for(auto & it : j["cache_heads"].items()) {
                std::string name = it.key();
                update_heads.caches.push_back({
                        name,
                        0,
                        it.value()["head"].get<size_t>(),
                        it.value()["revision"].get<size_t>()
                    });
            }

            return update_heads;
        }

        UpdateHeads parse_payload_update_heads(const UpdatePayload & payload) {
            UpdateHeads heads{
                {payload.j["blob_index_lo"].get<blob_index>(), payload.j["blob_index_hi"].get<blob_index>()}
            };

            for(auto & cache : payload.j["caches"]) {
                std::string name = cache["name"].get<std::string>();
                size_t from = cache["index_lo"].get<size_t>();
                size_t to = cache["index_hi"].get<size_t>();

                heads.caches.push_back({name, from, to});
            }

            return heads;
        }

        bool heads_apply(const UpdateHeads & heads, const GraphData & gd) {
            if(gd.write_head != heads.blobs.from) {
                std::cerr << "Blobs update (" + to_str(heads.blobs.from) + ") doesn't fit onto write head (" + to_str(gd.write_head.load()) + ")" << std::endl;
                return false;
            }

            // Check cache heads align
            for(auto & cache : heads.caches) {
#define GEN_CACHE(x, y) else if(cache.name == x) { \
                    auto ptr = gd.y->get(); \
                    if(ptr->revision() != cache.revision || ptr->size() != cache.from) {      \
                        std::cerr << "Update for cache '" + cache.name + "' doesn't fit. Current: (" << ptr->revision() << ", " << ptr->size() << "), update: (" << cache.revision << ", " << cache.from << ")" << std::endl; \
                        return false; \
                    } \
                }
                if(false) {}
                GEN_CACHE("_ETs_used", ETs_used)
                GEN_CACHE("_RTs_used", RTs_used)
                GEN_CACHE("_ENs_used", ENs_used)
                GEN_CACHE("_uid_lookup", uid_lookup)
                GEN_CACHE("_euid_lookup", euid_lookup)
                GEN_CACHE("_tag_lookup", tag_lookup)
                else
                    throw std::runtime_error("Unknown cache");
            }

            return true;
        }


        void Butler::send_update(Butler::GraphTrackingData & me) {
            if(me.gd->error_state != GraphData::ErrorState::OK)
                throw std::runtime_error("Can't send_update with a graph in an invalid state");
            if(!me.gd->is_primary_instance)
                throw std::runtime_error("We don't have primary role, how did we send an update??");

            // If we aren't connected, then we'll worry about sending out updates later.
            if(!network.connected) {
                // Make sure we know we aren't subscribed for later.
                me.gd->currently_subscribed = false;
                return;
            }

            // If we have yet to resubscribe to a graph, then wait for that
            // before trying to send out an update. The wait can happen in the caller (most likely the sync thread)
            if(me.gd->sync_head > 0 && !me.gd->currently_subscribed)
                return;

            UpdateHeads update_heads;
            {
                // Let's stop transactions from happening while we do this.
                LockGraphData lock{me.gd};

                update_heads = client_create_update_heads(*me.gd);
            }

            if(is_up_to_date(update_heads))
                return;

            UpdatePayload payload = create_update_payload(*me.gd, update_heads);

            if(me.gd->sync_head == 0)
                payload.j["msg_type"] = "full_graph";
            else
                payload.j["msg_type"] = "graph_update";

            payload.j["msg_version"] = 1;

            // Handling errors:
            // ================
            // Regardless of what happened, we need to queue a new shot at this.
            // A "queue" implies a timeout though and that's ugly. Instead, we have a choice of options:
            // a) try again immediately, this could lead to spam, maybe have a maximum try with a sleep in between (this blocks anything else happening with the graph though).
            // b) check if the websocket went down. If so, wait until the trigger message for when that comes back up.
            // c) wait for the next transaction. We could lose data that way though.
            // A mix between a) and b) seems good.

            const int max_attempts = 3;
            int attempts_left = max_attempts;
            for(; attempts_left > 0; attempts_left--) {
                // TODO: Check if websocket is down, and quit in that case.
                if(!network.connected) {
                    std::cerr << "Gave up updating as websocket is disconnected" << std::endl;
                    return;
                }

                // A little pause for each retry except the first.
                if(attempts_left != max_attempts)
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    

                GenericZefHubResponse response;
                try {
                    if(zwitch.developer_output())
                        std::cerr << "Trying to send update for graph " << me.uid << " of range " << update_heads.blobs.from << " to " << update_heads.blobs.to << std::endl;
                    response = wait_on_zefhub_message(payload.j, payload.rest, zefhub_generic_timeout, true, true);

                    if(!response.generic.success) {
                        // There's generally only one reason for a failure. But
                        // in any case return and leave this to try again in the
                        // sync loop. It's better to just keep retrying rather
                        // than crashing.

                        if(response.j.contains("upstream_head")) {
                            UpdateHeads parsed_heads = parse_message_update_heads(response.j);
                            apply_sync_heads(*me.gd, parsed_heads);
                        } else {
                            // Unknown error - wait a bit
                            std::cerr << "Unknown error received from ZH from our graph update ('" << response.generic.reason << "'). Setting graph to invalid state." << std::endl;
                            me.gd->error_state = GraphData::ErrorState::UNSPECIFIED_ERROR;
                            // std::this_thread::sleep_for(std::chrono::seconds(1));
                        }

                        return;
                    }
                } catch(const timeout_exception & e) {
                    std::cerr << "Got timeout during send_update. Going to retry" << std::endl;
                    continue;
                } catch(const disconnected_exception & e) {
                    std::cerr << "Disconnected during send_update. Going to retry" << std::endl;
                    continue;
                } catch(const std::exception & e) {
                    std::cerr << "An exception occurred during a send_update ('" << e.what() << "'). Setting graph to invalid state." << std::endl;
                    me.gd->error_state = GraphData::ErrorState::UNSPECIFIED_ERROR;
                    wake(me.gd->heads_locker);
                    return;
                }

                // if(response.j["blob_index_hi"].get<blob_index>() != me.gd->read_head)
                //     throw std::runtime_error("Somehow zefhub replied with a different blob update than we sent!");

                // Getting here should mean we succeeded.
                me.gd->currently_subscribed = true;
                apply_sync_heads(*me.gd, update_heads);
                return;
            }

            // If we get here, we ran out of attempts
            if(attempts_left != 0)
                throw std::runtime_error("Huh?");

            std::cerr << "Failed to send out graph update! Graph " << me.uid << " is likely out of sync." << std::endl;
            std::cerr << "Failed to send out graph update! Graph " << me.uid << " is likely out of sync." << std::endl;
            std::cerr << "Failed to send out graph update! Graph " << me.uid << " is likely out of sync." << std::endl;
            std::cerr << "Failed to send out graph update! Graph " << me.uid << " is likely out of sync." << std::endl;
            // We don't want to set this after all as it is better to enter a
            // never-ending cycle of resend attempts, than potentially lose
            // data.

            // me.gd->error_state = GraphData::ErrorState::UNSPECIFIED_ERROR;
            // throw std::runtime_error("Gave up after " + to_str(max_attempts) + " attempts of sending out graph update!");
            // It's always better to just return and let this try again rather than crashing.
            return;
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
                        std::cerr << "Setting graph into error state" << std::endl;
                        me->gd->error_state = GraphData::ErrorState::UNSPECIFIED_ERROR;
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
                me->gd->error_state = GraphData::ErrorState::UNSPECIFIED_ERROR;
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

                while(true) {
                    wait_pred(me.gd->heads_locker,
                              [&]() {
                                  return me.please_stop
                                      || (network.connected && me.gd->should_sync
                                          && (me.gd->sync_head == 0 || (me.gd->sync_head < me.gd->read_head.load())))
                                      || (!zwitch.write_thread_runs_subscriptions() && me.gd->latest_complete_tx > me.gd->manager_tx_head.load());
                              });
                                  
                    if(me.please_stop)
                        break;

                    // Only run subs if we are a reader or the write_thread flag is
                    // not set. TODO: In the future, probably indicate the
                    // difference between "anytime" subs and "in reaction to
                    // updates" subs.
                    if(!me.gd->is_primary_instance ||
                       !zwitch.write_thread_runs_subscriptions()) {
                        blob_index target_complete_tx = me.gd->latest_complete_tx;
                        while(!me.please_stop
                              && target_complete_tx > me.gd->manager_tx_head) {
                            // Do one tx at a time until we have nothing left to do.

                            // First check for subscriptions
                            EZefRef last_tx{me.gd->manager_tx_head, *me.gd};
                            EZefRef this_tx = last_tx >> BT.NEXT_TX_EDGE;

                            run_subscriptions(*me.gd, this_tx);

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
                    }

                    if(me.gd->error_state != GraphData::ErrorState::OK)
                        throw std::runtime_error("Sync worker for graph detected invalid state and is aborting");
                    // After doing everything, now is a good time to send out updates.
                    // TODO: Maybe this is better left to the main thread, and we just trigger it from here by placing a message on the queue.
                    if(me.gd->should_sync
                       && me.gd->is_primary_instance
                       && me.gd->sync_head < me.gd->read_head.load()
                       && want_upstream_connection())
                        send_update(me);
                }

                // This is the last hail mary before we shut down. Required for the logic of remove_graph_manager.
                if(me.gd->should_sync && me.gd->is_primary_instance)
                    send_update(me);

                me.sync_return_value.set_value(true);
            } catch(const std::exception & e) {
                std::cerr << "*** MAJOR FAILURE OF GRAPH SYNC THREAD!!!! ***" << std::endl;
                std::cerr << "*** MAJOR FAILURE OF GRAPH SYNC THREAD!!!! ***" << std::endl;
                std::cerr << "Some kind of exception occurred in the sync thread at the highest level: " << e.what() << std::endl;
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
                gtd->keep_alive_g.reset();
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
                if(gtd->gd->sync_head > 0) {
                    send_ZH_message({
                            {"msg_type", "unsubscribe_from_graph"},
                            {"graph_uid", str(gtd->uid)},
                        });
                }
                gtd->debug_last_action = "Sent out unsubscribe";

                // Before removing the manager we have to clean up memory, just
                // in case we are using a filegraph and we'd end up with two
                // threads doing things to the prefix.
                if(zwitch.developer_output()) {
                    std::cerr << "Unloading graph: " << uid(*gtd->gd) << std::endl;
                    // std::cerr << "Heads: sync: " << gtd->gd->sync_head.load() << ", read: " << gtd->gd->read_head.load() << ", write: " << gtd->gd->write_head.load() << std::endl;
                }
                gtd->gd->~GraphData();
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


        ////////////////////////////////////////////////
        // * Connection management

        std::string get_auto_connect() {
            if(!initialised_python_core) {
                std::cerr << "WARNING!!!" << std::endl;
                std::cerr << "WARNING!!!" << std::endl;
                std::cerr << "WARNING!!!" << std::endl;
                std::cerr << "WARNING!!!" << std::endl;
                std::cerr << "WARNING!!!" << std::endl;
                std::cerr << "C library wants to call into python module before it has finished loading. Specifically, it wants to determine the \"login.autoConnect\" value. Going to return \"auto\" to avoid creating a circular dependency." << std::endl;
                std::cerr << "WARNING!!!" << std::endl;
                std::cerr << "WARNING!!!" << std::endl;
                std::cerr << "WARNING!!!" << std::endl;
                std::cerr << "WARNING!!!" << std::endl;
                std::cerr << "WARNING!!!" << std::endl;
                return "auto";
            }
                
            py::gil_scoped_acquire acquire;
            auto get_config = py::module_::import("zef.ops._config").attr("get_config");
            return get_config("login.autoConnect").cast<std::string>();
        }

        bool Butler::want_upstream_connection() {
            if(network.uri == "")
                return false;

            // If we are running, then we want to continue.
            if(network.is_running())
                return true;

            std::string auto_connect = get_auto_connect();

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
                throw std::runtime_error("Not connecting to ZefHub until we have credentials. Make sure you have logged in using `login | run` to store your credentials.");
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
                    std::cerr << "Warning: waiting for connection with ZefHub is taking a long time.\n\nIf you would like to see more information enable debug messages through `zwitch.zefhub_communication_output(True)` or setting the environment variable `ZEFDB_VERBOSE=true`." << std::endl;
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
                throw std::runtime_error("Fatal connection error is aborting waiting for auth");
            debug_time_print("finish wait_for_auth");
            return true;
        }

        void Butler::determine_refresh_token() {
            std::optional<std::string> key_string = load_forced_zefhub_key();
            if(key_string) {
                if(*key_string == constants::zefhub_guest_key) {
                    std::cerr << "Connecting as guest user" << std::endl;
                    refresh_token = "";
                } else 
                    throw std::runtime_error("Need to reimplement getting token from email:password combination");
                    // auth_token = get_firebase_token_email(*key_string);
                return;
            } else if(!have_auth_credentials()) {
                no_credentials_warning = true;
                throw std::runtime_error("Have no existing credentials to determine auth token. You must login first, or set auto_connect=\"always\" to trigger the login process automatically.");
            } else {
                ensure_auth_credentials();
                if(have_logged_in_as_guest) {
                    refresh_token = "";
                } else {
                    auto credentials_file = zefdb_config_path() / "credentials";
                    std::ifstream file(credentials_file);
                    json j = json::parse(file);
                    refresh_token = j["refresh_token"].get<std::string>();
                }
            }
        }

        Butler::header_list_t Butler::prepare_send_headers(void) {
            determine_refresh_token();
            decltype(network)::header_list_t headers;
            std::string auth_token = get_firebase_token_refresh_token(refresh_token);
            headers.emplace_back("X-Auth-Token", auth_token);
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

            // TODO: Look this up
            std::string auto_connect = get_auto_connect();
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

        void Butler::handle_successful_auth() {
            if(zwitch.zefhub_communication_output())
                std::cerr << "Authenticated with ZefHub" << std::endl;
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
            send_ZH_message({
                    {"msg_type", "token"},
                    {"msg_version", 1},
                    {"action", "list"}
                });

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

        std::string get_default_zefhub_uri() {
            char * env = std::getenv("ZEFHUB_URL");
            if (env != nullptr)
                return std::string(env);

            // TODO: Load from file
            
            // return "http://localhost:5001";
            // return "wss://hub.zefhub.com";
            // return "wss://hubv2.zefhub.com";
            return "wss://hub.zefhub.io";
        }


        //////////////////////////////
        // * Credentials


        std::optional<std::string> load_forced_zefhub_key() {
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

            // Old location for fallback
            env = std::getenv("HOME");
            std::filesystem::path path2(env);
            path2 /= ".zefdb";
            path2 /= "zefhub.key";
            if (std::filesystem::exists(path2)) {
                std::ifstream file(path2);
                std::string output;
                std::getline(file, output);
                return output;
            }

            return {};
        }

        bool Butler::have_auth_credentials() {
            // TODO: This should become more sophisticated in the future
            if(load_forced_zefhub_key())
                return true;

            if(is_credentials_file_valid())
                return true;

            if(have_logged_in_as_guest)
                return true;

            return false;
        }

        bool Butler::is_credentials_file_valid() {
            auto credentials_file = zefdb_config_path() / "credentials";
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
                if(*forced_zefhub_key != constants::zefhub_guest_key)
                    get_firebase_token_email(*forced_zefhub_key);
            } else {
                auto credentials_file = zefdb_config_path() / "credentials";
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

                if(*(auth_server->reply) == "GUEST") {
                    have_logged_in_as_guest = true;
                    if(zwitch.zefhub_communication_output())
                        std::cerr << "Logging in as guest" << std::endl;
                } else {
                    have_logged_in_as_guest = false;
                    std::ofstream file(credentials_file);
                    file << *(auth_server->reply);
                    if(zwitch.zefhub_communication_output())
                        std::cerr << "Successful obtained credentials" << std::endl;
                }
            }
        }

        void Butler::user_login() {
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
            // // Always remove any graphs that were being synced with ZefHub. Do
            // // this first as the sync messages might require the right
            // // credentials.
            // std::vector<std::shared_ptr<Butler::GraphTrackingData>> saved_list;
            // {
            //     std::lock_guard lock(butler->graph_manager_list_mutex);
            //     saved_list = butler->graph_manager_list;
            // }
            // for(auto data : butler->graph_manager_list) {
            //     if(!data->gd->should_sync)
            //         continue;

            //     std::cerr << "Going to stop graph manager: " << uid(*(data->gd)) << std::endl;

            //     try {
            //         data->queue.set_closed(false);
            //         long_wait_or_kill(*data->managing_thread, data->return_value, data->uid);
            //     } catch(const std::exception & e) {
            //         std::cerr << "Exception while trying to close queue for graph manager thread: " << e.what() << std::endl;
            //     }
            // }
            

            // Now remove credentials
            if(load_forced_zefhub_key())
                throw std::runtime_error("Can't logout when an explicit key is given in ZEFHUB_AUTH_KEY or zefhub.key");
            if(have_auth_credentials()) {
                auto credentials_file = zefdb_config_path() / "credentials";
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


        ////////////////////////////////////////
        // * Memory management

        void ensure_or_get_range(void * ptr, size_t size) {
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

            char * home = std::getenv("HOME");
            std::filesystem::path path(home);
            path /= ".zef";
            path /= "graphs";
            path /= upstream_name;
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

        std::filesystem::path zefdb_config_path() {
            char * env = std::getenv("ZEFDB_CONFIG_PATH");
            if (env != nullptr)
                return std::string(env);

            env = std::getenv("HOME");
            if (env == nullptr)
                throw std::runtime_error("No HOME env set!");

            std::filesystem::path path(env);
            path /= ".zef";
            if(!std::filesystem::exists(path))
                std::filesystem::create_directories(path);
            return path;
        }


    }
}
