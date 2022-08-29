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


////////////////////////////////
// * Task handling and WS msgs

Butler::task_ptr Butler::add_task(bool is_online, double timeout) {
    return add_task(is_online, timeout, std::promise<Response>(), true);
};
Butler::task_ptr Butler::add_task(bool is_online, double timeout, std::promise<Response> && promise, bool acquire_future) {
    std::lock_guard lock(waiting_tasks_mutex);
#ifdef ZEF_DEBUG
    if(waiting_tasks.size() > 100)
        std::cerr << "Warning, there are a lot of tasks building up in the task_list! These should probably be removed at some point." << std::endl;
#endif
    // Don't do this, it interrupts the ability to trigger a connection to upstream
    // if(is_online && !network.connected)
    //     throw std::runtime_error("Not adding a new network task when we aren't online.");
   
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
            if (forget) {
                Butler::task_promise_ptr ret = *it;
                waiting_tasks.erase(it);
                return ret;
            } else {
                return *it;
            }
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
                                               task_promise->promise.set_exception(std::make_exception_ptr(Communication::disconnected_exception()));
                                               return true;
                                           }
                                           return false;
                                       }),
                        waiting_tasks.end()
                        );
}

Response wait_future(Butler::task_ptr task, std::optional<activity_callback_t> activity_callback={}) {
    if(task->timeout > 0) {
        while(true) {
            auto wait_time = Time(task->last_activity.load()) + task->timeout*seconds - now();
            wait_pred(task->locker,
                      [&task, &activity_callback]() { return (((bool)activity_callback) && task->messages.size() > 0)
                              || is_future_ready(task->future); },
                      std::chrono::duration<double>(wait_time.value));

            if (activity_callback) {
                // Taking manual control over the locker cv and mutex
                std::unique_lock lock(task->locker.mutex);
                while(!task->messages.empty()) {
                    std::string & item = task->messages.front();
                    // Let other messages arrive while we are computing
                    lock.unlock();

                    (*activity_callback)(item);

                    lock.lock();
                    task->messages.pop_front();
                }
            }

            if(is_future_ready(task->future)) {
                if(zwitch.developer_output())
                    std::cerr << "Zefhub message took " << (now() - task->started_time) << std::endl;
                break;
            }

            if(now() > Time(task->last_activity.load()) + task->timeout*seconds) {
                std::cerr << "Throwing timeout exception because last_activity was " << std::fixed << task->last_activity << " and now is " << now() << std::endl;
                throw Butler::timeout_exception();
            }
        }
    }
    return task->future.get();
}
Response Butler::wait_on_zefhub_message_any(json & j, const std::vector<std::string> & rest, double timeout, bool throw_on_failure, bool chunked, std::optional<activity_callback_t> activity_callback) {
    std::string msg_type = j["msg_type"].get<std::string>();
    task_ptr task = add_task(true, timeout);
    task->wants_messages = (bool)activity_callback;
    j["task_uid"] = task->task_uid;

    if(chunked) {
        send_chunked_ZH_message(task->task_uid, j, rest);
    }
    else
        // Note: send returns before the data is actually sent.
        send_ZH_message(j, rest);

    task->last_activity = now().seconds_since_1970;

    // Make sure we clean up the task from the task_list in all cases
    RAII_CallAtEnd call_at_end([this,&task]() {
        forget_task(task->task_uid);
    });
            
    // The timeout is an inactivity timeout
    auto result = wait_future(task, activity_callback);

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
    if(zefdb_protocol_version <= 5) {
        if(api_key != "")
            throw std::runtime_error("We got to a point of a using an API key but ZefHub is too old (" + to_str(zefdb_protocol_version) + ") to understand it.");
        j["who"] = get_firebase_token_refresh_token(refresh_token);
    } else {
        if(api_key != "") {
            j["who"] = json{{"api_key", api_key}};
        } else {
            j["who"] = json{
                {"token", get_firebase_token_refresh_token(refresh_token)}
            };
        }
    }
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
    if(butler_is_master)
        throw std::runtime_error("Should not be trying to send messages to ZefHub when we are running in offline mode.");
    if(!want_upstream_connection()) {
        throw Communication::disconnected_exception();
    }

    // We do our own wait here, so that this is a proper timeout.
    // network.wait_for_connected(constants::zefhub_reconnect_timeout);
    // wait_for_auth(constants::zefhub_reconnect_timeout);
    wait_for_auth();
    if(!network.connected)
        throw Communication::disconnected_exception();

    // Note that "fill out" has to happen after the auth wait, as we
    // won't know the protocol versions etc before then.
    fill_out_ZH_message(j);

    if(zwitch.debug_zefhub_json_output()) {
        std::cerr << "About to send out: " << j << std::endl;
        std::cerr << "REST sizes: [";
        for(auto & it : rest) {
            std::cerr << it.size() << ",";
        }
        std::cerr << "]" << std::endl;
    }

    auto zh_msg = Communication::prepare_ZH_message(j, rest);

    network.send(zh_msg);
}

void Butler::send_chunked_ZH_message(std::string main_task_uid, json & j, const std::vector<std::string> & rest) {
    if(zefdb_protocol_version <= 4) {
        send_ZH_message(j, rest);
        return;
    }

    Butler::task_promise_ptr main_task = find_task(main_task_uid);

    // Currently hardcoded but we need a way to extract this from config.
    int chunk_size = std::max(this->chunked_transfer_size, this->chunked_transfer_size_min);
    // This uid links all chunks in the transfer together.
    std::string chunk_uid = generate_random_task_uid();

    std::vector<int> rest_sizes(rest.size());
    for(int i = 0; i < rest.size(); i++)
        rest_sizes[i] = rest[i].size();

    fill_out_ZH_message(j);
    task_ptr task = add_task(true, chunk_timeout);
    send_ZH_message(json{
            {"msg_type", "chunked"},
            {"task_uid", task->task_uid},
            {"chunk_type", "new"},
            {"chunk_uid", chunk_uid},
            {"rest_sizes", rest_sizes},
            {"msg", j}
        });
    std::deque<std::tuple<task_ptr,Time,int>> futures;
    futures.emplace_back(task, now(), 1);

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
            main_task->task->last_activity = now().seconds_since_1970;
            // std::cerr << "Got back one response to a chunk after waiting: " << this_waited_time << std::endl;
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
                chunked_transfer_size = std::max(chunked_transfer_size/100, chunked_transfer_size_min);
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
            if(this_bytes.size() == 0) {
                std::cerr << "SIZE is ZERO: ";
                std::cerr << this_chunk_size << ", " << chunk_size << this->chunked_transfer_size << std::endl;
            }
            // std::cerr << "Sending a chunk, size: " << this_bytes.size() << ", start: " << cur_start << "/" << rest_size << ", rest_index: " << rest_index << std::endl;
            task = add_task(true, chunk_timeout);
            send_ZH_message(json{
                    {"task_uid", task->task_uid},
                    {"msg_type", "chunked"},
                    {"chunk_type", "payload"},
                    {"chunk_uid", chunk_uid},
                    {"rest_index", rest_index},
                    {"bytes_start", cur_start}
                }, {this_bytes});
            futures.emplace_back(task, now(), this_chunk_size);

            if(futures.size() >= this->chunked_transfer_queued) {
                // Wait on first future
                auto & it = futures.front();
                // Wait time = time spent in waiting here
                // Delta time = time since first starting the task
                // Size = is used to put the time in context - a super
                //        fast send of 10 bytes should not up later sends of
                //        1MB without caution.
                //
                // Note that sizes are a bit misleading too - zstd is
                // compressing the chunks so the calculated speed is not
                // exactly the same. This is open to huge problems so
                // need to revisit this I think.

                double first_wait_time = do_wait(std::get<0>(it));
                double first_delta_time = (now() - std::get<1>(it)).value;
                int first_size = std::get<2>(it);
                futures.pop_front();

                if(chunked_transfer_auto_adjust) {
                    // Now wait on more until the queue is half-empty, this
                    // gives us a way to check the response times. If they
                    // are ping-dominated then the spacing between these
                    // times will be heavily bimodal (i.e. one slow and the
                    // rest nearly instant afterwards).
                    double max_wait_time = first_wait_time;
                    double second_wait_time = 0;
                    double min_delta_time = first_delta_time;
                    int min_size = first_size;
                    while(futures.size() > this->chunked_transfer_queued / 2) {
                        auto & it = futures.front();
                        double this_wait_time = do_wait(std::get<0>(it));
                        if(this_wait_time > max_wait_time) {
                            second_wait_time = max_wait_time;
                            max_wait_time = this_wait_time;
                        } else if (this_wait_time > second_wait_time) {
                            second_wait_time = this_wait_time;
                        }

                        double this_delta_time = (now() - std::get<1>(it)).value;
                        min_delta_time = std::min(this_delta_time, min_delta_time);

                        int this_size = std::get<2>(it);
                        min_size = std::min(min_size, this_size);
                        futures.pop_front();

                        if(this_delta_time < chunk_timeout/ chunked_safety_factor) {
                            // Because we are so much smaller, we can grow a lot in here.
                            int new_chunk_size = (int)(min_size * chunked_safety_factor/2);
                            if(new_chunk_size > chunk_size) {
                                chunk_size = new_chunk_size;
                                if(zwitch.developer_output())
                                    std::cerr << "Increased chunk size (" << chunk_size << ") as the time to return (" << min_delta_time << ") is very small from a set of packets with min size (" << min_size << ")" << std::endl;
                            }
                        }
                    }

                    if(min_delta_time < chunk_timeout / chunked_safety_factor && second_wait_time < max_wait_time/chunked_safety_factor) {
                        // Although this is not much, it grows fast.
                        int new_chunk_size = (int)(min_size * 1.5);
                        if(new_chunk_size > chunk_size) {
                            chunk_size = new_chunk_size;
                            if(zwitch.developer_output())
                                std::cerr << "Increasing chunk size as the wait times are very noisy" << std::endl;
                        }
                    } else {
                        // std::cerr << "Leaving chunk size as is because the ratio is " << second_wait_time / max_wait_time << std::endl;
                    }
                }
            }

            cur_start += this_chunk_size;
        }
    }

    // Wait on remaining futures
    for(auto & it : futures) {
        do_wait(std::get<0>(it));
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
        int est_chunk_size = (int)(goal_time * speed);
        // if(est_chunk_size > chunk_size)
        if(zwitch.developer_output()) {
            std::cerr << "est_chunk_size: " << est_chunk_size << std::endl;
            std::cerr << "est_speed: " << speed << std::endl;
            std::cerr << "previous chunk_size: " << chunked_transfer_size << std::endl;
        }
        if(est_chunk_size < chunked_transfer_size)
            chunked_transfer_size = est_chunk_size;
        else
            chunked_transfer_size = (int)((chunked_transfer_size + est_chunk_size) / chunked_safety_factor / chunked_transfer_queued);
        if(zwitch.developer_output())
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
