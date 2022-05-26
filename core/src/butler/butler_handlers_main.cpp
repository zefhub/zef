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


template <typename T>
void Butler::handle_guest_message(T & content, Butler::msg_ptr & msg) {
    throw std::runtime_error("Not implemented guest message");
}

template <>
void Butler::handle_guest_message(LoadGraph & content, Butler::msg_ptr & msg) {
    if(is_BaseUID(content.tag_or_uid)) { // is uid
        load_graph_from_uid(msg, BaseUID::from_hex(content.tag_or_uid));
    } else if(starts_with(content.tag_or_uid, "file://")) {
        auto filename = content.tag_or_uid.substr(strlen("file://"));
        load_graph_from_file(msg, filename);
    } else {
        // tag_lookup_workers.emplace_back(&Butler::load_graph_from_tag_worker, this, msg);
        std::thread t(&Butler::load_graph_from_tag_worker, this, msg);
        // This might be a bad idea, but it simplifies the cleanup
        t.detach();
    }
}

template <>
void Butler::handle_guest_message(Reconnected & content, Butler::msg_ptr & msg) {
    std::lock_guard lock(graph_manager_list_mutex);

    // We need to broadcast this out with fresh messages each time.
    for(auto & manager : graph_manager_list)
        manager->queue.push(std::make_shared<RequestWrapper>(Reconnected{}), true);
}

template <>
void Butler::handle_guest_message(Disconnected & content, Butler::msg_ptr & msg) {
    std::lock_guard lock(graph_manager_list_mutex);

    cancel_online_tasks();

    // We need to broadcast this out with fresh messages each time.
    for(auto & manager : graph_manager_list)
        manager->queue.push(std::make_shared<RequestWrapper>(Disconnected{}), true);
}

template <>
void Butler::handle_guest_message(TokenQuery & content, Butler::msg_ptr & msg) {
    debug_time_print("start TokenQuery");
    if(content.names.size() > 0 && content.indices.size() > 0)
        throw std::runtime_error("Can't query names and indices at the same time");
    if(content.names.size() == 0 && content.indices.size() == 0) {
        msg->promise.set_value(TokenQueryResponse{true, {}});
        return;
    }

    if(content.create) {
        if(content.group == TokenQuery::ET && !zwitch.allow_dynamic_entity_type_definitions())
            throw std::runtime_error("ET creation is disallowed.");
        if(content.group == TokenQuery::RT && !zwitch.allow_dynamic_relation_type_definitions())
            throw std::runtime_error("RT creation is disallowed.");
        if(content.group == TokenQuery::EN && !zwitch.allow_dynamic_enum_type_definitions())
            throw std::runtime_error("EN creation is disallowed.");
        if(content.group == TokenQuery::KW && !zwitch.allow_dynamic_keyword_definitions())
            throw std::runtime_error("KW creation is disallowed.");

        if(check_env_bool("ZEFDB_OFFLINE_MODE") || check_env_bool("ZEFDB_DEVELOPER_LOCAL_TOKENS")) {
            auto & tokens = global_token_store();
            std::vector<TokenQueryResponse::pair> pairs;
            for(auto & name : content.names) {
                enum_indx indx;
                if(content.group == TokenQuery::ET)
                    indx = tokens.add_ET_from_string(name).entity_type_indx;
                else if(content.group == TokenQuery::RT)
                    indx = tokens.add_RT_from_string(name).relation_type_indx;
                else if(content.group == TokenQuery::EN)
                    indx = tokens.add_EN_from_string(internals::split_enum_string(name)).value;
                else if(content.group == TokenQuery::KW)
                    indx = tokens.add_KW_from_string(name).indx;
                else
                    throw std::runtime_error("Unknown group type");
                pairs.emplace_back(name, indx);

                add_to_early_tokens(content.group, name);
            }
            msg->promise.set_value(TokenQueryResponse{
                    true,
                    pairs
                });
            return;
        }

        if(check_env_bool("ZEFDB_DEVELOPER_EARLY_TOKENS") && before_first_graph) {
            for(auto & name : content.names)
                add_to_early_tokens(content.group, name);
        }
    }


    // This is a bit of the logic inside of wait_for_auth, so that we can put a more informative error message.
    if(!want_upstream_connection())
        throw std::runtime_error("We can't create new tokens unless we can connect to ZefHub. Either login using `login | run` to store your credentials or `login_as_guest | run` for a temporary guest login, or run in offline mode by restarting your python session with the environment variable `ZEFDB_OFFLINE_MODE=TRUE` set.");

    wait_for_auth();

    std::string group;
    switch(content.group) {
    case TokenQuery::ET:
        group = "ET";
        break;
    case TokenQuery::RT:
        group = "RT";
        break;
    case TokenQuery::EN:
        group = "EN";
        break;
    case TokenQuery::KW:
        group = "KW";
        break;
    default:
        throw std::runtime_error("Unknown group: " + to_str(content.group));
    };

    json j({
            {"msg_type", "token"},
            {"msg_version", 1},
            {"group", group}
        });

    auto & tokens = global_token_store();
    if(content.names.size() > 0) {
        j["action"] = content.create ? "add" : "query";
        // Filter the list by the ones we don't already know
        if(content.filter_first) {
            std::vector<std::string> filtered;
            auto fill_filtered = [&](const auto & pred) {
                std::copy_if(content.names.begin(), content.names.end(), std::back_inserter(filtered), pred);
            };

            if(content.group == TokenQuery::ET)
                fill_filtered([&](const std::string & name) { return !bool(tokens.ET_from_string(name)); });
            else if(content.group == TokenQuery::RT)
                fill_filtered([&](const std::string & name) { return !bool(tokens.RT_from_string(name)); });
            else if(content.group == TokenQuery::EN)
                fill_filtered([&](const std::string & name) { return !bool(tokens.EN_from_string(internals::split_enum_string(name))); });
            else if(content.group == TokenQuery::KW)
                fill_filtered([&](const std::string & name) { return !bool(tokens.KW_from_string(name)); });

            j["names"] = filtered;
            if(zwitch.developer_output())
                std::cerr << "Sending off index query for " << filtered.size() << group << ", from original size of " << content.names.size() << " tokens" << std::endl;
            if(filtered.size() == 0) {
                msg->promise.set_value(TokenQueryResponse{true, {}});
                if(zwitch.developer_output())
                    std::cerr << "Nothing left in filtered set - aborting" << std::endl;
                return;
            }
        } else {
            j["names"] = content.names;
        }
        if(zwitch.developer_output()) {
            std::cerr << "Sending off query for tokens in group " << group << ": ";
            for(auto & it : j["names"])
                std::cerr << "," << it;
            std::cerr << std::endl;
        }
    } else {
        if(content.create)
            throw std::runtime_error("Can't create an indx");
        j["action"] = "query";

        if(content.filter_first) {
            // Filter the list by the ones we don't already know
            std::vector<enum_indx> filtered;
            auto fill_filtered = [&](const auto & pred) {
                std::copy_if(content.indices.begin(), content.indices.end(), std::back_inserter(filtered), pred);
            };

            if(content.group == TokenQuery::ET)
                fill_filtered([&](const enum_indx & indx) { return !bool(tokens.string_from_ET(indx)); });
            else if(content.group == TokenQuery::RT)
                fill_filtered([&](const enum_indx & indx) { return !bool(tokens.string_from_RT(indx)); });
            else if(content.group == TokenQuery::EN)
                fill_filtered([&](const enum_indx & indx) { return !bool(tokens.string_from_EN(indx)); });
            else if(content.group == TokenQuery::KW)
                fill_filtered([&](const enum_indx & indx) { return !bool(tokens.string_from_KW(indx)); });

            j["indices"] = filtered;
            if(zwitch.developer_output())
                std::cerr << "Sending off name query for " << filtered.size() << group << ", from original size of " << content.indices.size() << " tokens" << std::endl;
            if(filtered.size() == 0) {
                msg->promise.set_value(TokenQueryResponse{true, {}});
                if(zwitch.developer_output())
                    std::cerr << "Nothing left in filtered set - aborting" << std::endl;
                return;
            }
        } else {
            j["indices"] = content.indices;
        }
    }

    task_ptr task = add_task(true, 0, std::move(msg->promise));
    j["task_uid"] = task->task_uid;
    send_ZH_message(j);

    debug_time_print("finish TokenQuery");
}

void Butler::handle_guest_message_passthrough(Butler::msg_ptr & msg, GraphData * gd) {
    auto data = find_graph_manager(gd);
    if(data)
        data->queue.push(std::move(msg));
    else
        msg->promise.set_exception(std::make_exception_ptr(std::runtime_error("Unable to find graph worker that handles this graph.")));
}

template<>
void Butler::handle_guest_message(NewGraph & content, Butler::msg_ptr & msg) {
    // This point is reached in 2 ways: either requesting a blank new graph, or
    // by requesting a recreation of a graph from blob/uid bytes. We can tell
    // the difference by the length of the blob_bytes string.
    
    // Create a new manager - first checking if there is already one.
    // Should be a useless check in the case that we didn't pass any bytes.
    BaseUID uid;
    if(content.payload) {
        if(content.payload->rest[0].length() < sizeof(blobs_ns::ROOT_NODE))
            throw std::runtime_error("Blobs too small to even contain a root node!");
        blobs_ns::ROOT_NODE * ptr = (blobs_ns::ROOT_NODE*)content.payload->rest[0].data();
        uid = ptr->uid;
    } else {
        uid = make_random_uid();
    }

    auto gtd = find_graph_manager(uid);
    if(gtd) {
        if(butler_is_master) {
            // If we are master, then this action of "NewGraph" might be
            // equivalent to having loaded a graph. If we are incredibly
            // unlucky, we might be unloading the graph while trying to load it
            // back in again. Hence, check that!
            //
            // This is dodgy to use polling, but I don't trust myself that I haven't got to a deadlock situation...
            if(!gtd->queue._closed) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                if(!gtd->queue._closed) {
                    msg->promise.set_value(GraphLoaded{"This UID is already registered in the butler (after trying to wait for closing to begin)."});
                    return;
                }
            }

            for(int iter = 0; iter < 10 ; iter++) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                gtd = find_graph_manager(uid);
                if(!gtd)
                    break;
            }
            if(gtd) {
                msg->promise.set_value(GraphLoaded{"Couldn't wait long enough for GTD to be destructed so a new GTD can be created for UID: " + str(uid)});
                return;
            }
        } else {
            // Just not allowed is we aren't in control of everything
            msg->promise.set_value(GraphLoaded{"This UID is already registered in the butler."});
            return;
        }
    }

    auto new_gtd = spawn_graph_manager(uid);
    new_gtd->queue.push(std::move(msg));
}

///////////////////////////////////////////////////////////
// *** The next few messages are simply passthroughs
//
// TODO: Maybe move these JSON parts to be next to their responses in
// high_level_api.cpp?

template <>
void Butler::handle_guest_message(ZearchQuery & content, Butler::msg_ptr & msg) {
    task_ptr task = add_task(true, 0, std::move(msg->promise));
    try {
        send_ZH_message({
                {"msg_type", "zearch"},
                {"task_uid", task->task_uid},
                {"zearch_term", content.query}
            });
    } catch(...) {
        auto task_promise = find_task(task->task_uid, true);
        task_promise->promise.set_exception(std::current_exception());
    }
}

template <>
void Butler::handle_guest_message(UIDQuery & content, Butler::msg_ptr & msg) {
    task_ptr task = add_task(true, 0, std::move(msg->promise));
    try {
        send_ZH_message({
                {"msg_type", "lookup_uid"},
                {"task_uid", task->task_uid},
                {"tag", content.query}
            });
    } catch(...) {
        auto task_promise = find_task(task->task_uid, true);
        task_promise->promise.set_exception(std::current_exception());
    }
}

template <>
void Butler::handle_guest_message(MergeRequest & content, Butler::msg_ptr & msg) {
    // Even if we don't use the task, it's easier to create it now with try/catch.
    task_ptr task = {};
    try {
        auto data = find_graph_manager(BaseUID::from_hex(content.target_guid));
        if(data) {
            // There is a bit of a race condition here, but only if the user has
            // done a fire-and-forget make_primary request somehow.
            if(data->gd->is_primary_instance) {
                // We can handle this ourselves.
                data->queue.push(std::move(msg));
                return;
            }
        }
        if(zwitch.developer_output())
            std::cerr << "Did not find graph locally to merge (or is not primary), passing upstream." << std::endl;
        if(butler_is_master)
            throw std::runtime_error("Butler as master does not allow for upstream delegation of merges.");

        if(content.task_uid) {
            // This is a remote request, so we should abort.
            throw std::runtime_error("Can't handle remote request anymore. Presumably we lost transactor role in between the request being sent out.");
        }

        task = add_task(true, 0, std::move(msg->promise));
        // Need to delegate to zefhub
        std::visit(overloaded {
                [&](MergeRequest::PayloadGraphDelta & payload) {
                    if(zefdb_protocol_version <= 1) {
                        auto task_promise = find_task(task->task_uid, true);
                        task_promise->promise.set_value(MergeRequestResponse{"ZefHub is too old to handle graph deltas."});
                        return;
                    } else {
                        send_ZH_message({
                                {"msg_type", "merge_request"},
                                {"msg_version", 2},
                                {"task_uid", task->task_uid},
                                {"target_guid", str(content.target_guid)},
                                {"payload", {
                                        {"type", "delta"},
                                        {"commands", payload.commands}
                                    }}
                            });
                    }
                },
                [](auto & other) {
                    throw std::runtime_error("Not implemented payload type");
                }
            }, content.payload);
    } catch(...) {
        if(content.task_uid) {
            // This is a remote request, so we should let upstream know of the problem.
            send_ZH_message({
                    {"msg_type", "merge_request_response"},
                    {"msg_version", 2},
                    {"task_uid", task->task_uid},
                    {"target_guid", str(content.target_guid)},
                    {"success", false},
                    {"reason", "Unknown exception"},
                });
        }
        if(task) {
            auto task_promise = find_task(task->task_uid, true);
            task_promise->promise.set_exception(std::current_exception());
        } else {
            msg->promise.set_exception(std::current_exception());
        }
    }
}

template <>
void Butler::handle_guest_message(OLD_STYLE_UserManagement & content, Butler::msg_ptr & msg) {
    task_ptr task = add_task(true, 0, std::move(msg->promise));
    try {
        json j{
                {"msg_type", "user_management"},
                {"task_uid", task->task_uid},
                {"action", content.action},
                {"subject", content.subject},
                {"target", content.target},
            };
        if(content.action == "add_user") {
            std::cerr << "Assuming firebase is the authority" << std::endl;
            j["authority"] = "firebase";
            j["authority_uid"] = content.extra;
        } else {
            j["extra"] = content.extra;
        }
        send_ZH_message(j);
    } catch(...) {
        auto task_promise = find_task(task->task_uid, true);
        task_promise->promise.set_exception(std::current_exception());
    }
}

template <>
void Butler::handle_guest_message(TokenManagement & content, Butler::msg_ptr & msg) {
    task_ptr task = add_task(true, 0, std::move(msg->promise));
    try {
        send_ZH_message({
                {"msg_type", "token_management"},
                {"task_uid", task->task_uid},
                {"action", content.action},
                {"token_group", content.token_group},
                {"token", content.token},
                {"target", content.target},
            });
    } catch(...) {
        auto task_promise = find_task(task->task_uid, true);
        task_promise->promise.set_exception(std::current_exception());
    }
}
