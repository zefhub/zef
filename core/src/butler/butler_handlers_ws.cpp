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

#include "butler/locking.h"

void handle_token_response(Butler & butler, json & j);
void handle_token_response(Butler & butler, json & j, Butler::task_promise_ptr & task);


void Butler::ws_open_handler(void) {
    if(should_stop)
        return;

    // Reset any variables that need it.
    zefdb_protocol_version = -1;
    // connection_authed = false;
    fatal_connection_error = false;

    json j({
            {"token", get_firebase_token_refresh_token(refresh_token)},
            {"desired_protocol_version", zefdb_protocol_version_max},
        });

    debug_time_print("before send handshake");
    network.send(j.dump());
}

void Butler::ws_close_handler(void) {
    if(should_stop)
        return;

    // TODO: Tell any graph managers that we lost the connection.
    // Note: this means we should cancel any tasks that rely on
    // network. This means we need to tag those tasks as being
    // appropriately network related (and for a good pattern, which
    // network they are related to, and which task the graph manager
    // is currently waiting on) rather than just using a timeout on
    // them.


    if(connection_authed)
        msg_push(Disconnected{}, false, true);
    connection_authed = false;
}

void Butler::ws_fatal_handler(std::string reason) {
    std::cerr << "FATAL: connection failure in background WS thread, reason: " << reason << std::endl;
    std::cerr << "If you would like to run in offline mode instead, please restart your python session and set the environment variable `ZEFDB_OFFLINE_MODE=TRUE`." << std::endl;
    update(auth_locker, fatal_connection_error, true);

    ws_close_handler();
}


void Butler::ws_message_handler(std::string msg) {
    if(should_stop)
        return;

    auto [j, rest]  = Communication::parse_ZH_message(msg);

    if(zwitch.debug_zefhub_json_output())
        std::cerr << j << std::endl;

    handle_incoming_message(j, rest);

    // The WS thread is the only one that is allowed to touch the incoming
    // chunked transfers, so we do the checks here - this should be quick and
    // kept that way.
    check_overdue_receiving_transfers();
}

void Butler::handle_incoming_message(json & j, std::vector<std::string> & rest) {
    if(!j.contains("protocol_type") || !j.contains("protocol_version") || !j.contains("msg_type")) {
        std::cerr << "Invalid message from upstream, doesn't contain protocol_type, protocol_version and msg_type. Ignoring" << std::endl;
        return;
    }

    std::string protocol_type = j["protocol_type"].get<std::string>();
    int protocol_version = j["protocol_version"].get<int>();
    std::string msg_type = j["msg_type"].get<std::string>();

    if(protocol_type != "ZEFDB") {
        std::cerr << "Don't know how to handle any protocol type other than 'ZEFDB' currently." << std::endl;
        return;
    }

    if (!connection_authed) {
        debug_time_print("received handshake response");
        // std::cerr << "Received message for auth" << std::endl;

        // Check response and either continue or close and return.
        try {
            if(msg_type == "failed_auth")
                throw std::runtime_error("Failed auth");
            else if(msg_type == "redirect") {
                auto target = j["target"].get<std::string>();
                if(zwitch.zefhub_communication_output())
                    std::cerr << "Redirecting to " << target << std::endl;
                network.uri = target;
                // Not sure if this is asking WSPP to pull the rug out from
                // under itself.
                network.restart(false);
                return;
            } else if(msg_type == "auth_success") {
                // TODO: in the future, we need to handle the msg type better
                // This should include a message from the server about what
                // versions it supports. Then we can set the protocol_version
                // number and also send the version that we support up to.
                int zefhub_version = j["desired_protocol_version"].get<int>();
                if(zefhub_version < zefdb_protocol_version_min) {
                    std::cerr << "ZefHub version is too low, can't communicate" << std::endl;
                    // Don't know if this will work! But then we should never get here...
                    stop_butler();
                    return;
                }
                zefdb_protocol_version = std::min(zefhub_version, zefdb_protocol_version_max);
                    
                handle_successful_auth();
            } else {
                throw std::runtime_error("Unexpected message when expecting auth response: " + msg_type);
            }
            if(zwitch.zefhub_communication_output())
                std::cerr << "Running with zefdb protocol version: " << zefdb_protocol_version.load() << std::endl;

        } catch(const std::runtime_error & e) {
            std::cerr << "Problem in authentication: " << e.what() << std::endl;
            network.close(true);
            return;
        }
    } else {
        if(protocol_version != zefdb_protocol_version) {
            std::cerr << "Don't know how to handle this protocol version (" << protocol_version << ") when we were originally communicating at version " << zefdb_protocol_version.load() << " - something must have changed in our communication for the protocol version to get outdated." << std::endl;
            return;
        }

        try {
            // Normal messages.
            //
            // Note: we should always ignore closed on any message push
            // here, as a graph manager may be closing, or it may be at
            // the end of the program.

            // std::cerr << "Received message: " << msg_type << std::endl;

            // Check the version if available for developer warnings
            int msg_version = 0;
            if(j.contains("msg_version"))
                msg_version = j["msg_version"].get<int>();
            
            if(!validate_message_version(msg_type, msg_version, zefdb_protocol_version))
                    std::cerr << "Warning: msg_version for " << msg_type << " is not appropriately matched with the negotiated client version." << std::endl;

            if(msg_type == "terminate") {
                handle_incoming_terminate(j);
                return;
            } else if(msg_type == "graph_update") {
                handle_incoming_graph_update(j, rest);
                return;
            } else if(msg_type == "update_tag_list") {
                handle_incoming_update_tag_list(j);
                return;
            } else if(msg_type == "merge_request") {
                handle_incoming_merge_request(j);
                return;
            } else if(msg_type == "chunked") {
                handle_incoming_chunked(j, rest);
                return;
            }

            if (!j.contains("task_uid"))
                throw std::runtime_error("Unknown message and does not contain task_uid either: " + msg_type);

            // If we get here, all of these messages use a task_uid
            // However, a task_uid of null means we are meant to handle this ourselves.
            if(j["task_uid"].is_null()) {
                try {
                if(msg_type == "token_response")
                    handle_token_response(*this, j);
                else
                    throw std::runtime_error("Unknown msg meant to be handled by WS loop: " + msg_type);
                } catch(const std::exception & exc) {
                    std::cerr << std::endl;
                    std::cerr << "MAJOR ERROR IN ZEFDB WS LOOP" << std::endl;
                    std::cerr << "MAJOR ERROR IN ZEFDB WS LOOP" << std::endl;
                    std::cerr << "MAJOR ERROR IN ZEFDB WS LOOP" << std::endl;
                    std::cerr << exc.what() << std::endl;
                    std::cerr << "Continuing anyway, although likely we are now in an invalid state" << std::endl;
                    std::cerr << "------------" << std::endl;
                }
                return;
            }

            // All of these messages must be passed down the line
            std::string task_uid = j["task_uid"].get<std::string>();
            task_promise_ptr task_promise = find_task(task_uid);
            if(!task_promise)
                throw std::runtime_error("Task uid isn't in the waiting list!");
            try {
                if(msg_type == "poke") {
                    if(zwitch.developer_output())
                        std::cerr << "Got a poke for task " << task_uid << std::endl;
                    task_promise->task->last_activity = now().seconds_since_1970;
                    return;
                }

                // If we're here, then we definitely are going to be finished
                // with the task
                forget_task(task_uid);

                if(zwitch.developer_output())
                    std::cerr << "Task response time was: " << now() - task_promise->task->started_time << std::endl;
                if(msg_type == "merge_request_response") {
                    auto msg = parse_ws_response<MergeRequestResponse>(j);
                    task_promise->promise.set_value(msg);
                } else if(msg_type == "token_response") {
                    handle_token_response(*this, j, task_promise);
                } else {
                    // The fallback
                    GenericZefHubResponse msg;
                    msg.generic = generic_from_json(j);
                    msg.j = j;
                    msg.rest = rest;
                    task_promise->promise.set_value(msg);
                }
                return;
            } catch(...) {
                task_promise->promise.set_exception(std::current_exception());
                forget_task(task_uid);
                throw;
            }

            // If we get here, nothing was done!
            throw std::runtime_error("Unknown message (with task_uid) type: " + msg_type);
        } catch(const std::exception & e) {
            std::cerr << "Exception while handling message (" << msg_type << "): " << e.what() << std::endl;
        }
    }

    // TODO: After everything, let's tidy up the task list for messages that have timed out.
}

// TODO: void TidyTaskList()



// TODO: Replace these with the individual type updates, which will occur in new messages from zefhub.
template<typename T1, typename T2>
void update_bidirection_name_map(thread_safe_bidirectional_map<T1,T2>& map_to_update, const T1 & indx, const T2 & name) {			
    if(map_to_update.contains(indx)) {
        auto it = map_to_update.at(indx);
        if(it != name) {
            std::cout << it << " doesn't agree with " << name << " (" << indx << ")" << std::endl;
            throw std::runtime_error("existing index assigned to a ET/RT does not agree with newly received ET/RT type list");
        }
    } else if(map_to_update.contains(name)) {
        auto it = map_to_update.at(name);
        if(it != indx) {
            std::cout << it << " doesn't agree with " << name << " (" << indx << ")" << std::endl;
            throw std::runtime_error("existing name assigned to a ET/RT does not agree with newly received ET/RT type list");
        }
    } else
        map_to_update.insert(indx, name);
}


void update_zef_enum_bidirectional_map(thread_safe_zef_enum_bidirectional_map& map_to_update, const enum_indx & indx, const std::string& name) {
    auto [enum_type,enum_value] = internals::split_enum_string(name);
    auto pair = std::make_pair(enum_type, enum_value);
    if(map_to_update.contains(indx)) {
        auto it = map_to_update.at(indx);
        if(it.first != enum_type || it.second != enum_value) {
                std::cout << it.first << "." << it.second << " doesn't agree with "
                          << enum_type << "." << enum_value << " for indx " << it.first << std::endl;
                throw std::runtime_error("existing index assigned to a enum does not agree with newly received enum type and name list");
        }
    } else if(map_to_update.contains(pair)) {
        auto it = map_to_update.at(pair);
        if(it != indx) {
                std::cout << it << " doesn't agree with "
                          << enum_type << "." << enum_value << " for indx " << indx << std::endl;
                throw std::runtime_error("existing name assigned to a enum does not agree with newly received enum type and name list");
        }
    } else
        map_to_update.insert(indx, pair);
}


void handle_token_response(Butler & butler, json & j) {
    auto& tokens = global_token_store();

    auto reason = j["reason"].get<std::string>();
    if(zwitch.developer_output())
        std::cerr << "Got a butler-handlable token response with reason: " << reason << std::endl;
    if(reason == "list") {
        auto vec1 = j["groups"]["ET"].get<std::vector<std::pair<token_value_t, std::string>>>();
        for(auto & it : vec1) {
            update_bidirection_name_map(tokens.ETs, std::get<0>(it), std::get<1>(it));
            if(zwitch.developer_output())
                std::cerr << "ET: " << std::get<1>(it) << std::endl;
        }

        auto vec2 = j["groups"]["RT"].get<std::vector<std::tuple<token_value_t, std::string>>>();
        for(auto & it : vec2) {
            update_bidirection_name_map(tokens.RTs, std::get<0>(it), std::get<1>(it));
            if(zwitch.developer_output())
                std::cerr << "RT: " << std::get<1>(it) << std::endl;
        }

        if(j["groups"].contains("KW")) {
            auto vec4 = j["groups"]["KW"].get<std::vector<std::tuple<token_value_t, std::string>>>();
            for(auto & it : vec4) {
                update_bidirection_name_map(tokens.KWs, std::get<0>(it), std::get<1>(it));
                if(zwitch.developer_output())
                    std::cerr << "KW: " << std::get<1>(it) << std::endl;
            }
        }

        auto vec3 = j["groups"]["EN"].get<std::vector<std::tuple<enum_indx, std::string>>>();
        for(auto & it : vec3) {
            update_zef_enum_bidirectional_map(tokens.ENs, std::get<0>(it), std::get<1>(it));
            if(zwitch.developer_output())
                std::cerr << "EN: " << std::get<1>(it) << std::endl;
        }
    } else {
        throw std::runtime_error("Shouldn't get here - unknown reason for token response: " + reason);
    }
}

void handle_token_response(Butler & butler, json & j, Butler::task_promise_ptr & task_promise) {
    TokenQueryResponse response;
    response.generic = generic_from_json(j);
    
    if(response.generic.success) {
        auto& tokens = global_token_store();

        auto reason = j["reason"].get<std::string>();
        if(reason == "added" || reason == "found") {
            std::string group = j["group"].get<std::string>();
            response.pairs = j["pairs"].get<std::vector<TokenQueryResponse::pair>>();

            for(auto & [name,indx] : response.pairs) {
                if (group == "ET")
                    update_bidirection_name_map(tokens.ETs, indx, name);
                else if(group == "RT")
                    update_bidirection_name_map(tokens.RTs, indx, name);
                else if(group == "KW")
                    update_bidirection_name_map(tokens.KWs, indx, name);
                else if(group == "EN")
                    update_zef_enum_bidirectional_map(tokens.ENs, indx, name);
            }
        } else {
            std::cerr << "WARNING: unexpected reason (" << response.generic.reason << ") during handle_token_response" << std::endl;
        }
    }

    task_promise->promise.set_value(response);
}




void Butler::handle_incoming_terminate(json & j) {
    std::cerr << "Server is terminating our connection: " << j["reason"].get<std::string>() << std::endl;
}

void Butler::handle_incoming_graph_update(json & j, std::vector<std::string> & rest) {
    GraphUpdate content;
    content.graph_uid = j["graph_uid"].get<std::string>();
    // content.blob_index_lo = j["blob_index_lo"].get<blob_index>();
    // content.blob_index_hi = j["blob_index_hi"].get<blob_index>();
    // content.last_tx = j["index_of_latest_complete_tx_node"].get<blob_index>();

    // content.blob_bytes = rest[0];

    content.payload = UpdatePayload{j, rest};

    auto data = find_graph_manager(BaseUID::from_hex(content.graph_uid));
    if(!data) {
        std::cerr << "Received graph update for unmanaged graph." << std::endl;
    } else {
        auto msg = std::make_shared<RequestWrapper>(content);
        data->queue.push(std::move(msg), true);
    }
}

void Butler::handle_incoming_update_tag_list(json & j) {
    OLD_STYLE_UpdateTagList content;
    content.graph_uid = j["graph_uid"].get<std::string>();
    content.tag_list = j["tag_list"].get<std::vector<std::string>>();

    auto data = find_graph_manager(BaseUID::from_hex(content.graph_uid));
    if(!data) {
        std::cerr << "Received updated tag list for unmanaged graph." << std::endl;
    } else {
        auto msg = std::make_shared<RequestWrapper>(content);
        data->queue.push(std::move(msg), true);
    }
}

void Butler::handle_incoming_merge_request(json & j) {
    // TODO: This should be spun off into a separate thread.

    std::optional<MergeRequest> content;

    std::string task_uid = j["task_uid"].get<std::string>();
    std::string target_guid = j["target_guid"].get<std::string>();
    int msg_version = 0;
    if(j.contains("msg_version"))
        msg_version = j["msg_version"].get<int>();
    int preferred_msg_version = 2;

    if(msg_version <= 0) {
            send_ZH_message({
                    {"msg_type", "merge_request_response"},
                    {"msg_version", 2},
                    {"task_uid", task_uid},
                    {"success", false},
                    {"reason", "Version too old"},
                });
            return;
    } else if(msg_version >= 1 && msg_version <= preferred_msg_version) {
        std::string payload_type = j["payload"]["type"].get<std::string>();
        if(payload_type == "delta") {
            content = MergeRequest{
                task_uid,
                target_guid,
                MergeRequest::PayloadGraphDelta{
                    j["payload"]["commands"].get<json>(),
                },
                msg_version
            };
        } else {
            send_ZH_message({
                    {"msg_type", "merge_request_response"},
                    {"msg_version", 2},
                    {"task_uid", task_uid},
                    {"success", false},
                    {"reason", "Don't understand payload type: '" + payload_type + "'"},
                });
            return;
        }
    } else {
        send_ZH_message({
                {"msg_type", "merge_request_response"},
                {"msg_version", preferred_msg_version},
                {"task_uid", task_uid},
                {"success", false},
                {"reason", "msg_version is too new for us"},
            });
        return;
    }

    auto data = find_graph_manager(BaseUID::from_hex(content->target_guid));
    if(!data) {
        std::cerr << "Received merge request for unmanaged graph." << std::endl;
        if(msg_version == 0) {
            send_ZH_message({
                    {"msg_type", "merge_request_response"},
                    {"task_uid", task_uid},
                    {"success", "0"},
                    {"reason", "Don't have target graph loaded"},
                    // {"indices", std::vector<blob_index>()},
                    {"indices", "[]"},
                    {"merged_tx_index", -1}
                });
        } else {
            send_ZH_message({
                    {"msg_type", "merge_request_response"},
                    {"msg_version", 1},
                    {"task_uid", task_uid},
                    {"success", false},
                    {"reason", "Don't have target graph loaded"},
                });
        }
    } else {
        auto msg = std::make_shared<RequestWrapper>(*content);
        data->queue.push(std::move(msg), true);
    }
}

bool is_allowed_chunk_msg(std::string msg_type) {
    if(msg_type == "graph_update")
        return true;
    if(msg_type == "full_graph")
        return true;
    return false;
}

void Butler::ack_failure(std::string task_uid, std::string reason) {
    if(zwitch.debug_zefhub_json_output())
        std::cerr << "Problem in chunked transfer: " << reason << std::endl;
    send_ZH_message({
            {"msg_type", "ACK"},
            {"task_uid", task_uid},
            {"success", false},
            {"reason", reason},
        });
}

void Butler::ack_success(std::string task_uid, std::string reason) {
    send_ZH_message({
            {"msg_type", "ACK"},
            {"task_uid", task_uid},
            {"success", true},
            {"reason", reason},
        });
}

void Butler::handle_incoming_chunked(json & j, std::vector<std::string> & rest) {
    // Note: making an assumption that the WS handler thread is the only thread
    // to ever touch this map.
    std::string chunk_type = j["chunk_type"];
    if(chunk_type == "new") {
        json msg = j["msg"];
        if(!is_allowed_chunk_msg(msg["msg_type"])) {
            ack_failure(j["task_uid"], "msg_type not allowed for chunked transfer");
            return;
        }

        BaseUID chunk_uid = BaseUID::from_hex(j["chunk_uid"]);
        std::vector<std::string> empty_rest(j["rest_sizes"].size());
        receiving_transfers.emplace(chunk_uid, ReceivingTransfer{chunk_uid, j["rest_sizes"], empty_rest, msg, now()});

        ack_success(j["task_uid"], "Started new chunk");
    } else if(chunk_type == "payload") {
        BaseUID chunk_uid = BaseUID::from_hex(j["chunk_uid"]);
        auto it = receiving_transfers.find(chunk_uid);
        if(it == receiving_transfers.end()) {
            ack_failure(j["task_uid"], "Don't know about chunk: " + to_str(chunk_uid));
            return;
        }

        auto & chunk = it->second;
        int rest_index = j["rest_index"];
        if(rest_index < 0 || rest_index > chunk.rest_sizes.size()) {
            ack_failure(j["task_uid"], "Invalid rest_index");
            return;
        }

        chunk.last_activity = now();
        ack_success(j["task_uid"], "Accepted chunk");

        chunk.buffer.emplace_back(j["bytes_start"].get<int>(), rest_index, rest[0]);

        // chunk.rest[rest_index] += rest[0];
        if(zwitch.developer_output())
            std::cerr << "Accepted a chunk" << std::endl;

        // Apply any buffered updates if we can
        while(true) {
            bool applied_one = false;
            for(auto it = chunk.buffer.begin(); it < chunk.buffer.end(); it++) {
                if(chunk.rest[it->rest_index].size() == it->bytes_start) {
                    chunk.rest[it->rest_index] += it->data;
                    if(zwitch.developer_output())
                        std::cerr << "Applied a buffered chunk start: " << it->bytes_start << " rest_index: " << it->rest_index << " size: " << it->data.size() << std::endl;
                    chunk.buffer.erase(it);
                    applied_one = true;
                    break;
                }

                if(chunk.rest[it->rest_index].size() > it->bytes_start) {
                    std::cerr << "MAJOR FAILURE OF CHUNKED TRANFSER: got chunk earlier than what we already know about." << std::endl;
                    std::cerr << "rest_index: " << it->rest_index << " cur_size: " << chunk.rest[it->rest_index].size() << " bytes_start: " << it->bytes_start << std::endl;
                    receiving_transfers.erase(chunk_uid);
                }
            }
            if(!applied_one)
                break;
        }

        // Check to see if we're finished.
        bool done = true;
        for(int index = 0; index < chunk.rest_sizes.size() ; index++) {
            if(chunk.rest[index].size() != chunk.rest_sizes[index])
                done = false;
        }

        if(done) {
            // As we are erasing, we need to copy it out first
            auto chunk_msg = chunk.msg;
            auto chunk_rest = chunk.rest;
            receiving_transfers.erase(chunk_uid);
            handle_incoming_message(chunk_msg, chunk_rest);
        }
    } else if(chunk_type == "cancel") {
        BaseUID chunk_uid = BaseUID::from_hex(j["chunk_uid"]);
        std::cerr << "Ignoring cancel chunk transfer request because this is not implemented yet." << std::endl;
    } else {
        ack_failure(j["task_uid"], "Don't know how to handle chunk type");
        return;
    }
}

void Butler::check_overdue_receiving_transfers() {
    // As this is called during the WS thread it needs to be handled quckly.

    std::vector<BaseUID> to_remove;
    for(auto & it : receiving_transfers) {
        auto & trans = it.second;

        if(trans.last_activity + chunk_timeout*seconds < now()) {
            to_remove.push_back(trans.uid);
            if(zwitch.developer_output()) {
                std::cerr << "Going to remove chunked transfer which has:" << std::endl;
                for(int ind = 0; ind < trans.rest_sizes.size(); ind++)
                    std::cerr << "rest_index: " << ind << " rest_size: " << trans.rest_sizes[ind] << " received up to " << trans.rest[ind].size() << std::endl;
                for(auto & it : trans.buffer)
                    std::cerr << "leftover accepted buffered chunk: rest_index: " << it.rest_index << " start: " << it.bytes_start << " size: " << it.data.size() << std::endl;
            }
        }
    }

    // std::cerr << "Incoming chunks: " << receiving_transfers.size() << ". Overdue: " << to_remove.size() << std::endl;

    for(auto & chunk_uid : to_remove) {
        if(zwitch.developer_output())
            std::cerr << "Removing a chunked transfer because it has passed its inactivity threshold" << std::endl;
        receiving_transfers.erase(chunk_uid);

        send_ZH_message({
                {"msg_type", "chunked"},
                {"chunk_uid", str(chunk_uid)},
                {"chunk_type", "cancel"}
            });
    }
}
