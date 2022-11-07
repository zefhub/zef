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
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, T & content, msg_ptr & msg) {
    throw std::runtime_error("Not implemented graph worker message");
}

// This is generic code for several functions.
void do_reconnect(Butler & butler, Butler::GraphTrackingData & me) {
    if(me.gd->currently_subscribed) {
        // std::cerr << "warning trying to reconnect when already connected... something is up" << std::endl;
        return;
    }

    if(!me.gd->should_sync)
        return;

    if(me.gd->sync_head == 0 && me.gd->is_primary_instance) {
        // We are set to sync, but we have never got around to letting zefhub know about our graph. So instead we'll wait for the sync thread to take care of it. We'll help it along by forcing a wake.
        wake(me.gd->heads_locker);

        if(zwitch.graph_event_output())
            std::cerr << "Waking sync thread to send full graph: " << std::endl;
        return;
    }

    if(zwitch.graph_event_output())
        std::cerr << "Resubscribing to graph: " << me.uid << std::endl;

    me.debug_last_action = "About to resubscribe";

    std::string working_layout = butler.upstream_layout();

    // Interesting point here - we could send out read_head, but
    // if we've written more to it then upstream would get
    // confused. But it is possible for sync_head to be 0 in
    // which case we'd get an "update" including the prefix back
    // from upstream.
    //
    // Going with sync_head for now. Can change the logic upstream.
    blob_index hash_to;
    uint64_t hash;
    UpdateHeads update_heads;
    {
        LockGraphData lock{me.gd};
        update_heads = client_create_update_heads(*me.gd);
        if(me.gd->sync_head == 0)
            hash_to = me.gd->read_head.load();
        else {
            hash_to = me.gd->sync_head.load();
            if(me.gd->read_head < hash_to)
                hash_to = me.gd->read_head;
        }

        hash = partial_hash(Graph(me.gd, false), hash_to, 0, working_layout);
    }
    json j = create_json_from_heads_from(update_heads);
    j["msg_type"] = "subscribe_to_graph";
    j["msg_version"] = 3;
    j["graph_uid"] = str(me.uid);
    j["hash"] = hash;
    j["hash_index"] = hash_to;

    auto response = butler.wait_on_zefhub_message<GenericZefHubResponse>(j);

    if(!response.generic.success) {
        butler.set_into_invalid_state(me);
        std::cerr << "UNKNOWN ERROR WHEN RESUBSCRIBING FOR GRAPH (" << me.uid << "): " << response.generic.reason << std::endl;
        std::cerr << "UNKNOWN ERROR WHEN RESUBSCRIBING FOR GRAPH (" << me.uid << "): " << response.generic.reason << std::endl;
        std::cerr << "UNKNOWN ERROR WHEN RESUBSCRIBING FOR GRAPH (" << me.uid << "): " << response.generic.reason << std::endl;
        std::cerr << "UNKNOWN ERROR WHEN RESUBSCRIBING FOR GRAPH (" << me.uid << "): " << response.generic.reason << std::endl;
        std::cerr << "UNKNOWN ERROR WHEN RESUBSCRIBING FOR GRAPH (" << me.uid << "): " << response.generic.reason << std::endl;
        throw std::runtime_error("Couldn't resubscribe");
        return;
    }

    int msg_version = response.j["msg_version"].get<int>();
    bool hash_agreed;
    if(msg_version <= 2)
        hash_agreed = true;
    // ZefHub was incorrectly just replying with the same version in response,
    // so we have to use other details in the response to validate this.
    else if(!response.j.contains("hash_agreed"))
        hash_agreed = true;
    else
        hash_agreed = response.j["hash_agreed"].get<bool>();
    if(!hash_agreed) {
        bool bad = true;
        if(response.j["hash_beyond_our_knowledge"].get<bool>()) {
            // We were ahead of upstream, see if we agree with what they had.
            auto our_hash = partial_hash(Graph(me.gd, false), response.j["hash_index"].get<blob_index>(), 0, working_layout);
            if(our_hash == response.j["hash"].get<uint64_t>()) {
                if(zwitch.graph_event_output())
                    std::cerr << "We were ahead of upstream but our hashes agree." << std::endl;
                bad = false;
            }
        }

        if(bad) {
            // TODO: Make sure we unsubscribe to the graph
            butler.set_into_invalid_state(me);
            std::cerr << "GRAPH (" << me.uid << ") DID NOT MATCH HASH WITH UPSTREAM WHEN RESUBSCRIBING: " << response.generic.reason << std::endl;
            std::cerr << "GRAPH (" << me.uid << ") DID NOT MATCH HASH WITH UPSTREAM WHEN RESUBSCRIBING: " << response.generic.reason << std::endl;
            std::cerr << "GRAPH (" << me.uid << ") DID NOT MATCH HASH WITH UPSTREAM WHEN RESUBSCRIBING: " << response.generic.reason << std::endl;
            std::cerr << "GRAPH (" << me.uid << ") DID NOT MATCH HASH WITH UPSTREAM WHEN RESUBSCRIBING: " << response.generic.reason << std::endl;
            throw std::runtime_error("Couldn't resubscribe");
            return;
        }
    }

    // Update heads from received message
    UpdateHeads received_heads = parse_message_update_heads(response.j);
    apply_sync_heads(*me.gd, received_heads);

    // Now we try and get the primary role back again, if we had it before.
    if(me.gd->is_primary_instance) {
        UpdateHeads our_heads;
        {
            LockGraphData lock{me.gd};
            our_heads = client_create_update_heads(*me.gd);
        }
        json j = create_json_from_heads_from(our_heads);
        j["msg_type"] = "make_primary";
        j["graph_uid"] = str(me.uid);
        j["take_on"] = true;
        auto response = butler.wait_on_zefhub_message(j);
        if(!response.generic.success) {
            std::cerr << "We were rejected when trying to reclaim transactor role: " << response.generic.reason << std::endl;

            if(me.gd->sync_head.load() == me.gd->write_head) {
                // We were up to date, so warn about this and demote our graph
                // ourselves.
                std::cerr << "We were unable to get back the primary role for (" << me.uid << ")! Downgrading our rights." << std::endl;
                me.gd->is_primary_instance = false;
                // TODO: Do a check to see whether we were ahead of upstream, in which case we should issue a bigger failure message.
            } else {
                std::cerr << "NEW DATA ON THE GRAPH WILL NOT MAKE IT TO ZEFHUB!!!!" << std::endl;
                std::cerr << "NEW DATA ON THE GRAPH WILL NOT MAKE IT TO ZEFHUB!!!!" << std::endl;
                std::cerr << "NEW DATA ON THE GRAPH WILL NOT MAKE IT TO ZEFHUB!!!!" << std::endl;
                std::cerr << "NEW DATA ON THE GRAPH WILL NOT MAKE IT TO ZEFHUB!!!!" << std::endl;
                std::cerr << "NEW DATA ON THE GRAPH WILL NOT MAKE IT TO ZEFHUB!!!!" << std::endl;
                std::cerr << "NEW DATA ON THE GRAPH WILL NOT MAKE IT TO ZEFHUB!!!!" << std::endl;
                std::cerr << "NEW DATA ON THE GRAPH WILL NOT MAKE IT TO ZEFHUB!!!!" << std::endl;
                throw std::runtime_error("Couldn't get primary role after resubscribing");
            }
        }
    }

    if(zwitch.graph_event_output())
    me.debug_last_action = "Resubscribed to zefhub";
    me.gd->currently_subscribed = true;

    // If we get here, then everything has gone well, but the sync thread may
    // not automatically wake up. So we trigger the heads_locker just in case.
    wake(me.gd->heads_locker);

    if(zwitch.graph_event_output()) {
        std::cerr << "Resubscribed to graph: " << str(me.uid) << std::endl;
        std::cerr << "Upstream/our head: " << me.gd->sync_head.load() << "/" << me.gd->read_head.load() << std::endl;
    }
}


void Butler::set_into_invalid_state(Butler::GraphTrackingData & me) {
    me.gd->error_state = GraphData::ErrorState::UNSPECIFIED_ERROR;
    if(me.gd->is_primary_instance && me.gd->should_sync) {
        std::cerr << "Giving up transactor role" << std::endl;
        me.queue.push(std::make_shared<RequestWrapper>(MakePrimary{Graph(*me.gd), false}), true);
    }
    // TODO: Maybe also unsubscribe here?
}


int resolve_memory_style(int mem_style, bool synced) {
    if(mem_style == MMap::MMAP_STYLE_AUTO) {
        char * var = std::getenv("ZEFDB_MEMORY_STYLE");
        if(var) {
            if(std::string(var) == "ANONYMOUS")
                return MMap::MMAP_STYLE_ANONYMOUS;
            else if(std::string(var) == "FILE_BACKED")
                return MMap::MMAP_STYLE_FILE_BACKED;
            else if(std::string(var) == "MALLOC")
                return MMap::MMAP_STYLE_MALLOC;
            else if(std::string(var) == "") {
                // Wait for default
            } else {
                std::cerr << "Don't understand ZEFDB_MEMORY_STYLE='" << var << "'. Using default" << std::endl;
            }
        }
        if(synced) {
            mem_style = MMap::MMAP_STYLE_FILE_BACKED;
        } else
            mem_style = MMap::MMAP_STYLE_ANONYMOUS;
    }
    if(mem_style == MMap::MMAP_STYLE_FILE_BACKED) {
        // TODO: Check if we have filesystem access here.
#ifdef ZEF_WIN32
        // Until support is there, need to force anonymous.
        mem_style = MMap::MMAP_STYLE_ANONYMOUS;
#endif
    }
    return mem_style;
}

void apply_update_with_caches(GraphData & gd, const UpdatePayload & payload_in, bool double_link, bool update_upstream) {
    LockGraphData lock{&gd};

    std::string working_layout = payload_in.j["data_layout_version"];
    UpdatePayload payload;
    if(working_layout == "0.2.0")
        payload = conversions::convert_payload_0_2_0_to_0_3_0(payload_in);
    else {
        force_assert(working_layout == "0.3.0");
        payload = payload_in;
    }

    UpdateHeads heads = parse_payload_update_heads(payload);

    if(!heads_apply(heads, gd))
        throw std::runtime_error("Heads of update don't fit onto graph.");

    const std::string & blob_bytes = payload.rest[0];
    size_t len = blob_bytes.length() / constants::blob_indx_step_in_bytes;
    if(len != heads.blobs.to - heads.blobs.from)
        throw std::runtime_error("Len of blob bytes doesn't match update heads: len=" + to_str(len) + ", blobs.to:" + heads.blobs.to + ", blobs.from:" + heads.blobs.from);


    // Check blob head aligns

    // Apply blob updates

    internals::include_new_blobs(gd, heads.blobs.from, heads.blobs.to, blob_bytes, double_link, false);
    update(gd.heads_locker, gd.latest_complete_tx, index(internals::get_latest_complete_tx_node(gd)));

    // Apply cache updates

    int indx = 1;
    for(auto & cache : heads.caches) {
#define GEN_CACHE(x, y) else if(cache.name == x) { \
            auto ptr = gd.y->get_writer(); \
            if(ptr->revision() != cache.revision || ptr->size() != cache.from)                \
                throw std::runtime_error("Update for cache '" + cache.name + "' doesn't fit - after we applied the blob updates."); \
            ptr->apply_diff(payload.rest[indx], ptr.ensure_func()); \
        } \

        if(false) {}
        GEN_CACHE("_ETs_used", ETs_used)
        GEN_CACHE("_RTs_used", RTs_used)
        GEN_CACHE("_ENs_used", ENs_used)
        GEN_CACHE("_uid_lookup", uid_lookup)
        GEN_CACHE("_euid_lookup", euid_lookup)
        GEN_CACHE("_tag_lookup", tag_lookup)
        GEN_CACHE("_av_hash_lookup", av_hash_lookup)
        else
            throw std::runtime_error("Unknown cache");
#undef GEN_CACHE

        indx++;
    }

    // This can be INCREDIBLY slow! This should be locked behind a zwitch if
    // used, as it can take forever to finish and locks the graph thread up.
    // if(!verification::verify_graph_double_linking(g))
    //     throw std::runtime_error("Bad double linking");

    // Note: we use payload_in here instead of payload as that may have clobbered the hash.
    if(payload_in.j.contains("hash_full_graph")){
        if(payload_in.j["hash_full_graph"].get<uint64_t>() != gd.hash(constants::ROOT_NODE_blob_index, gd.write_head, 0, working_layout)) {
            // This must be somewhere I can't find, but redoing here
            // blob_index indx = constants::ROOT_NODE_blob_index;
            // while(indx < gd.write_head) {
            //     EZefRef uzr{indx, gd};
            //     // visit([](auto & x) { std::cerr << x << std::endl; }, uzr);
            //     visit_blob([](auto & x) { manual_os_call(std::cerr, x) << std::endl; }, uzr);
            //     indx += num_blob_indexes_to_move(size_of_blob(uzr));
            // }

            throw std::runtime_error("Hashes disagree");
        }
        else
            if(zwitch.developer_output())
                std::cerr << "Full hash agreed for graph " << internals::get_graph_uid(gd) << std::endl;
    } else {
        if(zwitch.developer_output())
            std::cerr << "No full hash passed for graph update!" << internals::get_graph_uid(gd) << std::endl;
    }

    if(update_upstream) {
        apply_sync_heads(gd, heads);
    }

    if(zwitch.graph_event_output()) {
        std::cerr << "Update graph " << internals::get_graph_uid(gd) << " up to blob " << heads.blobs.to << ": \033[32m" << "success" << "\033[39m" << std::endl;
    }
}

template <>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, NewGraph & content, Butler::msg_ptr & msg) {
    try {
        if(me.gd != nullptr)
            throw std::runtime_error("Shouldn't have a GraphData already!");

        int mem_style = resolve_memory_style(content.mem_style, false);

        MMap::FileGraph * fg = nullptr;
        if(mem_style == MMap::MMAP_STYLE_FILE_BACKED) {
            auto fg_prefix = file_graph_prefix(me.uid, upstream_name());
            bool force_fresh = false;
            if(MMap::filegraph_exists(fg_prefix)) {
                if(content.payload)
                    force_fresh = true;
                else 
                    throw std::runtime_error("Filegraph (" + str(me.uid) + ") already exists (@ " + fg_prefix.string() + ") but we're trying to create a new graph!"); 
            }
            try {
                fg = new MMap::FileGraph(fg_prefix, me.uid, true, force_fresh);
            } catch(const MMap::FileAlreadyLocked & e) {
                std::cerr << "What an error in locking a brand new file. Why? Falling back to anonymous mmap for now. " << e.what() << std::endl;
                mem_style = MMap::MMAP_STYLE_ANONYMOUS;
                fg = nullptr;
            }
        }

        // The mmap steals the file graph ptr
        me.gd = create_GraphData(mem_style, fg, me.uid, !(content.payload || content.internal_use_only));
        // Grab a reference while we are manipulating things in here
        Graph _g{me.gd, false};

        // me.gd->is_primary_instance = !content.payload;
        me.gd->is_primary_instance = true;
        me.gd->should_sync = false;

        if(content.payload) {
            apply_update_with_caches(*me.gd, *content.payload, false, false);
            me.gd->manager_tx_head = me.gd->latest_complete_tx.load();
            // Note: don't set sync_head here, it should remain at 0.
        }

        if(!content.internal_use_only) {
            // Now we can kick off the sync thread, even if we aren't syncing just at the moment.
            spawn_graph_sync_thread(me);
        }

        msg->promise.set_value(GraphLoaded(_g));
    } catch (const std::runtime_error & e) {
        std::cerr << "Exception in NewGraph, going to cleanup graph manager thread. (" << e.what() << ")" << std::endl;
        me.please_stop = true;
        throw;
    }
}

template <>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, LoadGraph & content, Butler::msg_ptr & msg) {
    if(is_BaseUID(content.tag_or_uid) && str(me.uid) != content.tag_or_uid)
        throw std::runtime_error("Shouldn't get here with wrong uid: '" + str(me.uid) + "' - '" + content.tag_or_uid + "'");

    if(content.callback)
        (*content.callback)("GRAPH UID:" + str(me.uid));

    if(me.gd != nullptr) {
        if(content.callback)
            (*content.callback)("Graph " + str(me.uid) + " already present in butler");
        msg->promise.set_value(GraphLoaded(Graph{me.gd, false}));
        return;
    }

    // If something throws (like the filegraph above) need to clear this graph manager out from the list etc...
    try {
        /////////////////////////////////////////////////////////////////////
        // TODO: In the future this will be a general subscribe, when we'll get the info how to load the graph, or if we should manage it ourselves.

        // For now, pretend we've been told to manage it ourselves.
        // TODO: In the future, we'll get the file location from upstream. For now, we look it up.
        int mem_style = resolve_memory_style(content.mem_style, true);

        MMap::FileGraph * fg = nullptr;
        bool existed = false;
        if(mem_style == MMap::MMAP_STYLE_FILE_BACKED) {
            auto fg_prefix = file_graph_prefix(me.uid, upstream_name());
            existed = MMap::filegraph_exists(fg_prefix);
            try {
                fg = new MMap::FileGraph(fg_prefix, me.uid, true, false);
                if(fg->get_latest_blob_index() == 0)
                    existed = false;
            } catch(const MMap::FileAlreadyLocked & e) {
                // This is a little dodgy handling, while we can't allow 2
                // file backed graphs at the same time from different
                // processes.
                if(zwitch.graph_event_output())
                    std::cerr << "Couldn't acquire exclusive lock for file graph (" << fg_prefix << "), falling back to anonymous mmap." << std::endl;
                mem_style = MMap::MMAP_STYLE_ANONYMOUS;
                existed = false;
            }
        }

        // The mmap steals the file graph ptr
        me.gd = create_GraphData(mem_style, fg, me.uid, false);
        // Lock this down for safety while we're in here.
        Graph _g{me.gd, false};

        me.gd->is_primary_instance = false;
        // This shouldn't ever be needed.
        // me.gd->managing_thread_id = std::this_thread::get_id();

        wait_for_auth();

        // This is where all of the user-requested
        // options (like primary role) should come into
        // play.
        std::string working_layout = upstream_layout();

        if (existed) {
            if(content.callback)
                (*content.callback)("Loading from existing FileGraph");
            // TODO: Need to send hash along to confirm we're right if we've
            // actually got the latest
            // TODO: It is possible that we could change
            // this out for a call to do_reconnect, but there is some custom
            // logic to handle in here, with regards to safety checks on the
            // graph.
            me.gd->manager_tx_head = me.gd->latest_complete_tx.load();

            if(zwitch.developer_output()) {
                std::cerr << "Loaded GUID: " << me.uid << " has heads of read:tx = " << me.gd->read_head.load() << ":" << me.gd->latest_complete_tx.load() << std::endl;
            }

            GenericZefHubResponse response;

            while(true) {
                json j{
                        {"msg_type", "subscribe_to_graph"},
                        {"msg_version", 3},
                        {"graph_uid", str(me.uid)},
                };
                parse_filegraph_update_heads(*fg, j, working_layout);

                j["hash"] = partial_hash(Graph(me.gd, false), j["blobs_head"], 0, working_layout);
                j["hash_index"] = j["blobs_head"];
                response = wait_on_zefhub_message(j);

                if(response.generic.success)
                    break;

                if(response.generic.reason == "Already subscribed") {
                    std::cerr << "Upstream told us that we were already subscribed to the graph. Going to send an unsubscribe and try again." << std::endl;
                    auto unsub_response = wait_on_zefhub_message({
                            {"msg_type", "unsubscribe_from_graph"},
                            {"graph_uid", str(me.uid)},
                        });
                    if(!unsub_response.generic.success)
                        throw std::runtime_error("There was a problem in unsubscribing for an already subscribed graph: " + unsub_response.generic.reason);

                    continue;
                } else {
                    msg->promise.set_value(GraphLoaded(response.generic.reason));
                    me.please_stop = true;
                    return;
                }

                throw std::runtime_error("Unreachable code");
            }

            int msg_version = 0;
            if(response.j.contains("msg_version"))
                msg_version = response.j["msg_version"].get<int>();

            bool hash_agreed;
            if(msg_version <= 2)
                hash_agreed = true;
            // ZefHub was incorrectly just replying with the same version in response,
            // so we have to use other details in the response to validate this.
            else if(!response.j.contains("hash_agreed"))
                hash_agreed = true;
            else
                hash_agreed = response.j["hash_agreed"].get<bool>();
            if(!hash_agreed) {
                bool bad = true;
                if(response.j["hash_beyond_our_knowledge"].get<bool>()) {
                    // We were ahead of upstream, see if we agree with what they had.
                    auto our_hash = partial_hash(Graph(me.gd, false), response.j["hash_index"].get<blob_index>(), 0, working_layout);
                    if(our_hash == response.j["hash"].get<uint64_t>()) {
                        developer_output("We were ahead of upstream but our hashes agree.");
                        bad = false;
                    }
                }

                if(bad) {
                    // TODO: This could lose data if we had stuff that
                    // upstream didn't have. Handle this carefully.
                    std::cerr << "Warning hashes disagreed with upstream when loading graph. Going to wipe graph and try again from fresh." << std::endl;
                    std::filesystem::path path_prefix = fg->path_prefix;
                    // This is a little annoying - it's the only place that the
                    // GraphData killing code is reused. I hope I can get around
                    // this some other time.
                    MMap::destroy_mmap((void*)me.gd);
                    MMap::delete_filegraph_files(path_prefix);
                    // Note: the fg pointer was stolen by the mmap alloc info,
                    // so no `delete fg` here.
                    fg = new MMap::FileGraph(path_prefix, me.uid, true, false);
                    me.gd = create_GraphData(mem_style, fg, me.uid, false);
                    _g = Graph{me.gd, false};
                    me.gd->is_primary_instance = false;

                    // We need to inform upstream that we had to reset.
                    if(zefdb_protocol_version >= 7) {
                        json j{
                            {"msg_type", "graph_heads"},
                            {"msg_version", 1},
                            {"graph_uid", str(me.uid)},
                        };
                        parse_filegraph_update_heads(*fg, j, working_layout);
                        j["hash"] = partial_hash(Graph(me.gd, false), j["blobs_head"], 0, working_layout);
                        j["hash_index"] = j["blobs_head"];
                        auto response = wait_on_zefhub_message(j);
                        if(!response.generic.success) {
                            developer_output("Problem letting ZefHub know that we reset our heads.");
                            msg->promise.set_value(GraphLoaded(response.generic.reason));
                            me.please_stop = true;
                            return;
                        }

                        if(!response.j["hash_agreed"]) {
                            std::cerr << "A weird problem is happening with our resubscribe. We had to let upstream know about our actual graph heads, but even then hashes didn't agree. What is going on??" << std::endl;

                            std::cerr << "This will be handled better in the future, but for now we are going to abort this resubscribe. Delete your cached graph manually if you cannot resolve this problem another way." << std::endl;
                            msg->promise.set_value(GraphLoaded("Hashes disagreed after letting upstream know about our latest heads."));
                            me.please_stop = true;
                            return;
                        }
                    }
                }
            }

            me.gd->should_sync = true;
            me.gd->currently_subscribed = true;

            if(response.j.contains("tag_list"))
                me.gd->tag_list = response.j["tag_list"].get<std::vector<std::string>>();

            LockGraphData gd_lock{me.gd};
            UpdateHeads heads = parse_message_update_heads(response.j);
            apply_sync_heads(*me.gd, heads);
            developer_output("Upstream head: " + to_str(me.gd->sync_head.load()) + "/" + to_str(me.gd->read_head.load()));

            if(me.gd->sync_head < me.gd->read_head.load()) {
                std::cerr << "WARNING:" << std::endl;
                std::cerr << "WARNING:" << std::endl;
                std::cerr << "WARNING: loaded graph is ahead of upstream. You should take the transactor role to update upstream as soon as possible." << std::endl;
                std::cerr << "WARNING:" << std::endl;
                std::cerr << "WARNING:" << std::endl;
            }
        } else {
            if(content.callback)
                (*content.callback)("Requesing full graph of " + str(me.uid) + " from upstream");

            GenericZefHubResponse response;

            while(true) {
                response = wait_on_zefhub_message({
                        {"msg_type", "subscribe_to_graph"},
                        {"graph_uid_or_tag", str(me.uid)},
                    },
                    {},
                    zefhub_generic_timeout,
                    false,
                    false,
                    content.callback
                    );

                if(response.generic.success)
                    break;

                if(response.generic.reason == "Already subscribed") {
                    std::cerr << "Upstream told us that we were already subscribed to the graph. Going to send an unsubscribe and try again." << std::endl;
                    auto unsub_response = wait_on_zefhub_message({
                            {"msg_type", "unsubscribe_from_graph"},
                            {"graph_uid", str(me.uid)},
                        });
                    if(!unsub_response.generic.success)
                        throw std::runtime_error("There was a problem in unsubscribing for an already subscribed graph.");

                    continue;
                } else {
                    msg->promise.set_value(GraphLoaded(response.generic.reason));
                    me.please_stop = true;
                    return;
                }

                throw std::runtime_error("Unreachable code");
            }

            if(str(me.uid) != response.j["graph_uid"].get<std::string>())
                throw std::runtime_error("UIDs don't match up");

            // size_t len = response.rest[0].size();

            // // This has been copied from Graph(...) but it will probably disappear in the future anyway.
            // blob_index index_lo = constants::ROOT_NODE_blob_index;
            // blob_index index_hi = constants::ROOT_NODE_blob_index + len/constants::blob_indx_step_in_bytes;
            // internals::include_new_blobs(*me.gd, index_lo, index_hi, response.rest[0], false, false);
            // me.gd->read_head = index_hi;
            // me.gd->write_head = index_hi;
            // me.gd->sync_head = index_hi;
            // me.gd->latest_complete_tx = index(internals::get_latest_complete_tx_node(*me.gd));

            LockGraphData gd_lock{me.gd};
            UpdatePayload payload{response.j, response.rest};
            apply_update_with_caches(*me.gd, payload, false, true);

            me.gd->manager_tx_head = me.gd->latest_complete_tx.load();
            // internals::apply_actions_to_blob_range(*me.gd, index_lo, index_hi, true, false);
            me.gd->should_sync = true;
            me.gd->currently_subscribed = true;

            if(response.j.contains("tag_list"))
                me.gd->tag_list = response.j["tag_list"].get<std::vector<std::string>>();

            if(response.j.contains("upstream_head"))
                if(me.gd->read_head != response.j["upstream_head"].get<blob_index>())
                    throw std::runtime_error("The read head (" + to_str(me.gd->read_head) + ") doesn't agree with the upstream_head (" + to_str(response.j["upstream_head"].get<blob_index>()) + ") passed in.");
        }

        if(zwitch.developer_output())
            std::cerr << "Graph received up to " << me.gd->read_head.load() << std::endl;

        // Check the used tokens in case we need to get a whole batch at once.
        {
            auto ptr = me.gd->ETs_used->get();
            msg_push(TokenQuery{TokenQuery::ET, {}, ptr->as_vector(), false, true}, false, true);
        }
        {
            auto ptr = me.gd->RTs_used->get();
            msg_push(TokenQuery{TokenQuery::RT, {}, ptr->as_vector(), false, true}, false, true);
        }
        {
            auto ptr = me.gd->ENs_used->get();
            msg_push(TokenQuery{TokenQuery::EN, {}, ptr->as_vector(), false, true}, false, true);
        }

        // Now we can kick off the sync thread.
        spawn_graph_sync_thread(me);

        if(content.callback)
            (*content.callback)("Finished loading graph");

        msg->promise.set_value(GraphLoaded(_g));
    } catch (const std::runtime_error & e) {
        std::cerr << "Exception in LoadGraph, going to cleanup graph manager thread. (" << e.what() << ")" << std::endl;
        me.please_stop = true;
        throw;
    }
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, LocalGraph & content, Butler::msg_ptr & msg) {
    try {
        if(me.gd != nullptr) {
            // Treat this like a graph load.
            if(content.new_graph)
                throw std::runtime_error("Shouldn't have a GraphData already! (LocalGraph)");

            msg->promise.set_value(GraphLoaded(Graph(*me.gd)));
            return;
        }

        // Create a new file graph at the location, rather than in the usual place
        me.gd = create_GraphData(MMap::MMAP_STYLE_ANONYMOUS, nullptr, me.uid, content.new_graph);
        // Lock this down for safety while we're in here.
        Graph _g{me.gd, false};

        if(content.new_graph) {
            if(std::filesystem::exists(content.path))
                throw std::runtime_error("Local file (" + content.path.string() + ") already exists"); 
            if(content.path.has_parent_path())
                std::filesystem::create_directories(content.path.parent_path());
        } else {
            // We load the graph from disk and turn it into the same format as
            // if we received a graph from upstream.
            auto payload = internals::payload_from_local_file(content.path);

            LockGraphData gd_lock{me.gd};
            apply_update_with_caches(*me.gd, payload, false, true);

            me.gd->manager_tx_head = me.gd->latest_complete_tx.load();
            me.gd->sync_head = me.gd->read_head.load();
        }

        if(internals::get_graph_uid(*me.gd) != me.uid)
            throw std::runtime_error("Local graph UID differed from what was passed - weird internal inconsistency.");

        me.gd->is_primary_instance = true;
        me.gd->should_sync = false;
        me.gd->local_path = std::filesystem::absolute(content.path);

        if(content.new_graph) {
            save_local(*me.gd);
        }

        // Check the used tokens in case we need to get a whole batch at once.
        {
            auto ptr = me.gd->ETs_used->get();
            msg_push(TokenQuery{TokenQuery::ET, {}, ptr->as_vector(), false, true}, false, true);
        }
        {
            auto ptr = me.gd->RTs_used->get();
            msg_push(TokenQuery{TokenQuery::RT, {}, ptr->as_vector(), false, true}, false, true);
        }
        {
            auto ptr = me.gd->ENs_used->get();
            msg_push(TokenQuery{TokenQuery::EN, {}, ptr->as_vector(), false, true}, false, true);
        }

        // Now we can kick off the sync thread, even if we aren't syncing just at the moment.
        spawn_graph_sync_thread(me);

        msg->promise.set_value(GraphLoaded(Graph(*me.gd)));
    } catch (const std::runtime_error & e) {
        std::cerr << "Exception in LocalGraph, going to cleanup graph manager thread. (" << e.what() << ")" << std::endl;
        me.please_stop = true;
        throw;
    }
}


template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, DoneWithGraph & content, Butler::msg_ptr & msg) {
    // std::cerr << "In handle DoneWithGraph" << std::endl;
    if(me.gd->reference_count == 0) {
        me.please_stop = true;
        if(zwitch.graph_event_output())
            std::cerr << "Closing graph " << str(me.uid) << std::endl;
    }
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, LoadPage & content, Butler::msg_ptr & msg) {
    // TODO: This duplicates some things from ensure_or_alloc_range
    // Probably good idea to factor out if possible.

    try {
        auto & info = MMap::info_from_blob(content.ptr);
        size_t page_ind_low = MMap::ptr_to_page_ind(content.ptr);
        size_t page_ind_high = MMap::ptr_to_page_ind((char*)content.ptr+(content.size-1));

        if(butler_is_master) {
            std::cerr << "LoadPage problem details: " << std::endl;
            std::cerr << *me.gd << std::endl;
            std::cerr << "pages: " << page_ind_low << " to " << page_ind_high << std::endl;
            std::cerr << "Start blob index: " << (((char*)content.ptr) - ((char*)me.gd)) / constants::blob_indx_step_in_bytes << std::endl;
            std::cerr << "Content size: " << content.size << std::endl;
            // throw std::runtime_error("Shouldn't be sending a LoadPage request when we are the master.");
            std::cerr << "Shouldn't be sending a LoadPage request when we are the master." << std::endl;
            std::cerr << "Byte at this location: " << *(char*)content.ptr << std::endl;
            std::cerr << "Pointer offset from blob boundary: " << (((char*)content.ptr) - ((char*)me.gd)) % constants::blob_indx_step_in_bytes << std::endl;
        }

        for (auto page_ind = page_ind_low ; page_ind <= page_ind_high ; page_ind++) {
            if(!MMap::is_page_alloced(info, page_ind)) {
                bool need_upstream = false;
                if(info.style == MMap::MMAP_STYLE_FILE_BACKED) {
                    if(!info.file_graph->is_page_in_file(page_ind)) {
                        need_upstream = true;
                    } else {
                        std::cerr << "Doing a fake lazy load of page " << page_ind << std::endl;
                        ensure_page(info, page_ind);
                    }
                } else {
                    need_upstream = true;
                }
                if(need_upstream) {

                    if(true) {
                        std::cerr << "WARNING DOING DODGY FAKE ENSURE PAGE WHEN WE SHOULDN'T BE" << std::endl;
                        std::cerr << "WARNING DOING DODGY FAKE ENSURE PAGE WHEN WE SHOULDN'T BE" << std::endl;
                        std::cerr << "WARNING DOING DODGY FAKE ENSURE PAGE WHEN WE SHOULDN'T BE" << std::endl;
                        std::cerr << "WARNING DOING DODGY FAKE ENSURE PAGE WHEN WE SHOULDN'T BE" << std::endl;
                        std::cerr << "WARNING DOING DODGY FAKE ENSURE PAGE WHEN WE SHOULDN'T BE" << std::endl;
                        std::cerr << "WARNING DOING DODGY FAKE ENSURE PAGE WHEN WE SHOULDN'T BE" << std::endl;

                        print_backtrace_force();

                        ensure_page(info, page_ind);

                    } else {
                        // TODO: In the future, look upstream for the page.
                        // This will require a task, plus a possible way to
                        // timeout tasks. Note: the task should send out a
                        // request for all pages required, not just page_ind
                        // here. So maybe it is better to push to a vector
                        // here and handle the request at the end of this
                        // function.

                        // TODO: Implement a polling wait to timeout tasks.

                        // network.wait_for_connected(constants::zefhub_subscribe_to_graph_timeout_default);
                        // wait_for_auth(constants::zefhub_subscribe_to_graph_timeout_default);
                        wait_for_auth();
                        if(!network.connected)
                            throw std::runtime_error("Network did not connect in time.");

                        throw std::runtime_error("Don't know how to ask zefhub for new pages just yet!");
                    }
                }
            }
        }
        msg->promise.set_value(GenericResponse{true});
    } catch(...) {
        msg->promise.set_exception(std::current_exception());
    }
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, NotifySync & content, Butler::msg_ptr & msg) {
    if(me.gd->error_state != GraphData::ErrorState::OK) {
        msg->promise.set_value(GenericResponse{false, "Graph is in error state"});
        return;
    }

    if(me.gd->local_path != "") {
        msg->promise.set_value(GenericResponse{false, "Can't sync local graphs without giving up consistency"});
        return;
    }

    if(butler_is_master) {
        update(me.gd->heads_locker, me.gd->should_sync, content.sync);
        msg->promise.set_value(GenericResponse(true));
        return;
    }

    // TODO: If graph is local, treat this as a no-op? Probably better to leave
    // "convert to zefhub graph" as a more explicit statement.

    if(!want_upstream_connection()) {
        if(have_auth_credentials()) {
            msg->promise.set_value(GenericResponse{false, "Can't sync when we aren't connected to upstream."});
        } else {
            msg->promise.set_value(GenericResponse{false, "Can't sync without a login. Please run `login | run` to login to ZefHub first."});
        }
        return;
    }

    
    if(content.sync) {
        // Try and send out an update. We add in a wait on the
        // network, as the user calling sync() will expect us to
        // make more of an effort to get any updates to upstream.
        // network.wait_for_connected(constants::zefhub_reconnect_timeout);
        wait_for_auth(constants::zefhub_reconnect_timeout);
        if(upstream_layout() == "0.2.0") {
            // Only allow sync to turn on if we can convert the layout for this graph. (it is compatible)
            char * blobs_ptr = (char*)(me.gd) + constants::ROOT_NODE_blob_index * constants::blob_indx_step_in_bytes;
            char * end = (char*)(me.gd) + me.gd->read_head.load() * constants::blob_indx_step_in_bytes;
            size_t len = end - blobs_ptr;
            if(!conversions::can_convert_0_3_0_to_0_2_0(blobs_ptr, len)) {
                msg->promise.set_value(GenericResponse{false, "Can't sync a graph which is not comptabile with 0.2.0 data layout"});
                return;
            }
        }
    }

    update(me.gd->heads_locker, me.gd->should_sync, content.sync);

    // Note: if the graph was already set to sync, the manager should be
    // trying to update upstream after every TX close. If the client
    // waited after every TX then the graph would always be in sync.
    // However, this message can be used by the client thread to wait
    // for (force?) a sync to finish after it asynchronously closed a
    // TX.

    if(me.gd->is_primary_instance) {
        if (content.sync) {
            // wait_for_auth();
            if(!network.connected) {
                msg->promise.set_value(GenericResponse{false, "Network did not reconnect in time."});
                return;
            }
            // We try and do a force update here, even if the sync worker would
            // normally handle this, just so we can guarantee to the caller.
            // This occurs via us triggering the sync thread.
            blob_index sync_to = me.gd->read_head;
            wake(me.gd->heads_locker);
            // bool reached_sync = wait_pred(
            //     me.gd->heads_locker,
            //     [&]() { return me.gd->sync_head >= sync_to; },
            //     std::chrono::duration<double>(butler_generic_timeout.value));
            // if(!reached_sync) { 
            //     msg->promise.set_value(GenericResponse{false, "Timed out waiting for sync."});
            //     return;
            // }
            // We use poll here to bail out when network disconnection happens.
            // It would be preferable to listen to two CVs simultaneously, but
            // that is not natively supported. Maybe a rewrite of the locks
            // structure would help here.
            wait_pred_poll(me.gd->heads_locker,
                      [&]() {
                          return me.gd->sync_head >= sync_to || !network.connected || me.gd->error_state != GraphData::ErrorState::OK; }, std::chrono::seconds(1));

            if(!network.connected) {
                msg->promise.set_value(GenericResponse{false, "Lost network connection while trying to sync."});
                return;
            }
                
            if(!me.gd->in_sync()) {
                if(me.gd->error_state != GraphData::ErrorState::OK) {
                    msg->promise.set_value(GenericResponse{false, "Graph is in invalid state"});
                    return;
                }
                // The only reason we get here should be because another
                // thread is writing to the graph and the read/write
                // heads are out of sync.
                msg->promise.set_value(GenericResponse{false, "Read and write heads are out of sync - another thread is writing to the graph?"});
                return;
            }
        }
    }
    msg->promise.set_value(GenericResponse{true});
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, SetKeepAlive & content, Butler::msg_ptr & msg) {
    if(me.gd->error_state != GraphData::ErrorState::OK) {
        msg->promise.set_value(GenericResponse{false, "Graph is in error state"});
        return;
    }

    if(content.value) {
        me.keep_alive_g = Graph(*me.gd);
    } else {
        me.keep_alive_g.reset();
    }
    msg->promise.set_value(GenericResponse{true});
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, ForceUnsubscribe & content, Butler::msg_ptr & msg) {
    if(me.gd->error_state != GraphData::ErrorState::OK) {
        msg->promise.set_value(GenericResponse{false, "Graph is in error state"});
        return;
    }

    me.gd->currently_subscribed = false;

    if(!me.gd->should_sync) {
        std::cerr << "We didn't want to be subscribed anyway." << std::endl;
        return;
    }
    
    do_reconnect(*this, me);
    
    msg->promise.set_value(GenericResponse(true));
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, GraphUpdate & content, Butler::msg_ptr & msg) {
    if(me.gd->error_state != GraphData::ErrorState::OK) {
        msg->promise.set_value(GenericResponse{false, "Graph is in error state"});
        return;
    }
    if(me.gd->is_primary_instance)
        throw std::runtime_error("Shouldn't be receiving updates if we are the primary role!");

    // We first check that this update applies. If it doesn't then send a heads
    // update to zefhub.
    UpdateHeads heads = parse_payload_update_heads(content.payload);

    if(!heads_apply(heads, *me.gd)) {
        if(zefdb_protocol_version < 7) {
            // Error out
            throw std::runtime_error("Heads of update don't fit onto graph.");
        } else {
            if(zwitch.graph_event_output())
                std::cerr << "Warning: got an update we couldn't apply. Letting upstream know what our heads are." << std::endl;
            std::string working_layout = upstream_layout();
            UpdateHeads our_heads;
            {
                LockGraphData lock{me.gd};
                our_heads = client_create_update_heads(*me.gd);
            }
            json j = create_json_from_heads_latest(our_heads);
            j["msg_type"] = "graph_heads";
            j["msg_version"] = 1;
            j["graph_uid"] = str(me.uid);
            j["hash"] = partial_hash(Graph(me.gd, false), j["blobs_head"], 0, working_layout);
            j["hash_index"] = j["blobs_head"];
            auto response = wait_on_zefhub_message(j);
            if(!response.generic.success)
                throw std::runtime_error("Problem letting ZefHub know our latest heads.");
            return;
        }
    }

    // Everything is good - continue
    apply_update_with_caches(*me.gd, content.payload, true, true);

    // TODO: In the future, we need to acknowledge that we have applied this update successfully.
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, OLD_STYLE_UpdateTagList & content, Butler::msg_ptr & msg) {
    me.gd->tag_list = content.tag_list;
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, MergeRequest & content, Butler::msg_ptr & msg) {
    if(me.gd->error_state != GraphData::ErrorState::OK) {
        msg->promise.set_value(GenericResponse{false, "Graph is in error state"});
        return;
    }
    // This is the handling for actually doing a merge request. We should have primary role on the target graph here.

    // There are two error pathways here. Either we respond to a client thread
    // in the msg promise, or we respond to upstream using a task_uid. Both are
    // checked in the err_reply function below.

    auto err_reply = [&](std::string text) {
        std::cerr << "Merge request trying to send out error: " << text << std::endl;
        if(content.task_uid) {
            if(content.msg_version <= 0) {
                send_ZH_message({
                        {"msg_type", "merge_request_response"},
                        {"task_uid", *content.task_uid},
                        {"success", "0"},
                        {"reason", text},
                        {"indices", "[]"},
                        {"merged_tx_index", -1}
                    });
            } else {
                send_ZH_message({
                        {"msg_type", "merge_request_response"},
                        {"task_uid", *content.task_uid},
                        {"msg_version", 1},
                        {"success", false},
                        {"reason", text},
                    });
            }
        }
        msg->promise.set_value(MergeRequestResponse{text});
    };

    if(str(me.uid) != content.target_guid) {
        err_reply("We aren't the target graph! What is going on???");
        return;
    }

    if(!me.gd->is_primary_instance) {
        err_reply("Don't have primary role on target graph");
        return;
    }

    if(zwitch.graph_event_output())
        std::cerr << "Handling merge request for graph: " << me.uid << std::endl;

    if(content.msg_version == 1)
        err_reply("Disallowing older versions to merge into us.");

    Graph target_g(*me.gd);

    // Do different things based on the payload.
    std::visit(overloaded {
            [&](MergeRequest::PayloadGraphDelta & payload) {
                try {
                    // py::gil_scoped_acquire acquire;
                    // auto pymerge = py::module_::import("zef.core.internals.merges").attr("_graphdelta_merge");
                    // json receipt = pymerge(Graph(*me.gd), payload.delta);
                    json receipt = internals::pass_to_merge_handler(Graph(*me.gd), payload.commands);

                    if(content.task_uid) {
                        if(content.msg_version <= 0) {
                            throw std::runtime_error("Shouldn't get here - msg_version is zero");
                        } else {
                            send_ZH_message({
                                {"msg_type", "merge_request_response"},
                                {"msg_version", 1},
                                {"task_uid", *content.task_uid},
                                {"success", true},
                                {"reason", "merged"},
                                {"receipt", {
                                        {"type", "delta"},
                                        {"receipt", receipt},
                                        {"read_head", me.gd->read_head.load()}
                                    }}
                            });
                        }
                    }

                        msg->promise.set_value(MergeRequestResponse{
                                true,
                                MergeRequestResponse::ReceiptGraphDelta{receipt}
                            });
                } catch(const std::exception & e) {
                    err_reply(std::string("Unable to merge: ") + e.what());
                }
            }
        }, content.payload);
}

template<>
MergeRequestResponse Butler::parse_ws_response<MergeRequestResponse>(json j) {
    GenericResponse generic = generic_from_json(j);
    int msg_version = 0;
    if(j.contains("msg_version"))
        msg_version = j["msg_version"].get<int>();

    if(!generic.success)
        return MergeRequestResponse{generic};

    if(msg_version <= 0) {
        throw std::runtime_error("Shouldn't get old msg_version==0 resposne from merge request anymore.");
    } else {
        std::string receipt_type = j["receipt"]["type"].get<std::string>();
        if(receipt_type == "indices") {
            throw std::runtime_error("Shouldn't get old merge request indices resposne anymore.");
        } else if(receipt_type == "delta") {
            MergeRequestResponse::ReceiptGraphDelta receipt{
                j["receipt"]["receipt"].get<json>(),
                j["receipt"]["read_head"].get<blob_index>()
            };
            return MergeRequestResponse{generic, receipt};
        } else {
            throw std::runtime_error("Not implemented message receipt type: " + receipt_type);
        }
    }
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, NewTransactionCreated & content, Butler::msg_ptr & msg) {
    // Run the subscriptions in a separate thread.
    // if(me.gd->open_tx_thread != std::this_thread::get_id()) {
    //     throw std::runtime_error("Shouldn't get to NewTX in manager without having control over the graph!"); 
    // }

    // We should be in control of txs on the graph. Run subscriptions while we can
    // std::cerr << "**** F" << std::endl;
    // bool waiting = run_subscriptions(me, EZefRef(content.index, *me.gd), std::move(msg->promise));
    // internals::execute_queued_fcts(gd);

    // At this point, either the subscription execution thread is in control, or
    // there was nothing to do, allowing us to send out updates.

    // // Trying to sync here could be bad news if we block the graph manager
    // // thread while still executing. Use the indicator of the controlling thread
    // // to decide whether to wait or not. If the subscription executor is in
    // // control, it must be because a new tx will be created.
    // // if(me.gd->open_tx_thread == std::this_thread::get_id()) {
    // if(!waiting) {
    //     RAII_CallAtEnd{[&]() { update(me.gd->open_tx_thread_locker, me.gd->open_tx_thread, std::thread::id()); }};
    //     if(me.gd->should_sync) {
    //         // Skim through transactions that refer to old things.
    //         if(content.index > me.gd->sync_head)
    //             send_update(me);
    //     }
    // }

    // Nothing to do here!
    msg->promise.set_value(GenericResponse(true));
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, Reconnected & content, Butler::msg_ptr & msg) {
    if (!me.gd->should_sync) {
        msg->promise.set_value(GenericResponse(true));
        return;
    }
    
    do_reconnect(*this, me);

    msg->promise.set_value(GenericResponse(true));
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, Disconnected & content, Butler::msg_ptr & msg) {
    me.gd->currently_subscribed = false;

    msg->promise.set_value(GenericResponse(true));
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, MakePrimary & content, Butler::msg_ptr & msg) {
    if(me.gd->error_state != GraphData::ErrorState::OK && content.make_primary) {
        msg->promise.set_value(GenericResponse{false, "Graph is in error state"});
        return;
    }

    if(content.make_primary == me.gd->is_primary_instance) {
        // Nothing to do!
        msg->promise.set_value(GenericResponse(true));
        return;
    }

    if(butler_is_master) {
        me.gd->is_primary_instance = content.make_primary;
        msg->promise.set_value(GenericResponse(true));
        return;
    }

    UpdateHeads heads;
    {
        LockGraphData lock{me.gd};
        heads = client_create_update_heads(*me.gd);
    }

    // First make sure we believe we have everything that upstream has
    if(me.gd->read_head < me.gd->sync_head) {
        developer_output("Retrying syncing while taking transactor role");
        // TODO: This effectively spin-locks, need to fix up.
        me.queue.push(std::move(msg), true);
        return;
    }

    json j = create_json_from_heads_from(heads);
    j["msg_type"] = "make_primary";
    j["graph_uid"] = str(me.uid);
    j["take_on"] = content.make_primary;

    // Add a new task to the list.
    auto response = wait_on_zefhub_message(j);
    if(!response.generic.success) {
        if(response.j.contains("blobs_head")) {
            developer_output("While trying to take transactor role, we were told we were out of date. Going to sync and try again.");
            me.queue.push(std::move(msg), true);
            return;
        }
        if(content.make_primary)
            msg->promise.set_value(GenericResponse("Couldn't make graph primary: " + response.generic.reason));
        else
            msg->promise.set_value(GenericResponse("Couldn't take primary role away from graph: " + response.generic.reason));
    } else {
        me.gd->is_primary_instance = content.make_primary;
        msg->promise.set_value(GenericResponse(true));
    }
}

template<>
void Butler::graph_worker_handle_message(Butler::GraphTrackingData & me, TagGraph & content, Butler::msg_ptr & msg) {
    if(me.gd->error_state != GraphData::ErrorState::OK) {
        msg->promise.set_value(GenericResponse{false, "Graph is in error state"});
        return;
    }

    // Need to make sure we have started to sync, so that zefhub knows about us.
    if(!me.gd->should_sync) {
        msg->promise.set_value(GenericResponse("Can't tag graph when not being synchronised."));
        return;
    }
    if(!wait_diff(me.gd->heads_locker, me.gd->sync_head, 0, std::chrono::duration<double>(butler_generic_timeout))) {
        msg->promise.set_value(GenericResponse("Timed out waiting for graph to be synced."));
        return;
    }
    auto response = wait_on_zefhub_message({
            {"msg_type", "tag_graph"},
            {"graph_uid", str(me.uid)},
            {"name_tag", content.tag},
            {"adding_or_removing", content.remove ? "removing" : "adding"},
            {"force_if_name_tags_other_graph", content.force ? "1" : "0"}
        });
    if(!response.generic.success)
        msg->promise.set_value(GenericResponse("Couldn't tag graph: " + response.generic.reason));
    else
        msg->promise.set_value(GenericResponse(true));
}


std::string Butler::GraphTrackingData::info_str() {
    // This is a string for simplicity, but make it json so it can be parsed by an external program if needed.
    json j({
            {"uid", str(uid)},
            {"keep_alive", bool(keep_alive_g)},
            {"queue_size", queue.num_messages.load()},
            {"gd", to_str((void*)gd)},
            {"last_action", debug_last_action},
            {"sync_joinable", sync_thread && sync_thread->joinable()},
            {"manager_joinable", managing_thread && managing_thread->joinable()},
        });
    return j.dump();
}
