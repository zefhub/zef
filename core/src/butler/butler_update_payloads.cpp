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

UpdatePayload create_update_payload_current(GraphData & gd, const UpdateHeads & update_heads) {
    if(update_heads.blobs.from > update_heads.blobs.to)
        throw std::runtime_error("Somehow upstream is ahead of us and we're primary!");

    // TODO: need to find the latest complete tx in the blob range given.
    blob_index last_tx = gd.latest_complete_tx;
    if(last_tx > update_heads.blobs.to) {
        std::cerr << "WARNING: latest tx is ahead of blobs_to" << std::endl;
        std::cerr << "WARNING: latest tx is ahead of blobs_to" << std::endl;
        std::cerr << "WARNING: latest tx is ahead of blobs_to" << std::endl;
    }

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
        {"hash_full_graph", partial_hash(Graph(gd), update_heads.blobs.to, 0, "")},
        {"data_layout_version", internals::get_data_layout_version_info(gd)}
    };
    p.rest = {blobs};
    std::vector<json> caches;

    for(auto & cache : update_heads.caches) {
#define GEN_CACHE(x, y) else if(cache.name == x) {                      \
            if(cache.from != cache.to) {                                \
                caches.push_back({                                      \
                        {"name", x},                                    \
                        {"index_lo", cache.from},                       \
                        {"index_hi", cache.to},                         \
                        {"revision", cache.revision},                   \
                    });                                                 \
                auto ptr = gd.y->get();                                 \
                p.rest.push_back(ptr->create_diff(cache.from, cache.to)); \
            }                                                           \
        }

        if(false) {}
        GEN_CACHE("_ETs_used", ETs_used)
            GEN_CACHE("_RTs_used", RTs_used)
            GEN_CACHE("_ENs_used", ENs_used)
            GEN_CACHE("_uid_lookup", uid_lookup)
            GEN_CACHE("_euid_lookup", euid_lookup)
            GEN_CACHE("_tag_lookup", tag_lookup)
            GEN_CACHE("_av_hash_lookup", av_hash_lookup)
        else {
            throw std::runtime_error("Unknown cache");
        }
#undef GEN_CACHE

    }

    p.j["caches"] = caches;

    return p;
}
UpdatePayload create_update_payload(GraphData & gd, const UpdateHeads & update_heads, std::string target_layout) {
    if(target_layout == "")
        target_layout = "0.3.0";
    if(target_layout == "0.3.0")
        return create_update_payload_current(gd, update_heads);
    if(target_layout == "0.2.0")
        return conversions::create_update_payload_as_if_0_2_0(gd, update_heads);
    throw std::runtime_error("Don't know how to create update payload for layout: " + to_str(target_layout));
}

json create_json_from_heads_from(const UpdateHeads & update_heads) {
    json j{
        {"blobs_head", update_heads.blobs.from},
    };
    json caches;

    for(auto & cache : update_heads.caches) {
#define GEN_CACHE(x, y) else if(cache.name == x) {  \
            caches[x] = {                           \
                {"name", x},                        \
                {"head", cache.from},               \
                {"revision", cache.revision},       \
            };                                      \
        }                                           \

        if(false) {}
        GEN_CACHE("_ETs_used", ETs_used)
            GEN_CACHE("_RTs_used", RTs_used)
            GEN_CACHE("_ENs_used", ENs_used)
            GEN_CACHE("_uid_lookup", uid_lookup)
            GEN_CACHE("_euid_lookup", euid_lookup)
            GEN_CACHE("_tag_lookup", tag_lookup)
            GEN_CACHE("_av_hash_lookup", av_hash_lookup)
        else {
            throw std::runtime_error("Unknown cache");
        }
#undef GEN_CACHE

    }

    j["cache_heads"] = caches;

    return j;
}

json create_json_from_heads_latest(const UpdateHeads & update_heads) {
    json j{
        {"blobs_head", update_heads.blobs.to},
    };
    json caches;

    for(auto & cache : update_heads.caches) {
#define GEN_CACHE(x, y) else if(cache.name == x) {  \
            caches[x] = {                           \
                {"name", x},                        \
                {"head", cache.to},               \
                {"revision", cache.revision},       \
            };                                      \
        }                                           \

        if(false) {}
        GEN_CACHE("_ETs_used", ETs_used)
            GEN_CACHE("_RTs_used", RTs_used)
            GEN_CACHE("_ENs_used", ENs_used)
            GEN_CACHE("_uid_lookup", uid_lookup)
            GEN_CACHE("_euid_lookup", euid_lookup)
            GEN_CACHE("_tag_lookup", tag_lookup)
            GEN_CACHE("_av_hash_lookup", av_hash_lookup)
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

#define GEN_CACHE(x, y) {                                               \
        auto ptr = gd.y->get();                                         \
        update_heads.caches.push_back({x, ptr->upstream_size(), ptr->size(), ptr->revision()}); \
    }
    GEN_CACHE("_ETs_used", ETs_used)
        GEN_CACHE("_RTs_used", RTs_used)
        GEN_CACHE("_ENs_used", ENs_used)
        GEN_CACHE("_uid_lookup", uid_lookup)
        GEN_CACHE("_euid_lookup", euid_lookup)
        GEN_CACHE("_tag_lookup", tag_lookup)
        GEN_CACHE("_av_hash_lookup", av_hash_lookup)
#undef GEN_CACHE
        return update_heads;
}

void parse_filegraph_update_heads(MMap::FileGraph & fg, json & j, std::string working_layout) {
    j["blobs_head"] = fg.get_latest_blob_index();
    if(j["blobs_head"] == 0)
        j["blobs_head"] = constants::ROOT_NODE_blob_index;

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
    {
        decltype(GraphData::av_hash_lookup)::element_type mapping(fg, prefix->av_hash_lookup);
        cache_heads["_av_hash_lookup"] = json{
            {"head", mapping.get()->size()},
            {"revision", mapping.get()->revision()},
        };
    }

    conversions::modify_update_heads(cache_heads, working_layout);
    j["cache_heads"] = cache_heads;
}

void apply_sync_heads(GraphData & gd, const UpdateHeads & update_heads) {
    update(gd.heads_locker, [&]() {
        gd.sync_head = update_heads.blobs.to;

        for(auto & cache : update_heads.caches) {
#define GEN_CACHE(x, y) else if(cache.name == x) {  \
                auto ptr = gd.y->get_writer();      \
                ptr->upstream_size() = cache.to;    \
                ptr->revision() = cache.revision;   \
            }

            if(false) {}
            GEN_CACHE("_ETs_used", ETs_used)
                GEN_CACHE("_RTs_used", RTs_used)
                GEN_CACHE("_ENs_used", ENs_used)
                GEN_CACHE("_uid_lookup", uid_lookup)
                GEN_CACHE("_euid_lookup", euid_lookup)
                GEN_CACHE("_tag_lookup", tag_lookup)
                GEN_CACHE("_av_hash_lookup", av_hash_lookup)
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
#define GEN_CACHE(x, y) else if(cache.name == x) {                      \
            auto ptr = gd.y->get();                                     \
            if(ptr->revision() != cache.revision || ptr->size() != cache.from) { \
                std::cerr << "Update for cache '" + cache.name + "' doesn't fit. Current: (" << ptr->revision() << ", " << ptr->size() << "), update: (" << cache.revision << ", " << cache.from << ")" << std::endl; \
                return false;                                           \
            }                                                           \
        }
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

    // As we might be doing this fast, we need to make sure that the auth
    // handshake has happened so that we know the protocol version.
    wait_for_auth();
    force_assert(zefdb_protocol_version != -1);

    UpdatePayload payload;
    {
        LockGraphData lock{me.gd};
        if(upstream_layout() == "0.2.0")
            payload = conversions::create_update_payload_as_if_0_2_0(*me.gd, update_heads);
        else {
            force_assert(upstream_layout() == "0.3.0");
            payload = create_update_payload(*me.gd, update_heads);
        }
    }


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
            me.gd->currently_subscribed = false;
            return;
        }

        // A little pause for each retry except the first.
        if(attempts_left != max_attempts)
            std::this_thread::sleep_for(std::chrono::seconds(1));
                    

        GenericZefHubResponse response;
        try {
            if(zwitch.developer_output())
                std::cerr << "Trying to send update for graph " << me.uid << " of range " << update_heads.blobs.from << " to " << update_heads.blobs.to << std::endl;
            response = wait_on_zefhub_message(payload.j, payload.rest, zefhub_generic_timeout, false, true);

            if(!response.generic.success) {
                // There's generally only one reason for a failure. But
                // in any case return and leave this to try again in the
                // sync loop. It's better to just keep retrying rather
                // than crashing.

                if(response.j.contains("blobs_head")) {
                    UpdateHeads parsed_heads = parse_message_update_heads(response.j);
                    apply_sync_heads(*me.gd, parsed_heads);
                    developer_output("Sync head response was a failure but got an updated heads in response so going to try with that.");
                } else {
                    // Unknown error - wait a bit
                    std::cerr << response.j << std::endl;
                    std::cerr << "Unknown error received from ZH from our graph update ('" << response.generic.reason << "'). Setting graph to invalid state." << std::endl;
                    set_into_invalid_state(me);
                    // std::this_thread::sleep_for(std::chrono::seconds(1));
                }

                return;
            }
        } catch(const timeout_exception & e) {
            std::cerr << "Got timeout during send_update. Going to retry" << std::endl;
            continue;
        } catch(const Communication::disconnected_exception & e) {
            std::cerr << "Disconnected during send_update. Going to retry" << std::endl;
            continue;
        } catch(const std::exception & e) {
            std::cerr << "An exception occurred during a send_update ('" << e.what() << "'). Setting graph to invalid state." << std::endl;
            set_into_invalid_state(me);
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
