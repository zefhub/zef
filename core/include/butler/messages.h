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

#include "fwd_declarations.h"
#include "constants.h"
#include "graph.h"

#include <nlohmann/json.hpp>

namespace zefDB {

    
    namespace Messages {
        using json = nlohmann::json;

        struct UpdatePayload {
            json j;
            std::vector<std::string> rest;
        };


        struct GenericResponse {
            bool success;
            std::string reason;

            GenericResponse(bool success, std::string reason = "")
                : success(success), reason(reason) {}
            GenericResponse(const char * reason)
                : GenericResponse(std::string(reason)) {}
            GenericResponse(std::string reason)
                : success(false), reason(reason) {}
            GenericResponse() = default;
        };

        struct GenericZefHubResponse {
            GenericResponse generic;
            json j;
            std::vector<std::string> rest;
        };

        static_assert(sizeof(token_value_t) == sizeof(enum_indx));
        struct TokenQuery {
            enum Group { ET, RT, EN, KW };
            Group group;

            std::vector<std::string> names;
            // Surrogate for all indexes, they must be the same size.
            std::vector<enum_indx> indices;

            bool create;
            bool filter_first;
        };

        struct TokenQueryResponse {
            GenericResponse generic;

            using pair = std::pair<std::string,enum_indx>;
            // enum_indx is surrogate for all indexes, they must be the same size.
            std::vector<pair> pairs;
        };

        struct NewTransactionCreated {
            Graph g;
            blob_index index;
        };

        // Sent by the butler to graph managers when the network connection
        // upstream is reestablished.
        struct Reconnected {};
        struct Disconnected {};

        struct NewGraph {
            int mem_style;
            // The following is meant for low-level only, e.g. zefhub
            // std::string blob_bytes = "";
            std::optional<UpdatePayload> payload;

            // This flag is for creating internal graphs without a sync thread.
            bool internal_use_only = false;

            // For some reason, we need to manually do the move semantics here.
            // Maybe the default values are messing with the generated
            // constructors? Whatever it is, this cost a lot of time identifying
            // the source.
            NewGraph(int mem_style, bool internal_use_only) :
                mem_style(mem_style),
                internal_use_only(internal_use_only) {}
            NewGraph(int mem_style, UpdatePayload && payload, bool internal_use_only) :
                mem_style(mem_style),
                payload(std::move(payload)),
                internal_use_only(internal_use_only) {}
            NewGraph(NewGraph && other) :
                mem_style(other.mem_style),
                payload(std::move(other.payload)),
                internal_use_only(other.internal_use_only) {}

        };

        struct LocalGraph {
            std::filesystem::path path;
            bool new_graph;
        };

        using load_graph_callback_t = std::function<void(std::string)>;
        struct LoadGraph {
            std::string tag_or_uid;
            int mem_style = MMap::MMAP_STYLE_AUTO;
            std::optional<load_graph_callback_t> callback;
        };
        struct GraphLoaded {
            GenericResponse generic;
            std::optional<Graph> g;
            GraphLoaded(Graph g) : g(g), generic(true) {}
            GraphLoaded(std::string reason) : generic(reason) {}
            // disambiguate a const char[]
            GraphLoaded(const char * reason) : GraphLoaded(std::string(reason)) {}
            GraphLoaded() = default;
        };

        struct DoneWithGraph {
            GraphData * gd;
        };

        struct LoadPage {
            // We need to hold onto the graph reference
            Graph g;
            const void * ptr;
            size_t size;
            LoadPage(const void * ptr, size_t size)
                : ptr(ptr),
                  size(size),
                  g((GraphData*)MMap::blobs_ptr_from_blob(ptr), false) {}
        };
        struct NotifySync {
            // Inform about a possibly new value of sync, and do the sync if it
            // is true.
            Graph g;
            bool sync;
            NotifySync(const Graph & g, bool sync) : g(g), sync(sync) {}
        };
        struct SetKeepAlive {
            Graph g;
            bool value;
        };

        struct GraphUpdate {
            std::string graph_uid;

            UpdatePayload payload;
        };

        struct ForceUnsubscribe {
            std::string graph_uid;
        };

        struct MergeRequest {
            struct PayloadGraphDelta {
                // TODO: Currently this is got from python as a python object, but will later be a native C object.
                json commands;
            };
            // If task_uid is not set, then this has been generated locally
            std::optional<std::string> task_uid;
            std::string target_guid;
            std::variant<PayloadGraphDelta> payload;
            int msg_version = 2;
        };


        struct MergeRequestResponse {
            struct ReceiptIndices {
                blob_index merge_tx_index;
                std::vector<blob_index> merge_indices;
            };
            struct ReceiptGraphDelta {
                // TODO: Currently this is got from python as a python object, but will later be a native C object.
                json receipt;
                blob_index read_head;
            };

            GenericResponse generic;
            std::variant<std::monostate, ReceiptIndices, ReceiptGraphDelta> receipt;

            MergeRequestResponse(const GenericZefHubResponse & generic_zh)
                : generic(generic_zh.generic) {}
            MergeRequestResponse(const GenericResponse & generic, ReceiptIndices receipt)
                : generic(generic), receipt(receipt) {}
            MergeRequestResponse(const GenericResponse & generic, ReceiptGraphDelta receipt)
                : generic(generic), receipt(receipt) {}
            MergeRequestResponse(const GenericResponse & generic)
                : generic(generic) {}
        };

        struct OLD_STYLE_UserManagement {
            std::string action;
            std::string subject;
            std::string target;
            std::string extra;
        };
        struct TokenManagement {
            std::string action;
            std::string token_group;
            std::string token;
            std::string target;
        };

        struct OLD_STYLE_UpdateTagList {
            std::vector<std::string> tag_list;
            std::string graph_uid;
        };

        struct ZearchQuery {
            std::string query;
        };

        struct UIDQuery {
            std::string query;
        };

        struct MakePrimary {
            Graph g;
            bool make_primary;
        };

        struct TagGraph {
            Graph g;
            std::string tag;
            bool force;
            bool remove;
        };


        using Request = std::variant<
            std::monostate,

            TokenQuery,

            NewTransactionCreated,
            Reconnected,
            Disconnected,
            ForceUnsubscribe,
            NewGraph,
            LocalGraph,
            LoadGraph,
            DoneWithGraph,
            LoadPage,
            NotifySync,
            SetKeepAlive,
            GraphUpdate,
            MergeRequest,
            OLD_STYLE_UserManagement,
            TokenManagement,
            OLD_STYLE_UpdateTagList,
            ZearchQuery,
            UIDQuery,
            MakePrimary,
            TagGraph
            
            // SubprotocolMessage
            >;


        using Response = std::variant<
            std::monostate,
            GenericResponse,

            GraphLoaded,
            // These are only temporary until ZH is updated
            // OLD_STYLE_Update,
            GenericZefHubResponse,
            MergeRequestResponse,
            TokenQueryResponse
            
            // SubprotocolResponse
            >;


        inline bool validate_message_version(std::string msg_type, int msg_version, int client_version) {
            // This function lists invalid combination of msg_version and protocol version.
            if(msg_type == "merge_request") {
                if(msg_version >= 1 && client_version <= 1)
                    return false;
            }
            return true;
        }
    }
    namespace Butler {
        inline std::string msgqueue_to_str(std::monostate z) { return "monostate"; }
#define CREATE_NAME_STRING(x) inline std::string msgqueue_to_str(const Messages::x & z) { return #x; }
        CREATE_NAME_STRING(TokenQuery)
        CREATE_NAME_STRING(Reconnected)
        CREATE_NAME_STRING(Disconnected)
        CREATE_NAME_STRING(ForceUnsubscribe)
        CREATE_NAME_STRING(NewGraph)
        CREATE_NAME_STRING(LocalGraph)
        CREATE_NAME_STRING(LoadGraph)
        CREATE_NAME_STRING(DoneWithGraph)
        CREATE_NAME_STRING(LoadPage)
        CREATE_NAME_STRING(NotifySync)
        CREATE_NAME_STRING(SetKeepAlive)
        CREATE_NAME_STRING(GraphUpdate)
        CREATE_NAME_STRING(MergeRequest)
        CREATE_NAME_STRING(MergeRequestResponse)
        CREATE_NAME_STRING(OLD_STYLE_UserManagement)
        CREATE_NAME_STRING(TokenManagement)
        CREATE_NAME_STRING(OLD_STYLE_UpdateTagList)
        CREATE_NAME_STRING(ZearchQuery)
        CREATE_NAME_STRING(UIDQuery)
        CREATE_NAME_STRING(MakePrimary)
        CREATE_NAME_STRING(TagGraph)
        CREATE_NAME_STRING(NewTransactionCreated)

    }
}
