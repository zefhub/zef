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

#include <thread>
#include "scalars.h"
#include "butler/msgqueue.h"
#include "butler/messages.h"
#include "butler/communication.h"
#include "butler/auth.h"

#include <nlohmann/json.hpp>

namespace zefDB {

    LIBZEF_DLL_EXPORTED extern bool initialised_python_core;

    namespace Butler {

        using activity_callback_t = std::function<void(std::string)>;

		const double zefhub_generic_timeout(10);
		const double butler_generic_timeout(15);

        using namespace zefDB::Messages;

        struct RequestWrapper {
            // Guests get their results back by "callbacks" or promise/futures.
            // There are no other ways for the butler to communicate with
            // guests. Have to think what this means for subscriptions. However,
            // note that the butler doesn't know how many threads are running,
            // and if we wanted to allow for that, we'd need each thread to
            // "init" before doing anything.
            //
            // Note: to help prevent deadlock, we should try and make the
            // requester not hold onto the promise, only the future. This is so
            // that when the promise is deconstructed, the future will return
            // and not block forever. This is achieved using move semantics in
            // the message queue.
            std::promise<Response> promise;

            Request content;

            RequestWrapper(Request && content) : content(std::move(content)) {}
            RequestWrapper(std::promise<Response> promise, Request && content)
                : promise(std::move(promise)),
                  content(std::move(content)) {}
        };
        // }

        // I struggled with making this overload the to_str function. This did not
        // seem to trigger it, which I thought was something to do with the order of
        // definitions of the to_str functions. In the end I didn't want to waste
        // more time, so this is defined as a workaround.
    
        // template<>
        inline std::string msgqueue_to_str(const RequestWrapper & wrapper) {
            // return std::visit(&to_str, wrapper.content);
            return std::visit([](const auto & x) { return msgqueue_to_str(x); },
                              wrapper.content);
        }

        using json = nlohmann::json;
        inline std::string json_string_default(json j, const char field[]) {
            if(!j.contains(field))
                return "";
            return j[field].get<std::string>();
        }
            

        // If this is set, the butler no longer needs to commmunicate with
        // upstream to ask permission or take primary etc...
        extern bool butler_is_master;
        struct LIBZEF_DLL_EXPORTED Butler {
            using msg_ptr = MessageQueue<RequestWrapper>::ptr;
            // The main thread of the butler.
            std::unique_ptr<std::thread> thread;
            std::promise<bool> return_value;
            // Message queue MWSR for guests.
            MessageQueue<RequestWrapper> msgqueue;
            // A set of tasks that have begun, but are waiting on ZH feedback
            struct Task {
                // std::promise<Response> promise;
                std::future<Response> future;
                bool wants_messages;
                std::deque<std::string> messages;
                AtomicLockWrapper locker;
                std::string task_uid = generate_random_task_uid();
                Time started_time;
                // In the future, this might become a std::optional<...> with the responsible connection inside.
                // For now, this just indicates tasks that should be cancelled when the online connection goes down.
                bool is_online;
                // Time last_activity;
                std::atomic<double> last_activity;
                double timeout;
                // The nothing constructor is for receiving a looked-up task
                // only.
                Task() {}
                // Task(Time time, bool is_online, QuantityFloat timeout)
                //     : started_time(time),
                //       is_online(is_online),
                //       last_activity(time),
                //       timeout(timeout) {
                //     std::cerr << "Task " << task_uid << " is being constructed" << std::endl;
                // }
                Task(Task&&) = default;
                Task(const Task&) = delete;
                // Handle when making a Task from a RequestMesssage, which
                // is forwarding a client request straight to zefhub, letting
                // the client wait on the response rather than the butler.
                Task(Time time, bool is_online, double timeout, std::promise<Response> & promise, bool acquire_future)
                    : started_time(time),
                      is_online(is_online),
                      last_activity(time.seconds_since_1970),
                      timeout(timeout) {
                    if(acquire_future)
                        future = promise.get_future();
                }
                // This is to make it explicit to avoid problematic cases.
                Task& operator=(const Task &) = delete;
                Task& operator=(Task &&) = default;
            };

            using task_ptr = std::shared_ptr<Task>;

            // We keep the task promise separate to the task itself, to mirror
            // the ownership-management that c++ promises/futures perform.
            // Basically, we want the destruction of the task promise to be
            // possible (due to timeout or some other failure), which triggers
            // the future to have an exception set and "listener" can continue.
            struct TaskPromise {
                task_ptr task;
                std::promise<Response> promise;

                TaskPromise(task_ptr task, std::promise<Response> && promise)
                    : task(task),
                      promise(std::move(promise)) {}
            };
            using task_promise_ptr = std::shared_ptr<TaskPromise>;
            // std::forward_list<Task> waiting_tasks;
            // std::forward_list<task_ptr> waiting_tasks;
            // std::vector<Task> waiting_tasks;
            std::vector<task_promise_ptr> waiting_tasks;
            // For simplicity, need a lock
            std::mutex waiting_tasks_mutex;
            // Note: we can't have add_task return a reference here, as it will
            // reference a location in the vector which may later change from
            // multi-threaded behaviour.
            task_ptr add_task(bool is_online, double timeout);
            task_ptr add_task(bool is_online, double timeout, std::promise<Response> && promise, bool acquire_future=false);
            task_promise_ptr find_task(std::string task_uid, bool forget=false);
            bool forget_task(std::string task_uid);
            void cancel_online_tasks();

            void fill_out_ZH_message(json & j);
            void send_ZH_message(json & j, const std::vector<std::string> & rest = {});
            // This is to allow brace initialisation, while still preferring pass-by-reference.
            void send_ZH_message(json && j, const std::vector<std::string> & rest = {}) {
                send_ZH_message(j, rest);
            }
            void send_chunked_ZH_message(std::string main_task_uid, json & j, const std::vector<std::string> & rest);

            struct timeout_exception : public std::runtime_error {
                timeout_exception() : std::runtime_error("Timeout exception") {}
            };

            Response wait_on_zefhub_message_any(json & j, const std::vector<std::string> & rest = {}, double timeout = zefhub_generic_timeout, bool throw_on_failure = false, bool chunked = false, std::optional<activity_callback_t> activity_callback = {});

            template<class T=GenericZefHubResponse>
            T wait_on_zefhub_message(json & j, const std::vector<std::string> & rest = {}, double timeout = zefhub_generic_timeout, bool throw_on_failure = false, bool chunked = false, std::optional<activity_callback_t> activity_callback = {});

            // This is to allow brace initialisation, while still preferring pass-by-reference.
            Response wait_on_zefhub_message_any(json && j, const std::vector<std::string> & rest = {}, double timeout = zefhub_generic_timeout, bool throw_on_failure = false, bool chunked = false) {
                return wait_on_zefhub_message_any(j, rest, timeout, throw_on_failure);
            }

            // A convenience version for value-constructed json
            template<class T=GenericZefHubResponse>
            T wait_on_zefhub_message(json && j, const std::vector<std::string> & rest = {}, double timeout = zefhub_generic_timeout, bool throw_on_failure = false, bool chunked = false, std::optional<activity_callback_t> activity_callback = {}) {
                return wait_on_zefhub_message(j, rest, timeout, throw_on_failure, chunked, activity_callback);
            }


            void handle_incoming_message(json & j, std::vector<std::string> & rest);

            void handle_incoming_terminate(json & j);
            void handle_incoming_force_unsubscribe(json & j);
            void handle_incoming_graph_update(json & j, std::vector<std::string> & rest);
            void handle_incoming_update_tag_list(json & j);
            void handle_incoming_merge_request(json & j);
            void handle_incoming_chunked(json & j, std::vector<std::string> & rest);

            void ack_failure(std::string task_uid, std::string reason);
            void ack_success(std::string task_uid, std::string reason = "success");


            // Every thread should be listening to this bool.
            std::atomic_bool should_stop = false;


            // Networking things.
            Communication::PersistentConnection network;
            std::atomic_bool connection_authed = false;
            std::atomic_bool fatal_connection_error = false;
            std::atomic_bool no_credentials_warning = false;
            // The authentication string. This is set on connection.
            std::string refresh_token = "";
            std::string api_key = "";
            // The protocol version chosen for communication. This may have to be autodetected in earlier versions.
            std::atomic_int zefdb_protocol_version = -1;
            constexpr static int zefdb_protocol_version_min = 4;
            constexpr static int zefdb_protocol_version_max = 7;
            AtomicLockWrapper auth_locker;
            std::string upstream_layout();

            int chunked_transfer_size_user = -1;
            int chunked_transfer_size = 10*1024;
            int chunked_transfer_size_min = 1024;
            int chunked_transfer_queued = 10;
            double chunked_safety_factor = 5;
            bool chunked_transfer_auto_adjust = true;
            // The timeout expected for an ACK. As the websocketpp library
            // returns immediately, this is the timeout from BEFORE sending the
            // chunk.
            const double chunk_timeout = 10.0;

            // This estimation is meant to include the effects of ping (i.e. how
            // much can be transfer in a short time span). This makes it
            // non-linear but as long as we queue up a ton of messages then it
            // shouldn't be noticeable.
            double estimated_transfer_speed_accum_bytes = 0;
            double estimated_transfer_speed_accum_time = 0;
            // We limit the accumulation to create a simple "running average"
            const double limit_estimated_transfer_speed_accum_time = 300;

            struct ReceivingTransfer {
                BaseUID uid;
                std::vector<int> rest_sizes;
                std::vector<std::string> rest;
                json msg;

                // No atomic here as we are only accessing this in the WS thread.
                Time last_activity;

                struct BufferedMessage {
                    int bytes_start;
                    int rest_index;
                    std::string data;

                    BufferedMessage(int bytes_start, int rest_index, std::string data) :
                        bytes_start(bytes_start),
                        rest_index(rest_index),
                        data(data) {}
                };
                std::vector<BufferedMessage> buffer;
            };

            // Note: making an assumption that the WS handler thread is the only
            // thread to ever touch this map.
            std::unordered_map<BaseUID,ReceivingTransfer> receiving_transfers;
            void check_overdue_receiving_transfers();


            using header_list_t = Communication::PersistentConnection::header_list_t;
            header_list_t prepare_send_headers();
            void start_connection();
            void stop_connection();
            bool want_upstream_connection();
            // wait_for_auth: will start_connection if not already connected
            bool wait_for_auth(std::chrono::duration<double> timeout=std::chrono::seconds(-1));
            void determine_login_token();
            std::string who_am_i();
            // ensure_auth_credentials: if no credentials will pop up browser
            void ensure_auth_credentials();
            std::optional<std::string> load_forced_zefhub_key();
            bool have_auth_credentials();
            bool is_credentials_file_valid();
            // user_login: throws if already logged in, otherwise ensure_auth_credentials
            void user_login();
            // user_logout: noop if already logged out, otherwise delete credentials. If connected, reset the connection.
            void user_logout();

            bool have_logged_in_as_guest = false;
            std::optional<std::string> session_auth_key;


            Butler(std::string uri);


            void ws_open_handler(void); 
            void ws_message_handler(std::string msg);
            void ws_close_handler(void);
            void ws_fatal_handler(std::string reason);
            void handle_successful_auth(void);

            template <typename T>
            void handle_guest_message(T & content, msg_ptr & msg);
            void handle_guest_message_passthrough(msg_ptr & msg, GraphData * gd);

            void msgqueue_listener(void);
            std::future<Response> msg_push_internal(Request && content, bool ignore_closed=false);

            template <class T>
            T msg_push(Request && content);

            void msg_push(Request && content, bool wait = true, bool ignore_closed=false);

            // NOTE: This should not be used very much because it is far better
            // to keep the timeouts in one place i.e. the graph manager. The
            // only time this could be useful is if there is nobody blocking on
            // a ZH query, e.g. a zearch request.
            template <class T>
            T msg_push_timeout(Request && content, double timeout = butler_generic_timeout, bool ignore_closed=false);

            ////////////////////////////////////////////////////
            // * Graph manager functions

            struct GraphTrackingData {
                GraphData * gd;
                // Note: we need to have uid here, as this object starts its
                // life before the graph is loaded.
                BaseUID uid;
                // If we wish to keep the graph alive ever when all other
                // references disappear, we keep a reference in here.
                std::optional<Graph> keep_alive_g;

                std::unique_ptr<std::thread> managing_thread;
                std::unique_ptr<std::thread> sync_thread;
                std::promise<bool> return_value;
                std::promise<bool> sync_return_value;
                MessageQueue<RequestWrapper> queue;
                std::atomic<bool> please_stop = false;

                // blob_index latest_blob_upstream_knows_about = 0;
                // Note: this variable is really only so we don't spam zefhub.
                // At the moment, the graph manager should pause while it waits
                // to receive the acknowledgement so this should actually do
                // nothing. But if this changes in the future, we won't get caught out...
                // blob_index latest_blob_sent_out = 0;

                std::string info_str();

                std::string debug_last_action = "";
            };

            std::vector<std::shared_ptr<GraphTrackingData>> graph_manager_list;
            std::shared_mutex graph_manager_list_mutex;

            std::shared_ptr<GraphTrackingData> spawn_graph_manager(BaseUID uid);
            void spawn_graph_sync_thread(GraphTrackingData & me);
            std::shared_ptr<GraphTrackingData> find_graph_manager(GraphData * gd);
            std::shared_ptr<GraphTrackingData> find_graph_manager(BaseUID uid);
            void remove_graph_manager(std::shared_ptr<GraphTrackingData> gtd);
            std::vector<BaseUID> list_graph_manager_uids();
            
            void manage_graph_worker(std::shared_ptr<GraphTrackingData> graph_tracking_data);
            void manage_graph_sync_worker(Butler::GraphTrackingData & me);

            template <typename T>
            void graph_worker_handle_message(GraphTrackingData & graph_tracking_data, T & content, msg_ptr & msg);
            template <typename T>
            T parse_ws_response(json j);


            void load_graph_from_uid(msg_ptr & msg, BaseUID uid);
            // This is for threads that are just looking up the uid of a graph.
            // We have these, so that we don't block the main thread, and so
            // that we don't need to fake a "GraphTrackingData" which actually
            // has no graph.
            void load_graph_from_tag_worker(msg_ptr msg);
            void load_graph_from_file(msg_ptr & msg, std::filesystem::path dir);
            void send_update(GraphTrackingData & me);
            void set_into_invalid_state(GraphTrackingData & me);


            std::string upstream_name();
            std::filesystem::path credentials_path();

            std::optional<Graph> local_process_graph;
            Graph get_local_process_graph();
        };




        LIBZEF_DLL_EXPORTED void initialise_butler();
        LIBZEF_DLL_EXPORTED void initialise_butler(std::string zefhub_uri);
        LIBZEF_DLL_EXPORTED void initialise_butler_as_master();
        LIBZEF_DLL_EXPORTED bool is_butler_running();
        LIBZEF_DLL_EXPORTED std::shared_ptr<Butler> get_butler();
        LIBZEF_DLL_EXPORTED void stop_butler();
        // This is for debugging, and detecting which tokens need to be added to the everyone group.
        extern bool before_first_graph;
        void maybe_show_early_tokens();
        void add_to_early_tokens(TokenQuery::Group group, const std::string & name);
        LIBZEF_DLL_EXPORTED std::vector<std::string> early_token_list();
        LIBZEF_DLL_EXPORTED std::vector<std::string> created_token_list();

        LIBZEF_DLL_EXPORTED MMap::FileGraph create_file_graph(BaseUID tag_or_uid);
        LIBZEF_DLL_EXPORTED std::filesystem::path file_graph_prefix(BaseUID uid, std::string upstream_name);
        LIBZEF_DLL_EXPORTED std::filesystem::path local_graph_prefix(std::filesystem::path dir);
        LIBZEF_DLL_EXPORTED std::filesystem::path local_graph_uid_path(std::filesystem::path dir);

        LIBZEF_DLL_EXPORTED void ensure_or_get_range(const void * ptr, size_t size);

        GenericResponse generic_from_json(json j);

        ////////////////////////////////////////////////
        // * Graph update messages

        struct UpdateHeads {
            struct {
                blob_index from;
                blob_index to;
                int revision;
            } blobs;

            struct NamedHeadRange {
                std::string name;
                size_t from;
                size_t to;
                size_t revision;
            };
            std::vector<NamedHeadRange> caches;

        };
        inline std::ostream & operator<<(std::ostream & o, const UpdateHeads & heads) {
            o << "UpdateHeads(";
            o << "blobs:" << heads.blobs.from << ":" << heads.blobs.to;
            for(auto & cache : heads.caches)
                o << ", (" << cache.revision << ") " << cache.name << ":" << cache.from << ":" << cache.to;
            o << ")";
            return o;
        }

        LIBZEF_DLL_EXPORTED bool is_up_to_date(const UpdateHeads & update_heads);
        LIBZEF_DLL_EXPORTED bool heads_apply(const UpdateHeads & update_heads, const GraphData & gd);
        LIBZEF_DLL_EXPORTED UpdatePayload create_update_payload_current(GraphData & gd, const UpdateHeads & update_heads);
        LIBZEF_DLL_EXPORTED UpdatePayload create_update_payload(GraphData & gd, const UpdateHeads & update_heads, std::string target_layout="");
        LIBZEF_DLL_EXPORTED UpdateHeads client_create_update_heads(const GraphData & gd);
        LIBZEF_DLL_EXPORTED json create_json_from_heads_from(const UpdateHeads & update_heads);
        LIBZEF_DLL_EXPORTED json create_json_from_heads_latest(const UpdateHeads & update_heads);
        LIBZEF_DLL_EXPORTED void parse_filegraph_update_heads(MMap::FileGraph & fg, json & j, std::string working_layout);
        LIBZEF_DLL_EXPORTED UpdateHeads parse_payload_update_heads(const UpdatePayload & payload);
        LIBZEF_DLL_EXPORTED UpdateHeads parse_message_update_heads(const json & j);
        // UpdateHeads client_create_update_heads(const GraphData & gd);
        LIBZEF_DLL_EXPORTED void apply_update_with_caches(GraphData & gd, const UpdatePayload & payload, bool double_linking, bool update_upstream);
        LIBZEF_DLL_EXPORTED void apply_sync_heads(GraphData & gd, const UpdateHeads & update_heads);

    }
}

#include "butler.hpp"
