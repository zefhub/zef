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
#define ZEF_DEBUG

#include <variant>
#include <string>
#include <vector>
#include <optional>
#include <chrono>


#define force_assert(x) {if(!(x)) { throw std::runtime_error("Force assert failed: " #x); }}

namespace zefDB {
	const std::string data_layout_version = "0.3.0";

	using blob_index = int32_t;
	using edge_list_size_t = int32_t;
	using str = std::string;
	using bool_maybe = std::optional<bool>;
	using token_value_t = uint32_t;

	using enum_indx = uint32_t;
	
	using string_pair = std::pair<std::string, std::string>;
	// using ET_pair = std::tuple<et_enum_num_data, std::string>;
	// using RT_pair = std::tuple<et_enum_num_data, std::string>;
	using enum_tuple = std::tuple<enum_indx, std::string, std::string>;

	using value_hash_t = uint32_t;

    namespace MMap {
        constexpr size_t MB = 1024*1024;
        constexpr size_t GB = 1024*1024*1024;

        constexpr size_t ZEF_PAGE_SIZE = 1*MB;
        // constexpr size_t ZEF_PAGE_SIZE = 1024*4;
        // const size_t ZEF_UID_SHIFT = 32*GB;
        constexpr size_t ZEF_UID_SHIFT = 1*GB;
        // const size_t ZEF_UID_SHIFT = 16*MB;
        // const size_t ZEF_UID_SHIFT = 1024*4*100;
        constexpr size_t MAX_MMAP_SIZE = 2*ZEF_UID_SHIFT;
    }

	namespace constants {
		constexpr int random_number_random_device_buffer_size = 1024;
		constexpr int max_qd_fct_execution_number_for_one_explicit_tx_closing = 100000;

		constexpr enum_indx compiled_aet_types_max_indx = 65536;  // 2^16
		constexpr int data_layout_version_info_size = 62;
		constexpr int graph_revision_info_size = 64;

		// constexpr int main_mem_pool_size_in_bytes = 1048576 * 16 * 4;  // this absolutely needs to reamin a power of 2
		constexpr blob_index blob_indx_step_in_bytes = 16;
		constexpr int blob_uid_size_in_bytes = 16;
		constexpr blob_index ROOT_NODE_blob_index = 42;  // Sigh. The wastefulness of it all...	
	

		constexpr int default_local_edge_indexes_capacity_ROOT_NODE = 54;
		// constexpr int default_local_edge_indexes_capacity_TX_EVENT_NODE = 5;
		constexpr int default_local_edge_indexes_capacity_TX_EVENT_NODE = 6;
		constexpr int default_local_edge_indexes_capacity_RAE_INSTANCE_EDGE = 6;
		constexpr int default_local_edge_indexes_capacity_TO_DELEGATE_EDGE = 62;
		// constexpr int default_local_edge_indexes_capacity_ENTITY_NODE = 5;
		constexpr int default_local_edge_indexes_capacity_ENTITY_NODE = 7;
		// constexpr int default_local_edge_indexes_capacity_ATTRIBUTE_ENTITY_NODE = 6;
		constexpr int default_local_edge_indexes_capacity_ATTRIBUTE_ENTITY_NODE = 7;
		constexpr int default_local_edge_indexes_capacity_VALUE_NODE = 5;
		// constexpr int default_local_edge_indexes_capacity_RELATION_EDGE = 6;
		constexpr int default_local_edge_indexes_capacity_RELATION_EDGE = 5;
		// constexpr int default_local_edge_indexes_capacity_ASSIGN_TAG_NAME_EDGE = 2;
		constexpr int default_local_edge_indexes_capacity_ASSIGN_TAG_NAME_EDGE = 1;
		// constexpr int default_local_edge_indexes_capacity_FOREIGN_GRAPH_NODE = 2;
		constexpr int default_local_edge_indexes_capacity_FOREIGN_GRAPH_NODE = 1;
		constexpr int default_local_edge_indexes_capacity_CLONE_REL_ENT_EDGE = 2;
		// constexpr int default_local_edge_indexes_capacity_DEFERRED_EDGE_LIST_NODE = 14;
		constexpr int default_local_edge_indexes_capacity_DEFERRED_EDGE_LIST_NODE = 16;
		constexpr int default_local_edge_indexes_capacity_FOREIGN_ENTITY_NODE = 5;
		constexpr int default_local_edge_indexes_capacity_FOREIGN_ATTRIBUTE_ENTITY_NODE = 5;
		// constexpr int default_local_edge_indexes_capacity_FOREIGN_RELATION_EDGE = 5;
		constexpr int default_local_edge_indexes_capacity_FOREIGN_RELATION_EDGE = 3;

		
		constexpr int new_appended_edge_list_growth_factor = 3;
		
        // This is not necessary but seems like a good idea. If it starts being
        // a pain, then we can simply remove it.
		constexpr int max_tag_size = 10000;

		constexpr int EZefRefs_local_array_size = 7;
		constexpr int ZefRefs_local_array_size = 5;
		constexpr int ZefRefss_local_array_size = 5;

        // General nonspecific timeout
		// constexpr float zefhub_comm_timeout_default = 5.0;
		// constexpr float zefhub_comm_tick = 0.05;

		// constexpr float zefhub_await_updated_names_dict_timeout = zefhub_comm_timeout;
		// constexpr float zefhub_await_updated_names_dict_check_period = zefhub_comm_timeout;
        // TODO: Figure out timeout in terms of data transfer.
		// constexpr float zefhub_subscribe_to_graph_timeout_default = 60;
		// constexpr float zefhub_graph_update_timeout_default = 30;
		// constexpr float zefhub_merge_request_timeout_default = 60;
		constexpr auto zefhub_subscribe_to_graph_timeout_default = std::chrono::seconds(60);
		// constexpr float zefhub_graph_update_timeout_default = 30;
		// constexpr float zefhub_merge_request_timeout_default = 60;

		// constexpr auto butler_generic_timeout = std::chrono::seconds(15);
		// constexpr auto butler_generic_timeout = std::chrono::seconds(0);
		// constexpr auto zefhub_generic_timeout = std::chrono::seconds(60);
		constexpr auto zefhub_reconnect_timeout = std::chrono::seconds(10);

        constexpr char zefhub_guest_key[] = "GUEST";
	}

    namespace blobs_ns {
        // TODO: Need to put an assert in here to check the simple assumption for instantiation and ensure_range. 
        // Note: it isn't bad to have an over-estimate here.
        // Also note: also data (e.g. value assignment, or deferred edge list) are not part of this.
        //
        // Note: The alternative to single constant is to very accurately
        // calculate the blob size, before creating the blob. While do-able it
        // changes the logic of the code and will make it a little less clear.
        constexpr size_t max_basic_blob_size = 1024;
    }

}
