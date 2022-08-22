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
#include "constants.h"
#include "mmap.h"

#include <variant>
#include <string>
#include <vector>
#include <optional>
// TODO: Figure out later on what can be removed from here.


// Helper for debug mode only
#include <stdio.h>
#include <stdlib.h>


// Will just not deal with stack traces on windows
#ifdef ZEF_WIN32
inline void print_backtrace_force(int fd=2) {}
inline void print_backtrace(int fd=2) {}
#else
// From: https://stackoverflow.com/questions/77005/how-to-automatically-generate-a-stacktrace-when-my-program-crashes
#include <execinfo.h>
#include <unistd.h>
inline void print_backtrace_force(int fd=STDERR_FILENO) {
  void *array[50];
  size_t size;
  size = backtrace(array, 50);
  backtrace_symbols_fd(array, size, fd);
}
#ifdef DEBUG
inline void print_backtrace(int fd = STDERR_FILENO) {
    print_backtrace_force(fd);    
    abort();
}
#else
inline void print_backtrace(int fd=STDERR_FILENO) {}
#endif
#endif



namespace zefDB {
    using MMap::blobs_ptr_from_blob;

    // struct zef_enum_bidirectional_map;

	struct EZefRef;
	struct ZefRef;
	struct TimeSlice;
	struct GraphData;
	struct Graph;
	// enum class BlobType : unsigned char;

	//template <typename T> T& get(EZefRef uzr);

	//bool is_promotable_to_zefref(EZefRef uzr_to_promote, EZefRef reference_tx);	
	//std::ostream& operator << (std::ostream& os, EZefRef uzr);

	// namespace blobs_ns {
	// 	struct ROOT_NODE;
	// 	struct ENTITY_NODE;
	// }



	// struct AttributeEntityType;
	// struct AllActiveGraphDataTracker;
	// AllActiveGraphDataTracker& get_all_active_graph_data_tracker();
	//ENTITY_TYPE get_entity_type_from_string(const std::string& name);
	
	// std::string type_name(EZefRef uzr);
	// std::string type_name(ZefRef zr);

	
	namespace internals {
		// LIBZEF_DLL_EXPORTED EZefRef instantiate(BlobType bt, GraphData& gd);
		// LIBZEF_DLL_EXPORTED EZefRef instantiate(EZefRef src, BlobType bt, EZefRef trg, GraphData& gd);

		// LIBZEF_DLL_EXPORTED EZefRef get_or_create_and_get_tx(GraphData& gd);

		// template <typename T> T& get_next_free_writable_blob(GraphData& gd);
		// LIBZEF_DLL_EXPORTED void move_head_forward(GraphData& gd);		
				

		// void apply_actions_to_blob_range(Graph& g, blob_index blob_index_lo, blob_index blob_index_hi, bool ensure_idempotency);
		// void set_blobs_and_uids_from_bytes(Graph& g, blob_index start_index, blob_index end_index, const std::string& blob_bytes, const std::string& uid_bytes);
	}
}
