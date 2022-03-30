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
#include <execinfo.h>
#include <stdlib.h>
#include <unistd.h>


// From: https://stackoverflow.com/questions/77005/how-to-automatically-generate-a-stacktrace-when-my-program-crashes
inline void print_backtrace_force(int fd=STDERR_FILENO) {
  void *array[50];
  size_t size;
  size = backtrace(array, 50);
  backtrace_symbols_fd(array, size, fd);
}
#ifdef DEBUG
inline void print_backtrace(int fd = STDERR_FILENO) {
    print_backtrace_force(fd);    
}
#else
inline void print_backtrace(int fd=STDERR_FILENO) {}
#endif



namespace zefDB {
    using MMap::blobs_ptr_from_blob;

    // struct zef_enum_bidirectional_map;

	struct EZefRef;
	struct ZefRef;
	struct TimeSlice;
	struct GraphData;
	struct Graph;
	enum class BlobType : unsigned char;

	template <typename T> T& get(EZefRef uzr);

	bool is_promotable_to_zefref(EZefRef uzr_to_promote, EZefRef reference_tx);	
	std::ostream& operator << (std::ostream& os, EZefRef uzr);

	namespace blobs_ns {
		struct ROOT_NODE;
		struct ENTITY_NODE;
	}



	struct AtomicEntityType;
	// struct AllActiveGraphDataTracker;
	// AllActiveGraphDataTracker& get_all_active_graph_data_tracker();
	//ENTITY_TYPE get_entity_type_from_string(const std::string& name);
	
	// std::string type_name(EZefRef uzr);
	// std::string type_name(ZefRef zr);

	
	namespace internals {
		EZefRef instantiate(BlobType bt, GraphData& gd);
		EZefRef instantiate(EZefRef src, BlobType bt, EZefRef trg, GraphData& gd);

		EZefRef get_or_create_and_get_tx(GraphData& gd);
		void append_edge_index(EZefRef my_blob, blob_index edge_index_to_append, bool prevent_new_edgelist_creation = false);

		template <typename T> T& get_next_free_writable_blob(GraphData& gd);
		void move_head_forward(GraphData& gd);		
				

		// void apply_actions_to_blob_range(Graph& g, blob_index blob_index_lo, blob_index blob_index_hi, bool ensure_idempotency);
		// void set_blobs_and_uids_from_bytes(Graph& g, blob_index start_index, blob_index end_index, const std::string& blob_bytes, const std::string& uid_bytes);
	

		// AtomicEntityType get_aet_from_enum_type_name_string(const std::string& name);
		// AtomicEntityType get_aet_from_quantity_float_name_string(const std::string& name);
		// AtomicEntityType get_aet_from_quantity_int_name_string(const std::string& name);
	}
}
