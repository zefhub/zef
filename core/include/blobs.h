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

#include <ostream>
#include <iomanip>      // std::setprecision
#include <string_view>
#include <optional>
#include <variant>
#include "constants.h"
#include "range/v3/all.hpp"

#include "fwd_declarations.h"
#include "zefDB_utils.h"
#include "uids.h"
#include "scalars.h"
#include "zefref.h"
#include "append_structures.h"
#include "tokens.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;



namespace zefDB {


		namespace hostage_flags_masks{
		// use bit masking. std::bitset not suitable for compact representation of flags (we don't want to use 1byte per flag)
		// The actual flags will be stored inside the RelationEdge struct, e.g. a.hostage_flags: uint_8

		// Asking whether the target is hostage?   a.hostage_flags | take_trg_hostage == 0x00;
		constexpr uint8_t no_hostage 			= 0x00;	// 00000000

		constexpr uint8_t take_src_hostage 		= 0x01;	// 00000001
		constexpr uint8_t take_src_weak_hostage = 0x02;	// 00000010
		constexpr uint8_t take_trg_hostage 		= 0x04;	// 00000100
		constexpr uint8_t take_trg_weak_hostage = 0x08;	// 00001000
	}
	// =================================== Usage Example ==========================================
	// // ------------------------ setting -------------------------
	// a.hostage_flags |= hostage_flags_masks::take_trg_hostage;				// set this flag
	// a.hostage_flags = hostage_flags_masks::take_trg_hostage | hostage_flags_masks::take_src_weak_hostage;	// set multiple flags at once

	// // ------------------------ reading -------------------------
	// bool is_trg_hostage = (a.hostage_flags & hostage_flags_masks::take_trg_hostage) != 0;
	// bool is_src_hostage = (a.hostage_flags & hostage_flags_masks::take_src_hostage) != 0;
	// bool is_src_weak_hostage = (a.hostage_flags & hostage_flags_masks::take_src_weak_hostage) != 0;


	namespace blobs_ns {		
        

        // The index filled in for an unset subsequent deferred edge list.
        // We have access to indices up to the root node index.
        constexpr blob_index sentinel_subsequent_index = -1;

        struct edge_info {
			edge_list_size_t local_capacity;
            // This edge-holding blob can be in the middle of a deferred edge
            // list. Note: a value of zero indicates there are no edges yet, and
            // triggers special logic to avoid the issue of the start of the
            // edge list not necessarily lining up with a blob boundary.
            blob_index last_edge_holding_blob = 0;
            blob_index indices[1];

            edge_info(edge_list_size_t local) : local_capacity(local) {
                // Zero out
                memset(indices, 0, local_capacity*sizeof(blob_index));
                // Also set the sentinel
                indices[local_capacity] = sentinel_subsequent_index;
                uintptr_t ptr = (uintptr_t)&indices[local_capacity+1];
                assert((ptr % constants::blob_indx_step_in_bytes) == 0);
            }
        };
		
		struct _unspecified {
			BlobType this_BlobType = BlobType::_unspecified;
		};
				
		struct ROOT_NODE {
			BlobType this_BlobType = BlobType::ROOT_NODE;
			short actual_written_data_layout_version_info_size = 0;  // save this info separately in case we want to ever allow uids in raw form where '\0' may be contained
			short actual_written_graph_revision_info_size = 0;
			BaseUID uid;
			char data_layout_version_info[constants::data_layout_version_info_size]; // actual text needs to be null terminated '\0' withtin this range
			char graph_revision_info[constants::graph_revision_info_size];
            edge_info edges{constants::default_local_edge_indexes_capacity_ROOT_NODE};
		};

		struct TX_EVENT_NODE {
			BlobType this_BlobType = BlobType::TX_EVENT_NODE;
			Time time = Time{ std::numeric_limits<double>::quiet_NaN() };	 // time stamp when this transcation occurred
			TimeSlice time_slice = {0};  // a counter going up with each subsequent event
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_TX_EVENT_NODE};
		};

		struct NEXT_TX_EDGE {
			BlobType this_BlobType = BlobType::NEXT_TX_EDGE;
			blob_index source_node_index = 0;  // coming from a TX_EVENT_NODE or ROOTNode
			blob_index target_node_index = 0;	 // going to a 	ATOMIC_ENTITY_NODE_*, ENTITY_NODE_*, RELATION_EDGE_*
		};

		struct RAE_INSTANCE_EDGE {
			BlobType this_BlobType = BlobType::RAE_INSTANCE_EDGE;
			blob_index source_node_index = 0;  // coming from the ROOT_node
			blob_index target_node_index = 0;	 // going to a 	ATOMIC_ENTITY_NODE_*, ENTITY_NODE_*, RELATION_EDGE_*
            edge_info edges{constants::default_local_edge_indexes_capacity_RAE_INSTANCE_EDGE};
		};				

		struct TO_DELEGATE_EDGE {
			BlobType this_BlobType = BlobType::TO_DELEGATE_EDGE;
			blob_index source_node_index = 0;  // coming from the "_AllEntities" node
			blob_index target_node_index = 0;	 // going to the delegate node
            edge_info edges{constants::default_local_edge_indexes_capacity_TO_DELEGATE_EDGE};
		};		

		struct ENTITY_NODE {
			BlobType this_BlobType = BlobType::ENTITY_NODE;
			EntityType entity_type = ET.ZEF_Unspecified;
			TimeSlice instantiation_time_slice = { 0 };
			TimeSlice termination_time_slice = { 0 };
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_ENTITY_NODE};
		};
		
		struct ATOMIC_ENTITY_NODE {
			BlobType this_BlobType = BlobType::ATOMIC_ENTITY_NODE;
			AtomicEntityType my_atomic_entity_type;   // only the type enum value. This can't overflow, the value is never saved here
			TimeSlice instantiation_time_slice = { 0 };
			TimeSlice termination_time_slice = { 0 };
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_ATOMIC_ENTITY_NODE};
		};
		
		// Sometimes we just wan to save a value (that never changes) or we want to project one time slice of the entity and relation property graph 
		// out (e.g. for the simulation). No need to carry around the entire value assignment 
		// machineray and past values, if only the most recent are of interest
		struct ATOMIC_VALUE_NODE {
			BlobType this_BlobType = BlobType::ATOMIC_VALUE_NODE;
			AtomicEntityType my_atomic_entity_type;
			unsigned int buffer_size_in_bytes = 0;   // this needs to be set specifically for each data type (not only data of variable size!)
			// char data_buffer[1];	// for any type larger than a char, this is designed to overflow
            edge_info edges{constants::default_local_edge_indexes_capacity_ATOMIC_VALUE_NODE};
		};

		struct RELATION_EDGE {
			BlobType this_BlobType = BlobType::RELATION_EDGE;
			uint8_t hostage_flags = 0;	// access via hostage_flags_masks:: ...
			RelationType relation_type = RT.ZEF_Unspecified;
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
			TimeSlice instantiation_time_slice = { 0 };
			TimeSlice termination_time_slice = { 0 };
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_RELATION_EDGE};
		};
		
		// these edges are created together with any new entity node or domain edge
		struct DELEGATE_INSTANTIATION_EDGE {
			BlobType this_BlobType = BlobType::DELEGATE_INSTANTIATION_EDGE;
			blob_index source_node_index = 0;  // coming from a TX_EVENT_NODE
			blob_index target_node_index = 0;	 // going to a TO_DELEGATE_EDGE edge 
		};		

		struct DELEGATE_RETIREMENT_EDGE {
			BlobType this_BlobType = BlobType::DELEGATE_RETIREMENT_EDGE;
			blob_index source_node_index = 0;  // coming from a TX_EVENT_NODE
			blob_index target_node_index = 0;	 // going to a TO_DELEGATE_EDGE edge 
		};		

		// these edges are created together with any new entity node or domain edge
		struct INSTANTIATION_EDGE {
			BlobType this_BlobType = BlobType::INSTANTIATION_EDGE;
			blob_index source_node_index = 0;  // coming from a TX_EVENT_NODE
			blob_index target_node_index = 0;	 // going to the entity node between the scneario node and the newly created node
		};
				// these edges are created together with any new entity node or domain edge
		struct TERMINATION_EDGE {
			BlobType this_BlobType = BlobType::TERMINATION_EDGE;
			blob_index source_node_index = 0;  // coming from a TX_EVENT_NODE
			blob_index target_node_index = 0;	 // going to the entity node between the scneario node and the newly created node
		};

		struct ATOMIC_VALUE_ASSIGNMENT_EDGE {
			BlobType this_BlobType = BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE;
			AtomicEntityType my_atomic_entity_type;
			unsigned int buffer_size_in_bytes = 0;    // this needs to be set specifically for each data type (not only data of variable size!)
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
			char data_buffer[1];	// for any type larger than a char, this is designed to overflow
		};

		struct DEFERRED_EDGE_LIST_NODE {
			BlobType this_BlobType = BlobType::DEFERRED_EDGE_LIST_NODE;
            // This used to be preceeding_edge_list, but now it is easier to set
            // the first. This makes it not a doubly-linked list anymore.
			blob_index first_blob = 0;
            // The deferred edge list doesn't need the last_edge_holding_blob so
            // we make a custom version of "edges" here.
            struct deferred_edge_info {
                edge_list_size_t local_capacity = constants::default_local_edge_indexes_capacity_DEFERRED_EDGE_LIST_NODE;
                blob_index indices[1];
                // Note: we explicitly don't set the sentinel here, as the size
                // of the edge list is determined after constructing the object.
            } edges;
		};

		struct ASSIGN_TAG_NAME_EDGE {   // this is an edge between the tx in which it was assigned and the 'TO_INSTANCE' edge for the specific Blob to be tagged*
			BlobType this_BlobType = BlobType::ASSIGN_TAG_NAME_EDGE;
			unsigned int buffer_size_in_bytes = 0;
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
			// char rel_ent_tag_name_buffer[constants::rel_ent_tag_name_buffer_size];	// fixed size: a larger size is not permitted. We don't want two data structures that are allowed to overflow
            edge_info edges{constants::default_local_edge_indexes_capacity_ASSIGN_TAG_NAME_EDGE};
		};

		struct NEXT_TAG_NAME_ASSIGNMENT_EDGE {   // can be inserted between two BlobType::ASSIGN_TAG_NAME_EDGE to enable efficient temporal resolving of tag name values
			BlobType this_BlobType = BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE;			
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
		};


		struct FOREIGN_GRAPH_NODE {
			BlobType this_BlobType = BlobType::FOREIGN_GRAPH_NODE;
			int internal_foreign_graph_index = 0;   // what is the internal visitor number for this graph? For the foreign graph delegate node, this remains set to 0 (no action is triggered)
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_FOREIGN_GRAPH_NODE};
		};

		struct ORIGIN_RAE_EDGE {
			BlobType this_BlobType = BlobType::ORIGIN_RAE_EDGE;
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
		};

		struct ORIGIN_GRAPH_EDGE {
			BlobType this_BlobType = BlobType::ORIGIN_GRAPH_EDGE;
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
		};

		struct FOREIGN_ENTITY_NODE {
			BlobType this_BlobType = BlobType::FOREIGN_ENTITY_NODE;
			EntityType entity_type = ET.ZEF_Unspecified;
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_FOREIGN_ENTITY_NODE};
		};

		struct FOREIGN_ATOMIC_ENTITY_NODE {
			BlobType this_BlobType = BlobType::FOREIGN_ATOMIC_ENTITY_NODE;
			AtomicEntityType atomic_entity_type;   // only the type enum value. This can't overflow, the value is never saved here
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_FOREIGN_ATOMIC_ENTITY_NODE};
		};

		struct FOREIGN_RELATION_EDGE {
			BlobType this_BlobType = BlobType::FOREIGN_RELATION_EDGE;
			RelationType relation_type = RT.ZEF_Unspecified;
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_FOREIGN_RELATION_EDGE};
		};


	} //namespace blobs_ns





	// Return the size of the ZefRef in bytes, accounting for possible overflow etc.
	// Future improvement: use std::visit with std::variant blobs_ns to make this robust to changes in the BlobType field location.
	// But this should hardly ever change and we can catch it with tests
	LIBZEF_DLL_EXPORTED blob_index size_of_blob(EZefRef b);

	// similar to std::visit for std::variants. Given a uzr, will dispatch a function overloaded for all relevant blob types to the respective one.
	const auto visit = [](auto fct_to_apply, EZefRef uzr) {		
		switch (get<BlobType>(uzr)) {
		case BlobType::_unspecified: { throw std::runtime_error("visit called for an unspecified EZefRef");  }
		case BlobType::ROOT_NODE: { return fct_to_apply(*((blobs_ns::ROOT_NODE*)uzr.blob_ptr)); }
		case BlobType::TX_EVENT_NODE: { return fct_to_apply(*((blobs_ns::TX_EVENT_NODE*)uzr.blob_ptr)); }
		case BlobType::RAE_INSTANCE_EDGE: { return fct_to_apply(*((blobs_ns::RAE_INSTANCE_EDGE*)uzr.blob_ptr)); }
		case BlobType::TO_DELEGATE_EDGE: { return fct_to_apply(*((blobs_ns::TO_DELEGATE_EDGE*)uzr.blob_ptr)); }
		case BlobType::NEXT_TX_EDGE: { return fct_to_apply(*((blobs_ns::NEXT_TX_EDGE*)uzr.blob_ptr)); }
		case BlobType::ENTITY_NODE: { return fct_to_apply(*((blobs_ns::ENTITY_NODE*)uzr.blob_ptr)); }
		case BlobType::ATOMIC_ENTITY_NODE: { return fct_to_apply(*((blobs_ns::ATOMIC_ENTITY_NODE*)uzr.blob_ptr)); }
		case BlobType::ATOMIC_VALUE_NODE: { return fct_to_apply(*((blobs_ns::ATOMIC_VALUE_NODE*)uzr.blob_ptr)); }
		case BlobType::RELATION_EDGE: { return fct_to_apply(*((blobs_ns::RELATION_EDGE*)uzr.blob_ptr)); }
		case BlobType::DELEGATE_INSTANTIATION_EDGE: { return fct_to_apply(*((blobs_ns::DELEGATE_INSTANTIATION_EDGE*)uzr.blob_ptr)); }
		case BlobType::DELEGATE_RETIREMENT_EDGE: { return fct_to_apply(*((blobs_ns::DELEGATE_RETIREMENT_EDGE*)uzr.blob_ptr)); }
		case BlobType::INSTANTIATION_EDGE: { return fct_to_apply(*((blobs_ns::INSTANTIATION_EDGE*)uzr.blob_ptr)); }
		case BlobType::TERMINATION_EDGE: { return fct_to_apply(*((blobs_ns::TERMINATION_EDGE*)uzr.blob_ptr)); }
		case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE*)uzr.blob_ptr)); }
		case BlobType::DEFERRED_EDGE_LIST_NODE: { return fct_to_apply(*((blobs_ns::DEFERRED_EDGE_LIST_NODE*)uzr.blob_ptr)); }
		case BlobType::ASSIGN_TAG_NAME_EDGE: { return fct_to_apply(*((blobs_ns::ASSIGN_TAG_NAME_EDGE*)uzr.blob_ptr)); }
		case BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::NEXT_TAG_NAME_ASSIGNMENT_EDGE*)uzr.blob_ptr)); }
		case BlobType::FOREIGN_GRAPH_NODE: { return fct_to_apply(*((blobs_ns::FOREIGN_GRAPH_NODE*)uzr.blob_ptr)); }
		case BlobType::ORIGIN_RAE_EDGE: { return fct_to_apply(*((blobs_ns::ORIGIN_RAE_EDGE*)uzr.blob_ptr)); }
		case BlobType::ORIGIN_GRAPH_EDGE: { return fct_to_apply(*((blobs_ns::ORIGIN_GRAPH_EDGE*)uzr.blob_ptr)); }
		case BlobType::FOREIGN_ENTITY_NODE: { return fct_to_apply(*((blobs_ns::FOREIGN_ENTITY_NODE*)uzr.blob_ptr)); }
		case BlobType::FOREIGN_ATOMIC_ENTITY_NODE: { return fct_to_apply(*((blobs_ns::FOREIGN_ATOMIC_ENTITY_NODE*)uzr.blob_ptr)); }
		case BlobType::FOREIGN_RELATION_EDGE: { return fct_to_apply(*((blobs_ns::FOREIGN_RELATION_EDGE*)uzr.blob_ptr)); }
        default: { print_backtrace(); throw std::runtime_error("Unknown blob type"); }
		}
	};

	const auto visit_blob_with_edges = [](auto fct_to_apply, EZefRef uzr) {		
		switch (get<BlobType>(uzr)) {
		case BlobType::_unspecified: { throw std::runtime_error("visit called for an unspecified EZefRef");  }
		case BlobType::ROOT_NODE: { return fct_to_apply(*((blobs_ns::ROOT_NODE*)uzr.blob_ptr)); }
		case BlobType::TX_EVENT_NODE: { return fct_to_apply(*((blobs_ns::TX_EVENT_NODE*)uzr.blob_ptr)); }
		case BlobType::RAE_INSTANCE_EDGE: { return fct_to_apply(*((blobs_ns::RAE_INSTANCE_EDGE*)uzr.blob_ptr)); }
		case BlobType::TO_DELEGATE_EDGE: { return fct_to_apply(*((blobs_ns::TO_DELEGATE_EDGE*)uzr.blob_ptr)); }
		case BlobType::ENTITY_NODE: { return fct_to_apply(*((blobs_ns::ENTITY_NODE*)uzr.blob_ptr)); }
		case BlobType::ATOMIC_ENTITY_NODE: { return fct_to_apply(*((blobs_ns::ATOMIC_ENTITY_NODE*)uzr.blob_ptr)); }
		case BlobType::ATOMIC_VALUE_NODE: { return fct_to_apply(*((blobs_ns::ATOMIC_VALUE_NODE*)uzr.blob_ptr)); }
		case BlobType::RELATION_EDGE: { return fct_to_apply(*((blobs_ns::RELATION_EDGE*)uzr.blob_ptr)); }
		case BlobType::DEFERRED_EDGE_LIST_NODE: { return fct_to_apply(*((blobs_ns::DEFERRED_EDGE_LIST_NODE*)uzr.blob_ptr)); }
		case BlobType::ASSIGN_TAG_NAME_EDGE: { return fct_to_apply(*((blobs_ns::ASSIGN_TAG_NAME_EDGE*)uzr.blob_ptr)); }
		case BlobType::FOREIGN_GRAPH_NODE: { return fct_to_apply(*((blobs_ns::FOREIGN_GRAPH_NODE*)uzr.blob_ptr)); }
		case BlobType::FOREIGN_ENTITY_NODE: { return fct_to_apply(*((blobs_ns::FOREIGN_ENTITY_NODE*)uzr.blob_ptr)); }
		case BlobType::FOREIGN_ATOMIC_ENTITY_NODE: { return fct_to_apply(*((blobs_ns::FOREIGN_ATOMIC_ENTITY_NODE*)uzr.blob_ptr)); }
		case BlobType::FOREIGN_RELATION_EDGE: { return fct_to_apply(*((blobs_ns::FOREIGN_RELATION_EDGE*)uzr.blob_ptr)); }
        default: { print_backtrace(); throw std::runtime_error("Blobtype expected to have edges but it didn't"); }
        }
	};

	const auto visit_blob_with_source_target = [](auto fct_to_apply, EZefRef uzr) {		
		switch (get<BlobType>(uzr)) {
		case BlobType::_unspecified: { throw std::runtime_error("visit called for an unspecified EZefRef");  }
        case BlobType::RAE_INSTANCE_EDGE: { return fct_to_apply(*((blobs_ns::RAE_INSTANCE_EDGE*)uzr.blob_ptr)); }
        case BlobType::TO_DELEGATE_EDGE: { return fct_to_apply(*((blobs_ns::TO_DELEGATE_EDGE*)uzr.blob_ptr)); }
        case BlobType::NEXT_TX_EDGE: { return fct_to_apply(*((blobs_ns::NEXT_TX_EDGE*)uzr.blob_ptr)); }
        case BlobType::RELATION_EDGE: { return fct_to_apply(*((blobs_ns::RELATION_EDGE*)uzr.blob_ptr)); }
        case BlobType::DELEGATE_INSTANTIATION_EDGE: { return fct_to_apply(*((blobs_ns::DELEGATE_INSTANTIATION_EDGE*)uzr.blob_ptr)); }
        case BlobType::DELEGATE_RETIREMENT_EDGE: { return fct_to_apply(*((blobs_ns::DELEGATE_RETIREMENT_EDGE*)uzr.blob_ptr)); }
        case BlobType::INSTANTIATION_EDGE: { return fct_to_apply(*((blobs_ns::INSTANTIATION_EDGE*)uzr.blob_ptr)); }
        case BlobType::TERMINATION_EDGE: { return fct_to_apply(*((blobs_ns::TERMINATION_EDGE*)uzr.blob_ptr)); }
        case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE*)uzr.blob_ptr)); }
        case BlobType::ASSIGN_TAG_NAME_EDGE: { return fct_to_apply(*((blobs_ns::ASSIGN_TAG_NAME_EDGE*)uzr.blob_ptr)); }
        case BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::NEXT_TAG_NAME_ASSIGNMENT_EDGE*)uzr.blob_ptr)); }
        case BlobType::ORIGIN_RAE_EDGE: { return fct_to_apply(*((blobs_ns::ORIGIN_RAE_EDGE*)uzr.blob_ptr)); }
        case BlobType::ORIGIN_GRAPH_EDGE: { return fct_to_apply(*((blobs_ns::ORIGIN_GRAPH_EDGE*)uzr.blob_ptr)); }
        case BlobType::FOREIGN_RELATION_EDGE: { { return fct_to_apply(*((blobs_ns::FOREIGN_RELATION_EDGE*)uzr.blob_ptr)); } }
        default: { print_backtrace(); throw std::runtime_error("Blobtype expected to have source/target but it didn't"); }
        }
	};

	const auto visit_blob_with_data_buffer = [](auto fct_to_apply, EZefRef uzr) {		
		switch (get<BlobType>(uzr)) {
		case BlobType::_unspecified: { throw std::runtime_error("visit called for an unspecified EZefRef");  }
        case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE*)uzr.blob_ptr)); }
        case BlobType::ASSIGN_TAG_NAME_EDGE: { return fct_to_apply(*((blobs_ns::ASSIGN_TAG_NAME_EDGE*)uzr.blob_ptr)); }
        case BlobType::ATOMIC_VALUE_NODE: { return fct_to_apply(*((blobs_ns::ATOMIC_VALUE_NODE*)uzr.blob_ptr)); }
        default: { print_backtrace(); throw std::runtime_error("Blobtype expected to have data buffer but it didn't"); }
        }
	};


	struct LIBZEF_DLL_EXPORTED Sentinel {}; // used throughout. TODO: Sentinel was introduced all over the place within structs: migrate all of them to use this.
	LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, Sentinel sent);

	namespace internals {
        size_t edge_list_end_offset(EZefRef uzr);
        void static_checks();

		// return a pointer to the buffer storing the 8 byte uid.
		// The buffer location offset within the blob depends on the blob type.
		// this function dispatches correctly.
		inline BaseUID& blob_uid_ref(EZefRef uzr){
			using namespace blobs_ns;
			switch(get<BlobType>(uzr.blob_ptr)){
				case BlobType::ROOT_NODE: 					{return get<ROOT_NODE>(uzr.blob_ptr).uid; }
				case BlobType::TX_EVENT_NODE: 				{return get<TX_EVENT_NODE>(uzr.blob_ptr).uid; }
				case BlobType::ENTITY_NODE: 				{return get<ENTITY_NODE>(uzr.blob_ptr).uid; }
				case BlobType::ATOMIC_ENTITY_NODE: 			{return get<ATOMIC_ENTITY_NODE>(uzr.blob_ptr).uid; }
				case BlobType::RELATION_EDGE: 				{return get<RELATION_EDGE>(uzr.blob_ptr).uid; }
				case BlobType::FOREIGN_GRAPH_NODE: 			{return get<FOREIGN_GRAPH_NODE>(uzr.blob_ptr).uid; }
				case BlobType::FOREIGN_ENTITY_NODE: 		{return get<FOREIGN_ENTITY_NODE>(uzr.blob_ptr).uid; }
				case BlobType::FOREIGN_ATOMIC_ENTITY_NODE:	{return get<FOREIGN_ATOMIC_ENTITY_NODE>(uzr.blob_ptr).uid; }
				case BlobType::FOREIGN_RELATION_EDGE: 		{return get<FOREIGN_RELATION_EDGE>(uzr.blob_ptr).uid; }

            default: {print_backtrace_force(); throw std::runtime_error("blob_uid_ref called for ZefRef without a uid"); }
			}
		}
		inline bool has_uid(EZefRef uzr){
			using namespace blobs_ns;
			switch(get<BlobType>(uzr.blob_ptr)){
				case BlobType::ROOT_NODE:
				case BlobType::TX_EVENT_NODE:
				case BlobType::ENTITY_NODE:
				case BlobType::ATOMIC_ENTITY_NODE:
				case BlobType::RELATION_EDGE:
				case BlobType::FOREIGN_GRAPH_NODE:
				case BlobType::FOREIGN_ENTITY_NODE:
				case BlobType::FOREIGN_ATOMIC_ENTITY_NODE:
				case BlobType::FOREIGN_RELATION_EDGE:
                    return true;
				default:
                    return false;
			}
		}

		inline bool has_source_target_node(EZefRef uzr) {
			BlobType this_BlobType = get<BlobType>(uzr);
			switch (this_BlobType) {

				case BlobType::RAE_INSTANCE_EDGE:
				case BlobType::TO_DELEGATE_EDGE:
				case BlobType::NEXT_TX_EDGE:
				case BlobType::RELATION_EDGE:
				case BlobType::DELEGATE_INSTANTIATION_EDGE:
				case BlobType::DELEGATE_RETIREMENT_EDGE:
				case BlobType::INSTANTIATION_EDGE:
				case BlobType::TERMINATION_EDGE:
				case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE:
				case BlobType::ASSIGN_TAG_NAME_EDGE:
				case BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE:
				case BlobType::ORIGIN_RAE_EDGE:
				case BlobType::ORIGIN_GRAPH_EDGE:
				case BlobType::FOREIGN_RELATION_EDGE: {
                    return true;
                }

				case BlobType::_unspecified:
				case BlobType::ROOT_NODE:
				case BlobType::TX_EVENT_NODE:
				case BlobType::ENTITY_NODE:
				case BlobType::ATOMIC_ENTITY_NODE:
				case BlobType::ATOMIC_VALUE_NODE:
				case BlobType::DEFERRED_EDGE_LIST_NODE:
				case BlobType::FOREIGN_GRAPH_NODE:
				case BlobType::FOREIGN_ENTITY_NODE:
                case BlobType::FOREIGN_ATOMIC_ENTITY_NODE: {
                    return false;
                }
                default:
                    throw std::runtime_error("Shouldn't have got here in has_source_target_node!");
            }
			return false; // should never reach here, suppress compiler warnings
		}

		// helper function to check whether such a list exists as an attribute
		inline bool has_edge_list(EZefRef uzr) {
			BlobType this_BlobType = get<BlobType>(uzr);
			switch (this_BlobType) {
				case BlobType::ROOT_NODE:
				case BlobType::TX_EVENT_NODE:
				case BlobType::RAE_INSTANCE_EDGE:
				case BlobType::TO_DELEGATE_EDGE:
				case BlobType::ENTITY_NODE:
				case BlobType::ATOMIC_ENTITY_NODE:
				case BlobType::ATOMIC_VALUE_NODE:
				case BlobType::RELATION_EDGE:
				case BlobType::DEFERRED_EDGE_LIST_NODE:
				case BlobType::ASSIGN_TAG_NAME_EDGE:
				case BlobType::FOREIGN_GRAPH_NODE:
				case BlobType::FOREIGN_ENTITY_NODE:
				case BlobType::FOREIGN_ATOMIC_ENTITY_NODE:
				case BlobType::FOREIGN_RELATION_EDGE: {
                    return true;
                }
				case BlobType::_unspecified:
				case BlobType::NEXT_TX_EDGE:
				case BlobType::DELEGATE_INSTANTIATION_EDGE:
				case BlobType::DELEGATE_RETIREMENT_EDGE:
				case BlobType::INSTANTIATION_EDGE:
				case BlobType::TERMINATION_EDGE:
				case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE:
				case BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE:
				case BlobType::ORIGIN_RAE_EDGE:
				case BlobType::ORIGIN_GRAPH_EDGE: {
                    return false;
                }
                default:
                    throw std::runtime_error("Shouldn't have got here in has_edge_list!");
			}
			return false; // should never reach here, suppress compiler warnings
		}

		// helper function to check whether such a list exists as an attribute
		inline bool has_data_buffer(EZefRef uzr) {
			BlobType this_BlobType = get<BlobType>(uzr);
			switch (this_BlobType) {
				case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE:
				case BlobType::ATOMIC_VALUE_NODE:
				case BlobType::ASSIGN_TAG_NAME_EDGE: {
                    return true;
                }
				case BlobType::_unspecified:
				case BlobType::NEXT_TX_EDGE:
				case BlobType::DELEGATE_INSTANTIATION_EDGE:
				case BlobType::DELEGATE_RETIREMENT_EDGE:
				case BlobType::INSTANTIATION_EDGE:
				case BlobType::TERMINATION_EDGE:
				case BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE:
				case BlobType::ORIGIN_RAE_EDGE:
				case BlobType::ORIGIN_GRAPH_EDGE:
				case BlobType::ROOT_NODE:
				case BlobType::TX_EVENT_NODE:
				case BlobType::RAE_INSTANCE_EDGE:
				case BlobType::TO_DELEGATE_EDGE:
				case BlobType::ENTITY_NODE:
				case BlobType::ATOMIC_ENTITY_NODE:
				case BlobType::RELATION_EDGE:
				case BlobType::DEFERRED_EDGE_LIST_NODE:
				case BlobType::FOREIGN_GRAPH_NODE:
				case BlobType::FOREIGN_ENTITY_NODE:
				case BlobType::FOREIGN_ATOMIC_ENTITY_NODE:
				case BlobType::FOREIGN_RELATION_EDGE: {
                    return false;
                }
                default:
                    throw std::runtime_error("Shouldn't have got here in has_edge_list!");
			}
			return false; // should never reach here, suppress compiler warnings
		}
		
		inline blob_index source_node_index(EZefRef uzr) {
            return visit_blob_with_source_target([](auto & s) { return s.source_node_index; },
                                                 uzr);
        }
		
        inline blob_index target_node_index(EZefRef uzr){
            return visit_blob_with_source_target([](auto & s) { return s.target_node_index; },
                                                 uzr);
        }

        // return a pointer: in apply apply_action_blob we want to overwrite this index
        template<class T>
        blob_index* subsequent_deferred_edge_list_index(T & s) {
            return &s.edges.indices[s.edges.local_capacity];
        }
        template<class T>
        const blob_index* subsequent_deferred_edge_list_index(const T & s) {
            return &s.edges.indices[s.edges.local_capacity];
        }
        template<>
        inline blob_index* subsequent_deferred_edge_list_index(EZefRef & uzr) {
            return visit_blob_with_edges([](auto & s) { return subsequent_deferred_edge_list_index(s); }, uzr);
        }

        // return a pointer: in apply apply_action_blob we want to overwrite this index
        template<class T>
        blob_index* last_edge_holding_blob(T & s) {
            return &s.edges.last_edge_holding_blob;
        }

        template<>
        inline blob_index* last_edge_holding_blob(blobs_ns::DEFERRED_EDGE_LIST_NODE & s) {
            throw std::runtime_error("Should never get here!");
        }

        template<>
        inline blob_index* last_edge_holding_blob(EZefRef & uzr) {
            return visit_blob_with_edges([](auto & x) { return(last_edge_holding_blob(x)); }, uzr);
        }
        
        template<class T>
        blob_index* edge_indexes(const T & s) {
            return (blob_index*)s.edges.indices;
        }
        template<>
        inline blob_index* edge_indexes(const EZefRef & uzr) {
            return visit_blob_with_edges([](auto & x) { return(edge_indexes(x)); }, uzr);
        }

        inline blob_index local_edge_indexes_capacity(EZefRef uzr) {
            return visit_blob_with_edges([](auto & s) { return s.edges.local_capacity; },
                                         uzr);
		}

		// template <typename T>
		// bool deferred_edge_list_exists(T& s) {
		// 	return s.edges.subsequent_deferred_edge_list_index != 0;
		// }
		
		// template <typename T>
		// bool edge_index_array_is_full(T& my_blob_struct) {			
        //     // The original version which should work.
		// 	// return my_blob_struct.edge_indexes[edge_indexes_capacity(my_blob_struct)-1] != 0;
        //     // Newer version - since I added occupancy, this is possible and
        //     // less prone to errors if the indices are no longer zeroed out.
		// 	return my_blob_struct.edges.occupancy == my_blob_struct.edges.local_capacity;
		// }

        // Utilty function used by last_set_edge_index and append_edge_index.
        // Finds the subindex: 0,1,2,3 which is the first empty edge index (or
        // subsequent list index)
        LIBZEF_DLL_EXPORTED int subindex_in_last_blob(blob_index * last_blob);
        LIBZEF_DLL_EXPORTED blob_index last_set_edge_index(EZefRef uzr);

        inline char* get_data_buffer(blobs_ns::ATOMIC_VALUE_NODE & blob) {
            // The data sits right after the edge list
            return (char*)&blob.edges.indices[blob.edges.local_capacity+1];
		};
        inline char* get_data_buffer(blobs_ns::ASSIGN_TAG_NAME_EDGE & blob) {
            // The data sits right after the edge list
            return (char*)&blob.edges.indices[blob.edges.local_capacity+1];
		};
        inline char* get_data_buffer(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE & blob) {
            // The data is explicit in this blob
            return blob.data_buffer;
		};

        inline const char* get_data_buffer(const blobs_ns::ATOMIC_VALUE_NODE & blob) {
            // The data sits right after the edge list
            return (char*)&blob.edges.indices[blob.edges.local_capacity+1];
		};
        inline const char* get_data_buffer(const blobs_ns::ASSIGN_TAG_NAME_EDGE & blob) {
            // The data sits right after the edge list
            return (char*)&blob.edges.indices[blob.edges.local_capacity+1];
		};
        inline const char* get_data_buffer(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE & blob) {
            // The data is explicit in this blob
            return blob.data_buffer;
		};

        inline const char* get_data_buffer(EZefRef ezr) {
            return visit_blob_with_data_buffer([](auto & x) { return(get_data_buffer(x)); }, ezr);
		};

        inline const size_t get_data_buffer_size(EZefRef ezr) {
            return visit_blob_with_data_buffer([](auto & x) { return(x.buffer_size_in_bytes); }, ezr);
		};
        

        // e.g. when determining the EZefRefs out_edges(my_uzr), we need to know how much space to allocate for the EZefRefs object.
		blob_index total_edge_index_list_size_upper_limit(EZefRef uzr);
	}



	// given an actual blob size in bytes: how many indexes (blobs_ns are spaced / aligned in units of blob_indx_step_in_bytes) 
	// do we have to move forward to reach the next blob
	constexpr blob_index num_blob_indexes_to_move(int blob_size_in_bytes) {
		return (blob_size_in_bytes / constants::blob_indx_step_in_bytes)
			+ ((blob_size_in_bytes % constants::blob_indx_step_in_bytes) != 0);
	}

	inline blob_index blob_index_from_ptr(void * ptr) {
		auto offset_in_bytes = (char*)ptr - (char*)MMap::blobs_ptr_from_blob(ptr);
		assert(offset_in_bytes % constants::blob_indx_step_in_bytes == 0);
		return blob_index(offset_in_bytes / constants::blob_indx_step_in_bytes);
	}



	inline blob_index blob_index_size(EZefRef b) {
		return num_blob_indexes_to_move(size_of_blob(b));
	}

	// depending on the size / possible overflow of the current ZefRef, the shift to the next
	// ZefRef does not only depend on the local ZefRef type. But it can be uniquely determined from
	// locally saved length variables and size.
	// Round up in units of blob_indx_step_in_bytes and move that distant forwards from current ZefRef
	inline EZefRef get_next(EZefRef b) {
		return EZefRef((void*)(std::uintptr_t(b.blob_ptr) + blob_index_size(b) * constants::blob_indx_step_in_bytes));
	}

    LIBZEF_DLL_EXPORTED json blob_to_json(EZefRef ezr);



//                   _ _                 _                  __                         _                _           _                                  
//                  (_) |_ ___ _ __ __ _| |_ ___  _ __     / _| ___  _ __      ___  __| | __ _  ___    (_)_ __   __| | _____  _____  ___               
//    _____ _____   | | __/ _ \ '__/ _` | __/ _ \| '__|   | |_ / _ \| '__|    / _ \/ _` |/ _` |/ _ \   | | '_ \ / _` |/ _ \ \/ / _ \/ __|  _____ _____ 
//   |_____|_____|  | | ||  __/ | | (_| | || (_) | |      |  _| (_) | |      |  __/ (_| | (_| |  __/   | | | | | (_| |  __/>  <  __/\__ \ |_____|_____|
//                  |_|\__\___|_|  \__,_|\__\___/|_|      |_|  \___/|_|       \___|\__,_|\__, |\___|   |_|_| |_|\__,_|\___/_/\_\___||___/              
//                                                                                       |___/                                                   



struct LIBZEF_DLL_EXPORTED AllEdgeIndexes {
	struct Iterator;
	struct Sentinel;
	EZefRef uzr_with_edges;
    bool force_to_write_head;

	AllEdgeIndexes() = delete;
    AllEdgeIndexes(EZefRef uzr, bool force_to_write_head=false)
    : uzr_with_edges(uzr),
    force_to_write_head(force_to_write_head) {};
    AllEdgeIndexes(ZefRef zr, bool force_to_write_head=false)
    : uzr_with_edges(zr.blob_uzr),
    force_to_write_head(force_to_write_head) {};

	Iterator begin() const;
	Sentinel end() const;
};

struct AllEdgeIndexes::Iterator {
	// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
	using value_type = blob_index;
	using reference = blob_index&;
	using pointer = blob_index*;
	using iterator_category = std::input_iterator_tag;
	using difference_type = ptrdiff_t;

	blob_index* ptr_to_current_edge_element = nullptr;
	EZefRef current_blob_pointed_to{ nullptr };
	blob_index* ptr_to_last_edge_element_in_current_blob = nullptr;
    // The read head is recorded at the start of the iteration, so that we have
    // stability in accessing the graph. For read-only graphs this is the
    // read-head, but for graphs that we are writing to, this is the write_head.
    blob_index stable_last_blob = 0;

	// pre-increment op: this one is used mostly
	Iterator& operator++();

	// post incremenet
	void operator++(int) {
		//AllEdgeIndexes::Iterator holder(*this);  // create copy to return before incrementing
		(void)++* this;
		//return holder;
	}

	reference operator*() {return *ptr_to_current_edge_element; }
	std::add_const_t<reference> operator*() const { return *ptr_to_current_edge_element; }


	bool operator==(const AllEdgeIndexes::Sentinel& sent) const;
	bool operator!=(const AllEdgeIndexes::Sentinel& sent) const;
	bool operator!=(const AllEdgeIndexes::Iterator& it) const { return true; };
	
	template <typename T>
	bool operator==(T&& t) const { return !(*this != std::forward<T>(t)); }
};


struct AllEdgeIndexes::Sentinel {
	bool operator!=(const Iterator& it) const
	{
		return it != *this;
	}
	template <typename T>
	bool operator==(T&& t) const { return !(*this != std::forward<T>(t)); }
};










LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ROOT_NODE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::TX_EVENT_NODE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::NEXT_TX_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::RAE_INSTANCE_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::TO_DELEGATE_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ENTITY_NODE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ATOMIC_ENTITY_NODE& this_blob);
std::string value_blob_to_str(AtomicEntityType buffer_type, const char* buffer_ptr, unsigned int buffer_size=0);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ATOMIC_VALUE_NODE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::RELATION_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::DELEGATE_INSTANTIATION_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::DELEGATE_RETIREMENT_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::INSTANTIATION_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::TERMINATION_EDGE& this_blob);
	// local representation only: only show edge indexes saved in this DEFERRED_EDGE_LIST_NODE
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::DEFERRED_EDGE_LIST_NODE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ASSIGN_TAG_NAME_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::NEXT_TAG_NAME_ASSIGNMENT_EDGE& this_blob);


LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::FOREIGN_GRAPH_NODE& this_blob);

LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ORIGIN_RAE_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ORIGIN_GRAPH_EDGE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::FOREIGN_ENTITY_NODE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::FOREIGN_ATOMIC_ENTITY_NODE& this_blob);
LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::FOREIGN_RELATION_EDGE& this_blob);

    // If anyone can understand why I need this, and can't just use << (in
    // butler_handlers_graph_manager.cpp) then please fix it and tell me!
    template<class T>
    std::ostream& manual_os_call (std::ostream& os, const T& this_blob) {
        os << this_blob;
        return os;
    }



using ScalarVariant = std::variant<
    bool,
    int,
    double,
    str,
    Time,
    ZefEnumValue,
    QuantityFloat,
    QuantityInt,

    BlobType,
    EntityType,
    RelationType
>;

	inline bool operator<= (EZefRef uzr, BlobType bt) { return BT(uzr) == bt; }

	inline bool operator<= (EZefRef uzr, AtomicEntityTypeStruct AET) { return uzr <= BT.ATOMIC_ENTITY_NODE; }
	inline bool operator<= (EZefRef uzr, EntityTypeStruct ET) { return uzr <= BT.ENTITY_NODE; }
	inline bool operator<= (EZefRef uzr, RelationTypeStruct RT) { return uzr <= BT.RELATION_EDGE; }

	inline bool operator<= (EZefRef uzr, EntityType et) { return (uzr <= ET) && (ET(uzr) == et); }
	inline bool operator<= (EZefRef uzr, RelationType rt) { return (uzr <= RT) && (RT(uzr) == rt); }


	inline bool operator<= (EZefRef uzr, AtomicEntityType aet_super) { return (uzr <= AET) && (AET(uzr) <= aet_super); }
    inline bool operator<= (EZefRef uzr, AtomicEntityTypeStruct::Enum_ aet_super) { return (uzr <= AET) && (AET(uzr) <= aet_super); }
    inline bool operator<= (EZefRef uzr, AtomicEntityTypeStruct::QuantityFloat_ aet_super) { return (uzr <= AET) && (AET(uzr) <= aet_super); }
    inline bool operator<= (EZefRef uzr, AtomicEntityTypeStruct::QuantityInt_ aet_super) { return (uzr <= AET) && (AET(uzr) <= aet_super); }

	inline bool operator<= (ZefRef zr, BlobType bt) { return zr.blob_uzr <= bt; }

	inline bool operator<= (ZefRef zr, AtomicEntityTypeStruct AET) { return zr.blob_uzr <= AET; }
	inline bool operator<= (ZefRef zr, EntityTypeStruct ET) { return zr.blob_uzr <= ET; }
	inline bool operator<= (ZefRef zr, RelationTypeStruct RT) { return zr.blob_uzr <= RT; }

	inline bool operator<= (ZefRef zr, EntityType et) { return zr.blob_uzr <= et; }
	inline bool operator<= (ZefRef zr, RelationType rt) { return zr.blob_uzr <= rt; }


	inline bool operator<= (ZefRef zr, AtomicEntityType aet_super) { return zr.blob_uzr <= aet_super; }
    inline bool operator<= (ZefRef zr, AtomicEntityTypeStruct::Enum_ aet_super) { return zr.blob_uzr <= aet_super; }
    inline bool operator<= (ZefRef zr, AtomicEntityTypeStruct::QuantityFloat_ aet_super) { return zr.blob_uzr <= aet_super; }
    inline bool operator<= (ZefRef zr, AtomicEntityTypeStruct::QuantityInt_ aet_super) { return zr.blob_uzr <= aet_super; }


}
