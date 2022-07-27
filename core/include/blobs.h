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

            edge_info(const edge_info &) = delete;
            edge_info & operator=(const edge_info &) = delete;
        };
		
		struct _unspecified {
			BlobType this_BlobType = BlobType::_unspecified;
            _unspecified & operator=(const _unspecified & other) = delete;
            _unspecified (const _unspecified & other) = delete;
		};
				
		struct ROOT_NODE {
			BlobType this_BlobType = BlobType::ROOT_NODE;
			short actual_written_data_layout_version_info_size = 0;  // save this info separately in case we want to ever allow uids in raw form where '\0' may be contained
			short actual_written_graph_revision_info_size = 0;
			BaseUID uid;
			char data_layout_version_info[constants::data_layout_version_info_size]; // actual text needs to be null terminated '\0' withtin this range
			char graph_revision_info[constants::graph_revision_info_size];
            edge_info edges{constants::default_local_edge_indexes_capacity_ROOT_NODE};
            ROOT_NODE & operator=(const ROOT_NODE & other) = delete;
            ROOT_NODE (const ROOT_NODE & other) = delete;
            ROOT_NODE () = default;
		};

		struct TX_EVENT_NODE {
			BlobType this_BlobType = BlobType::TX_EVENT_NODE;
			Time time = Time{ std::numeric_limits<double>::quiet_NaN() };	 // time stamp when this transcation occurred
			TimeSlice time_slice = {0};  // a counter going up with each subsequent event
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_TX_EVENT_NODE};
            TX_EVENT_NODE & operator=(const TX_EVENT_NODE & other) = delete;
            TX_EVENT_NODE (const TX_EVENT_NODE & other) = delete;
            TX_EVENT_NODE () = default;
		};

		struct NEXT_TX_EDGE {
			BlobType this_BlobType = BlobType::NEXT_TX_EDGE;
			blob_index source_node_index = 0;  // coming from a TX_EVENT_NODE or ROOTNode
			blob_index target_node_index = 0;	 // going to a 	ATTRIBUTE_ENTITY_NODE_*, ENTITY_NODE_*, RELATION_EDGE_*
            NEXT_TX_EDGE & operator=(const NEXT_TX_EDGE & other) = delete;
            NEXT_TX_EDGE (const NEXT_TX_EDGE & other) = delete;
            NEXT_TX_EDGE () = default;
		};

		struct RAE_INSTANCE_EDGE {
			BlobType this_BlobType = BlobType::RAE_INSTANCE_EDGE;
			blob_index source_node_index = 0;  // coming from the ROOT_node
			blob_index target_node_index = 0;	 // going to a 	ATTRIBUTE_ENTITY_NODE_*, ENTITY_NODE_*, RELATION_EDGE_*
            edge_info edges{constants::default_local_edge_indexes_capacity_RAE_INSTANCE_EDGE};
            RAE_INSTANCE_EDGE & operator=(const RAE_INSTANCE_EDGE & other) = delete;
            RAE_INSTANCE_EDGE (const RAE_INSTANCE_EDGE & other) = delete;
            RAE_INSTANCE_EDGE () = default;
		};				

		struct TO_DELEGATE_EDGE {
			BlobType this_BlobType = BlobType::TO_DELEGATE_EDGE;
			blob_index source_node_index = 0;  // coming from the "_AllEntities" node
			blob_index target_node_index = 0;	 // going to the delegate node
            edge_info edges{constants::default_local_edge_indexes_capacity_TO_DELEGATE_EDGE};
            TO_DELEGATE_EDGE & operator=(const TO_DELEGATE_EDGE & other) = delete;
            TO_DELEGATE_EDGE (const TO_DELEGATE_EDGE & other) = delete;
            TO_DELEGATE_EDGE () = default;
		};		

		struct ENTITY_NODE {
			BlobType this_BlobType = BlobType::ENTITY_NODE;
			EntityType entity_type = ET.ZEF_Unspecified;
			TimeSlice instantiation_time_slice = { 0 };
			TimeSlice termination_time_slice = { 0 };
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_ENTITY_NODE};
            ENTITY_NODE & operator=(const ENTITY_NODE & other) = delete;
            ENTITY_NODE (const ENTITY_NODE & other) = delete;
            ENTITY_NODE () = default;
		};
		
        // This was called ATOMIC_ENTITY_NODE in data layout <= 0.2.0.
        // The layout did not change in 0.3.0.
		struct ATTRIBUTE_ENTITY_NODE {
			BlobType this_BlobType = BlobType::ATTRIBUTE_ENTITY_NODE;
			ValueRepType primitive_type;
			TimeSlice instantiation_time_slice = { 0 };
			TimeSlice termination_time_slice = { 0 };
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_ATTRIBUTE_ENTITY_NODE};
            ATTRIBUTE_ENTITY_NODE& operator=(const ATTRIBUTE_ENTITY_NODE & other) = delete;
            ATTRIBUTE_ENTITY_NODE(const ATTRIBUTE_ENTITY_NODE & other) = delete;
            ATTRIBUTE_ENTITY_NODE() = default;
		};

        // Introduced in data layout 0.3.0
		struct VALUE_TYPE_EDGE {
			BlobType this_BlobType = BlobType::VALUE_TYPE_EDGE;
			blob_index source_node_index = 0;  // coming from an ATTRIBUTE_ENTITY_NODE
			blob_index target_node_index = 0;  // goes to an VALUE_NODE
            VALUE_TYPE_EDGE& operator=(const VALUE_TYPE_EDGE & other) = delete;
            VALUE_TYPE_EDGE(const VALUE_TYPE_EDGE & other) = delete;
            VALUE_TYPE_EDGE() = default;
		};

        // Introduced in data layout 0.3.0
		struct VALUE_EDGE {
			BlobType this_BlobType = BlobType::VALUE_EDGE;
			blob_index source_node_index = 0;  // coming from an ATTRIBUTE_VALUE_ASSIGNMENT_EDGE
			blob_index target_node_index = 0;  // goes to an VALUE_NODE
            VALUE_EDGE& operator=(const VALUE_EDGE & other) = delete;
            VALUE_EDGE(const VALUE_EDGE & other) = delete;
            VALUE_EDGE() = default;
		};
		
        // Even though this was present earlier, it wasn't used until data layout 0.3.0
		struct VALUE_NODE {
			BlobType this_BlobType = BlobType::VALUE_NODE;
			unsigned int buffer_size_in_bytes = 0;   // this needs to be set specifically for each data type (not only data of variable size!)
            // This is the start of the data_buffer but that must contain the rep_type too
			ValueRepType rep_type;

            // There is also an edge info but we need to dynamically calculate its offset based on the data_buffer size
            // edge_info edges{constants::default_local_edge_indexes_capacity_ATOMIC_VALUE_NODE};

            VALUE_NODE& operator=(const VALUE_NODE & other) = delete;
            VALUE_NODE(const VALUE_NODE & other) = delete;
            VALUE_NODE() = default;
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
            RELATION_EDGE & operator=(const RELATION_EDGE & other) = delete;
            RELATION_EDGE(const RELATION_EDGE & other) = delete; 
            RELATION_EDGE() = default;
        };
		
		// these edges are created together with any new entity node or domain edge
		struct DELEGATE_INSTANTIATION_EDGE {
			BlobType this_BlobType = BlobType::DELEGATE_INSTANTIATION_EDGE;
			blob_index source_node_index = 0;  // coming from a TX_EVENT_NODE
			blob_index target_node_index = 0;	 // going to a TO_DELEGATE_EDGE edge 
            DELEGATE_INSTANTIATION_EDGE & operator=(const DELEGATE_INSTANTIATION_EDGE & other) = delete;
            DELEGATE_INSTANTIATION_EDGE(const DELEGATE_INSTANTIATION_EDGE & other) = delete; 
            DELEGATE_INSTANTIATION_EDGE() = default;
        };		

		struct DELEGATE_RETIREMENT_EDGE {
			BlobType this_BlobType = BlobType::DELEGATE_RETIREMENT_EDGE;
			blob_index source_node_index = 0;  // coming from a TX_EVENT_NODE
			blob_index target_node_index = 0;	 // going to a TO_DELEGATE_EDGE edge 
            DELEGATE_RETIREMENT_EDGE & operator=(const DELEGATE_RETIREMENT_EDGE & other) = delete;
            DELEGATE_RETIREMENT_EDGE(const DELEGATE_RETIREMENT_EDGE & other) = delete; 
            DELEGATE_RETIREMENT_EDGE() = default;
        };		

		// these edges are created together with any new entity node or domain edge
		struct INSTANTIATION_EDGE {
			BlobType this_BlobType = BlobType::INSTANTIATION_EDGE;
			blob_index source_node_index = 0;  // coming from a TX_EVENT_NODE
			blob_index target_node_index = 0;	 // going to the entity node between the scneario node and the newly created node
            INSTANTIATION_EDGE & operator=(const INSTANTIATION_EDGE & other) = delete;
            INSTANTIATION_EDGE(const INSTANTIATION_EDGE & other) = delete; 
            INSTANTIATION_EDGE() = default;
        };
				// these edges are created together with any new entity node or domain edge
		struct TERMINATION_EDGE {
			BlobType this_BlobType = BlobType::TERMINATION_EDGE;
			blob_index source_node_index = 0;  // coming from a TX_EVENT_NODE
			blob_index target_node_index = 0;	 // going to the entity node between the scneario node and the newly created node
            TERMINATION_EDGE & operator=(const TERMINATION_EDGE & other) = delete;
            TERMINATION_EDGE(const TERMINATION_EDGE & other) = delete; 
            TERMINATION_EDGE() = default;
        };

        // Deprecated in data layout 0.3.0
		struct ATOMIC_VALUE_ASSIGNMENT_EDGE {
			BlobType this_BlobType = BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE;
			ValueRepType rep_type;
			unsigned int buffer_size_in_bytes = 0;    // this needs to be set specifically for each data type (not only data of variable size!)
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
			char data_buffer[1];	// for any type larger than a char, this is designed to overflow
            ATOMIC_VALUE_ASSIGNMENT_EDGE & operator=(const ATOMIC_VALUE_ASSIGNMENT_EDGE & other) = delete;
            ATOMIC_VALUE_ASSIGNMENT_EDGE(const ATOMIC_VALUE_ASSIGNMENT_EDGE & other) = delete; 
            ATOMIC_VALUE_ASSIGNMENT_EDGE() = default;
        };

        // Introduced in data layout 0.3.0
		struct ATTRIBUTE_VALUE_ASSIGNMENT_EDGE {
			BlobType this_BlobType = BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE;
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
            // This is in lieu of a proper edge list, so that we stay within one
            // blob (an edge list would have to include sizes plus deferred edge list capability)
            blob_index value_edge_index = 0;
            ATTRIBUTE_VALUE_ASSIGNMENT_EDGE & operator=(const ATTRIBUTE_VALUE_ASSIGNMENT_EDGE & other) = delete;
            ATTRIBUTE_VALUE_ASSIGNMENT_EDGE(const ATTRIBUTE_VALUE_ASSIGNMENT_EDGE & other) = delete; 
            ATTRIBUTE_VALUE_ASSIGNMENT_EDGE() = default;
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
            DEFERRED_EDGE_LIST_NODE & operator=(const DEFERRED_EDGE_LIST_NODE & other) = delete;
            DEFERRED_EDGE_LIST_NODE(const DEFERRED_EDGE_LIST_NODE & other) = delete; 
            DEFERRED_EDGE_LIST_NODE() = default;
        };

        struct ASSIGN_TAG_NAME_EDGE {   // this is an edge between the tx in which it was assigned and the 'TO_INSTANCE' edge for the specific Blob to be tagged*
			BlobType this_BlobType = BlobType::ASSIGN_TAG_NAME_EDGE;
			unsigned int buffer_size_in_bytes = 0;
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
			// char rel_ent_tag_name_buffer[constants::rel_ent_tag_name_buffer_size];	// fixed size: a larger size is not permitted. We don't want two data structures that are allowed to overflow
            edge_info edges{constants::default_local_edge_indexes_capacity_ASSIGN_TAG_NAME_EDGE};
            ASSIGN_TAG_NAME_EDGE & operator=(const ASSIGN_TAG_NAME_EDGE & other) = delete;
            ASSIGN_TAG_NAME_EDGE(const ASSIGN_TAG_NAME_EDGE & other) = delete; 
            ASSIGN_TAG_NAME_EDGE() = default;
		};

        struct NEXT_TAG_NAME_ASSIGNMENT_EDGE {   // can be inserted between two BlobType::ASSIGN_TAG_NAME_EDGE to enable efficient temporal resolving of tag name values
            BlobType this_BlobType = BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE;			
            blob_index source_node_index = 0;
            blob_index target_node_index = 0;
            NEXT_TAG_NAME_ASSIGNMENT_EDGE & operator=(const NEXT_TAG_NAME_ASSIGNMENT_EDGE & other) = delete;
            NEXT_TAG_NAME_ASSIGNMENT_EDGE(const NEXT_TAG_NAME_ASSIGNMENT_EDGE & other) = delete; 
            NEXT_TAG_NAME_ASSIGNMENT_EDGE() = default;
        };


		struct FOREIGN_GRAPH_NODE {
			BlobType this_BlobType = BlobType::FOREIGN_GRAPH_NODE;
			int internal_foreign_graph_index = 0;   // what is the internal visitor number for this graph? For the foreign graph delegate node, this remains set to 0 (no action is triggered)
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_FOREIGN_GRAPH_NODE};
            FOREIGN_GRAPH_NODE & operator=(const FOREIGN_GRAPH_NODE & other) = delete;
            FOREIGN_GRAPH_NODE(const FOREIGN_GRAPH_NODE & other) = delete; 
            FOREIGN_GRAPH_NODE() = default;
        };

		struct ORIGIN_RAE_EDGE {
			BlobType this_BlobType = BlobType::ORIGIN_RAE_EDGE;
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
            ORIGIN_RAE_EDGE & operator=(const ORIGIN_RAE_EDGE & other) = delete;
            ORIGIN_RAE_EDGE(const ORIGIN_RAE_EDGE & other) = delete; 
            ORIGIN_RAE_EDGE() = default;
        };

		struct ORIGIN_GRAPH_EDGE {
			BlobType this_BlobType = BlobType::ORIGIN_GRAPH_EDGE;
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
            ORIGIN_GRAPH_EDGE & operator=(const ORIGIN_GRAPH_EDGE & other) = delete;
            ORIGIN_GRAPH_EDGE(const ORIGIN_GRAPH_EDGE & other) = delete; 
            ORIGIN_GRAPH_EDGE() = default;
        };

		struct FOREIGN_ENTITY_NODE {
			BlobType this_BlobType = BlobType::FOREIGN_ENTITY_NODE;
			EntityType entity_type = ET.ZEF_Unspecified;
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_FOREIGN_ENTITY_NODE};
            FOREIGN_ENTITY_NODE & operator=(const FOREIGN_ENTITY_NODE & other) = delete;
            FOREIGN_ENTITY_NODE(const FOREIGN_ENTITY_NODE & other) = delete; 
            FOREIGN_ENTITY_NODE() = default;
        };

        // This was called FOREIGN_ATOMIC_ENTITY_NODE in data layout <= 0.2.0.
        // The layout did not change in 0.3.0.
		struct FOREIGN_ATTRIBUTE_ENTITY_NODE {
			BlobType this_BlobType = BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE;
			ValueRepType primitive_type;
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_FOREIGN_ATTRIBUTE_ENTITY_NODE};
            FOREIGN_ATTRIBUTE_ENTITY_NODE & operator=(const FOREIGN_ATTRIBUTE_ENTITY_NODE & other) = delete;
            FOREIGN_ATTRIBUTE_ENTITY_NODE(const FOREIGN_ATTRIBUTE_ENTITY_NODE & other) = delete; 
            FOREIGN_ATTRIBUTE_ENTITY_NODE() = default;
        };

		struct FOREIGN_RELATION_EDGE {
			BlobType this_BlobType = BlobType::FOREIGN_RELATION_EDGE;
			RelationType relation_type = RT.ZEF_Unspecified;
			blob_index source_node_index = 0;
			blob_index target_node_index = 0;
			BaseUID uid;
            edge_info edges{constants::default_local_edge_indexes_capacity_FOREIGN_RELATION_EDGE};
            FOREIGN_RELATION_EDGE & operator=(const FOREIGN_RELATION_EDGE & other) = delete;
            FOREIGN_RELATION_EDGE(const FOREIGN_RELATION_EDGE & other) = delete; 
            FOREIGN_RELATION_EDGE() = default;
        };


	} //namespace blobs_ns



    // General note: all versions of functions with _ (e.g. _size_of_blob) act
    // on pointers, but this is DANGEROUS, as the memory may not be loaded (lazy
    // loading). Hence, these are protected behind a differnt name rather than
    // using function overloading.


	// Return the size of the ZefRef in bytes, accounting for possible overflow etc.
	// Future improvement: use std::visit with std::variant blobs_ns to make this robust to changes in the BlobType field location.
	// But this should hardly ever change and we can catch it with tests
	LIBZEF_DLL_EXPORTED blob_index _size_of_blob(void * ptr);
	inline blob_index size_of_blob(EZefRef b) {
        return _size_of_blob(b.blob_ptr);
    }

	// similar to std::visit for std::variants. Given a uzr, will dispatch a function overloaded for all relevant blob types to the respective one.
	const auto _visit_blob = [](auto fct_to_apply, void * ptr) {
		switch (get<BlobType>(ptr)) {
		case BlobType::_unspecified: { throw std::runtime_error("visit called for an unspecified EZefRef");  }
		case BlobType::ROOT_NODE: { return fct_to_apply(*((blobs_ns::ROOT_NODE*)ptr)); }
		case BlobType::TX_EVENT_NODE: { return fct_to_apply(*((blobs_ns::TX_EVENT_NODE*)ptr)); }
		case BlobType::RAE_INSTANCE_EDGE: { return fct_to_apply(*((blobs_ns::RAE_INSTANCE_EDGE*)ptr)); }
		case BlobType::TO_DELEGATE_EDGE: { return fct_to_apply(*((blobs_ns::TO_DELEGATE_EDGE*)ptr)); }
		case BlobType::NEXT_TX_EDGE: { return fct_to_apply(*((blobs_ns::NEXT_TX_EDGE*)ptr)); }
		case BlobType::ENTITY_NODE: { return fct_to_apply(*((blobs_ns::ENTITY_NODE*)ptr)); }
		case BlobType::ATTRIBUTE_ENTITY_NODE: { return fct_to_apply(*((blobs_ns::ATTRIBUTE_ENTITY_NODE*)ptr)); }
		case BlobType::VALUE_NODE: { return fct_to_apply(*((blobs_ns::VALUE_NODE*)ptr)); }
		case BlobType::RELATION_EDGE: { return fct_to_apply(*((blobs_ns::RELATION_EDGE*)ptr)); }
		case BlobType::DELEGATE_INSTANTIATION_EDGE: { return fct_to_apply(*((blobs_ns::DELEGATE_INSTANTIATION_EDGE*)ptr)); }
		case BlobType::DELEGATE_RETIREMENT_EDGE: { return fct_to_apply(*((blobs_ns::DELEGATE_RETIREMENT_EDGE*)ptr)); }
		case BlobType::INSTANTIATION_EDGE: { return fct_to_apply(*((blobs_ns::INSTANTIATION_EDGE*)ptr)); }
		case BlobType::TERMINATION_EDGE: { return fct_to_apply(*((blobs_ns::TERMINATION_EDGE*)ptr)); }
		case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE*)ptr)); }
		case BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE*)ptr)); }
		case BlobType::DEFERRED_EDGE_LIST_NODE: { return fct_to_apply(*((blobs_ns::DEFERRED_EDGE_LIST_NODE*)ptr)); }
		case BlobType::ASSIGN_TAG_NAME_EDGE: { return fct_to_apply(*((blobs_ns::ASSIGN_TAG_NAME_EDGE*)ptr)); }
		case BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::NEXT_TAG_NAME_ASSIGNMENT_EDGE*)ptr)); }
		case BlobType::FOREIGN_GRAPH_NODE: { return fct_to_apply(*((blobs_ns::FOREIGN_GRAPH_NODE*)ptr)); }
		case BlobType::ORIGIN_RAE_EDGE: { return fct_to_apply(*((blobs_ns::ORIGIN_RAE_EDGE*)ptr)); }
		case BlobType::ORIGIN_GRAPH_EDGE: { return fct_to_apply(*((blobs_ns::ORIGIN_GRAPH_EDGE*)ptr)); }
		case BlobType::FOREIGN_ENTITY_NODE: { return fct_to_apply(*((blobs_ns::FOREIGN_ENTITY_NODE*)ptr)); }
		case BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE: { return fct_to_apply(*((blobs_ns::FOREIGN_ATTRIBUTE_ENTITY_NODE*)ptr)); }
		case BlobType::FOREIGN_RELATION_EDGE: { return fct_to_apply(*((blobs_ns::FOREIGN_RELATION_EDGE*)ptr)); }
		case BlobType::VALUE_TYPE_EDGE: { return fct_to_apply(*((blobs_ns::VALUE_TYPE_EDGE*)ptr)); }
		case BlobType::VALUE_EDGE: { return fct_to_apply(*((blobs_ns::VALUE_EDGE*)ptr)); }
        default: { print_backtrace(); throw std::runtime_error("Unknown blob type"); }
		}
	};
	const auto visit_blob = [](auto fct_to_apply, EZefRef uzr) {		
        return _visit_blob(fct_to_apply, uzr.blob_ptr);
    };

    template<class T>
    blobs_ns::edge_info & blob_edge_info(T & s) {
        return s.edges;
    }
    template<class T>
    const blobs_ns::edge_info & blob_edge_info(const T & s) {
        return s.edges;
    }

    template<>
    inline blobs_ns::edge_info & blob_edge_info(blobs_ns::VALUE_NODE & s) {
        uintptr_t ptr = (uintptr_t)&s.rep_type + sizeof(decltype(s.rep_type)) + s.buffer_size_in_bytes;
        // We need to make sure the blob edge info is aligned to a blob_index so
        // that our requirements for edges to be able to exist in sets of 4 per
        // blob.
        if(ptr % sizeof(blob_index) != 0)
            ptr += sizeof(blob_index) - (ptr % sizeof(blob_index));

        return *(blobs_ns::edge_info*)ptr;
    }
    template<>
    inline const blobs_ns::edge_info & blob_edge_info(const blobs_ns::VALUE_NODE & s) {
        return *(blobs_ns::edge_info*)((char*)&s.rep_type + sizeof(decltype(s.rep_type)) + s.buffer_size_in_bytes);
    }

    inline blobs_ns::DEFERRED_EDGE_LIST_NODE::deferred_edge_info & blob_edge_info(blobs_ns::DEFERRED_EDGE_LIST_NODE & s) {
        return s.edges;
    }
    inline const blobs_ns::DEFERRED_EDGE_LIST_NODE::deferred_edge_info & blob_edge_info(const blobs_ns::DEFERRED_EDGE_LIST_NODE & s) {
        return s.edges;
    }


    const auto _visit_blob_with_edges_internal = [](auto fct_to_apply, auto & x) {
        return fct_to_apply(blob_edge_info(x));
    };

    const auto _visit_blob_with_edges = [](auto fct_to_apply, void * ptr) {		
		switch (get<BlobType>(ptr)) {
		case BlobType::_unspecified: { throw std::runtime_error("visit called for an unspecified EZefRef");  }
		case BlobType::ROOT_NODE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::ROOT_NODE*)ptr)); }
		case BlobType::TX_EVENT_NODE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::TX_EVENT_NODE*)ptr)); }
		case BlobType::RAE_INSTANCE_EDGE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::RAE_INSTANCE_EDGE*)ptr)); }
		case BlobType::TO_DELEGATE_EDGE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::TO_DELEGATE_EDGE*)ptr)); }
		case BlobType::ENTITY_NODE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::ENTITY_NODE*)ptr)); }
		case BlobType::ATTRIBUTE_ENTITY_NODE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::ATTRIBUTE_ENTITY_NODE*)ptr)); }
		case BlobType::VALUE_NODE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::VALUE_NODE*)ptr)); }
        // An ATTRIBUTE_VALUE_ASSIGNMENT_EDGE doesn't have an edges member, only a single edge
        case BlobType::RELATION_EDGE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::RELATION_EDGE*)ptr)); }
		case BlobType::DEFERRED_EDGE_LIST_NODE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::DEFERRED_EDGE_LIST_NODE*)ptr)); }
		case BlobType::ASSIGN_TAG_NAME_EDGE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::ASSIGN_TAG_NAME_EDGE*)ptr)); }
		case BlobType::FOREIGN_GRAPH_NODE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::FOREIGN_GRAPH_NODE*)ptr)); }
		case BlobType::FOREIGN_ENTITY_NODE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::FOREIGN_ENTITY_NODE*)ptr)); }
		case BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::FOREIGN_ATTRIBUTE_ENTITY_NODE*)ptr)); }
		case BlobType::FOREIGN_RELATION_EDGE: { return _visit_blob_with_edges_internal(fct_to_apply, *((blobs_ns::FOREIGN_RELATION_EDGE*)ptr)); }
        default: { print_backtrace(); throw std::runtime_error("Blobtype expected to have edges but it didn't"); }
        }
	};
	const auto visit_blob_with_edges = [](auto fct_to_apply, EZefRef uzr) {		
        return _visit_blob_with_edges(fct_to_apply, uzr.blob_ptr);
    };

	const auto _visit_blob_with_source_target = [](auto fct_to_apply, void * ptr) {		
		switch (get<BlobType>(ptr)) {
		case BlobType::_unspecified: { throw std::runtime_error("visit called for an unspecified EZefRef");  }
        case BlobType::RAE_INSTANCE_EDGE: { return fct_to_apply(*((blobs_ns::RAE_INSTANCE_EDGE*)ptr)); }
        case BlobType::TO_DELEGATE_EDGE: { return fct_to_apply(*((blobs_ns::TO_DELEGATE_EDGE*)ptr)); }
        case BlobType::NEXT_TX_EDGE: { return fct_to_apply(*((blobs_ns::NEXT_TX_EDGE*)ptr)); }
        case BlobType::RELATION_EDGE: { return fct_to_apply(*((blobs_ns::RELATION_EDGE*)ptr)); }
        case BlobType::DELEGATE_INSTANTIATION_EDGE: { return fct_to_apply(*((blobs_ns::DELEGATE_INSTANTIATION_EDGE*)ptr)); }
        case BlobType::DELEGATE_RETIREMENT_EDGE: { return fct_to_apply(*((blobs_ns::DELEGATE_RETIREMENT_EDGE*)ptr)); }
        case BlobType::INSTANTIATION_EDGE: { return fct_to_apply(*((blobs_ns::INSTANTIATION_EDGE*)ptr)); }
        case BlobType::TERMINATION_EDGE: { return fct_to_apply(*((blobs_ns::TERMINATION_EDGE*)ptr)); }
        case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE*)ptr)); }
        case BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE*)ptr)); }
        case BlobType::ASSIGN_TAG_NAME_EDGE: { return fct_to_apply(*((blobs_ns::ASSIGN_TAG_NAME_EDGE*)ptr)); }
        case BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::NEXT_TAG_NAME_ASSIGNMENT_EDGE*)ptr)); }
        case BlobType::ORIGIN_RAE_EDGE: { return fct_to_apply(*((blobs_ns::ORIGIN_RAE_EDGE*)ptr)); }
        case BlobType::ORIGIN_GRAPH_EDGE: { return fct_to_apply(*((blobs_ns::ORIGIN_GRAPH_EDGE*)ptr)); }
        case BlobType::FOREIGN_RELATION_EDGE: { { return fct_to_apply(*((blobs_ns::FOREIGN_RELATION_EDGE*)ptr)); } }
        case BlobType::VALUE_TYPE_EDGE: { { return fct_to_apply(*((blobs_ns::VALUE_TYPE_EDGE*)ptr)); } }
        case BlobType::VALUE_EDGE: { { return fct_to_apply(*((blobs_ns::VALUE_TYPE_EDGE*)ptr)); } }
        default: { print_backtrace(); throw std::runtime_error("Blobtype expected to have source/target but it didn't"); }
        }
	};
	const auto visit_blob_with_source_target = [](auto fct_to_apply, EZefRef uzr) {		
        return _visit_blob_with_source_target(fct_to_apply, uzr.blob_ptr);
    };

	const auto _visit_blob_with_data_buffer = [](auto fct_to_apply, void * ptr) {		
		switch (get<BlobType>(ptr)) {
		case BlobType::_unspecified: { throw std::runtime_error("visit called for an unspecified EZefRef");  }
        case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE: { return fct_to_apply(*((blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE*)ptr)); }
        case BlobType::ASSIGN_TAG_NAME_EDGE: { return fct_to_apply(*((blobs_ns::ASSIGN_TAG_NAME_EDGE*)ptr)); }
        case BlobType::VALUE_NODE: { return fct_to_apply(*((blobs_ns::VALUE_NODE*)ptr)); }
        default: { print_backtrace(); throw std::runtime_error("Blobtype expected to have data buffer but it didn't"); }
        }
	};
	const auto visit_blob_with_data_buffer = [](auto fct_to_apply, EZefRef uzr) {		
        return _visit_blob_with_data_buffer(fct_to_apply, uzr.blob_ptr);
    };


	struct LIBZEF_DLL_EXPORTED Sentinel {}; // used throughout. TODO: Sentinel was introduced all over the place within structs: migrate all of them to use this.
	LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, Sentinel sent);

	namespace internals {
        size_t edge_list_end_offset(EZefRef uzr);
        void static_checks();

		// return a pointer to the buffer storing the 8 byte uid.
		// The buffer location offset within the blob depends on the blob type.
		// this function dispatches correctly.
		inline BaseUID& _blob_uid_ref(void * ptr){
			using namespace blobs_ns;
			switch(get<BlobType>(ptr)){
				case BlobType::ROOT_NODE: 					{return get<ROOT_NODE>(ptr).uid; }
				case BlobType::TX_EVENT_NODE: 				{return get<TX_EVENT_NODE>(ptr).uid; }
				case BlobType::ENTITY_NODE: 				{return get<ENTITY_NODE>(ptr).uid; }
				case BlobType::ATTRIBUTE_ENTITY_NODE:		{return get<ATTRIBUTE_ENTITY_NODE>(ptr).uid; }
				case BlobType::RELATION_EDGE: 				{return get<RELATION_EDGE>(ptr).uid; }
				case BlobType::FOREIGN_GRAPH_NODE: 			{return get<FOREIGN_GRAPH_NODE>(ptr).uid; }
				case BlobType::FOREIGN_ENTITY_NODE: 		{return get<FOREIGN_ENTITY_NODE>(ptr).uid; }
				case BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE:	{return get<FOREIGN_ATTRIBUTE_ENTITY_NODE>(ptr).uid; }
				case BlobType::FOREIGN_RELATION_EDGE: 		{return get<FOREIGN_RELATION_EDGE>(ptr).uid; }

            default: {print_backtrace_force(); throw std::runtime_error("blob_uid_ref called for ZefRef without a uid"); }
			}
		}
		inline BaseUID& blob_uid_ref(EZefRef uzr){
            return _blob_uid_ref(uzr.blob_ptr);
        }

		inline bool _has_uid(void * ptr){
			using namespace blobs_ns;
			switch(get<BlobType>(ptr)){
				case BlobType::ROOT_NODE:
				case BlobType::TX_EVENT_NODE:
				case BlobType::ENTITY_NODE:
				case BlobType::ATTRIBUTE_ENTITY_NODE:
				case BlobType::RELATION_EDGE:
				case BlobType::FOREIGN_GRAPH_NODE:
				case BlobType::FOREIGN_ENTITY_NODE:
				case BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE:
				case BlobType::FOREIGN_RELATION_EDGE:
                    return true;
				default:
                    return false;
			}
		}
		inline bool has_uid(EZefRef uzr){
            return _has_uid(uzr.blob_ptr);
        }

		inline bool _has_source_target_node(void * ptr) {
			BlobType this_BlobType = get<BlobType>(ptr);
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
				case BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE:
				case BlobType::ASSIGN_TAG_NAME_EDGE:
				case BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE:
				case BlobType::ORIGIN_RAE_EDGE:
				case BlobType::ORIGIN_GRAPH_EDGE:
				case BlobType::VALUE_TYPE_EDGE:
				case BlobType::VALUE_EDGE:
				case BlobType::FOREIGN_RELATION_EDGE: {
                    return true;
                }
                default:
                    return false;
            }
		}
		inline bool has_source_target_node(EZefRef uzr) {
            return _has_source_target_node(uzr.blob_ptr);
        }

		// helper function to check whether such a list exists as an attribute
		inline bool _has_edge_list(void * ptr) {
			BlobType this_BlobType = get<BlobType>(ptr);
			switch (this_BlobType) {
				case BlobType::ROOT_NODE:
				case BlobType::TX_EVENT_NODE:
				case BlobType::RAE_INSTANCE_EDGE:
				case BlobType::TO_DELEGATE_EDGE:
				case BlobType::ENTITY_NODE:
				case BlobType::ATTRIBUTE_ENTITY_NODE:
				case BlobType::VALUE_NODE:
                // Note this has one edge not an entire edge list
                // case BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE:
				case BlobType::RELATION_EDGE:
				case BlobType::DEFERRED_EDGE_LIST_NODE:
				case BlobType::ASSIGN_TAG_NAME_EDGE:
				case BlobType::FOREIGN_GRAPH_NODE:
				case BlobType::FOREIGN_ENTITY_NODE:
				case BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE:
				case BlobType::FOREIGN_RELATION_EDGE: {
                    return true;
                }
                default:
                    return false;
			}
		}
		inline bool has_edge_list(EZefRef uzr) {
            return _has_edge_list(uzr.blob_ptr);
        }

        // This is subtly different, the blob has edges but might not have an edge list
		inline bool _has_edges(void * ptr) {
			BlobType this_BlobType = get<BlobType>(ptr);
            return _has_edge_list(ptr) || this_BlobType == BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE;
		}
		inline bool has_edges(EZefRef uzr) {
            return _has_edges(uzr.blob_ptr);
        }


		// helper function to check whether such a list exists as an attribute
		inline bool _has_data_buffer(void * ptr) {
			BlobType this_BlobType = get<BlobType>(ptr);
			switch (this_BlobType) {
				case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE:
				case BlobType::VALUE_NODE:
				case BlobType::ASSIGN_TAG_NAME_EDGE: {
                    return true;
                }
                default:
                    return false;
			}
			return false; // should never reach here, suppress compiler warnings
		}
		inline bool has_data_buffer(EZefRef uzr) {
            return _has_data_buffer(uzr.blob_ptr);
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
        inline blob_index* subsequent_deferred_edge_list_index(T & edges) {
            // This is only for edge_info and deferred_edge_info
            return &edges.indices[edges.local_capacity];
        }
        template<class T>
        inline const blob_index* subsequent_deferred_edge_list_index(const T & edges) {
            // This is only for edge_info and deferred_edge_info
            return &edges.indices[edges.local_capacity];
        }

        inline blob_index* subsequent_deferred_edge_list_index(EZefRef & uzr) {
            return visit_blob_with_edges([](auto & edges) { return subsequent_deferred_edge_list_index(edges); }, uzr);
        }

        // return a pointer: in apply apply_action_blob we want to overwrite this index
        inline blob_index* last_edge_holding_blob(EZefRef & uzr) {
            return visit_blob_with_edges(overloaded {
                    [](blobs_ns::DEFERRED_EDGE_LIST_NODE::deferred_edge_info &) -> blob_index* {
                        throw std::runtime_error("Should never get here!");
                    },
                    [](auto & x) { return &x.last_edge_holding_blob; },
                }, uzr);
        }
        
        inline blob_index* edge_indexes(const EZefRef & uzr) {
            return visit_blob_with_edges([](auto & edges) { return (blob_index*)edges.indices; }, uzr);
        }

        inline blob_index local_edge_indexes_capacity(const EZefRef & uzr) {
            return visit_blob_with_edges([](auto & edges) { return edges.local_capacity; },
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

        inline char* get_data_buffer(blobs_ns::VALUE_NODE & blob) {
            // The value data sits right after the VRT, although the "value" itself begins at the VRT.
            return (char*)&blob.rep_type + sizeof(decltype(blob.rep_type));
		};
        inline char* get_data_buffer(blobs_ns::ASSIGN_TAG_NAME_EDGE & blob) {
            // The data sits right after the edge list
            return (char*)&blob.edges.indices[blob.edges.local_capacity+1];
		};
        inline char* get_data_buffer(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE & blob) {
            // The data is explicit in this blob
            return blob.data_buffer;
		};

        inline const char* get_data_buffer(const blobs_ns::VALUE_NODE & blob) {
            // The value data sits right after the VRT, although the "value" itself begins at the VRT.
            return (char*)&blob.rep_type + sizeof(decltype(blob.rep_type));
		};
        inline const char* get_data_buffer(const blobs_ns::ASSIGN_TAG_NAME_EDGE & blob) {
            // The data sits right after the edge list
            return (char*)&blob.edges.indices[blob.edges.local_capacity+1];
		};
        inline const char* get_data_buffer(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE & blob) {
            // The data is explicit in this blob
            return blob.data_buffer;
		};

        inline const char* get_data_buffer(const EZefRef ezr) {
            return visit_blob_with_data_buffer([](const auto & x) { return get_data_buffer(x); }, ezr);
		};

        template<typename T>
        inline size_t get_data_buffer_size(const T & blob) {
            return blob.buffer_size_in_bytes;
		};
        template size_t get_data_buffer_size(const blobs_ns::VALUE_NODE & blob);
        template size_t get_data_buffer_size(const blobs_ns::ASSIGN_TAG_NAME_EDGE & blob);
        template size_t get_data_buffer_size(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE & blob);

        template<>
        inline size_t get_data_buffer_size(const EZefRef & ezr) {
            return visit_blob_with_data_buffer([](const auto & x) { return get_data_buffer_size(x); }, ezr);
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

    // Note that this is not "from_ptr" as in an arbitrary data buffer, but the
    // pointer has to refer to a GraphData location, but we just don't trust the
    // blob pointed at is correct yet. Using an arbitrary pointer (not in a
    // GraphData) will give the wrong result.
	inline blob_index blob_index_from_ptr(void * ptr) {
		auto offset_in_bytes = (char*)ptr - (char*)MMap::blobs_ptr_from_blob(ptr);
		assert(offset_in_bytes % constants::blob_indx_step_in_bytes == 0);
		return blob_index(offset_in_bytes / constants::blob_indx_step_in_bytes);
	}



	inline blob_index _blob_index_size(void * ptr) {
		return num_blob_indexes_to_move(_size_of_blob(ptr));
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
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ATTRIBUTE_ENTITY_NODE& this_blob);
    std::string value_blob_to_str(ValueRepType buffer_type, const char* buffer_ptr, unsigned int buffer_size=0);
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::VALUE_NODE& this_blob);
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::RELATION_EDGE& this_blob);
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& this_blob);
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE& this_blob);
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
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::FOREIGN_ATTRIBUTE_ENTITY_NODE& this_blob);
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::FOREIGN_RELATION_EDGE& this_blob);
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::VALUE_TYPE_EDGE& this_blob);
    LIBZEF_DLL_EXPORTED std::ostream& operator<< (std::ostream& os, const blobs_ns::VALUE_EDGE& this_blob);

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


    inline bool is_zef_subtype(EZefRef uzr, BlobType bt) { return BT(uzr) == bt; }

    inline bool is_zef_subtype(EZefRef uzr, ValueRepTypeStruct VRT) {
        return is_zef_subtype(uzr, BT.ATTRIBUTE_ENTITY_NODE)
            || is_zef_subtype(uzr, BT.VALUE_NODE);
    }
    inline bool is_zef_subtype(EZefRef uzr, EntityTypeStruct ET) { return is_zef_subtype(uzr, BT.ENTITY_NODE); }
    inline bool is_zef_subtype(EZefRef uzr, RelationTypeStruct RT) { return is_zef_subtype(uzr, BT.RELATION_EDGE); }

    inline bool is_zef_subtype(EZefRef uzr, EntityType et) { return is_zef_subtype(uzr, ET) && (ET(uzr) == et); }
    inline bool is_zef_subtype(EZefRef uzr, RelationType rt) { return is_zef_subtype(uzr, RT) && (RT(uzr) == rt); }


    inline bool is_zef_subtype(EZefRef uzr, ValueRepType vrt_super) { return is_zef_subtype(uzr, VRT) && is_zef_subtype(VRT(uzr), vrt_super); }
    inline bool is_zef_subtype(EZefRef uzr, ValueRepTypeStruct::Enum_ vrt_super) { return is_zef_subtype(uzr, VRT) && is_zef_subtype(VRT(uzr), vrt_super); }
    inline bool is_zef_subtype(EZefRef uzr, ValueRepTypeStruct::QuantityFloat_ vrt_super) { return is_zef_subtype(uzr, VRT) && is_zef_subtype(VRT(uzr), vrt_super); }
    inline bool is_zef_subtype(EZefRef uzr, ValueRepTypeStruct::QuantityInt_ vrt_super) { return is_zef_subtype(uzr, VRT) && is_zef_subtype(VRT(uzr), vrt_super); }

    inline bool is_zef_subtype(ZefRef zr, BlobType bt) { return is_zef_subtype(zr.blob_uzr, bt); }

    inline bool is_zef_subtype(ZefRef zr, ValueRepTypeStruct VRT) { return is_zef_subtype(zr.blob_uzr, VRT); }
    inline bool is_zef_subtype(ZefRef zr, EntityTypeStruct ET) { return is_zef_subtype(zr.blob_uzr, ET); }
    inline bool is_zef_subtype(ZefRef zr, RelationTypeStruct RT) { return is_zef_subtype(zr.blob_uzr, RT); }

    inline bool is_zef_subtype(ZefRef zr, EntityType et) { return is_zef_subtype(zr.blob_uzr, et); }
    inline bool is_zef_subtype(ZefRef zr, RelationType rt) { return is_zef_subtype(zr.blob_uzr, rt); }


    inline bool is_zef_subtype(ZefRef zr, ValueRepType vrt_super) { return is_zef_subtype(zr.blob_uzr, vrt_super); }
    inline bool is_zef_subtype(ZefRef zr, ValueRepTypeStruct::Enum_ vrt_super) { return is_zef_subtype(zr.blob_uzr, vrt_super); }
    inline bool is_zef_subtype(ZefRef zr, ValueRepTypeStruct::QuantityFloat_ vrt_super) { return is_zef_subtype(zr.blob_uzr, vrt_super); }
    inline bool is_zef_subtype(ZefRef zr, ValueRepTypeStruct::QuantityInt_ vrt_super) { return is_zef_subtype(zr.blob_uzr, vrt_super); }


}
