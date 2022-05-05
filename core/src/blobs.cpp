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

#include "blobs.h"
#include "graph.h"
#include "zwitch.h"
#include "high_level_api.h"
#include "butler/butler.h"

#include <doctest/doctest.h>



/*[[[cog
import cog
blobs = {
	'_unspecified' : {'has_source_target_node': False, 'has_edge_list': False,},						# can be seen analogous to a git branch. Main branch for actual transactions
	'ROOT_NODE' : {'has_source_target_node': False, 'has_edge_list': True,},						# can be seen analogous to a git branch. Main branch for actual transactions
	'TX_EVENT_NODE' : {'has_source_target_node': False, 'has_edge_list': True},					# a transaction event that leads to atomic append writes. Only used for the 'ROOT' scenario branch.
	'RAE_INSTANCE_EDGE' : {'has_source_target_node': True, 'has_edge_list': True},				# VALUE_ASSIGNMENT_EDGE_* flow into this edge (sic!) to avoid clutter from all different scenarios on one terminal entity node

	# ---------------------   An internal edge to go from the "_AllRelations" or "_AllEntities" entity to the respective delegate relation or entity. ---------------------
	# ---------------------  Takes on the role of the former root node in being the docking point for the scenario edge		-----------------------------------------------
	'TO_DELEGATE_EDGE' : {'has_source_target_node': True, 'has_edge_list': True},			#
	'NEXT_TX_EDGE' : {'has_source_target_node': True, 'has_edge_list': False},				# point from a) TX_EVENT_NODE --> TX_EVENT_NODE     or b)  ROOT_NODE --> TX_EVENT_NODE
	'ENTITY_NODE' : {'has_source_target_node': False, 'has_edge_list': True, 'is_low_level_edge': False},					# custom entity nodes known at compile time
	'ATOMIC_ENTITY_NODE' : {'has_source_target_node': False, 'has_edge_list': True},				# entity node that an atomic value can be assigned to.
	'ATOMIC_VALUE_NODE' : {'has_source_target_node': False, 'has_edge_list': True},
	#   ------------------- these can be seen to be more associated with the relations used in knowledge graphs -------------------
	'RELATION_EDGE' : {'has_source_target_node': True, 'has_edge_list': True},					# domain specific relations with names known at compile time
	'DELEGATE_INSTANTIATION_EDGE' : {'has_source_target_node': True, 'has_edge_list': False},
	'DELEGATE_RETIREMENT_EDGE' : {'has_source_target_node': True, 'has_edge_list': False},
	'INSTANTIATION_EDGE' : {'has_source_target_node': True, 'has_edge_list': False},			# a node or relation can be instantiated out of the blue or cloned from another edge.
	'TERMINATION_EDGE' : {'has_source_target_node': True, 'has_edge_list': False},
	'ATOMIC_VALUE_ASSIGNMENT_EDGE' : {'has_source_target_node': True, 'has_edge_list': False},
	'DEFERRED_EDGE_LIST_NODE' : {'has_source_target_node': False, 'has_edge_list': True},				# if a trailing edge list runs out of space, the very last element
	'ASSIGN_TAG_NAME_EDGE' : {'has_source_target_node': True, 'has_edge_list': True},
	'NEXT_TAG_NAME_ASSIGNMENT_EDGE' : {'has_source_target_node': True, 'has_edge_list': False},
	'FOREIGN_GRAPH_NODE' : {'has_source_target_node': False, 'has_edge_list': True},

	# --------------- newly added ------------

	'ORIGIN_RAE_EDGE' : {'has_source_target_node': True, 'has_edge_list': False},
	'ORIGIN_GRAPH_EDGE': {'has_source_target_node': True, 'has_edge_list': False},
	'FOREIGN_ENTITY_NODE': {'has_source_target_node': False, 'has_edge_list': True},
	'FOREIGN_ATOMIC_ENTITY_NODE':  {'has_source_target_node': False, 'has_edge_list': True},
	'FOREIGN_RELATION_EDGE': {'has_source_target_node': True, 'has_edge_list': True},
	}
BlobType_names = blobs.keys()

all_blob_type_names = [*BlobType_names]
]]]*/
//[[[end]]]





namespace zefDB {

    // Return the size of the ZefRef in bytes, accounting for possible overflow etc.
	// Future improvement: use std::visit with std::variant blobs_ns to make this robust to changes in the BlobType field location.
	// But this should hardly ever change and we can catch it with tests
	blob_index size_of_blob(EZefRef b) {
		using namespace blobs_ns;

        blob_index size = visit([](auto & x) { return sizeof(x); },
                                b);
        if(internals::has_edge_list(b))
            size += visit_blob_with_edges([](auto & x) {
                // Using offsetof to get the proper location of the indices.
                // Even though it will be fine with int32s, going to be extra
                // safe, just in case this changes in the future.
                size_t backtrack_amount = sizeof(edge_info) - offsetof(edge_info, indices);
                // Note: +1 is because there is another edge for the subsequent
                // edge list.
                return -backtrack_amount + (x.edges.local_capacity + 1)*sizeof(blob_index);
            }, b);

        // TODO: This is probably worthwhile splitting off to a "visit_blob_with_data" too.
		BlobType* BlobType_ptr = (BlobType*)b.blob_ptr;  // we know that the very first byte is the BlobType for all ZefRef structs
		switch (*BlobType_ptr) {
		case BlobType::ATOMIC_VALUE_NODE: {
            size += get<ATOMIC_VALUE_NODE>(b).buffer_size_in_bytes;
            break;
        }
		case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE: {
            // Using offsetof to get the proper location of the start of the
            // data_buffer..
            size_t backtrack_amount = sizeof(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE) - offsetof(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE, data_buffer);
            size += get<ATOMIC_VALUE_NODE>(b).buffer_size_in_bytes - backtrack_amount;
            break;
        }
		case BlobType::ASSIGN_TAG_NAME_EDGE: {
            size += get<ASSIGN_TAG_NAME_EDGE>(b).buffer_size_in_bytes;
            break;
        }
        default: {}
		}		
		return size;
	}

	std::ostream& operator<< (std::ostream& os, Sentinel sent) {
		os << "<sentinel: not set>";
		return os;
	}

	namespace internals {

        size_t edge_list_end_offset(EZefRef uzr) {
            assert(has_edge_list(uzr));
            // Check the default edge list sizes, that they are perfectly aligned with a blob.
            return visit_blob_with_edges([](auto & obj) {
                size_t start_offset = (uintptr_t)(&obj.edges.indices) - (uintptr_t)(&obj);

                // std::cerr << "Start: " << start_offset;
                // std::cerr << " capacity: " << obj.edges.local_capacity;
                // std::cerr << " calced: " << (start_offset + (obj.edges.local_capacity + 1)*sizeof(blob_index));
                // std::cerr << " calced(mod): " << (start_offset + (obj.edges.local_capacity + 1)*sizeof(blob_index)) % constants::blob_indx_step_in_bytes;
                // std::cerr << " direct: " << (uintptr_t)(subsequent_deferred_edge_list_index(obj)+1) - (uintptr_t)&obj;
                // std::cerr << " direct(mod): " << ((uintptr_t)(subsequent_deferred_edge_list_index(obj)+1) - (uintptr_t)&obj) % constants::blob_indx_step_in_bytes;
                // Need +1 for the subsequent edge list index.
                return (start_offset + (obj.edges.local_capacity + 1)*sizeof(blob_index)) % constants::blob_indx_step_in_bytes;
            }, uzr);
        }
                    
            


        void static_checks() {
            // We create a mmap here as further down we use EZefRefs. These call
            // into ensure mmap etc...
            void * mem = MMap::create_mmap();
            MMap::ensure_or_alloc_range(mem, 1024);

            size_t size;
            EZefRef uzr;

#define BLOB(x) new (mem) blobs_ns::x{}; \
            uzr = EZefRef{mem}; \
            size = size_of_blob(EZefRef{mem}); \
            std::cerr << std::setw(30) << #x << " is " << size/float(constants::blob_indx_step_in_bytes) << " blobs big."; \
            if(size > constants::blob_indx_step_in_bytes && (size%constants::blob_indx_step_in_bytes)/float(constants::blob_indx_step_in_bytes) != 0) \
                std::cerr << "  *******************"; \
            std::cerr << std::endl; \
            /* Also check the default edge list sizes, that they are perfectly aligned with a blob. */ \
            if(has_edge_list(uzr)) { \
                size_t offset = edge_list_end_offset(uzr); \
                if(offset != 0) \
                    std::cerr << "DEFAULT EDGE LIST FOR " << #x << " IS INCORRECT: " << offset << std::endl; \
            }

            BLOB(ROOT_NODE);
            BLOB(TX_EVENT_NODE);
            BLOB(NEXT_TX_EDGE);
            BLOB(RAE_INSTANCE_EDGE);
            BLOB(TO_DELEGATE_EDGE);
            BLOB(ENTITY_NODE);
            BLOB(ATOMIC_ENTITY_NODE);
            BLOB(ATOMIC_VALUE_NODE);
            BLOB(RELATION_EDGE);
            BLOB(DELEGATE_INSTANTIATION_EDGE);
            BLOB(DELEGATE_RETIREMENT_EDGE);
            BLOB(INSTANTIATION_EDGE);
            BLOB(TERMINATION_EDGE);
            BLOB(ATOMIC_VALUE_ASSIGNMENT_EDGE);
            BLOB(DEFERRED_EDGE_LIST_NODE);
            BLOB(ASSIGN_TAG_NAME_EDGE);
            BLOB(NEXT_TAG_NAME_ASSIGNMENT_EDGE);
            BLOB(FOREIGN_GRAPH_NODE);
            BLOB(ORIGIN_RAE_EDGE);
            BLOB(ORIGIN_GRAPH_EDGE);
            BLOB(FOREIGN_ENTITY_NODE);
            BLOB(FOREIGN_ATOMIC_ENTITY_NODE);
            BLOB(FOREIGN_RELATION_EDGE);

            MMap::destroy_mmap(mem);
#undef BLOB
            std::cerr << "AVAE: a: " << offsetof(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE, this_BlobType) << std::endl;
            std::cerr << "AVAE: b: " << offsetof(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE, my_atomic_entity_type) << std::endl;
            std::cerr << "AVAE: c: " << offsetof(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE, buffer_size_in_bytes) << std::endl;
            std::cerr << "AVAE: d: " << offsetof(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE, source_node_index) << std::endl;
            std::cerr << "AVAE: e: " << offsetof(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE, target_node_index) << std::endl;
            std::cerr << "AVAE: f: " << offsetof(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE, data_buffer) << std::endl;
            std::cerr << "AVAE: size: " << sizeof(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE) << std::endl;
        }

		// e.g. when determining the EZefRefs out_edges(my_uzr), we need to know how much space to allocate for the EZefRefs object.
		blob_index total_edge_index_list_size_upper_limit(EZefRef uzr){
			blob_index * ptr = subsequent_deferred_edge_list_index(uzr);			
            Butler::ensure_or_get_range(ptr, sizeof(blob_index));
			blob_index next_edge_list_indx = *ptr;
            assert(next_edge_list_indx != 0);
			return next_edge_list_indx == blobs_ns::sentinel_subsequent_index
				? local_edge_indexes_capacity(uzr)
				: local_edge_indexes_capacity(uzr) + total_edge_index_list_size_upper_limit(EZefRef{ next_edge_list_indx, *graph_data(uzr) });
		}

        blob_index last_set_edge_index(EZefRef uzr) {
            return visit_blob_with_edges( overloaded {
                    [](blobs_ns::DEFERRED_EDGE_LIST_NODE & x)->blob_index {
                        throw std::runtime_error("Shouldn't get here - trying to get last_set_edge_index on a deferred edge list");
                    },
                    [uzr](auto & s) {
                        blob_index last_blob_index = s.edges.last_edge_holding_blob;

                        // Handle the case of no edges first
                        if(last_blob_index == 0)
                            return 0;

                        blob_index * last_blob = (blob_index*)ptr_from_blob_index(last_blob_index, *graph_data(uzr));
                        int subindex = subindex_in_last_blob(last_blob);

                        // Note: subindex-1 could == -1 here but that should be fine.
                        return std::abs(last_blob[subindex-1]);
                    }
                }, uzr);
        }

        int subindex_in_last_blob(blob_index * last_blob) {
            // Note: the logic here is complicated because the start of the edge
            // list may not be aligned with a blob. However, when last_blob is
            // non-zero, it means there is always at least one edge present, so
            // we can work backwards to first find that one edge, then advance
            // by one. That preexisting edge could also be in the prior blob.

            static_assert(constants::blob_indx_step_in_bytes == 4*sizeof(blob_index));
            // Starting at 2, not 3, as the final index should always be empty
            assert(last_blob[3] == 0 || last_blob[3] == blobs_ns::sentinel_subsequent_index);
            int previous_search = 2;
            for(; previous_search >= 0; previous_search--) {
                if(last_blob[previous_search] != 0 && last_blob[previous_search] != blobs_ns::sentinel_subsequent_index)
                    break;
            }
            if(previous_search == -1)
                assert(last_blob[-1] != 0);
            return previous_search + 1;
        }


	}


//                   _ _                 _                  __                         _                _           _                                  
//                  (_) |_ ___ _ __ __ _| |_ ___  _ __     / _| ___  _ __      ___  __| | __ _  ___    (_)_ __   __| | _____  _____  ___               
//    _____ _____   | | __/ _ \ '__/ _` | __/ _ \| '__|   | |_ / _ \| '__|    / _ \/ _` |/ _` |/ _ \   | | '_ \ / _` |/ _ \ \/ / _ \/ __|  _____ _____ 
//   |_____|_____|  | | ||  __/ | | (_| | || (_) | |      |  _| (_) | |      |  __/ (_| | (_| |  __/   | | | | | (_| |  __/>  <  __/\__ \ |_____|_____|
//                  |_|\__\___|_|  \__,_|\__\___/|_|      |_|  \___/|_|       \___|\__,_|\__, |\___|   |_|_| |_|\__,_|\___/_/\_\___||___/              
//                                                                                       |___/                                                   

    bool AllEdgeIndexes::Iterator::operator==(const AllEdgeIndexes::Sentinel& sent) const
    {
        // This is handling the case of a deferred edge list that is beyond our write head.
        if(ptr_to_current_edge_element == ptr_to_last_edge_element_in_current_blob) {
            // No tests necessary here anymore - the logic is in the operator++.
            // If it stops at the end, then this is the end. We want this to be
            // consistent, even if there are writes that occur in between the
            // operator++ call and this operator== call.
            return true;
        } else {
            // the edge list is filled with 0's if no more edges to be linked
            if(*ptr_to_current_edge_element == 0 || *ptr_to_current_edge_element >= stable_last_blob)
                return true;
        }
        return false;
    }

    bool AllEdgeIndexes::Iterator::operator!=(const AllEdgeIndexes::Sentinel& sent) const {
        return !(*this == sent);
    }



    AllEdgeIndexes::Iterator AllEdgeIndexes::begin() const {
        blob_index* ptr_to_first_el_in_array = internals::edge_indexes(uzr_with_edges);
        GraphData & gd = Graph(uzr_with_edges).my_graph_data();
        // Need to ensure here for the entire edge list, as normally
        // EZefRef{...} just does the minimum blob amount.
        Butler::ensure_or_get_range(uzr_with_edges.blob_ptr, size_of_blob(uzr_with_edges));

        blob_index last_blob;
        if(force_to_write_head || (gd.is_primary_instance && gd.open_tx_thread == std::this_thread::get_id()))
            last_blob = gd.write_head.load();
        else
            last_blob = gd.read_head.load();

        blob_index * temp = ptr_to_first_el_in_array + internals::local_edge_indexes_capacity(uzr_with_edges);
        assert(*temp != 0);
        return AllEdgeIndexes::Iterator{
            ptr_to_first_el_in_array,  // returns an optional ptr to the first element of the array for the respective uzr		
            uzr_with_edges,
            // The minus 1 is because the last edge index is actually the subsequent deferred edge list
            ptr_to_first_el_in_array + internals::local_edge_indexes_capacity(uzr_with_edges),
            last_blob
        };
    }


    AllEdgeIndexes::Sentinel AllEdgeIndexes::end() const {
        return AllEdgeIndexes::Sentinel{};
    }

    AllEdgeIndexes::Iterator& AllEdgeIndexes::Iterator::operator++() {
        ++ptr_to_current_edge_element;
        // if we are at the end of the local list and there is another edge list attached:
        if(ptr_to_current_edge_element == ptr_to_last_edge_element_in_current_blob) {
            blob_index subsequent_edge_list = *ptr_to_current_edge_element;
            // The below is commented out, because we want to allow the iterator
            // to land "beyond" the end of the edge list. This is then the
            // condition for it to be at the end. Similarly, we don't want to
            // progress beyond the end of the edge list if this exceeds the
            // last known write/read blob.
            //
            // if (subsequent_edge_list == blobs_ns::sentinel_subsequent_index)
            //     throw std::runtime_error("Shouldn't get here! Trying to iterate beyond list which has no subsequent edge list");

            // std::cerr << "sub: " << subsequent_edge_list << " -- stable_last_blob: " << stable_last_blob << std::endl;
            if (subsequent_edge_list != blobs_ns::sentinel_subsequent_index && subsequent_edge_list < stable_last_blob) {
                GraphData* gd = graph_data(current_blob_pointed_to);
                // This is for debugging a potential issue with zefhub, but will likely stay long-term in some kind of form anyway.

                current_blob_pointed_to = EZefRef{ subsequent_edge_list, *gd };
                // Need to ensure here for the entire edge list, as normally
                // EZefRef{...} just does the minimum blob amount.
                Butler::ensure_or_get_range(current_blob_pointed_to.blob_ptr, size_of_blob(current_blob_pointed_to));
                ptr_to_current_edge_element = internals::edge_indexes(current_blob_pointed_to);
                ptr_to_last_edge_element_in_current_blob = ptr_to_current_edge_element + internals::local_edge_indexes_capacity(current_blob_pointed_to);
            }
        }
        return *this;
    }





    template<class T>
    void show_edges_extra(std::ostream& os, const T& obj) {
        os << "\"final_blob\": " << obj.edges.last_edge_holding_blob;
    }

    template<>
    void show_edges_extra(std::ostream& os, const blobs_ns::DEFERRED_EDGE_LIST_NODE& obj) {
        os << "\"first_blob\": " << obj.first_blob;
    }

    template<class T>
	void show_edges(std::ostream& os, const T& obj) {
		os << "\"local_capacity\": " << obj.edges.local_capacity << ", ";
		os << "\"indices\": [";
        int i = 0;
        blob_index * indices = internals::edge_indexes(obj);
		for (; i < obj.edges.local_capacity ; i++) {
            if(indices[i] == 0)
                break;
            os << " " << indices[i];
		}
		os << " ]";
        os << " (" << i << "), ";
		os << "\"subsequent\": " << *internals::subsequent_deferred_edge_list_index(obj) << ", ";
        show_edges_extra(os, obj);
    }






	std::ostream& operator<< (std::ostream& os, const blobs_ns::ROOT_NODE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}


	std::ostream& operator<< (std::ostream& os, const blobs_ns::TX_EVENT_NODE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"time_slice\": " << *this_blob.time_slice << ", ";
		os << "\"time\": " << this_blob.time << ", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}

	std::ostream& operator<< (std::ostream& os, const blobs_ns::NEXT_TX_EDGE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index;
		os << "}";
		return os;
	}

	std::ostream& operator<< (std::ostream& os, const blobs_ns::RAE_INSTANCE_EDGE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}

	std::ostream& operator<< (std::ostream& os, const blobs_ns::TO_DELEGATE_EDGE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}

	std::ostream& operator<< (std::ostream& os, const blobs_ns::ENTITY_NODE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "{\"EntityType\": " << this_blob.entity_type << ", ";
		os << "{\"instantiation_time_slice\": " << this_blob.instantiation_time_slice.value << ", ";
		os << "{\"termination_time_slice\": " << this_blob.termination_time_slice.value << ", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}

	std::ostream& operator<< (std::ostream& os, const blobs_ns::ATOMIC_ENTITY_NODE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "{\"AtomicEntityType\": " << this_blob.my_atomic_entity_type << ", ";
		os << "{\"instantiation_time_slice\": " << this_blob.instantiation_time_slice.value << ", ";
		os << "{\"termination_time_slice\": " << this_blob.termination_time_slice.value << ", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}

	std::string value_blob_to_str(AtomicEntityType buffer_type, const char* buffer_ptr, unsigned int buffer_size) {
		// buffer_size is only required for types of non-fixed size. e.g. string

		switch (buffer_type.value) {
		case AET._unspecified.value: return "\"no type was specified.\"";
		case AET.Float.value: return to_str(*(double*)buffer_ptr);
		case AET.Int.value: return to_str(*(int*)buffer_ptr);
		case AET.Bool.value: return to_str(*(bool*)buffer_ptr);
		case AET.String.value: return [buffer_size](const char* buffer_ptr) { std::stringstream ss; ss << "\"" << std::string_view(buffer_ptr, buffer_size) << "\""; return ss.str(); }(buffer_ptr);
		case AET.Time.value: return to_str(*(Time*)buffer_ptr);
		default: {
			switch (buffer_type.value % 16) {
			case 1: return to_str(*((ZefEnumValue*)buffer_ptr));
			case 2: return to_str(*((QuantityFloat*)buffer_ptr));
			case 3: return to_str(*((QuantityInt*)buffer_ptr));
			default: throw std::runtime_error("AET type convversion for this type not implemented in value_blob_to_str.");
			}
		}
		}
	}

	std::ostream& operator<< (std::ostream& os, const blobs_ns::ATOMIC_VALUE_NODE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"AtomicEntityType\": " << this_blob.my_atomic_entity_type << ", ";
		os << "\"buffer_size_in_bytes\": " << this_blob.buffer_size_in_bytes << ", ";
        show_edges(os, this_blob);
		os << "\"value\": ";
		os << value_blob_to_str(this_blob.my_atomic_entity_type, internals::get_data_buffer(this_blob), this_blob.buffer_size_in_bytes);
		os << "}";
		return os;
	}

	std::ostream& operator<< (std::ostream& os, const blobs_ns::RELATION_EDGE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"RelationType\": " << this_blob.relation_type << ", ";
		os << "{\"instantiation_time_slice\": " << this_blob.instantiation_time_slice.value << ", ";
		os << "{\"termination_time_slice\": " << this_blob.termination_time_slice.value << ", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}



	std::ostream& operator<< (std::ostream& os, const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& this_blob) {
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"AtomicEntityType\": " << this_blob.my_atomic_entity_type << ", ";
		os << "\"buffer_size_in_bytes\": " << this_blob.buffer_size_in_bytes << ", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
		os << "\"value\": ";
		os << value_blob_to_str(this_blob.my_atomic_entity_type, &this_blob.data_buffer[0], this_blob.buffer_size_in_bytes);
		os << "}";
		return os;
	}
	

	std::ostream& operator<< (std::ostream& os, const blobs_ns::DELEGATE_INSTANTIATION_EDGE& this_blob) {
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
		os << "}";
		return os;
	}
		
	

	std::ostream& operator<< (std::ostream& os, const blobs_ns::DELEGATE_RETIREMENT_EDGE& this_blob) {
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
		os << "}";
		return os;
	}
		

	std::ostream& operator<< (std::ostream& os, const blobs_ns::INSTANTIATION_EDGE& this_blob) {
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
		os << "}";
		return os;
	}

	

	std::ostream& operator<< (std::ostream& os, const blobs_ns::TERMINATION_EDGE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
		os << "}";
		return os;
	}


	// local representation only: only show edge indexes saved in this DEFERRED_EDGE_LIST_NODE
	std::ostream& operator<< (std::ostream& os, const blobs_ns::DEFERRED_EDGE_LIST_NODE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		// os << "\"preceding_edge_list_index\": " << this_blob.preceding_edge_list_index << ", ";
        // os << this_blob.edges;
        os << "\"first_blob\": " << this_blob.first_blob << ", ";
		os << "}";
		return os;
	}


	std::ostream& operator<< (std::ostream& os, const blobs_ns::ASSIGN_TAG_NAME_EDGE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
		os << "\"name\": " << std::string(internals::get_data_buffer(this_blob), this_blob.buffer_size_in_bytes) << ", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}
	
	std::ostream& operator<< (std::ostream& os, const blobs_ns::NEXT_TAG_NAME_ASSIGNMENT_EDGE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
		os << "]}";
		return os;
	}


	std::ostream& operator<< (std::ostream& os, const blobs_ns::FOREIGN_GRAPH_NODE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}
	
	





	std::ostream& operator<< (std::ostream& os, const blobs_ns::ORIGIN_RAE_EDGE& this_blob) {
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
		os << "}";
		return os;
	}


	


	std::ostream& operator<< (std::ostream& os, const blobs_ns::ORIGIN_GRAPH_EDGE& this_blob) {
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
		os << "}";
		return os;
	}


	


	std::ostream& operator<< (std::ostream& os, const blobs_ns::FOREIGN_ENTITY_NODE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "{\"EntityType\": \"" << this_blob.entity_type << "\", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}
	


	std::ostream& operator<< (std::ostream& os, const blobs_ns::FOREIGN_ATOMIC_ENTITY_NODE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "{\"AtomicEntityType\": \"" << this_blob.atomic_entity_type << "\", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}


	

	std::ostream& operator<< (std::ostream& os, const blobs_ns::FOREIGN_RELATION_EDGE& this_blob) {
		using namespace ranges;
		os << "{\"BlobType\": \"" << this_blob.this_BlobType << "\", ";
		os << "{\"RelationType\": \"" << this_blob.relation_type << "\", ";
		os << "\"source_node_index\": " << this_blob.source_node_index << ", ";
		os << "\"target_node_index\": " << this_blob.target_node_index << ", ";
        show_edges(os, this_blob);
		os << "}";
		return os;
	}

    json blob_to_json_details(const blobs_ns::_unspecified blob) {
        throw std::runtime_error("Shouldn't never get here");
    }

    json blob_to_json_details(const blobs_ns::ROOT_NODE blob) {
        return json{
			{"data_layout_version_info", std::string(blob.data_layout_version_info, blob.actual_written_data_layout_version_info_size)},
			{"graph_revision_info", std::string(blob.graph_revision_info, blob.actual_written_graph_revision_info_size)},
        };
    }

    json blob_to_json_details(const blobs_ns::TX_EVENT_NODE & blob) {
        return json{
			{"time", blob.time },
			{"time_slice", blob.time_slice },
		};
    }

    json blob_to_json_details(const blobs_ns::NEXT_TX_EDGE & blob) {
        return json{};
    }

    json blob_to_json_details(const blobs_ns::RAE_INSTANCE_EDGE & blob) {
        return json{};				
    }

    json blob_to_json_details(const blobs_ns::TO_DELEGATE_EDGE & blob) {
        return json{};		
    }

    json blob_to_json_details(const blobs_ns::ENTITY_NODE & blob) {
        return json{
			{"entity_type", blob.entity_type },
			{"instantiation_time_slice", blob.instantiation_time_slice },
			{"termination_time_slice", blob.termination_time_slice },
		};
    }
		
    json blob_to_json_details(const blobs_ns::ATOMIC_ENTITY_NODE & blob) {
        return json{
			{"my_atomic_entity_type", blob.my_atomic_entity_type},
			{"instantiation_time_slice", blob.instantiation_time_slice },
			{"termination_time_slice", blob.termination_time_slice },
		};
    }
		
    json blob_to_json_details(const blobs_ns::ATOMIC_VALUE_NODE & blob) {
        return json{
			{"my_atomic_entity_type", blob.my_atomic_entity_type},
		};
    }

    json blob_to_json_details(const blobs_ns::RELATION_EDGE & blob) {
        return json{
			{"hostage_flags", blob.hostage_flags },
			{"relation_type", blob.relation_type },
			{"instantiation_time_slice", blob.instantiation_time_slice },
			{"termination_time_slice", blob.termination_time_slice },
		};
    }
		
		// these edges are created together with any new entity node or domain edge
    json blob_to_json_details(const blobs_ns::DELEGATE_INSTANTIATION_EDGE & blob) {
        return json{};
    };		

    json blob_to_json_details(const blobs_ns::DELEGATE_RETIREMENT_EDGE & blob) {
        return json{};		
    }

		// these edges are created together with any new entity node or domain edge
    json blob_to_json_details(const blobs_ns::INSTANTIATION_EDGE & blob) {
        return json{};
    }
    json blob_to_json_details(const blobs_ns::TERMINATION_EDGE & blob) {
        return json{};
    }

    json blob_to_json_details(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE & blob) {
        return json{
			{"my_atomic_entity_type", blob.my_atomic_entity_type},
        };
    }

    json blob_to_json_details(const blobs_ns::DEFERRED_EDGE_LIST_NODE & blob) {
        throw std::runtime_error("Shouldn't not get here");
    };

    json blob_to_json_details(const blobs_ns::ASSIGN_TAG_NAME_EDGE & blob) {
        return json{};
    }

    json blob_to_json_details(const blobs_ns::NEXT_TAG_NAME_ASSIGNMENT_EDGE & blob) {   // can be inserted between two BlobType::ASSIGN_TAG_NAME_EDGE to enable efficient temporal resolving of tag name values
        return json{};
    }

    json blob_to_json_details(const blobs_ns::FOREIGN_GRAPH_NODE & blob) {
        return json{
			{"internal_foreign_graph_index", blob.internal_foreign_graph_index },
		};
    }

    json blob_to_json_details(const blobs_ns::ORIGIN_RAE_EDGE & blob) {
        return json{};
    }

    json blob_to_json_details(const blobs_ns::ORIGIN_GRAPH_EDGE & blob) {
        return json{};
    }

    json blob_to_json_details(const blobs_ns::FOREIGN_ENTITY_NODE & blob) {
        return json{
			{"entity_type", blob.entity_type },
		};
    }

    json blob_to_json_details(const blobs_ns::FOREIGN_ATOMIC_ENTITY_NODE & blob) {
        return json{
			{"atomic_entity_type", blob.atomic_entity_type},
		};
    }

    json blob_to_json_details(const blobs_ns::FOREIGN_RELATION_EDGE & blob) {
        return json{
			{"relation_type", blob.relation_type },
		};
    }


    json blob_to_json(EZefRef ezr) {
        if(get<BlobType>(ezr) == BlobType::DEFERRED_EDGE_LIST_NODE) {
            // throw std::runtime_error("We don't want to export deferred edge lists.");
            return json{};
        }

        json j;
        j["type"] = to_str(get<BlobType>(ezr));
        j["_old_index"] = index(ezr);
        if(internals::has_edge_list(ezr)) {
            std::vector<blob_index> v;
            for(auto & item : AllEdgeIndexes(ezr))
                v.push_back(item);
            j["edges"] = v;
        }
        if(internals::has_source_target_node(ezr)) {
            j["source_node_index"] = internals::source_node_index(ezr);
            j["target_node_index"] = internals::target_node_index(ezr);
        }
        if(internals::has_data_buffer(ezr)) {
            j["data_buffer"] = std::string(internals::get_data_buffer(ezr), internals::get_data_buffer_size(ezr));
        }
        if(internals::has_uid(ezr)) {
            j["uid"] = str(internals::blob_uid_ref(ezr));
            j["_internalUID"] = str(internals::blob_uid_ref(ezr));
        } else {
            j["_internalUID"] = index(ezr);
        }

        // j.update(visit([](auto & x) { return blob_to_json_details(x); }, ezr));
        json details = visit([](auto & x) { return blob_to_json_details(x); }, ezr);
        if(!details.is_null())
            j.update(details);

        return j;
    }

}


