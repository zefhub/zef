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

#include <random>
#include <chrono>
#include <ctime>
#include <functional>
#include <iterator>
#include <fstream>
#include "range/v3/all.hpp"

#include "fwd_declarations.h"
#include "zefDB_utils.h"
#include "scalars.h"
#include "zefref.h"
#include "blobs.h"
/* #include "zef_script.h" */
#include "graph.h"

namespace zefDB {
	
    struct SerializedValue {
        std::string type;
        std::string data;
        bool operator==(const SerializedValue & other) const {
            return type == other.type && data == other.data;
        }
    };
    inline std::ostream& operator<<(std::ostream& os, SerializedValue & serialized_value) {
        os << "SerializedValue{'" << serialized_value.type << "'}";
        return os;
    }



    

    LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& os, EZefRef uzr);
    LIBZEF_DLL_EXPORTED std::string low_level_blob_info(const EZefRef & uzr);


    LIBZEF_DLL_EXPORTED std::ostream& operator<<(std::ostream& o, const EZefRefs& uzrs);
    LIBZEF_DLL_EXPORTED std::ostream& operator<<(std::ostream& o, const EZefRefss& uzrss);


	LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& o, Graph& g);







	namespace internals{
		// have similar API to instantiate and link low level blobs / EZefRefs
		LIBZEF_DLL_EXPORTED EZefRef instantiate(BlobType bt, GraphData& gd);
		LIBZEF_DLL_EXPORTED EZefRef instantiate(EZefRef src, BlobType bt, EZefRef trg, GraphData& gd);






		// return a reference to the specified blob type (to be assigned to a reference that can then be used.)
		// Use the upcoming region in memory, i.e. at the location of the write head at that point
		// i.e. assume that the write head is pointing at the next free location just after the last blob (accounting for possible overflow)
		template <typename T>
		T& get_next_free_writable_blob(GraphData& gd) {
			auto blob_struct_ptr = (T*)(std::uintptr_t(&gd) + gd.write_head * constants::blob_indx_step_in_bytes);
			return *blob_struct_ptr;
		}


		LIBZEF_DLL_EXPORTED void move_head_forward(GraphData& gd);





//                                      _            __             _        _                             _   _                           
//                   ___ _ __ ___  __ _| |_ ___     / /   __ _  ___| |_     | |_ _ __ __ _ _ __  ___  __ _| |_(_) ___  _ __                
//    _____ _____   / __| '__/ _ \/ _` | __/ _ \   / /   / _` |/ _ \ __|    | __| '__/ _` | '_ \/ __|/ _` | __| |/ _ \| '_ \   _____ _____ 
//   |_____|_____| | (__| | |  __/ (_| | ||  __/  / /   | (_| |  __/ |_     | |_| | | (_| | | | \__ \ (_| | |_| | (_) | | | | |_____|_____|
//                  \___|_|  \___|\__,_|\__\___| /_/     \__, |\___|\__|     \__|_|  \__,_|_| |_|___/\__,_|\__|_|\___/|_| |_|              
//                                                       |___/                                             

		LIBZEF_DLL_EXPORTED EZefRef get_or_create_and_get_tx(GraphData& gd);
		
		LIBZEF_DLL_EXPORTED EZefRef get_or_create_and_get_tx(Graph& g);

		LIBZEF_DLL_EXPORTED EZefRef get_or_create_and_get_tx(EZefRef some_blob_to_specify_which_graph);


//                            _     _ _                         _               _           _                                  
//                   __ _  __| | __| (_)_ __   __ _     ___  __| | __ _  ___   (_)_ __   __| | _____  _____  ___               
//    _____ _____   / _` |/ _` |/ _` | | '_ \ / _` |   / _ \/ _` |/ _` |/ _ \  | | '_ \ / _` |/ _ \ \/ / _ \/ __|  _____ _____ 
//   |_____|_____| | (_| | (_| | (_| | | | | | (_| |  |  __/ (_| | (_| |  __/  | | | | | (_| |  __/>  <  __/\__ \ |_____|_____|
//                  \__,_|\__,_|\__,_|_|_| |_|\__, |   \___|\__,_|\__, |\___|  |_|_| |_|\__,_|\___/_/\_\___||___/              
//                                            |___/               |___/                                                 


		// this function is instantiated for the various blobs_ns structs: we need to access edge_indexes
		// which may be located in different memory locations (within the struct) for the different structs.
		const auto get_deferred_edge_list_blob = [](auto& my_blob_struct)->EZefRef {			
			return EZefRef(
				my_blob_struct.edges.subsequent_deferred_edge_list_index, //index 
				*graph_data(EZefRef((void*)&my_blob_struct))
			);
		};

		const auto get_final_deferred_edge_list_blob_struct = [](auto& my_blob_struct) {
			EZefRef uzr{
				my_blob_struct.edges.final_deferred_edge_list_index, //index 
				*graph_data(EZefRef((void*)&my_blob_struct))
			};
            return (blobs_ns::DEFERRED_EDGE_LIST_NODE*)uzr.blob_ptr;
		};

        template<class T>
        edge_list_size_t edge_indexes_capacity(const T& b) {
            return b.edges.local_capacity;
        }


		EZefRef create_new_deferred_edge_list(GraphData& gd, int edge_list_length = constants::default_local_edge_indexes_capacity_DEFERRED_EDGE_LIST_NODE);


		// should work for any ZefRef that has incoming / outgoing edges.
		// Depending on the ZefRef-type, the list of edges may be in a locally
		// different memory area of the struct. If the list is full, create a 
		// new DEFERRED_EDGE_LIST_NODE: enable this recursively.
		LIBZEF_DLL_EXPORTED bool append_edge_index(EZefRef uzr, blob_index edge_index_to_append, bool prevent_new_edgelist_creation=false);
		LIBZEF_DLL_EXPORTED blob_index idempotent_append_edge_index(EZefRef uzr, blob_index edge_index_to_append);









		














		// for a given ZefRef, find its uid and return as a string
		LIBZEF_DLL_EXPORTED str get_uid_as_hex_str(EZefRef uzr);
		


		// the uid of a graph is defined as the uid of its root blob
		LIBZEF_DLL_EXPORTED str get_uid_as_hex_str(Graph& g);	

		LIBZEF_DLL_EXPORTED BaseUID get_graph_uid(const GraphData& gd);
		LIBZEF_DLL_EXPORTED BaseUID get_graph_uid(const Graph& g);
		LIBZEF_DLL_EXPORTED BaseUID get_graph_uid(const EZefRef& uzr);
		LIBZEF_DLL_EXPORTED BaseUID get_blob_uid(const EZefRef& uzr);
		







		void assign_uid(EZefRef uzr, BaseUID uid);

		// us to check in Instances(uzr)  to throw if the zr does fundamentally not have a delegate
		inline bool has_delegate(BlobType bt) {
			switch (bt) {
			case BT.ENTITY_NODE: return true;
			case BT.ATOMIC_ENTITY_NODE: return true;
			case BT.RELATION_EDGE: return true;
			case BT.TX_EVENT_NODE: return true;
			case BT.ROOT_NODE: return true;
			default: return false;
			};
		}

        inline bool is_foreign_rae_blob(BlobType bt) {
			switch (bt) {
			case BT.FOREIGN_ENTITY_NODE: return true;
			case BT.FOREIGN_ATOMIC_ENTITY_NODE: return true;
			case BT.FOREIGN_RELATION_EDGE: return true;
			// case BT.FOREIGN_TX_EVENT_NODE: return true;
			// case BT.FOREIGN_ROOT_NODE: return true;
            case BT.FOREIGN_GRAPH_NODE: return true;
			default: return false;
			};
        }

        LIBZEF_DLL_EXPORTED bool is_root(EZefRef z);
        LIBZEF_DLL_EXPORTED bool is_delegate(EZefRef z);
        LIBZEF_DLL_EXPORTED bool is_delegate_relation_group(EZefRef z);
        LIBZEF_DLL_EXPORTED bool has_delegate(EZefRef z);

        template <typename T>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const T & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const int & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const double & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const bool & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, char const * const & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const Time & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const QuantityFloat & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const QuantityInt & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const ZefEnumValue & value_to_be_assigned);

        template<>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const std::string & value_to_be_assigned);
        template<>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const SerializedValue & value_to_be_assigned);

        template<class T>
        bool is_compatible_type(AtomicEntityType aet);

        template<> bool is_compatible_type<bool>(AtomicEntityType aet);
        template<> bool is_compatible_type<int>(AtomicEntityType aet);
        template<> bool is_compatible_type<double>(AtomicEntityType aet);
        template<> bool is_compatible_type<str>(AtomicEntityType aet);
        template<> bool is_compatible_type<const char*>(AtomicEntityType aet);
        template<> bool is_compatible_type<Time>(AtomicEntityType aet);
        template<> bool is_compatible_type<SerializedValue>(AtomicEntityType aet);

        template<> bool is_compatible_type<ZefEnumValue>(AtomicEntityType aet);
        template<> bool is_compatible_type<QuantityFloat>(AtomicEntityType aet);
        template<> bool is_compatible_type<QuantityInt>(AtomicEntityType aet);

        bool is_compatible(bool _, AtomicEntityType aet);
        bool is_compatible(int _, AtomicEntityType aet);
        bool is_compatible(double _, AtomicEntityType aet);
        bool is_compatible(str _, AtomicEntityType aet);
        bool is_compatible(const char * _, AtomicEntityType aet);
        bool is_compatible(Time _, AtomicEntityType aet);
        bool is_compatible(SerializedValue _, AtomicEntityType aet);

        bool is_compatible(ZefEnumValue en, AtomicEntityType aet);
        bool is_compatible(QuantityFloat q, AtomicEntityType aet);
        bool is_compatible(QuantityInt q, AtomicEntityType aet);

        // Because the pair-product of all types is a large space, we are going
        // to leave this as a header-only function.

        // Stupid class required to make the special case of T == V work out.
        template<typename T>
        struct value_cast_Impl {
            template<typename V>
            static T impl(V val) {
                throw std::runtime_error("Unable to convert");
            }
            static T impl(T val) {
                return val;
            }
        };

        template<> template<>
		inline double value_cast_Impl<double>::impl(int val) { return double(val); }
        template<> template<>
		inline int value_cast_Impl<int>::impl(double val) { 
			if (fabs(val - round(val)) > 1E-8)
				throw std::runtime_error("converting a double to an int, but the double was numerically not sufficiently close to an in to make rounding safe");
			return int(val);
		}
        template<> template<>
		inline bool value_cast_Impl<bool>::impl(int val) { 
			if(val == 1) return true; 
			if(val == 0) return false; 
			throw std::runtime_error("converting an int to a bool, but the value was neither 0 or 1");
		}
        template<> template<>
		inline int value_cast_Impl<int>::impl(bool val) { 
			if(val) return 1; 
			else return 0;
		}

		template<typename T, typename V>
		T value_cast(V val) {
            return value_cast_Impl<T>::impl(val);
        }


        template<class T>
        T value_from_node(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template<class T>
        T value_from_node(const blobs_ns::ATOMIC_VALUE_NODE& av);

        extern template str value_from_node<str>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae); 
        extern template SerializedValue value_from_node<SerializedValue>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae); 
        extern template double value_from_node<double>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template int value_from_node<int>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template QuantityInt value_from_node<QuantityInt>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template QuantityFloat value_from_node<QuantityFloat>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template ZefEnumValue value_from_node<ZefEnumValue>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template bool value_from_node<bool>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);

        extern template str value_from_node<str>(const blobs_ns::ATOMIC_VALUE_NODE& av); 
        extern template SerializedValue value_from_node<SerializedValue>(const blobs_ns::ATOMIC_VALUE_NODE& av); 
        extern template double value_from_node<double>(const blobs_ns::ATOMIC_VALUE_NODE& av);
        extern template int value_from_node<int>(const blobs_ns::ATOMIC_VALUE_NODE& av);
        extern template QuantityInt value_from_node<QuantityInt>(const blobs_ns::ATOMIC_VALUE_NODE& av);
        extern template QuantityFloat value_from_node<QuantityFloat>(const blobs_ns::ATOMIC_VALUE_NODE& av);
        extern template ZefEnumValue value_from_node<ZefEnumValue>(const blobs_ns::ATOMIC_VALUE_NODE& av);
        extern template bool value_from_node<bool>(const blobs_ns::ATOMIC_VALUE_NODE& av);

		template <typename T>    
		T abs_val(T val) {       
			return val > 0 ? val : -val;
		}                        
                                 
                                 
		inline EZefRef get_RAE_INSTANCE_EDGE(EZefRef my_entity_or_rel) {
			for (auto ind : AllEdgeIndexes(my_entity_or_rel)) {
				if (ind < 0) {   
					auto candidate = EZefRef(abs_val(ind), *graph_data(my_entity_or_rel));
					if (get<BlobType>(candidate) == BlobType::RAE_INSTANCE_EDGE) return candidate;
				}                
			}                    
			throw std::runtime_error("We should not have landed here in get_RAE_INSTANCE_EDGE: there should have been one el to return");
			return my_entity_or_rel; // hack to suppress compiler warnings
		}                        

		inline EZefRef get_TO_DELEGATE_EDGE(EZefRef my_entity_or_rel) {
			for (auto ind : AllEdgeIndexes(my_entity_or_rel)) {
				if (ind < 0) {   
					auto candidate = EZefRef(abs_val(ind), *graph_data(my_entity_or_rel));
					if (get<BlobType>(candidate) == BlobType::TO_DELEGATE_EDGE) return candidate;
				}                
			}                    
			throw std::runtime_error("We should not have landed here in get_TO_DELEGATE_EDGE: there should have been one el to return");
			return my_entity_or_rel; // hack to suppress compiler warnings
		}                        


    }
}
