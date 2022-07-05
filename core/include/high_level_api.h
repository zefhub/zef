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

#include <type_traits>  // std::is_same<T, animal>::value
#include <unordered_map>
#include <algorithm>
#include <variant>
#include "range/v3/all.hpp"
// #include <pybind11/embed.h>  // to get variables (e.g. the z ZefRef) from the C++ space accessible in the python zefhook script

#include "fwd_declarations.h"
#include "zefDB_utils.h"
#include "scalars.h"
#include "zefref.h"
#include "blobs.h"
/* #include "zef_script.h" */
#include "graph.h"

#include "low_level_api.h"
#include "tensor.h"
#include "synchronization.h"

#include "butler/butler.h"

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




//                                   _                    __      _                            
//                         ___ _ __ | |_ _ __ _   _      / _| ___| |_ ___                      
//    _____ _____ _____   / _ \ '_ \| __| '__| | | |    | |_ / __| __/ __|   _____ _____ _____ 
//   |_____|_____|_____| |  __/ | | | |_| |  | |_| |    |  _| (__| |_\__ \  |_____|_____|_____|
//                        \___|_| |_|\__|_|   \__, |    |_|  \___|\__|___/                     
//                                            |___/                                            

    // Note: to_index is not inclusive
    LIBZEF_DLL_EXPORTED EZefRefs blobs(GraphData& gd, blob_index from_index=constants::ROOT_NODE_blob_index, blob_index to_index=0);
	
	inline EZefRefs blobs(Graph& g, blob_index from_index=constants::ROOT_NODE_blob_index, blob_index to_index=0) {
		return blobs(g.my_graph_data(), from_index, to_index);
	}



	// LIBZEF_DLL_EXPORTED str type_name_(EZefRef uzr, int delegate_order_shift = 0);

	// LIBZEF_DLL_EXPORTED std::string type_name(EZefRef uzr);
	// LIBZEF_DLL_EXPORTED std::string type_name(ZefRef zr);

	
	LIBZEF_DLL_EXPORTED bool is_root(EZefRef z);
	LIBZEF_DLL_EXPORTED bool is_root(ZefRef z);
	LIBZEF_DLL_EXPORTED bool is_delegate(EZefRef z);
	LIBZEF_DLL_EXPORTED bool is_delegate(ZefRef z);
	LIBZEF_DLL_EXPORTED bool is_delegate_group(EZefRef z);
	LIBZEF_DLL_EXPORTED bool is_delegate_group(ZefRef z);
	LIBZEF_DLL_EXPORTED bool has_delegate(EZefRef z);
	LIBZEF_DLL_EXPORTED bool has_delegate(ZefRef z);





    LIBZEF_DLL_EXPORTED EZefRefs all_raes(const Graph& graph);


    namespace internals {


        template <typename T>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const T & value_to_be_assigned);

        template<>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const std::string & value_to_be_assigned);
        // There are other variants of copy_to_buffer but we leave them to be instantiated in the cpp file.

        template<class T>
        void assign_value(EZefRef my_atomic_entity, T value_to_be_assigned);
        extern template void assign_value(EZefRef my_atomic_entity, bool value_to_be_assigned);
        extern template void assign_value(EZefRef my_atomic_entity, int value_to_be_assigned);
        extern template void assign_value(EZefRef my_atomic_entity, double value_to_be_assigned);
        extern template void assign_value(EZefRef my_atomic_entity, str value_to_be_assigned);
        extern template void assign_value(EZefRef my_atomic_entity, const char* value_to_be_assigned);
        extern template void assign_value(EZefRef my_atomic_entity, Time value_to_be_assigned);
        extern template void assign_value(EZefRef my_atomic_entity, SerializedValue value_to_be_assigned);
        extern template void assign_value(EZefRef my_atomic_entity, ZefEnumValue value_to_be_assigned);
        extern template void assign_value(EZefRef my_atomic_entity, QuantityFloat value_to_be_assigned);
        extern template void assign_value(EZefRef my_atomic_entity, QuantityInt value_to_be_assigned);


        template <typename T>
        T value(ZefRef my_atomic_entity);
        template<> double value<double>(ZefRef my_atomic_entity);
        template<> int value<int>(ZefRef my_atomic_entity);
        template<> bool value<bool>(ZefRef my_atomic_entity);
        template<> std::string value<std::string>(ZefRef my_atomic_entity);
        template<> Time value<Time>(ZefRef my_atomic_entity);
        template<> SerializedValue value<SerializedValue>(ZefRef my_atomic_entity);
        template<> ZefEnumValue value<ZefEnumValue>(ZefRef my_atomic_entity);
        template<> QuantityFloat value<QuantityFloat>(ZefRef my_atomic_entity);
        template<> QuantityInt value<QuantityInt>(ZefRef my_atomic_entity);

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


        template<class T>
        T get_final_value_for_op_hat(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet);

        template<> str get_final_value_for_op_hat<str>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet); 
        template<> SerializedValue get_final_value_for_op_hat<SerializedValue>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet); 

        template<> double get_final_value_for_op_hat<double>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet);
        template<> int get_final_value_for_op_hat<int>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet);

        extern template QuantityInt get_final_value_for_op_hat<QuantityInt>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet);
        extern template QuantityFloat get_final_value_for_op_hat<QuantityFloat>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet);
        extern template ZefEnumValue get_final_value_for_op_hat<ZefEnumValue>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet);

    }

    LIBZEF_DLL_EXPORTED bool is_promotable_to_zefref(EZefRef uzr_to_promote, EZefRef reference_tx);
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
    LIBZEF_DLL_EXPORTED std::variant<EntityType, RelationType, AtomicEntityType> rae_type(EZefRef uzr);
    inline std::variant<EntityType, RelationType, AtomicEntityType> rae_type(ZefRef zr) {
        return rae_type(zr.blob_uzr);
    }                            
                                 
                                 
                                 
                                 
	LIBZEF_DLL_EXPORTED void make_primary(Graph& g, bool take_on=true);		
    LIBZEF_DLL_EXPORTED void tag(const Graph& g, const std::string& name_tag, bool force_if_name_tags_other_graph=false, bool adding=true) ;
    LIBZEF_DLL_EXPORTED std::tuple<Graph,EZefRef> zef_get(const std::string& some_uid_or_name_tag) ;	
	LIBZEF_DLL_EXPORTED std::vector<std::string> zearch(const std::string& zearch_term) ;
	LIBZEF_DLL_EXPORTED std::optional<std::string> lookup_uid(const std::string& tag) ;
	LIBZEF_DLL_EXPORTED void sync(Graph& g, bool do_sync=true) ;
	LIBZEF_DLL_EXPORTED void pageout(Graph& g) ;
	LIBZEF_DLL_EXPORTED void set_keep_alive(Graph& g, bool keep_alive=true) ;
	LIBZEF_DLL_EXPORTED void user_management(const std::string action, const std::string subject, const std::string target, const std::string extra);
    LIBZEF_DLL_EXPORTED void token_management(std::string action, std::string token_group, std::string token, std::string target);
    LIBZEF_DLL_EXPORTED void token_management(std::string action, EntityType et, std::string target);
    LIBZEF_DLL_EXPORTED void token_management(std::string action, RelationType rt, std::string target);
    LIBZEF_DLL_EXPORTED void token_management(std::string action, ZefEnumValue en, std::string target);
    LIBZEF_DLL_EXPORTED void token_management(std::string action, Keyword kw, std::string target);

                                 
	LIBZEF_DLL_EXPORTED ZefRef tag(ZefRef z, const TagString& tag_name, bool force_if_name_tags_other_rel_ent=false);
	LIBZEF_DLL_EXPORTED EZefRef tag(EZefRef z, const TagString& tag_name, bool force_if_name_tags_other_rel_ent=false);
	inline ZefRef tag(ZefRef z, const std::string tag_name, bool force_if_name_tags_other_rel_ent=false) {
        return tag(z, TagString(tag_name), force_if_name_tags_other_rel_ent);
    }                            
                                 
	inline EZefRef tag(EZefRef z, const std::string tag_name, bool force_if_name_tags_other_rel_ent=false) {
        return tag(z, TagString(tag_name), force_if_name_tags_other_rel_ent);
    }                            
                                 
                                 
                                 
                                 
                                 
                                 
                                 
	namespace internals {
                                 
                                 
		//                  _           _              _   _       _                     _   _ _                       
		//                 (_)_ __  ___| |_ __ _ _ __ | |_(_) __ _| |_ ___     ___ _ __ | |_(_) |_ _   _               
		//    _____ _____  | | '_ \/ __| __/ _` | '_ \| __| |/ _` | __/ _ \   / _ \ '_ \| __| | __| | | |  _____ _____ 
		//   |_____|_____| | | | | \__ \ || (_| | | | | |_| | (_| | ||  __/  |  __/ | | | |_| | |_| |_| | |_____|_____|
		//                 |_|_| |_|___/\__\__,_|_| |_|\__|_|\__,_|\__\___|   \___|_| |_|\__|_|\__|\__, |              
		//                                                                                         |___/        
                                 
				// add RAE_INSTANCE_EDGE and instantiation_edge
				// factored out to be used in instantiate_entity, instantiate_relation for both enum and string
		LIBZEF_DLL_EXPORTED void hook_up_to_schema_nodes(EZefRef my_rel_ent, GraphData& gd, std::optional<BaseUID> given_uid = {}, BlobType instantiaion_or_clone_edge_bt = BT.INSTANTIATION_EDGE);
                                 
                                 
                                 
		// called when creating a relation: prevent the user from linking any blobs_ns other than these
 		inline auto assert_is_this_a_rae = [](EZefRef uzr) {
			switch (*(BlobType*)uzr.blob_ptr) {
			case BlobType::ENTITY_NODE: return true;
			case BlobType::ATOMIC_ENTITY_NODE: return true;
			case BlobType::RELATION_EDGE: return true;
			default: {
                // print_backtrace();
                throw std::runtime_error("asserting is a RAE failed");
            }
			}                    
		};                       
                                 
                                 
		// called when creating a relation: prevent the user from linking any blobs_ns other than these
 		inline auto assert_blob_can_be_linked_via_relation = [](EZefRef uzr) {
			switch (*(BlobType*)uzr.blob_ptr) {
			case BlobType::ENTITY_NODE: return true;
			case BlobType::ATOMIC_ENTITY_NODE: return true;
			case BlobType::RELATION_EDGE: return true;
			case BlobType::TX_EVENT_NODE: return true;
			case BlobType::ROOT_NODE: return true;
			default: {
                // print_backtrace();
                throw std::runtime_error("attempting to link a blob that cannot be linked via a relation");
            }
			}                    
		};                       
                                 
                                 
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

		LIBZEF_DLL_EXPORTED bool is_terminated(EZefRef my_rel_ent);



		// how do we pass a C++ object on to a python fct we execute from within C++? Turns out to be not as easy as other conversions with pybind. 
		// Just create a function that can be accessed both by C++ / Python and stores it in a local static
		LIBZEF_DLL_EXPORTED ZefRef internal_temp_storage_used_by_zefhooks(ZefRef new_z_val= ZefRef{ EZefRef{}, EZefRef{} }, bool should_save=false);


	} // internals namespace



	// create the new entity node my_entity
	// add a RAE_INSTANCE_EDGE between n1 and the ROOT scenario node
	// open / get the existing transaction tx_nd
	// add an INSTANTIATION_EDGE between tx_nd --> my_entity
	// set uid
	LIBZEF_DLL_EXPORTED ZefRef instantiate(EntityType entity_type, GraphData& gd, std::optional<BaseUID> given_uid_maybe = {});

	inline ZefRef instantiate(EntityType entity_type, const Graph& g, std::optional<BaseUID> given_uid_maybe = {}) {
        return instantiate(entity_type, g.my_graph_data(), given_uid_maybe);
    }

	inline ZefRef instantiate(RelationType relation_type, GraphData& gd, std::optional<BaseUID> given_uid_maybe = {}) {
        throw std::runtime_error("Cannot instantiate a relation on its own. You must pass a source and target in the form instantiate(src, rt, tgt, g)");
    }
	inline ZefRef instantiate(RelationType relation_type, const Graph& g, std::optional<BaseUID> given_uid_maybe = {}) {
        return instantiate(relation_type, g.my_graph_data(), given_uid_maybe);
    }

	// for ATOMIC_ENTITY_NODE
	LIBZEF_DLL_EXPORTED ZefRef instantiate(AtomicEntityType my_atomic_entity_type, GraphData& gd, std::optional<BaseUID> given_uid_maybe = {});
	inline ZefRef instantiate(AtomicEntityType my_atomic_entity_type, const Graph& g, std::optional<BaseUID> given_uid_maybe = {}) {
        return instantiate(my_atomic_entity_type, g.my_graph_data(), given_uid_maybe);
    }



	// -------------- my_source or my_target (or both) could be part of a foreign graph! -------------
	// -------------- how do we specify which graph is the reference graph? 
	//     a) pass ref graph as a separate argument
	//     b) provide method on ref graph?
	LIBZEF_DLL_EXPORTED ZefRef instantiate(EZefRef my_source, RelationType relation_type, EZefRef my_target, GraphData& gd, std::optional<BaseUID> given_uid_maybe = {});
	inline ZefRef instantiate(EZefRef my_source, RelationType relation_type, EZefRef my_target, const Graph& g, std::optional<BaseUID> given_uid_maybe = {}) {
        return instantiate(my_source, relation_type, my_target, g.my_graph_data(), given_uid_maybe);
    }
	inline ZefRef instantiate(ZefRef my_source, RelationType relation_type, ZefRef my_target, GraphData& gd, std::optional<BaseUID> given_uid_maybe = {}) {
		return instantiate(EZefRef(my_source), relation_type, EZefRef(my_target), gd, given_uid_maybe);
	}
	inline ZefRef instantiate(ZefRef my_source, RelationType relation_type, ZefRef my_target, const Graph& g, std::optional<BaseUID> given_uid_maybe = {}) {
        return instantiate(my_source, relation_type, my_target, g.my_graph_data(), given_uid_maybe);
    }



	namespace internals {
		EZefRef get_or_create_and_get_foreign_rae(Graph& target_graph, std::variant<EntityType, AtomicEntityType, std::tuple<EZefRef, RelationType, EZefRef>> ae_or_entity_type, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid);
		LIBZEF_DLL_EXPORTED EZefRef merge_entity_(Graph& target_graph, EntityType entity_type, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid);
		LIBZEF_DLL_EXPORTED EZefRef merge_atomic_entity_(Graph& target_graph, AtomicEntityType atomic_entity_type, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid);
		LIBZEF_DLL_EXPORTED EZefRef merge_relation_(Graph& target_graph, RelationType relation_type, EZefRef src, EZefRef trg, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid);
		

        LIBZEF_DLL_EXPORTED EZefRef local_entity(EZefRef uzr);
	}

    LIBZEF_DLL_EXPORTED nlohmann::json merge(const nlohmann::json & j, Graph target_graph, bool fire_and_forget=false);



}
