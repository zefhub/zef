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

    // Note: to_index is not inclusive
    LIBZEF_DLL_EXPORTED EZefRefs blobs(GraphData& gd, blob_index from_index=constants::ROOT_NODE_blob_index, blob_index to_index=0);
	
	inline EZefRefs blobs(Graph& g, blob_index from_index=constants::ROOT_NODE_blob_index, blob_index to_index=0) {
		return blobs(g.my_graph_data(), from_index, to_index);
	}

    LIBZEF_DLL_EXPORTED EZefRefs all_raes(const Graph& graph);


    LIBZEF_DLL_EXPORTED std::ostream& operator<<(std::ostream& o, const ZefRef& zr);
    LIBZEF_DLL_EXPORTED std::ostream& operator<<(std::ostream& o, const ZefRefs& zrs);
    LIBZEF_DLL_EXPORTED std::ostream& operator<<(std::ostream& o, const ZefRefss& zrss);

    template<class T>
    void assign_value(EZefRef my_atomic_entity, const T & value_to_be_assigned);
    LIBZEF_DLL_EXPORTED extern template void assign_value(EZefRef my_atomic_entity, const bool & value_to_be_assigned);
    LIBZEF_DLL_EXPORTED extern template void assign_value(EZefRef my_atomic_entity, const int & value_to_be_assigned);
    LIBZEF_DLL_EXPORTED extern template void assign_value(EZefRef my_atomic_entity, const double & value_to_be_assigned);
    LIBZEF_DLL_EXPORTED extern template void assign_value(EZefRef my_atomic_entity, const str & value_to_be_assigned);
    // LIBZEF_DLL_EXPORTED extern template void assign_value(EZefRef my_atomic_entity, const charconst * value_to_be_assigned &);
    LIBZEF_DLL_EXPORTED extern template void assign_value(EZefRef my_atomic_entity, const Time & value_to_be_assigned);
    LIBZEF_DLL_EXPORTED extern template void assign_value(EZefRef my_atomic_entity, const SerializedValue & value_to_be_assigned);
    LIBZEF_DLL_EXPORTED extern template void assign_value(EZefRef my_atomic_entity, const ZefEnumValue & value_to_be_assigned);
    LIBZEF_DLL_EXPORTED extern template void assign_value(EZefRef my_atomic_entity, const QuantityFloat & value_to_be_assigned);
    LIBZEF_DLL_EXPORTED extern template void assign_value(EZefRef my_atomic_entity, const QuantityInt & value_to_be_assigned);
    template<>
    LIBZEF_DLL_EXPORTED void assign_value(EZefRef my_atomic_entity, const EZefRef & value_to_be_assigned);
    template<>
    LIBZEF_DLL_EXPORTED void assign_value(EZefRef my_atomic_entity, const value_variant_t & value_to_be_assigned);

    template<class T>
    void assign_value(ZefRef my_atomic_entity, const T & value_to_be_assigned) {
        assign_value(my_atomic_entity.blob_uzr, value_to_be_assigned);
    }
    template void assign_value(ZefRef my_atomic_entity, const bool & value_to_be_assigned);
    template void assign_value(ZefRef my_atomic_entity, const int & value_to_be_assigned);
    template void assign_value(ZefRef my_atomic_entity, const double & value_to_be_assigned);
    template void assign_value(ZefRef my_atomic_entity, const str & value_to_be_assigned);
    // template void assign_value(ZefRef my_atomic_entity, const charconst * & value_to_be_assigned);
    template void assign_value(ZefRef my_atomic_entity, const Time & value_to_be_assigned);
    template void assign_value(ZefRef my_atomic_entity, const SerializedValue & value_to_be_assigned);
    template void assign_value(ZefRef my_atomic_entity, const ZefEnumValue & value_to_be_assigned);
    template void assign_value(ZefRef my_atomic_entity, const QuantityFloat & value_to_be_assigned);
    template void assign_value(ZefRef my_atomic_entity, const QuantityInt & value_to_be_assigned);
    template void assign_value(ZefRef my_atomic_entity, const EZefRef & value_to_be_assigned);

    template<>
    inline void assign_value(ZefRef my_atomic_entity, const ZefRef & value_to_be_assigned) {
        assign_value(my_atomic_entity, value_to_be_assigned.blob_uzr);
    }

    template <typename T>
    std::optional<T> value_from_ae(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<double> value_from_ae<double>(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<int> value_from_ae<int>(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<bool> value_from_ae<bool>(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<std::string> value_from_ae<std::string>(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<Time> value_from_ae<Time>(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<SerializedValue> value_from_ae<SerializedValue>(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<ZefEnumValue> value_from_ae<ZefEnumValue>(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<QuantityFloat> value_from_ae<QuantityFloat>(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<QuantityInt> value_from_ae<QuantityInt>(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<AttributeEntityType> value_from_ae<AttributeEntityType>(ZefRef my_atomic_entity);
    LIBZEF_DLL_EXPORTED extern template std::optional<value_variant_t> value_from_ae<value_variant_t>(ZefRef my_atomic_entity);

    LIBZEF_DLL_EXPORTED bool is_promotable_to_zefref(EZefRef uzr_to_promote, EZefRef reference_tx);
    LIBZEF_DLL_EXPORTED bool is_promotable_to_zefref(EZefRef uzr_to_promote);
                                 
    // These definitions are not used here, but are handy to synchronise several things which call into the value functions.
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
    LIBZEF_DLL_EXPORTED std::variant<EntityType, RelationType, AttributeEntityType> rae_type(EZefRef uzr);
    inline std::variant<EntityType, RelationType, AttributeEntityType> rae_type(ZefRef zr) {
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
			case BlobType::ATTRIBUTE_ENTITY_NODE: return true;
			case BlobType::RELATION_EDGE: return true;
			case BlobType::VALUE_NODE: return true;
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
			case BlobType::ATTRIBUTE_ENTITY_NODE: return true;
			case BlobType::VALUE_NODE: return true;
			case BlobType::RELATION_EDGE: return true;
			case BlobType::TX_EVENT_NODE: return true;
			case BlobType::ROOT_NODE: return true;
			default: {
                // print_backtrace();
                throw std::runtime_error("attempting to link a blob that cannot be linked via a relation");
            }
			}                    
		};                       
                                 
                                 
		LIBZEF_DLL_EXPORTED bool is_terminated(EZefRef my_rel_ent);



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
	LIBZEF_DLL_EXPORTED ZefRef instantiate(AttributeEntityType my_atomic_entity_type, GraphData& gd, std::optional<BaseUID> given_uid_maybe = {});
	inline ZefRef instantiate(AttributeEntityType my_atomic_entity_type, const Graph& g, std::optional<BaseUID> given_uid_maybe = {}) {
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


    // Value nodes
    template<typename T>
	ZefRef instantiate_value_node(const T & value, Graph& g);
    LIBZEF_DLL_EXPORTED extern template ZefRef instantiate_value_node(const bool & value, Graph& g);
    LIBZEF_DLL_EXPORTED extern template ZefRef instantiate_value_node(const int & value, Graph& g);
    LIBZEF_DLL_EXPORTED extern template ZefRef instantiate_value_node(const double & value, Graph& g);
    LIBZEF_DLL_EXPORTED extern template ZefRef instantiate_value_node(const str & value, Graph& g);
    LIBZEF_DLL_EXPORTED extern template ZefRef instantiate_value_node(const Time & value, Graph& g);
    LIBZEF_DLL_EXPORTED extern template ZefRef instantiate_value_node(const ZefEnumValue & value, Graph& g);
    LIBZEF_DLL_EXPORTED extern template ZefRef instantiate_value_node(const QuantityFloat & value, Graph& g);
    LIBZEF_DLL_EXPORTED extern template ZefRef instantiate_value_node(const QuantityInt & value, Graph& g);
    LIBZEF_DLL_EXPORTED extern template ZefRef instantiate_value_node(const SerializedValue & value, Graph& g);
    LIBZEF_DLL_EXPORTED extern template ZefRef instantiate_value_node(const AttributeEntityType & value, Graph& g);




	namespace internals {
		EZefRef get_or_create_and_get_foreign_rae(Graph& target_graph, std::variant<EntityType, AttributeEntityType, std::tuple<EZefRef, RelationType, EZefRef>> ae_or_entity_type, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid);
		LIBZEF_DLL_EXPORTED EZefRef merge_entity_(Graph& target_graph, EntityType entity_type, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid);
		LIBZEF_DLL_EXPORTED EZefRef merge_atomic_entity_(Graph& target_graph, AttributeEntityType atomic_entity_type, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid);
		LIBZEF_DLL_EXPORTED EZefRef merge_relation_(Graph& target_graph, RelationType relation_type, EZefRef src, EZefRef trg, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid);
		

        LIBZEF_DLL_EXPORTED EZefRef local_entity(EZefRef uzr);
	}

    LIBZEF_DLL_EXPORTED nlohmann::json merge(const nlohmann::json & j, Graph target_graph, bool fire_and_forget=false);


    // Expose some low_level_api things as high_level_api with ZefRefs supported too.
    using internals::is_root;
    using internals::is_delegate;
    using internals::is_delegate_relation_group;
    using internals::has_delegate;

    inline bool is_root(ZefRef z) {
        return internals::is_root(z.blob_uzr);
    }
    inline bool is_delegate(ZefRef z) {
        return internals::is_delegate(z.blob_uzr);
    }
    inline bool is_delegate_relation_group(ZefRef z) {
        return internals::is_delegate_relation_group(z.blob_uzr);
    }
    inline bool has_delegate(ZefRef z) {
        return internals::has_delegate(z.blob_uzr);
    }

}
