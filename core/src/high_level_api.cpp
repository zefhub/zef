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

#include "high_level_api.h"
#include "zefops.h"
#include "ops_imperative.h"
#include "synchronization.h"
#include "external_handlers.h"
#include <iterator>
#include <unordered_set>
#include <doctest/doctest.h>

namespace zefDB {

//                                   _                    __      _                            
//                         ___ _ __ | |_ _ __ _   _      / _| ___| |_ ___                      
//    _____ _____ _____   / _ \ '_ \| __| '__| | | |    | |_ / __| __/ __|   _____ _____ _____ 
//   |_____|_____|_____| |  __/ | | | |_| |  | |_| |    |  _| (__| |_\__ \  |_____|_____|_____|
//                        \___|_| |_|\__|_|   \__, |    |_|  \___|\__|___/                     
//                                            |___/                                            

	EZefRefs blobs(GraphData& gd, blob_index from_index, blob_index to_index) {
        if (to_index == 0)
            to_index = gd.write_head;
        
		blob_index len_to_reserve = to_index - from_index;
		auto res = EZefRefs(len_to_reserve, &gd);

		EZefRef* pos_to_write_to = res._get_array_begin();
        // Need to use indices in case we go off the edge of memory.
        blob_index cur_index = from_index;
		while (cur_index < to_index) {
            EZefRef uzr{cur_index, gd};
			*(pos_to_write_to++) = uzr;
            cur_index += blob_index_size(uzr);
		}
		blob_index actual_len_written = pos_to_write_to - res._get_array_begin();
		res.len = actual_len_written;
		if(res.delegate_ptr!=nullptr) res.delegate_ptr->len = actual_len_written;
		return res;
	}
	
    EZefRefs all_raes(const Graph& graph) {
        return imperative::filter<EZefRef>(blobs(graph),
                               [](EZefRef x) {
                                   return ((BT(x) == BT.ENTITY_NODE)
                                           || (BT(x) == BT.ATTRIBUTE_ENTITY_NODE)
                                           || (BT(x) == BT.RELATION_EDGE)
                                           || (BT(x) == BT.VALUE_NODE))
                                       && !is_delegate(x);
                               });
    }

    std::ostream& operator<<(std::ostream& o, const ZefRef& zr) {
        //o << "<ZefRef with ptr to " << zr.blob_uzr << ">";
        // o << "<ZefRef with" << std::endl;
        // o << "blob_uzr=" << zr.blob_uzr << std::endl;
        // o << "tx=" << zr.tx;
        // o << " >";
        o << "<ZefRef #" << index(zr);

        if(is_delegate(zr))
            o << " DELEGATE";
            
        if (BT(zr) == BT.ENTITY_NODE)
            o << " " << ET(zr);
        else if (BT(zr) == BT.RELATION_EDGE)
            o << " " << RT(zr);
        else if (BT(zr) == BT.ATTRIBUTE_ENTITY_NODE) {
            if(internals::is_delegate(zr.blob_uzr))
                o << " " << VRT(zr);
            else
                o << " " << AET(zr);
        }
        else if (BT(zr) == BT.TX_EVENT_NODE)
            o << " TX at slice=" << TimeSlice(zr.blob_uzr).value << " seen from";
        else
            o << " " << BT(zr);
        o << " slice=" << TimeSlice(zr.tx).value;
        o << ">";
        return o;
    }

    std::ostream& operator<<(std::ostream& o, const ZefRefs& zrs) {
        return o << "<ZefRefs with " << length(zrs) << " items>";
    }
    std::ostream& operator<<(std::ostream& o, const ZefRefss& zrss) {
        return o << "<ZefRefs with " << length(zrss) << " items>";
    }


	bool is_promotable_to_zefref(EZefRef uzr_to_promote, EZefRef reference_tx) {
		if (reference_tx.blob_ptr == nullptr)
			throw std::runtime_error("is_promotable_to_zefref called on EZefRef pointing to nullptr");
			
		if (get<BlobType>(reference_tx) != BlobType::TX_EVENT_NODE)
			throw std::runtime_error("is_promotable_to_zefref called with a reference_tx that is not of blob type TX_EVENT_NODE");
		
        return is_promotable_to_zefref(uzr_to_promote);
	}

	bool is_promotable_to_zefref(EZefRef uzr_to_promote) {
		if( BT(uzr_to_promote) == BT.ATTRIBUTE_ENTITY_NODE
			|| BT(uzr_to_promote) == BT.ENTITY_NODE
			|| BT(uzr_to_promote) == BT.RELATION_EDGE
			|| BT(uzr_to_promote) == BT.VALUE_NODE
			|| BT(uzr_to_promote) == BT.TX_EVENT_NODE
			|| BT(uzr_to_promote) == BT.ROOT_NODE)
            return true;
        return false;
    }

    std::variant<EntityType, RelationType, AttributeEntityType> rae_type(EZefRef uzr) {
        // Given any ZefRef or EZefRef, return the ET, RT or AET. Throw an error if it is a different blob type.
        if (BT(uzr)==BT.ENTITY_NODE)
            return ET(uzr);
        else if (BT(uzr)==BT.RELATION_EDGE)
            return RT(uzr);
        else if (BT(uzr)==BT.ATTRIBUTE_ENTITY_NODE) {
            if(internals::is_delegate(uzr))
                return VRT(uzr);
            else
                return AET(uzr);
        } else
            throw std::runtime_error("Item is not a RAE blob type: " + to_str(BT(uzr)));
    }



	void make_primary(Graph& g, bool take_on) {
        auto butler = Butler::get_butler();
        auto response = butler->msg_push<Messages::GenericResponse>(Messages::MakePrimary{g,take_on});
        if(!response.success) {
            if(take_on)
                throw std::runtime_error("Unable to take primary role: " + response.reason);
            else
                throw std::runtime_error("Unable to give up primary role: " + response.reason);
        } else {
            if(take_on) {
                // Turn sync on automatically
                sync(g);
            }
        }
	}
		

    void tag(const Graph& g, const std::string& name_tag, bool force_if_name_tags_other_graph, bool adding) {
        if(!g.my_graph_data().should_sync)
            throw std::runtime_error("Can't tag when graph is not being synchronised");

        auto butler = Butler::get_butler();
        auto response = butler->msg_push<Messages::GenericResponse>(Messages::TagGraph{g, name_tag, force_if_name_tags_other_graph, !adding});
        if(!response.success)
            throw std::runtime_error("Unable to tag graph: " + response.reason);

    }



    std::tuple<Graph,EZefRef> zef_get(const std::string& some_uid_or_name_tag) {
        // std::string task_uid = tasks::generate_random_task_uid();
		// tasks::for_zm::zef_get(some_uid_or_name_tag, task_uid);

        // std::string graph_uid;
        // tasks::wait_zm_with_timeout("Timeout: No zef_get response received.",
        //                      [&]() {
        //                          return tasks::pop_zm_response<tasks::zef_get_response>(task_uid,
        //                                                                                 [&] (auto & obj) {
        //                              if (!obj.success) {
        //                                  std::string msg = "Could not find uid or name tag '" + some_uid_or_name_tag + "', reason was: " + obj.response;
        //                                  cout << msg;
        //                                  throw std::runtime_error(msg);
        //                              }
        //                              graph_uid = obj.guid;
        //                              if (obj.uid != some_uid_or_name_tag)
        //                                  throw("Inconsistent return from ZefHub for uid");
        //                          });
        //                      });

        // Graph g(graph_uid);

		// EZefRef uzr_with_requested_uid = g[some_uid_or_name_tag];
		// return std::make_tuple(g, uzr_with_requested_uid);
            throw std::runtime_error("To be implemented");
	}
	
    std::vector<std::string> zearch(const std::string& zearch_term) {
        auto butler = Butler::get_butler();
        auto response = butler->msg_push_timeout<Messages::GenericZefHubResponse>(Messages::ZearchQuery{zearch_term});
        if(!response.generic.success)
            throw std::runtime_error("Failed with zearch: " + response.generic.reason);
        return response.j["matches"].get<std::vector<std::string>>();
	}

    std::optional<std::string> lookup_uid(const std::string& tag) {
        auto butler = Butler::get_butler();
        auto response = butler->msg_push_timeout<Messages::GenericZefHubResponse>(Messages::UIDQuery{tag});
        if(!response.generic.success)
            throw std::runtime_error("Failed with uid lookup: " + response.generic.reason);
        if(response.j.contains("graph_uid"))
            return response.j["graph_uid"].get<std::string>();
        else
            return std::optional<std::string>();
    }


    void sync(Graph& g, bool do_sync) {
		// the graph should be sent out immediately before a tx is closed etc.. 
		// Make sure that a graph can only be cloned from another graph if no tx is open!
        g.sync(do_sync);
	}

    void pageout(Graph& g) {
        GraphData & gd = g.my_graph_data();
        auto & info = MMap::info_from_blobs(&gd);
        MMap::page_out_mmap(info);
    }

    void set_keep_alive(Graph& g, bool keep_alive) {
        auto butler = Butler::get_butler();
        auto response = butler->msg_push<Messages::GenericResponse>(Messages::SetKeepAlive{g,keep_alive});
        if(!response.success) {
                throw std::runtime_error("Unable to set keep alive: " + response.reason);
        }
    }

	void user_management(std::string action, std::string subject, std::string target, std::string extra) {
        auto butler = Butler::get_butler();
        auto response = butler->msg_push_timeout<Messages::GenericZefHubResponse>(Messages::OLD_STYLE_UserManagement{action, subject, target, extra});

        if(!response.generic.success)
            throw std::runtime_error("Failed with user management: " + response.generic.reason);
	}

	void token_management(std::string action, std::string token_group, std::string token, std::string target) {
        auto butler = Butler::get_butler();
        auto response = butler->msg_push_timeout<Messages::GenericZefHubResponse>(Messages::TokenManagement{action, token_group, token, target});

        if(!response.generic.success)
            throw std::runtime_error("Failed with token management: " + response.generic.reason);
	}

	void token_management(std::string action, EntityType et, std::string target) {
        token_management(action, "ET", str(et), target);
	}
	void token_management(std::string action, RelationType rt, std::string target) {
        token_management(action, "RT", str(rt), target);
	}
	void token_management(std::string action, ZefEnumValue en, std::string target) {
        token_management(action, "EN", en.enum_type() + "." + en.enum_value(), target);
	}
	void token_management(std::string action, Keyword kw, std::string target) {
        token_management(action, "KW", str(kw), target);
	}





	ZefRef tag(ZefRef z, const TagString& tag_name_value, bool force_if_name_tags_other_rel_ent) {		
		using namespace internals;
		if (tag_name_value.s.size() > constants::max_tag_size)
			throw std::runtime_error("the maximum length of a tag name for a ZefRef that can be assigned is " + to_str(constants::max_tag_size));
		GraphData& gd = Graph(z).my_graph_data();
		auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);   // at least one new delegate will be created. Open a tx in case none are open from the outside
		EZefRef tx_event = get_or_create_and_get_tx(gd);
        // Need to manage the instantiation ourselves since we need to provide the correct size.
        void * new_ptr = (void*)(std::uintptr_t(&gd) + gd.write_head * constants::blob_indx_step_in_bytes);
        // Do a preliminary ensure here just to get memory to fill in the rest of the blob size details.
        MMap::ensure_or_alloc_range(new_ptr, blobs_ns::max_basic_blob_size);
        new (new_ptr) blobs_ns::ASSIGN_TAG_NAME_EDGE;

        blobs_ns::ASSIGN_TAG_NAME_EDGE & assign_tag_name = *(blobs_ns::ASSIGN_TAG_NAME_EDGE*)new_ptr;
        EZefRef trg{imperative::traverse_in_edge(z.blob_uzr, BT.RAE_INSTANCE_EDGE)};
        assign_tag_name.source_node_index = index(tx_event);
        assign_tag_name.target_node_index = index(trg);
        // Note: using copy_to_buffer also handles ensuring memory availability.
        internals::copy_to_buffer(get_data_buffer(assign_tag_name),
                       assign_tag_name.buffer_size_in_bytes,
                       tag_name_value.s);

        // Only now does the blob know its size and we can advance
        EZefRef assign_tag_name_uzr = EZefRef(new_ptr);
        move_head_forward(gd);
        append_edge_index(tx_event, index(assign_tag_name_uzr));
        append_edge_index(trg, -index(assign_tag_name_uzr));

		// check if this tag was previously used: if yes (it is in the keydict), then create an edge of type BT.NEXT_TAG_NAME_ASSIGNMENT_EDGE from the new to the previous edge
		Graph g = Graph(gd);
		if (g.contains(TagString{tag_name_value})) {
			// find the last ASSIGN_TAG_NAME_EDGE edge before this one using the same tag name
			auto previous_assign_tag_name_edge = g[tag_name_value]
				< BT.RAE_INSTANCE_EDGE
				< L[BT.ASSIGN_TAG_NAME_EDGE]
				| sort[std::function<int(EZefRef)>([](EZefRef uz)->int { return get<blobs_ns::TX_EVENT_NODE>(uz | source).time_slice.value; })]  // there could be multiple with the same tag name, even for the same REL_ENT, we want the latest one
				| last;
			internals::instantiate(
				previous_assign_tag_name_edge,
				BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE,
				assign_tag_name_uzr,
				gd);
		}
		// we don't need to worry about double linking edges here (set last arg to false). Only enter the tag into the dictionary
		apply_action_ASSIGN_TAG_NAME_EDGE(gd, assign_tag_name_uzr, true);  // use the replay system to execute the action. IO-monad-like :)
		return z;
	}


	EZefRef tag(EZefRef uz, const TagString& tag_name_value, bool force_if_name_tags_other_rel_ent) {
		return tag(imperative::now(uz), tag_name_value, force_if_name_tags_other_rel_ent).blob_uzr;
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
		void hook_up_to_schema_nodes(EZefRef my_rel_ent, GraphData& gd, std::optional<BaseUID> given_uid_maybe, BlobType instantiaion_or_clone_edge_bt) {
			// this function is used for instantiation of new REL_ENTS and cloning these from other graphs. Pass the blob type for the edge in.
			// In case of cloning, not all a parameters are set immediately, but the calling function will revisit these bytes and set the 
			// a) ancestor rel_ent uid  b) originating graph uid   c) tx uid on originating graph
			EZefRef tx_event = get_or_create_and_get_tx(gd);
			EZefRef rel_ent_edge = internals::instantiate(BT.RAE_INSTANCE_EDGE, gd);
			auto& RAE_INSTANCE_EDGE = get<blobs_ns::RAE_INSTANCE_EDGE>(rel_ent_edge);			
			// don't use a 'move_head_forward(gd);' here! For this section it is very tricky to get the imperative order right when setting up the blobs

			RAE_INSTANCE_EDGE.target_node_index = index(my_rel_ent);
			blob_index RAE_INSTANCE_EDGE_indx = index(rel_ent_edge);
			append_edge_index(my_rel_ent, -RAE_INSTANCE_EDGE_indx);
            EZefRef z_delegate = *imperative::delegate_to_ezr(delegate_of(my_rel_ent), Graph(gd), true);
 			EZefRef to_delegate_edge = z_delegate < BT.TO_DELEGATE_EDGE;  // pass the relation or entity along to enable extarcting the type
			
			RAE_INSTANCE_EDGE.source_node_index = index(to_delegate_edge);
			append_edge_index(to_delegate_edge, RAE_INSTANCE_EDGE_indx);  // incoming edges are represented as the negative index
			auto rel_ent_edge_uzr = EZefRef(RAE_INSTANCE_EDGE_indx, gd);

			internals::instantiate(tx_event, instantiaion_or_clone_edge_bt, rel_ent_edge_uzr, gd);

            if(has_uid(my_rel_ent)) {
                if (given_uid_maybe) {
                    assign_uid(my_rel_ent, *given_uid_maybe); // write the uid in binary form into the uid buffer at pointer_to_uid_start
                }
                else {
                    assign_uid(my_rel_ent, make_random_uid());  // this also adds the uid to the dict key_dict with this index
                }
            }
        }



		bool is_terminated(EZefRef my_rel_ent) {
            return !imperative::exists_at_now(my_rel_ent);
		}
    }



	

    // This needs to come before the definition of instantiate with AEs
    template<typename T>
	ZefRef instantiate_value_node(const T & value, GraphData& gd) {
        if (!gd.is_primary_instance)
            throw std::runtime_error("'instantiate_value_node' called for a graph which is not a primary instance. This is not allowed. Shame on you!");

        auto maybe_node = internals::search_value_node(value, gd);
        if(maybe_node)
            return imperative::now(*maybe_node);

        auto this_tx = Transaction(gd);
        EZefRef tx_event = internals::get_or_create_and_get_tx(gd);

        EZefRef value_node = internals::instantiate_value_node(value, gd);
        internals::hook_up_to_schema_nodes(value_node, gd);
        // internals::instantiate(tx_event, BT.INSTANTIATION_EDGE, value_node, gd);

        return imperative::now(value_node); 
    }

    template<typename T>
	ZefRef instantiate_value_node(const T & value, Graph& g) {
        return instantiate_value_node(value, g.my_graph_data());
    }
    template ZefRef instantiate_value_node(const bool & value, Graph& g);
    template ZefRef instantiate_value_node(const int & value, Graph& g);
    template ZefRef instantiate_value_node(const double & value, Graph& g);
    template ZefRef instantiate_value_node(const str & value, Graph& g);
    template ZefRef instantiate_value_node(const Time & value, Graph& g);
    template ZefRef instantiate_value_node(const ZefEnumValue & value, Graph& g);
    template ZefRef instantiate_value_node(const QuantityFloat & value, Graph& g);
    template ZefRef instantiate_value_node(const QuantityInt & value, Graph& g);
    template ZefRef instantiate_value_node(const SerializedValue & value, Graph& g);
    template ZefRef instantiate_value_node(const AttributeEntityType & value, Graph& g);



	// create the new entity node my_entity
	// add a RAE_INSTANCE_EDGE between n1 and the ROOT scenario node
	// open / get the existing transaction tx_nd
	// add an INSTANTIATION_EDGE between tx_nd --> my_entity
	// set uid
	ZefRef instantiate(EntityType entity_type, GraphData& gd, std::optional<BaseUID> given_uid_maybe) {
		if (!gd.is_primary_instance)
			throw std::runtime_error("'instantiate entity' called for a graph which is not a primary instance. This is not allowed. Shame on you!");
		using namespace internals;

		// allocate this structure in the memory pool and move head forward
		auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);

		EZefRef my_entity = internals::instantiate(BT.ENTITY_NODE, gd);
		auto& entity_struct = get<blobs_ns::ENTITY_NODE>(my_entity);
		entity_struct.entity_type.entity_type_indx = entity_type.entity_type_indx;

		hook_up_to_schema_nodes(my_entity, gd, given_uid_maybe);
		EZefRef tx_node{ gd.index_of_open_tx_node, gd };
		entity_struct.instantiation_time_slice.value = get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice.value;

        apply_action_ENTITY_NODE(gd, my_entity, true);

		auto new_entity = ZefRef{ my_entity, tx_node };		
		return new_entity;
	}

	// for ATOMIC_ENTITY_NODE
	ZefRef instantiate(AttributeEntityType aet, GraphData& gd, std::optional<BaseUID> given_uid_maybe) {
		if (!gd.is_primary_instance)
			throw std::runtime_error("'instantiate atomic entity' called for a graph which is not a primary instance. This is not allowed. Shame on you!");
		auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);

		EZefRef my_entity = internals::instantiate(BT.ATTRIBUTE_ENTITY_NODE, gd);
		auto& entity_struct = get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(my_entity);		

		EZefRef tx_node{ gd.index_of_open_tx_node, gd };
		entity_struct.instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice;


        // TODO:
        // 1. Always create value node for value type
        // 2. Determine primitive VRT for type
        // 3. Hook up to schema nodes
        // 4. Create value type


        ValueRepType primitive_vrt;
        if(aet._is_complex()) {
            // Determine VRT from python
            primitive_vrt = internals::pass_to_determine_primitive_type(aet);
        } else {
            primitive_vrt = aet.rep_type;
        }
            

		// AtomicEntityType has a const member and we can't set this afterwards. Construct into place (call constructor on specified address)
		// 'placement new': see https://www.stroustrup.com/C++11FAQ.html#unions
        new(&entity_struct.primitive_type) ValueRepType{ primitive_vrt.value };  // take on same type for the atomic entity delegate

        internals::hook_up_to_schema_nodes(my_entity, gd, given_uid_maybe);

        ZefRef value_node = instantiate_value_node(aet, gd);
        internals::instantiate(my_entity, BT.VALUE_TYPE_EDGE, value_node.blob_uzr, gd);

        internals::apply_action_ATTRIBUTE_ENTITY_NODE(gd, my_entity, true);
		
		auto new_entity = ZefRef{ my_entity, tx_node };
		return new_entity;
	}


	// -------------- my_source or my_target (or both) could be part of a foreign graph! -------------
	// -------------- how do we specify which graph is the reference graph? 
	//     a) pass ref graph as a separate argument
	//     b) provide method on ref graph?
	ZefRef instantiate(EZefRef my_source, RelationType relation_type, EZefRef my_target, GraphData& gd, std::optional<BaseUID> given_uid_maybe) {
		// TODO: we are working in the most recent, current time slice. Check if my_source and my_target exist here!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		if (!gd.is_primary_instance)
			throw std::runtime_error("'instantiate relation' called for a graph which is not a primary instance. This is not allowed. Shame on you!");

		// TODO: check whether my_source/my_target belong to the local graph. If not, whether they belong to a view graph !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		// If their graph is not a local view graph, add it as a view graph
		// check whether local proxy exists. If not, create local proxy
		// tasks::apply_immediate_updates_from_zm();
		using namespace internals;
		auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);
 		assert_blob_can_be_linked_via_relation(my_source);
 		assert_blob_can_be_linked_via_relation(my_target);




		//blobs_ns::RELATION_EDGE& rel_struct = get_next_free_writable_blob<blobs_ns::RELATION_EDGE>(gd);
		//rel_struct.this_BlobType = BlobType::RELATION_EDGE;
		//// RelationType has a const member and we can't set this afterwards. Construct into place (call constructor on specified address)
		//// 'placement new': see https://www.stroustrup.com/C++11FAQ.html#unions
		//new(&rel_struct.relation_type) RelationType(relation_type.relation_type_indx);  // take on same type for the delegate group relation	
		//move_head_forward(gd);
		//rel_struct.source_node_index = index(my_source);
		//rel_struct.target_node_index = index(my_target);
		//EZefRef this_rel((void*)&rel_struct);
		//blob_index this_rel_index = index(this_rel);
		//append_edge_index(my_source, this_rel_index);
		//append_edge_index(my_target, -this_rel_index);



		// replaced by the following
		EZefRef this_rel = internals::instantiate(my_source, BT.RELATION_EDGE, my_target, gd);
		auto& rel_struct = get<blobs_ns::RELATION_EDGE>(this_rel);
		// RelationType has a const member and we can't set this afterwards. Construct into place (call constructor on specified address)
		// 'placement new': see https://www.stroustrup.com/C++11FAQ.html#unions
		new(&rel_struct.relation_type) RelationType(relation_type.relation_type_indx);  // take on same type for the delegate group relation	



		


 		hook_up_to_schema_nodes(this_rel, gd, given_uid_maybe);
		EZefRef tx_node{ gd.index_of_open_tx_node, gd };
		rel_struct.instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice;

        apply_action_RELATION_EDGE(gd, this_rel, true);

		auto new_relation = ZefRef{ this_rel, tx_node };
		return new_relation;
	}
    namespace internals {

        auto can_be_cloned = [](ZefRef zz)->bool { return
			(BT(zz) == BT.ENTITY_NODE ||
				BT(zz) == BT.ATTRIBUTE_ENTITY_NODE ||
				BT(zz) == BT.RELATION_EDGE
				) && !is_delegate(zz);
		};


		// used in both clone and merge
		auto get_or_create_and_get_foreign_graph = [](Graph& target_graph, const BaseUID& graph_uid)->EZefRef {
			if (graph_uid == get_graph_uid(target_graph)) return target_graph[constants::ROOT_NODE_blob_index];   // rel_ent to clone comes from the graph itself: indicate this by having the cloned_from edge come out of the root node
			if (target_graph.contains(graph_uid) && BT(target_graph[graph_uid]) == BT.FOREIGN_GRAPH_NODE) return target_graph[graph_uid];   // the foreign graph is already present
			GraphData& gd = target_graph.my_graph_data();
			auto new_foreign_graph_uzr = internals::instantiate(BT.FOREIGN_GRAPH_NODE, gd);
			// copy the origin graph's uid into the uid space of the FOREIGN_GRAPH_NODE node on the target graph
			// from_hex(graph_uid, uid_ptr_from_blob(new_foreign_graph_uzr.blob_ptr)); // write the uid in binary form into the uid buffer at pointer_to_uid_start
			assign_uid(new_foreign_graph_uzr, graph_uid); // write the uid in binary form into the uid buffer at pointer_to_uid_start
			apply_action_FOREIGN_GRAPH_NODE(gd, new_foreign_graph_uzr, true);  // use the replay system to execute the action.
			return new_foreign_graph_uzr;
		};







		//#                                                                  ____       ___      _            _                                                    
		//#                           _ __ ___    ___  _ __   __ _   ___    |___ \     / _ \    | |__    ___ | | _ __    ___  _ __  ___                            
		//#   _____  _____  _____    | '_ ` _ \  / _ \| '__| / _` | / _ \     __) |   | | | |   | '_ \  / _ \| || '_ \  / _ \| '__|/ __|       _____  _____  _____ 
		//#  |_____||_____||_____|   | | | | | ||  __/| |   | (_| ||  __/    / __/  _ | |_| |   | | | ||  __/| || |_) ||  __/| |   \__ \      |_____||_____||_____|
		//#                          |_| |_| |_| \___||_|    \__, | \___|   |_____|(_) \___/    |_| |_| \___||_|| .__/  \___||_|   |___/                           
		//#                                                  |___/                                              |_|                                                


		EZefRef find_origin_rae(EZefRef z_instance) {
			/*
				Used by the low level routine attaching the src / trg for a BT.FOREIGN_RELATION :

			a) If there is a BT.FOREIGN_ENTITY / BT.FOREIGN_ATOMIC_ENTITY / BT.FOREIGN_RELATION
			for this instance, this will be returned.
				b) If the origin rae is on the graph, it may be the currently alive rae or it may not.
				In either case, don't hook up the FOREIGN_RELATION to the the instance itself, but to its
				BT.RAE_INSTANCE_EDGE edge to no clutter.
			*/
			// assert BT(z_instance) in { BT.ENTITY_NODE, BT.ATOMIC_ENTITY_NODE, BT.RELATION_EDGE }

			auto z_rae_inst = (z_instance < BT.RAE_INSTANCE_EDGE);
			auto origin_candidates = z_rae_inst >> L[BT.ORIGIN_RAE_EDGE];
			return length(origin_candidates) == 1 ? (origin_candidates | only) : z_rae_inst;
		}


		EZefRef get_or_create_and_get_foreign_rae(Graph& target_graph, std::variant<EntityType, AttributeEntityType, std::tuple<EZefRef, RelationType, EZefRef>> ae_or_entity_type, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid) {
			if (target_graph.contains(origin_entity_uid)) {
				//if (rae_type != (target_graph[origin_entity_uid])) throw std::runtime_error("the entity type of the RAE's uid did not match the expected entity type in get_or_create_and_get_foreign_rae");
				return target_graph[origin_entity_uid];
			}
			// the foreign (atomic)entity is not in this graph
			GraphData& gd = target_graph.my_graph_data();
			EZefRef new_foreign_entity_uzr = std::visit(overloaded{
				[&](EntityType et) {
					EZefRef new_foreign_entity_uzr = instantiate(BT.FOREIGN_ENTITY_NODE, gd);
					reinterpret_cast<blobs_ns::FOREIGN_ENTITY_NODE*>(new_foreign_entity_uzr.blob_ptr)->entity_type = et;
					return new_foreign_entity_uzr;
				},
				[&](AttributeEntityType aet) {
					EZefRef new_foreign_entity_uzr = instantiate(BT.FOREIGN_ATTRIBUTE_ENTITY_NODE, gd);

                    ValueRepType primitive_vrt;
                    if(aet._is_complex()) {
                        // Determine VRT from python
                        primitive_vrt = pass_to_determine_primitive_type(aet);
                    } else {
                        primitive_vrt = aet.rep_type;
                    }

                    new(&(reinterpret_cast<blobs_ns::FOREIGN_ATTRIBUTE_ENTITY_NODE*>(new_foreign_entity_uzr.blob_ptr)->primitive_type)) ValueRepType{ primitive_vrt.value };  // take on same type for the atomic entity delegate

                    ZefRef value_node = zefDB::instantiate_value_node(aet, gd);
                    instantiate(new_foreign_entity_uzr, BT.VALUE_TYPE_EDGE, value_node.blob_uzr, gd);

					return new_foreign_entity_uzr;
				},	
				[&](const std::tuple<EZefRef, RelationType, EZefRef>& trip) {
					EZefRef src = find_origin_rae(std::get<0>(trip));
					EZefRef trg = find_origin_rae(std::get<2>(trip));
					EZefRef new_foreign_entity_uzr = instantiate(src, BT.FOREIGN_RELATION_EDGE, trg, gd);				// Init with 0: we don't know the src or trg nodes yet in all cases
					reinterpret_cast<blobs_ns::FOREIGN_RELATION_EDGE*>(new_foreign_entity_uzr.blob_ptr)->relation_type = std::get<1>(trip);
					return new_foreign_entity_uzr;
				},				
				}, ae_or_entity_type);
			assign_uid( new_foreign_entity_uzr, origin_entity_uid); // write the uid in binary form into the uid buffer at pointer_to_uid_start
			EZefRef my_foreign_graph = get_or_create_and_get_foreign_graph(target_graph, origin_graph_uid);
			instantiate(new_foreign_entity_uzr, BT.ORIGIN_GRAPH_EDGE, my_foreign_graph, gd);
            // Note: the apply_action traverses the ORIGIN_GRAPH_EDGE so this needs to happen after the link to the origin graph is created.
			apply_action_lookup(gd, new_foreign_entity_uzr, true);  // use the replay system to execute the action.
			return new_foreign_entity_uzr;
		};



		bool is_rae_foregin(EZefRef z) {
			// small utility function to distinguish e.g. FOREIGN_ENTITY_NODE vs ENTITY_NODE
			return BT(z) == BT.FOREIGN_ENTITY_NODE
				|| BT(z) == BT.FOREIGN_ATTRIBUTE_ENTITY_NODE
				|| BT(z) == BT.FOREIGN_RELATION_EDGE;
		}


		EZefRef merge_entity_(Graph& target_graph, EntityType entity_type, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid) {
		/* 
		* This function is called once it is clear that no living member of the RAE's lineage exists.
		* It will definitely create a new instance and possibly a ForeignEntity (if this does not exist yet).
		* Does not open a tx: this is done by the outer context function 
		*/
			GraphData& gd = target_graph.my_graph_data();
			EZefRef foreign_or_local_entity = get_or_create_and_get_foreign_rae(target_graph, entity_type, origin_entity_uid, origin_graph_uid);
			EZefRef z_target_for_origin_rae = is_rae_foregin(foreign_or_local_entity) ? foreign_or_local_entity : (foreign_or_local_entity < BT.RAE_INSTANCE_EDGE);
			auto new_entity = EZefRef(instantiate(entity_type, gd));		// a new local uid will be generated
			instantiate(EZefRef(new_entity) < BT.RAE_INSTANCE_EDGE, BT.ORIGIN_RAE_EDGE, z_target_for_origin_rae, gd);
			return new_entity;
		}


		
		EZefRef merge_atomic_entity_(Graph & target_graph, AttributeEntityType atomic_entity_type, const BaseUID & origin_entity_uid, const BaseUID & origin_graph_uid) {
			/*
			* This function is called once it is clear that no living member of the RAE's lineage exists.
			* It will definitely create a new instance and possibly a ForeignEntity (if this does not exist yet).
			* Does not open a tx: this is done by the outer context function
			*/
			GraphData& gd = target_graph.my_graph_data();
			EZefRef foreign_or_local_entity = get_or_create_and_get_foreign_rae(target_graph, atomic_entity_type, origin_entity_uid, origin_graph_uid);
			EZefRef z_target_for_origin_rae = is_rae_foregin(foreign_or_local_entity) ? foreign_or_local_entity : (foreign_or_local_entity < BT.RAE_INSTANCE_EDGE);
			auto new_atomic_entity = EZefRef(instantiate(atomic_entity_type, gd));		// a new local uid will be generated
			instantiate(EZefRef(new_atomic_entity) < BT.RAE_INSTANCE_EDGE, BT.ORIGIN_RAE_EDGE, z_target_for_origin_rae, gd);
			return new_atomic_entity;
		}


		EZefRef merge_relation_(Graph& target_graph, RelationType relation_type, EZefRef src, EZefRef trg, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid) {
			/*
			* This function is called once it is clear that no living member of the RAE's lineage exists.
			* It will definitely create a new instance and possibly a ForeignEntity (if this does not exist yet).
			* Does not open a tx: this is done by the outer context function
			*/
			GraphData& gd = target_graph.my_graph_data();
			EZefRef foreign_or_local_entity = get_or_create_and_get_foreign_rae(target_graph, std::make_tuple(src, relation_type, trg), origin_entity_uid, origin_graph_uid);
			EZefRef z_target_for_origin_rae = is_rae_foregin(foreign_or_local_entity) ? foreign_or_local_entity : (foreign_or_local_entity < BT.RAE_INSTANCE_EDGE);
			auto new_rel = EZefRef(instantiate(src, relation_type, trg, gd));
			instantiate(EZefRef(new_rel) < BT.RAE_INSTANCE_EDGE, BT.ORIGIN_RAE_EDGE, z_target_for_origin_rae, gd);
			return new_rel;
		}

        EZefRef local_entity(EZefRef uzr) {
            if (BT(uzr) != BT.FOREIGN_ENTITY_NODE &&
                BT(uzr) != BT.FOREIGN_RELATION_EDGE &&
                BT(uzr) != BT.FOREIGN_ATTRIBUTE_ENTITY_NODE)
                throw std::runtime_error("local_entity can only be applied to BT.FOREIGN_* blobs, not" + to_str(BT(uzr)));

            return imperative::target(imperative::traverse_in_node(uzr, BT.ORIGIN_RAE_EDGE));
        }
	}


    nlohmann::json merge(const nlohmann::json & j, Graph target_graph, bool fire_and_forget) {
       auto butler = Butler::get_butler();

       Messages::MergeRequest msg {
           {},
           internals::get_graph_uid(target_graph),
           Messages::MergeRequest::PayloadGraphDelta{j},
       };

       if(fire_and_forget) {
           butler->msg_push_internal(std::move(msg));
           // Empty reply is a little weird, but need to return something
           return {};
       }

       auto response =
           butler->msg_push_timeout<Messages::MergeRequestResponse>(
               std::move(msg),
               Butler::zefhub_generic_timeout
           );

       if(!response.generic.success)
           throw std::runtime_error("Unable to perform merge: " + response.generic.reason);

       // Need to make sure the latest updates from the graph
       // have been received before continuing here.
       // TODO: This is not possible now, but need to do this in the future.

       auto r = std::get<Messages::MergeRequestResponse::ReceiptGraphDelta>(response.receipt);
       // Wait for graph to be up to date before deserializing
       auto & gd = target_graph.my_graph_data();
       bool reached_sync = wait_pred(gd.heads_locker,
                                     [&]() { return gd.read_head >= r.read_head; },
                                     // std::chrono::duration<double>(Butler::butler_generic_timeout.value));
                                     std::chrono::duration<double>(60.0));
       if(!reached_sync)
           throw std::runtime_error("Did not sync in time to handle merge receipt.");
       
       return r.receipt;
    }


    Delegate delegate_of(EZefRef ezr) {
        return delegate_of(imperative::delegate_rep(ezr));
    }









    template <typename T>
    std::optional<T> value_from_ae(ZefRef z_ae) {
        using namespace internals;
        // check that the type T corresponds to the type of atomic entity of the zefRef
        if (get<BlobType>(z_ae.blob_uzr) != BlobType::ATTRIBUTE_ENTITY_NODE) 
            throw std::runtime_error("ZefRef | value.something called for a ZefRef not pointing to an ATOMIC_ENTITY_NODE blob.");			
        auto & ae = get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(z_ae.blob_uzr);
        if (!is_compatible_rep_type<T>(ae.primitive_type))
            throw std::runtime_error("ZefRef | value called, but the specified return type does not agree with the type of the ATOMIC_ENTITY_NODE pointed to (" + to_str(ae.primitive_type) + ")");

        GraphData& gd = *graph_data(z_ae);
        EZefRef ref_tx = z_ae.tx;
            
        if (!imperative::exists_at(z_ae.blob_uzr, z_ae.tx))
            throw std::runtime_error("ZefRef | value.something called, but the rel_ent pointed to does not exists in the reference frame tx specified.");

        auto tx_time_slice = [](EZefRef uzr)->TimeSlice { return get<blobs_ns::TX_EVENT_NODE>(uzr).time_slice; };
        TimeSlice ref_time_slice = tx_time_slice(ref_tx);
        auto result_candidate_edge = EZefRef(nullptr);
        // Ranges don't work for AllEdgeIndexes class and we want this part to be lazy, do it the ugly imperative way for now
        // This is one of the critical parts where we want lazy evaluation.
        // Enter the pyramid of death. 
        for (auto ind : AllEdgeIndexes(imperative::traverse_in_edge(z_ae.blob_uzr, BT.RAE_INSTANCE_EDGE))) {
            if (ind < 0) {
                auto incoming_val_assignment_edge = EZefRef(-ind, gd);
                if (get<BlobType>(incoming_val_assignment_edge) == BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE ||
                    get<BlobType>(incoming_val_assignment_edge) == BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE) {
                    if (tx_time_slice(imperative::source(incoming_val_assignment_edge)) <= ref_time_slice) result_candidate_edge = incoming_val_assignment_edge;
                    else break;
                }
            }
        }
        if (result_candidate_edge.blob_ptr == nullptr) return {};  // no assignment edge was found
        else if (get<BlobType>(result_candidate_edge) == BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE) {
            auto & avae = get<blobs_ns::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE>(result_candidate_edge);
            // FIXME: can optimise this by assuming node types, leaving as is for debugging purposes.
            EZefRef value_edge{avae.value_edge_index, *graph_data(&avae)};
            EZefRef value_node = imperative::target(value_edge);
            return internals::value_from_node<T>(get<blobs_ns::VALUE_NODE>(value_node));
        } else {
            // Deprecated
            return internals::value_from_node<T>(get<blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE>(result_candidate_edge));
        }
    }
    template std::optional<double> value_from_ae<double>(ZefRef ae);
    template std::optional<int> value_from_ae<int>(ZefRef ae);
    template std::optional<bool> value_from_ae<bool>(ZefRef ae);
    template std::optional<std::string> value_from_ae<std::string>(ZefRef ae);
    template std::optional<Time> value_from_ae<Time>(ZefRef ae);
    template std::optional<SerializedValue> value_from_ae<SerializedValue>(ZefRef ae);
    template std::optional<ZefEnumValue> value_from_ae<ZefEnumValue>(ZefRef ae);
    template std::optional<QuantityFloat> value_from_ae<QuantityFloat>(ZefRef ae);
    template std::optional<QuantityInt> value_from_ae<QuantityInt>(ZefRef ae);
    template std::optional<value_variant_t> value_from_ae<value_variant_t>(ZefRef ae);
    template std::optional<AttributeEntityType> value_from_ae<AttributeEntityType>(ZefRef ae);
        


    // Deliberate name change here - assign_value should now work by creating a
    // value node and then calling this function.
    void assign_value_node(EZefRef z_ae, EZefRef z_value_node) {
        GraphData& gd = *graph_data(z_ae);
        if (!gd.is_primary_instance)
            throw std::runtime_error("'assign value' called for a graph which is not a primary instance. This is not allowed. Shame on you!");
        if (graph_data(z_ae) != graph_data(z_value_node))
            throw std::runtime_error("Can't assign a value node from a different GraphData.");
        if (get<BlobType>(z_ae) != BlobType::ATTRIBUTE_ENTITY_NODE)
            throw std::runtime_error("assign_value_node called for node that is not of type ATOMIC_ENTITY_NODE. This is not possible.");
        if (get<BlobType>(z_value_node) != BlobType::VALUE_NODE)
            throw std::runtime_error("assign_value_node called with value node that is not of type VALUE_NODE.");

        auto & ae = get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(z_ae);
        auto & value_node = get<blobs_ns::VALUE_NODE>(z_value_node);
        if (internals::is_terminated(z_ae))
            throw std::runtime_error("assign_value_node called on already terminated entity or relation");
        if (!internals::is_compatible(internals::value_from_node<value_variant_t>(value_node), AET(ae))) {
            ValueRepType vrt = std::visit([](auto x) { return get_vrt_from_ctype(x); },
                                          internals::value_from_node<value_variant_t>(value_node));

            throw std::runtime_error("assign value called with value node (primitive type " + to_str(vrt) + ") that cannot be assigned to this aet of type " + to_str(AET(ae)));
        }

        // TODO: only perform any value assignment if the new value to be assigned
        // here is different from the most recent one
        //
        // can check the last edge and see if it points to the same value node

        auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);
        EZefRef tx_event = internals::get_or_create_and_get_tx(gd);
        EZefRef my_rel_ent_instance = internals::get_RAE_INSTANCE_EDGE(z_ae);
        blobs_ns::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE& avae = internals::get_next_free_writable_blob<blobs_ns::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE>(gd);
        MMap::ensure_or_alloc_range(&avae, blobs_ns::max_basic_blob_size);
        avae.this_BlobType = BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE;		
        blob_index avae_index = index(EZefRef((void*)&avae));

        internals::move_head_forward(gd);   // keep this low level function here! The buffer size is not fixed and 'instantiate' was not designed for this case

        // The value edge to link to the value node
        blobs_ns::VALUE_EDGE& value_edge = internals::get_next_free_writable_blob<blobs_ns::VALUE_EDGE>(gd);
        MMap::ensure_or_alloc_range(&value_edge, blobs_ns::max_basic_blob_size);
        value_edge.this_BlobType = BlobType::VALUE_EDGE;		
        value_edge.source_node_index = avae_index;
        value_edge.target_node_index = index(z_value_node);
        blob_index value_edge_index = index(EZefRef((void*)&value_edge));

        // Note: this has to happen before any appending to edges
        internals::move_head_forward(gd);

        internals::append_edge_index(z_value_node, -value_edge_index);

        // Now fill in the avae struct
        avae.source_node_index = index(tx_event);
        avae.target_node_index = index(my_rel_ent_instance);
        avae.value_edge_index = value_edge_index;
        internals::append_edge_index(tx_event, avae_index);
        internals::append_edge_index(my_rel_ent_instance, -avae_index);
    }

    template<>
    void assign_value(EZefRef z_ae, const EZefRef & value) {
        if(get<BlobType>(z_ae) != BlobType::ATTRIBUTE_ENTITY_NODE)
            throw std::runtime_error("assign_value called for node that is not of type ATTRIBUTE_ENTITY_NODE.");
        if(get<BlobType>(value) != BlobType::VALUE_NODE)
            throw std::runtime_error("assign_value called for value EZefRef that is not of type VALUE_NODE.");
        if(graph_data(z_ae) != graph_data(value)) {
            assign_value(z_ae,
                         internals::value_from_node<value_variant_t>(get<blobs_ns::VALUE_NODE>(value)));
            return;
        } else {
            assign_value_node(z_ae, value);
        }
    }

    template<>
    void assign_value(EZefRef z_ae, const value_variant_t & value) {
        std::visit([&z_ae](auto & x) { assign_value(z_ae, x); },
                   value);
    }

    template<class T>
    void assign_value(EZefRef z_ae, const T & value) {
        if(get<BlobType>(z_ae) != BlobType::ATTRIBUTE_ENTITY_NODE)
            throw std::runtime_error("assign_value called for node that is not of type ATTRIBUTE_ENTITY_NODE.");
        auto & ae = get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(z_ae);
        ValueRepType primitive_type = get_vrt_from_ctype(value);

        if(!internals::is_compatible(value, AET(z_ae))) {
            std::string aet_s;
            AttributeEntityType aet = AET(z_ae);
            if(aet.complex_value)
                aet_s = "A complex AET";
            else
                aet_s = to_str(aet.rep_type);
            throw std::runtime_error("assign_value got value which can't fit into this attribute entity. AET='" + aet_s + "', value_type='" + typeid(T).name() + "', value='" + to_str(value) + "'.");
        }

        auto & gd = *graph_data(z_ae);
        // Need a transaction to keep both the value node and assignment together
        auto this_tx = Transaction(gd);

        ZefRef z_value_node = instantiate_value_node(value, gd);

        assign_value_node(z_ae, z_value_node.blob_uzr);
    }

    template void assign_value(EZefRef z_ae, const bool & value);
    template void assign_value(EZefRef z_ae, const int & value);
    template void assign_value(EZefRef z_ae, const double & value);
    template void assign_value(EZefRef z_ae, const str & value);
    // template void assign_value(EZefRef z_ae, const charconst * value &);
    template void assign_value(EZefRef z_ae, const Time & value);
    template void assign_value(EZefRef z_ae, const SerializedValue & value);
    template void assign_value(EZefRef z_ae, const ZefEnumValue & value);
    template void assign_value(EZefRef z_ae, const QuantityFloat & value);
    template void assign_value(EZefRef z_ae, const QuantityInt & value);


}