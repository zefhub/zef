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
                                           || (BT(x) == BT.ATOMIC_ENTITY_NODE)
                                           || (BT(x) == BT.RELATION_EDGE))
                                       && !is_delegate(x);
                               });
    }



	bool is_promotable_to_zefref(EZefRef uzr_to_promote, EZefRef reference_tx) {
		if (reference_tx.blob_ptr == nullptr)
			throw std::runtime_error("is_promotable_to_zefref called on EZefRef pointing to nullptr");
			
		if (get<BlobType>(reference_tx) != BlobType::TX_EVENT_NODE)
			throw std::runtime_error("is_promotable_to_zefref called with a reference_tx that is not of blob type TX_EVENT_NODE");
		
		if( !( BT(uzr_to_promote) == BT.ATOMIC_ENTITY_NODE
			|| BT(uzr_to_promote) == BT.ENTITY_NODE
			|| BT(uzr_to_promote) == BT.RELATION_EDGE
			|| BT(uzr_to_promote) == BT.TX_EVENT_NODE
			|| BT(uzr_to_promote) == BT.ROOT_NODE)
		)
			throw std::runtime_error("is_promotable_to_zefref called on a EZefRef of a blob type that cannot become a ZefRef");
		// has to be instantiated and not terminated yet at the point in time signaled by the reference_tx		

		return true;
	}

    std::variant<EntityType, RelationType, AtomicEntityType> rae_type(EZefRef uzr) {
        // Given any ZefRef or EZefRef, return the ET, RT or AET. Throw an error if it is a different blob type.
        if (BT(uzr)==BT.ENTITY_NODE)
            return ET(uzr);
        else if (BT(uzr)==BT.RELATION_EDGE)
            return RT(uzr);
        else if (BT(uzr)==BT.ATOMIC_ENTITY_NODE)
            return AET(uzr);
        else
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
        EZefRef trg{z.blob_uzr < BT.RAE_INSTANCE_EDGE};
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
		return tag(uz | now, tag_name_value, force_if_name_tags_other_rel_ent).blob_uzr;
	}




	bool is_root(EZefRef z) {
		return index(z) == constants::ROOT_NODE_blob_index;
	}
	bool is_root(ZefRef z) { return is_root(z.blob_uzr); }
	
	bool is_delegate(EZefRef z) {
        // Note: internals::has_delegate is different to has_delegate below.
        if(!internals::has_delegate(BT(z)))
            return false;
		// we may additionally want to use (for efficiency) the spec. determined fact that if a rel_ent 
		// has an incoming edge of type BT.TO_DELEGATE_EDGE, this is always the first one in the edge list.
		return (z < L[BT.TO_DELEGATE_EDGE]).len == 1;
	};
	bool is_delegate(ZefRef z) { return is_delegate(z.blob_uzr); }



	bool is_delegate_group(EZefRef z) {
		if (BT(z) != BT.RELATION_EDGE)
			return false;
		EZefRef first_in_edge = z | ins | first;
		return (BT(first_in_edge) == BT.RAE_INSTANCE_EDGE) ?
			false :								// if the first in edge is a RAE_INSTANCE_EDGE, it is not a delegate, i.e. also not a delegate group
			is_root(first_in_edge | source);	// the first in edge is a  BT.TO_DELEGATE_EDGE: could be a delegate edge or delegate group
	}
	bool is_delegate_group(ZefRef z) { return is_delegate_group(z.blob_uzr); }


	bool has_delegate(EZefRef z) {
		return (z < L[BT.RAE_INSTANCE_EDGE]).len == 1;
	};
	bool has_delegate(ZefRef z) { return has_delegate(z.blob_uzr); }




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
 			EZefRef to_delegate_edge = *imperative::delegate_to_ezr(delegate_of(my_rel_ent), Graph(gd), true) < BT.TO_DELEGATE_EDGE;  // pass the relation or entity along to enable extarcting the type
			
			RAE_INSTANCE_EDGE.source_node_index = index(to_delegate_edge);
			append_edge_index(to_delegate_edge, RAE_INSTANCE_EDGE_indx);  // incoming edges are represented as the negative index
			auto rel_ent_edge_uzr = EZefRef(RAE_INSTANCE_EDGE_indx, gd);

			internals::instantiate(tx_event, instantiaion_or_clone_edge_bt, rel_ent_edge_uzr, gd);

			if (given_uid_maybe) {
				assign_uid(my_rel_ent, *given_uid_maybe); // write the uid in binary form into the uid buffer at pointer_to_uid_start
			}
			else {
				assign_uid(my_rel_ent, make_random_uid());  // this also adds the uid to the dict key_dict with this index
			}
        }



		bool is_terminated(EZefRef my_rel_ent) {
            return !imperative::exists_at_now(my_rel_ent);
		}



	


		// how do we pass a C++ object on to a python fct we execute from within C++? Turns out to be not as easy as other conversions with pybind. 
		// Just create a function that can be accessed both by C++ / Python and stores it in a local static
		ZefRef internal_temp_storage_used_by_zefhooks(ZefRef new_z_val, bool should_save) {
			static ZefRef z = ZefRef{ EZefRef{}, EZefRef{} };
			if (should_save)
				z = new_z_val;
			return z;
		}
	}    // internals namespace



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
	ZefRef instantiate(AtomicEntityType my_atomic_entity_type, GraphData& gd, std::optional<BaseUID> given_uid_maybe) {
		if (!gd.is_primary_instance)
			throw std::runtime_error("'instantiate atomic entity' called for a graph which is not a primary instance. This is not allowed. Shame on you!");
		using namespace internals;
		auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);

		EZefRef my_entity = internals::instantiate(BT.ATOMIC_ENTITY_NODE, gd);
		auto& entity_struct = get<blobs_ns::ATOMIC_ENTITY_NODE>(my_entity);		
		
		// AtomicEntityType has a const member and we can't set this afterwards. Construct into place (call constructor on specified address)
		// 'placement new': see https://www.stroustrup.com/C++11FAQ.html#unions
		new(&entity_struct.my_atomic_entity_type) AtomicEntityType{ my_atomic_entity_type.value };  // take on same type for the atomic entity delegate

		hook_up_to_schema_nodes(my_entity, gd, given_uid_maybe);
		EZefRef tx_node{ gd.index_of_open_tx_node, gd };
		entity_struct.instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice;

        apply_action_ATOMIC_ENTITY_NODE(gd, my_entity, true);
		
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
				BT(zz) == BT.ATOMIC_ENTITY_NODE ||
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


		EZefRef get_or_create_and_get_foreign_rae(Graph& target_graph, std::variant<EntityType, AtomicEntityType, std::tuple<EZefRef, RelationType, EZefRef>> ae_or_entity_type, const BaseUID& origin_entity_uid, const BaseUID& origin_graph_uid) {
			if (target_graph.contains(origin_entity_uid)) {
				//if (rae_type != (target_graph[origin_entity_uid])) throw std::runtime_error("the entity type of the RAE's uid did not match the expected entity type in get_or_create_and_get_foreign_rae");
				return target_graph[origin_entity_uid];
			}
			// the foreign (atomic)entity is not in this graph
			GraphData& gd = target_graph.my_graph_data();
			EZefRef new_foreign_entity_uzr = std::visit(overloaded{
				[&](EntityType et) {
					EZefRef new_foreign_entity_uzr = internals::instantiate(BT.FOREIGN_ENTITY_NODE, gd);
					reinterpret_cast<blobs_ns::FOREIGN_ENTITY_NODE*>(new_foreign_entity_uzr.blob_ptr)->entity_type = et;
					return new_foreign_entity_uzr;
				},
				[&](AtomicEntityType aet) {
					EZefRef new_foreign_entity_uzr = internals::instantiate(BT.FOREIGN_ATOMIC_ENTITY_NODE, gd);
					// AtomicEntityType has a const member and we can't set this afterwards. Construct into place (call constructor on specified address)
					// 'placement new': see https://www.stroustrup.com/C++11FAQ.html#unions
					new(&(reinterpret_cast<blobs_ns::FOREIGN_ATOMIC_ENTITY_NODE*>(new_foreign_entity_uzr.blob_ptr)->atomic_entity_type)) AtomicEntityType{ aet.value };  // take on same type for the atomic entity delegate
					return new_foreign_entity_uzr;
				},	
				[&](const std::tuple<EZefRef, RelationType, EZefRef>& trip) {
					EZefRef src = find_origin_rae(std::get<0>(trip));
					EZefRef trg = find_origin_rae(std::get<2>(trip));
					EZefRef new_foreign_entity_uzr = internals::instantiate(src, BT.FOREIGN_RELATION_EDGE, trg, gd);				// Init with 0: we don't know the src or trg nodes yet in all cases
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
				|| BT(z) == BT.FOREIGN_ATOMIC_ENTITY_NODE
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


		
		EZefRef merge_atomic_entity_(Graph & target_graph, AtomicEntityType atomic_entity_type, const BaseUID & origin_entity_uid, const BaseUID & origin_graph_uid) {
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
                BT(uzr) != BT.FOREIGN_ATOMIC_ENTITY_NODE)
                throw std::runtime_error("local_entity can only be applied to BT.FOREIGN_* blobs, not" + to_str(BT(uzr)));

            return imperative::target(imperative::traverse_in_node(uzr, BT.ORIGIN_RAE_EDGE));
        }
	}


    nlohmann::json merge(const nlohmann::json & j, Graph target_graph, bool fire_and_forget) {
       auto butler = Butler::get_butler();

       Messages::MergeRequest msg {
           {},
           target_graph|uid,
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










	namespace internals {

		template <typename T>
		void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const T & value_to_be_assigned) {
			new(data_buffer_ptr) T(value_to_be_assigned);  // placement new: call copy ctor: copy assignment may not be defined
			buffer_size_in_bytes = sizeof(value_to_be_assigned);
		}

        template<>
		void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const std::string & value_to_be_assigned) {
            MMap::ensure_or_alloc_range(data_buffer_ptr, std::max(value_to_be_assigned.size(), blobs_ns::max_basic_blob_size));
			std::memcpy(data_buffer_ptr, value_to_be_assigned.data(), value_to_be_assigned.size());
			buffer_size_in_bytes = value_to_be_assigned.size();
		}

        template<>
		void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const SerializedValue & value_to_be_assigned) {
            buffer_size_in_bytes = value_to_be_assigned.type.size() + value_to_be_assigned.data.size() + 2*sizeof(int);

            MMap::ensure_or_alloc_range(data_buffer_ptr, std::max((size_t)buffer_size_in_bytes, blobs_ns::max_basic_blob_size));
            char * cur = data_buffer_ptr;
            *(int*)cur = value_to_be_assigned.type.size();
            cur += sizeof(int);
            *(int*)cur = value_to_be_assigned.data.size();
            cur += sizeof(int);

			std::memcpy(cur, value_to_be_assigned.type.data(), value_to_be_assigned.type.size());
            cur += value_to_be_assigned.type.size();
			std::memcpy(cur, value_to_be_assigned.data.data(), value_to_be_assigned.data.size());
		}


		// use overloading instead of partial template specialization here
		// if both types agree, generate a default function to copy
		template<typename T>
		auto cast_it_for_fucks_sake(T & val, T just_for_type)->T { return val; }
		inline double cast_it_for_fucks_sake(int val, double just_for_type) { return double(val); }
		inline int cast_it_for_fucks_sake(double val, int just_for_type) { 
			if (fabs(val - round(val)) > 1E-8)
				throw std::runtime_error("converting a double to an int, but the double was numerically not sufficiently close to an in to make rounding safe");
			return int(val); 			
		}
		inline bool cast_it_for_fucks_sake(int val, bool just_for_type) { 
			if(val == 1) return true; 
			if(val == 0) return false; 
			throw std::runtime_error("converting an int to a bool, but the value was neither 0 or 1");
		}
		inline bool cast_it_for_fucks_sake(bool val, int just_for_type) { 
			if(val) return 1; 
			else return 0;
		}

		template<typename InType, typename OutType>
		OutType cast_it_for_fucks_sake(InType val, OutType just_for_type) {
			throw std::runtime_error(std::string("Unknown conversion"));
        }


        // template<class T>
        // bool is_compatible_type(AtomicEntityType aet);

        template<> bool is_compatible_type<bool>(AtomicEntityType aet) { return aet == AET.Bool; }
        template<> bool is_compatible_type<int>(AtomicEntityType aet) { return aet == AET.Int || aet == AET.Float || aet == AET.Bool; }   // we can also assign an int to a bool
        template<> bool is_compatible_type<double>(AtomicEntityType aet) { return aet == AET.Float || aet == AET.Int; }
        template<> bool is_compatible_type<str>(AtomicEntityType aet) { return aet == AET.String; }
        template<> bool is_compatible_type<const char*>(AtomicEntityType aet) { return aet == AET.String; }
        template<> bool is_compatible_type<Time>(AtomicEntityType aet) { return aet == AET.Time; }
        template<> bool is_compatible_type<SerializedValue>(AtomicEntityType aet) { return aet == AET.Serialized; }

        template<> bool is_compatible_type<ZefEnumValue>( AtomicEntityType aet) {
            int offset = aet.value % 16;
            if (offset != 1) return false;   // Enums encoded by an offset of 1
            return true;
        }
        template<> bool is_compatible_type<QuantityFloat>(AtomicEntityType aet) {
            int offset = aet.value % 16;
            if (offset != 2) return false;   // QuantityFloat encoded by an offset of 2
            return true;		
        }
        template<> bool is_compatible_type<QuantityInt>(AtomicEntityType aet) {
            int offset = aet.value % 16;		
            if (offset != 3) return false;   // QuantityInt encoded by an offset of 3
            return true;
        }


        bool is_compatible(bool _, AtomicEntityType aet) { return is_compatible_type<bool>(aet); }
        bool is_compatible(int _, AtomicEntityType aet) { return is_compatible_type<int>(aet); }
        bool is_compatible(double _, AtomicEntityType aet) { return is_compatible_type<double>(aet); }
        bool is_compatible(str _, AtomicEntityType aet) { return is_compatible_type<str>(aet); }
        bool is_compatible(const char * _, AtomicEntityType aet) { return is_compatible_type<const char *>(aet); }
        bool is_compatible(Time _, AtomicEntityType aet) { return is_compatible_type<Time>(aet); }
        bool is_compatible(SerializedValue _, AtomicEntityType aet) { return is_compatible_type<SerializedValue>(aet); }

        bool is_compatible(ZefEnumValue en, AtomicEntityType aet) {
            int offset = aet.value % 16;
            return is_compatible_type<ZefEnumValue>(aet)
                && (ZefEnumValue{ (aet.value - offset) }.enum_type() == en.enum_type());
        }
        bool is_compatible(QuantityFloat q, AtomicEntityType aet) {
            int offset = aet.value % 16;
            return is_compatible_type<QuantityFloat>(aet)
                && ((aet.value - offset) == q.unit.value);
        }
        bool is_compatible(QuantityInt q, AtomicEntityType aet) {
            int offset = aet.value % 16;
            return is_compatible_type<QuantityInt>(aet)
                && ((aet.value - offset) == q.unit.value);
        }


		// use template specialization for the return value in the fct 'auto operator^ (ZefRef my_atomic_entity, T op) -> std::optional<decltype(op._x)>' below.
		// string values are saved as a char array. We could return a string_view, but for simplicity and pybind11, instantiate an std::string for now

        template<>
		str get_final_value_for_op_hat<str>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet) {
            Butler::ensure_or_get_range(aae.data_buffer, aae.buffer_size_in_bytes);
            return std::string(aae.data_buffer, aae.buffer_size_in_bytes);
		}

        template<>
		SerializedValue get_final_value_for_op_hat<SerializedValue>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet) {
            Butler::ensure_or_get_range(aae.data_buffer, aae.buffer_size_in_bytes);
            char * cur = aae.data_buffer;
            int type_len = *(int*)cur;
            cur += sizeof(int);
            int data_len = *(int*)cur;
            cur += sizeof(int);
            std::string type_str(cur, type_len);
            cur += type_len;
            std::string data_str(cur, data_len);
            return SerializedValue{type_str, data_str};
		}

        template<>
		double get_final_value_for_op_hat<double>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet) {
            // This needs to be specialised to convert, while allowing the other variants to avoid this unncessary check
            if (aet == AET.Float)
                return cast_it_for_fucks_sake(*(double*)(aae.data_buffer), double());
            else// if(aet == AET.Int)
                return cast_it_for_fucks_sake(*(int*)(aae.data_buffer), double());
		}
        template<>
		int get_final_value_for_op_hat<int>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet) {
            // This needs to be specialised to convert, while allowing the other variants to avoid this unncessary check
            if (aet == AET.Float) {
                return cast_it_for_fucks_sake(*(double*)(aae.data_buffer), int());
            } else if(aet == AET.Int) {
                return cast_it_for_fucks_sake(*(int*)(aae.data_buffer), int());
            } else //(aet == AET.Bool) {
                return cast_it_for_fucks_sake(*(bool*)(aae.data_buffer), int());
		}

		// for contiguous POD types with compile-time determined size, we can use this template
		template <typename T>
		T get_final_value_for_op_hat(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet) {
			return *(T*)(aae.data_buffer);  // project onto type
		}

        template QuantityInt get_final_value_for_op_hat<QuantityInt>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet);
        template QuantityFloat get_final_value_for_op_hat<QuantityFloat>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet);
        template ZefEnumValue get_final_value_for_op_hat<ZefEnumValue>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, AtomicEntityType aet);






		template <typename T>
		T _value(ZefRef my_atomic_entity) {
			using namespace internals;
			// check that the type T corresponds to the type of atomic entity of the zefRef
			if (get<BlobType>(my_atomic_entity.blob_uzr) != BlobType::ATOMIC_ENTITY_NODE) 
				throw std::runtime_error("ZefRef | value.something called for a ZefRef not pointing to an ATOMIC_ENTITY_NODE blob.");			
            AtomicEntityType aet = get<blobs_ns::ATOMIC_ENTITY_NODE>(my_atomic_entity.blob_uzr).my_atomic_entity_type;
			if (!is_compatible_type<T>(aet))
				throw std::runtime_error("ZefRef | value called, but the specified return type does not agree with the type of the ATOMIC_ENTITY_NODE pointed to (" + to_str(get<blobs_ns::ATOMIC_ENTITY_NODE>(my_atomic_entity.blob_uzr).my_atomic_entity_type) + ")");

			GraphData& gd = *graph_data(my_atomic_entity);			
			EZefRef ref_tx = my_atomic_entity.tx;
			
			if (!imperative::exists_at(my_atomic_entity.blob_uzr, my_atomic_entity.tx))
				throw std::runtime_error("ZefRef | value.something called, but the rel_ent pointed to does not exists in the reference frame tx specified.");

			auto tx_time_slice = [](EZefRef uzr)->TimeSlice { return get<blobs_ns::TX_EVENT_NODE>(uzr).time_slice; };
			TimeSlice ref_time_slice = tx_time_slice(ref_tx);
			auto result_candidate_edge = EZefRef(nullptr);
			// Ranges don't work for AllEdgeIndexes class and we want this part to be lazy, do it the ugly imperative way for now
			// This is one of the critical parts where we want lazy evaluation.
			// Enter the pyramid of death. 
			for (auto ind : AllEdgeIndexes(my_atomic_entity.blob_uzr < BT.RAE_INSTANCE_EDGE)) {
				if (ind < 0) {
					auto incoming_val_assignment_edge = EZefRef(-ind, gd);
					if (get<BlobType>(incoming_val_assignment_edge) == BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE) {
						if (tx_time_slice(incoming_val_assignment_edge | source) <= ref_time_slice) result_candidate_edge = incoming_val_assignment_edge;
						else break;
					}
				}
			}
			if (result_candidate_edge.blob_ptr == nullptr) return {};  // no assignment edge was found
			else return internals::get_final_value_for_op_hat<T>(get<blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE>(result_candidate_edge), aet);
		}
        template<> double value<double>(ZefRef my_atomic_entity) { return _value<double>(my_atomic_entity); }
        template<> int value<int>(ZefRef my_atomic_entity) { return _value<int>(my_atomic_entity); }
        template<> bool value<bool>(ZefRef my_atomic_entity) { return _value<bool>(my_atomic_entity); }
        template<> std::string value<std::string>(ZefRef my_atomic_entity) { return _value<std::string>(my_atomic_entity); }
        template<> Time value<Time>(ZefRef my_atomic_entity) { return _value<Time>(my_atomic_entity); }
        template<> SerializedValue value<SerializedValue>(ZefRef my_atomic_entity) { return _value<SerializedValue>(my_atomic_entity); }
        template<> ZefEnumValue value<ZefEnumValue>(ZefRef my_atomic_entity) { return _value<ZefEnumValue>(my_atomic_entity); }
        template<> QuantityFloat value<QuantityFloat>(ZefRef my_atomic_entity) { return _value<QuantityFloat>(my_atomic_entity); }
        template<> QuantityInt value<QuantityInt>(ZefRef my_atomic_entity) { return _value<QuantityInt>(my_atomic_entity); }
        


        //TODO: in python bindings when assigning a bool, the art needs to be set to 'Int'?
        template <typename T>
        void assign_value(EZefRef my_atomic_entity, T value_to_be_assigned) {
            GraphData& gd = *graph_data(my_atomic_entity);
            AtomicEntityType my_ae_aet = get<blobs_ns::ATOMIC_ENTITY_NODE>(my_atomic_entity).my_atomic_entity_type;
            if (!gd.is_primary_instance)
                throw std::runtime_error("'assign value' called for a graph which is not a primary instance. This is not allowed. Shame on you!");
            // tasks::apply_immediate_updates_from_zm();
            using namespace internals;
            if (*(BlobType*)my_atomic_entity.blob_ptr != BlobType::ATOMIC_ENTITY_NODE)
                throw std::runtime_error("assign_value called for node that is not of type ATOMIC_ENTITY_NODE. This is not possible.");
            if (is_terminated(my_atomic_entity))
                throw std::runtime_error("assign_value called on already terminated entity or relation");
            if (!is_compatible(value_to_be_assigned, AET(my_atomic_entity)))
                throw std::runtime_error("assign value called with type (" + to_str(value_to_be_assigned) + ") that cannot be assigned to this aet of type " + to_str(AET(my_atomic_entity)));

            // only perform any value assignment if the new value to be assigned here is different from the most recent one
            //auto most_recent_value = my_atomic_entity | now | value.Float;   // TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);
            EZefRef tx_event = get_or_create_and_get_tx(gd);
            EZefRef my_rel_ent_instance = get_RAE_INSTANCE_EDGE(my_atomic_entity);
            blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& my_value_assignment_edge = get_next_free_writable_blob<blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE>(gd);
            // Note: for strings, going to ensure even more memory later
            MMap::ensure_or_alloc_range(&my_value_assignment_edge, blobs_ns::max_basic_blob_size);
            my_value_assignment_edge.this_BlobType = BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE;		
            new(&my_value_assignment_edge.my_atomic_entity_type) AtomicEntityType{ my_ae_aet.value };  // set the const value
            switch (AET(my_atomic_entity).value) {
            case AET.Int.value: { internals::copy_to_buffer(my_value_assignment_edge.data_buffer, my_value_assignment_edge.buffer_size_in_bytes, cast_it_for_fucks_sake(value_to_be_assigned, int())); break; }
            case AET.Float.value: { internals::copy_to_buffer(my_value_assignment_edge.data_buffer, my_value_assignment_edge.buffer_size_in_bytes, cast_it_for_fucks_sake(value_to_be_assigned, double()));	break; }
            case AET.String.value: { internals::copy_to_buffer(my_value_assignment_edge.data_buffer, my_value_assignment_edge.buffer_size_in_bytes, cast_it_for_fucks_sake(value_to_be_assigned, str())); break; }
            case AET.Bool.value: { internals::copy_to_buffer(my_value_assignment_edge.data_buffer, my_value_assignment_edge.buffer_size_in_bytes, cast_it_for_fucks_sake(value_to_be_assigned, bool())); break;	}
            case AET.Time.value: { internals::copy_to_buffer(my_value_assignment_edge.data_buffer, my_value_assignment_edge.buffer_size_in_bytes, cast_it_for_fucks_sake(value_to_be_assigned, Time{})); break; }
            case AET.Serialized.value: { internals::copy_to_buffer(my_value_assignment_edge.data_buffer, my_value_assignment_edge.buffer_size_in_bytes, cast_it_for_fucks_sake(value_to_be_assigned, SerializedValue{})); break; } 
            default: {switch (AET(my_atomic_entity).value % 16) {
                    case 1: { internals::copy_to_buffer(my_value_assignment_edge.data_buffer, my_value_assignment_edge.buffer_size_in_bytes, cast_it_for_fucks_sake(value_to_be_assigned, ZefEnumValue{ 0 })); break; }
                    case 2: { internals::copy_to_buffer(my_value_assignment_edge.data_buffer, my_value_assignment_edge.buffer_size_in_bytes, cast_it_for_fucks_sake(value_to_be_assigned, QuantityFloat(0.0, EN.Unit._undefined))); break; }
                    case 3: { internals::copy_to_buffer(my_value_assignment_edge.data_buffer, my_value_assignment_edge.buffer_size_in_bytes, cast_it_for_fucks_sake(value_to_be_assigned, QuantityInt(0, EN.Unit._undefined))); break; }
                    default: {throw std::runtime_error("value assignment case not implemented"); }
                    }}
            }

            move_head_forward(gd);   // keep this low level function here! The buffer size is not fixed and 'instantiate' was not designed for this case
            my_value_assignment_edge.source_node_index = index(tx_event);
            my_value_assignment_edge.target_node_index = index(my_rel_ent_instance);
            blob_index this_val_assignment_edge_index = index(EZefRef((void*)&my_value_assignment_edge));
            append_edge_index(tx_event, this_val_assignment_edge_index);
            append_edge_index(my_rel_ent_instance, -this_val_assignment_edge_index);

            apply_action_ATOMIC_VALUE_ASSIGNMENT_EDGE(gd, EZefRef((void*)&my_value_assignment_edge), true);
        }

        template void assign_value(EZefRef my_atomic_entity, bool value_to_be_assigned);
        template void assign_value(EZefRef my_atomic_entity, int value_to_be_assigned);
        template void assign_value(EZefRef my_atomic_entity, double value_to_be_assigned);
        template void assign_value(EZefRef my_atomic_entity, str value_to_be_assigned);
        template void assign_value(EZefRef my_atomic_entity, const char* value_to_be_assigned);
        template void assign_value(EZefRef my_atomic_entity, Time value_to_be_assigned);
        template void assign_value(EZefRef my_atomic_entity, SerializedValue value_to_be_assigned);
        template void assign_value(EZefRef my_atomic_entity, ZefEnumValue value_to_be_assigned);
        template void assign_value(EZefRef my_atomic_entity, QuantityFloat value_to_be_assigned);
        template void assign_value(EZefRef my_atomic_entity, QuantityInt value_to_be_assigned);

    }
}