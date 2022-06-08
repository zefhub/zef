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

#include "fwd_declarations.h"
#include "scalars.h"
#include "zefref.h"
#include "graph.h"

namespace zefDB {


	



	// not sure if we need this (currently using string_view to pass uids around in binary form) 
	// if a uid is ever passed around by value
	//struct uid {
	//	char data[16];
	//	bool operator==(const uid& rhs) { return memcmp(data, rhs.data, sizeof(data))==0; }  // compare two uids bitwise, i.e. by value
	//	explicit operator std::string() const { return to_hex(data, sizeof(data)); }
	//};







	// if the zefscription manager receives update on ANY graph from zefhub, it sets a flag that any graph operation in the main thread 
	// will see (before executing the next called graph operation) and perform the respective updates.
	// In an async setting, one may inject a function checking this flag into the event loop.
	// This may be required in case one relies on live updates, but does not actively call graph fcts

  GraphData* get_address_or_load_and_get_address_of_graph(std::string_view graph_uid_bytes);
	











	//                                           _                      _   _                  _     _       _                       
	//                   _____  _____  ___ _   _| |_ ___      __ _  ___| |_(_) ___  _ __      | |__ | | ___ | |__  ___               
	//    _____ _____   / _ \ \/ / _ \/ __| | | | __/ _ \    / _` |/ __| __| |/ _ \| '_ \     | '_ \| |/ _ \| '_ \/ __|  _____ _____ 
	//   |_____|_____| |  __/>  <  __/ (__| |_| | ||  __/   | (_| | (__| |_| | (_) | | | |    | |_) | | (_) | |_) \__ \ |_____|_____|
	//                  \___/_/\_\___|\___|\__,_|\__\___|    \__,_|\___|\__|_|\___/|_| |_|    |_.__/|_|\___/|_.__/|___/              
	//                                                                                                                               
	
	namespace internals {
        // Apply action blob now uses a blob_index to refer to "from where should I update double-linking".
        // But we keep the bool version for backwards compatibility.

        void apply_action_blob(GraphData& gd, EZefRef uzr_to_blob, bool fill_caches);
        void apply_action_lookup(GraphData& gd, EZefRef uzr, bool fill_key_dict);

        void apply_action_ROOT_NODE(GraphData& gd, EZefRef uzr_to_blob, bool fill_key_dict);

        void apply_action_ATOMIC_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict) ;										 
        void apply_action_RELATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict) ;								 
        void apply_action_TX_EVENT_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        // void apply_action_DEFERRED_EDGE_LIST_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict, blob_index latest_blob_to_double_link, std::unordered_map<blob_index,blob_index> & latest_deferred);
        void apply_action_DEFERRED_EDGE_LIST_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_ASSIGN_TAG_NAME_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_FOREIGN_GRAPH_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_FOREIGN_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_FOREIGN_ATOMIC_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_FOREIGN_RELATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        void apply_action_TERMINATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_key_dict);
        LIBZEF_DLL_EXPORTED void apply_action_ATOMIC_VALUE_ASSIGNMENT_EDGE(GraphData & gd, EZefRef uzr, bool fill_key_dict);

        void unapply_action_blob(GraphData& gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_ROOT_NODE(GraphData& gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_ATOMIC_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_RELATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_TX_EVENT_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_DEFERRED_EDGE_LIST_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_ASSIGN_TAG_NAME_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_FOREIGN_GRAPH_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_FOREIGN_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_FOREIGN_ATOMIC_ENTITY_NODE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_FOREIGN_RELATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_TERMINATION_EDGE(GraphData & gd, EZefRef uzr_to_blob, bool fill_caches);
        void unapply_action_ATOMIC_VALUE_ASSIGNMENT_EDGE(GraphData & gd, EZefRef uzr, bool fill_caches);



        LIBZEF_DLL_EXPORTED void apply_double_linking(GraphData& gd, blob_index start_index, blob_index end_index);
        LIBZEF_DLL_EXPORTED void apply_actions_to_blob_range_only_key_dict(GraphData& gd, blob_index blob_index_lo, blob_index blob_index_hi);


		// This function is triggered and run on the main thread after any update for a graph view comes in. 
		// Responsibilities:
		//		1) All updates to the graph states (additional dictionary entries, foreign graph ptrs, ...)
		//		2) Search through all local graphs: if an updated graph is contained as a foreign graph somewhere, 
		//		   create a new transaction to mark update
      LIBZEF_DLL_EXPORTED void process_latest_updates_of_non_owned_graphs(std::vector<Graph*> graphs_updated_by_zefscription_manager);





	}
}
