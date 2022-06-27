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
	
    

    LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& os, EZefRef uzr);
    LIBZEF_DLL_EXPORTED std::string low_level_blob_info(const EZefRef & uzr);



    LIBZEF_DLL_EXPORTED std::ostream& operator<<(std::ostream& o, const ZefRef& zr);

    LIBZEF_DLL_EXPORTED std::ostream& operator<<(std::ostream& o, const ZefRefs& zrs);
    LIBZEF_DLL_EXPORTED std::ostream& operator<<(std::ostream& o, const ZefRefss& zrss);
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

	}
}
