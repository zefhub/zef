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

#include <doctest/doctest.h>
#include <chrono>
#include "low_level_api.h"
#include "high_level_api.h"

namespace zefDB {
	

	std::ostream& operator << (std::ostream& os, EZefRef uzr) {
		if (uzr.blob_ptr == nullptr) {
			os << "<EZefRef: ZefRef reference not set>";
			return os;
		}
		os << "<EZefRef #" << index(uzr);
		if (*(BlobType*)uzr.blob_ptr == BlobType::_unspecified) {
			os << "  -  reference set to uninitialized memory ZefRef>";
			return os;
		}

        if(is_delegate(uzr))
            os << " DELEGATE";
            
        if (BT(uzr) == BT.ENTITY_NODE)
            os << " " << ET(uzr);
        else if (BT(uzr) == BT.RELATION_EDGE)
            os << " " << RT(uzr);
        else if (BT(uzr) == BT.ATOMIC_ENTITY_NODE)
            os << " " << AET(uzr);
        else if (BT(uzr) == BT.TX_EVENT_NODE)
            os << " TX at slice=" << time_slice(uzr).value;
        else
            os << " " << BT(uzr);

		// visit([&os](auto& my_blob) {os << my_blob; }, uzr);
		// os << ", size=" << size_of_blob(uzr)<<" bytes"
		// 	//<< ", size in indexes=" << num_blob_indexes_to_move(size_of_blob(uzr))
		// 	//<< ", graph_data address= " << &graph_data(uzr)
		// 	//<< ", blob address= " << (BlobType*)uzr.blob_ptr
        //    << (internals::has_uid(uzr) ? (", uid=" + str(internals::get_blob_uid(uzr))) : "")
        //    << ">";
        os << ">";
		return os;
	}

    std::string low_level_blob_info(const EZefRef & uzr) {
        return to_str(uzr);
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
        else if (BT(zr) == BT.ATOMIC_ENTITY_NODE)
            o << " " << AET(zr);
        else if (BT(zr) == BT.TX_EVENT_NODE)
            o << " TX at slice=" << time_slice(zr.blob_uzr).value << " seen from";
        else
            o << " " << BT(zr);
        o << " slice=" << time_slice(zr.tx).value;
        o << ">";
        return o;
    }

    std::ostream& operator<<(std::ostream& o, const ZefRefs& zrs) {
        return o << "<ZefRefs with " << length(zrs) << " items>";
    }
    std::ostream& operator<<(std::ostream& o, const ZefRefss& zrss) {
        return o << "<ZefRefs with " << length(zrss) << " items>";
    }
    std::ostream& operator<<(std::ostream& o, const EZefRefs& uzrs) {
        return o << "<EZefRefs with " << length(uzrs) << " items>";
    }
    std::ostream& operator<<(std::ostream& o, const EZefRefss& uzrss) {
        return o << "<EZefRefs with " << length(uzrss) << " items>";
    }



	std::ostream& operator << (std::ostream& o, Graph& g) {
		auto& gd = g.my_graph_data();
		o << "<Zef low level Graph:\n";
		o << "graph data=" << gd;
		o << "\n";
		auto uzr = gd.get_ROOT_node();
		//while (get_BlobType(uzr.blob_ptr) != BlobType::_unspecified) {
		while (uzr) {
			o << uzr;
			uzr = get_next(uzr);
		}
		o << ">\n";
		return o;
	}







	

	namespace internals{




		// have similar API to instantiate and link low level blobs / EZefRefs
		EZefRef instantiate(BlobType bt, GraphData& gd) {
			using namespace blobs_ns;
            void * new_ptr = (void*)(std::uintptr_t(&gd) + gd.write_head * constants::blob_indx_step_in_bytes);
            MMap::ensure_or_alloc_range(new_ptr, max_basic_blob_size);
            *(BlobType*)new_ptr = bt;
			auto this_new_blob = EZefRef(new_ptr);
			
			switch (bt) {
			case BlobType::ROOT_NODE: { new (new_ptr) ROOT_NODE; break; }
			case BlobType::TX_EVENT_NODE: { new (new_ptr) TX_EVENT_NODE; break; }
			case BlobType::RAE_INSTANCE_EDGE: { new (new_ptr) RAE_INSTANCE_EDGE; break; }
			case BlobType::TO_DELEGATE_EDGE: { new (new_ptr) TO_DELEGATE_EDGE; break; }
			case BlobType::ENTITY_NODE: { new (new_ptr) ENTITY_NODE; break; }
			case BlobType::ATOMIC_ENTITY_NODE: { new (new_ptr) ATOMIC_ENTITY_NODE; break; }
			case BlobType::RELATION_EDGE: { new (new_ptr) RELATION_EDGE; break; }
			case BlobType::DEFERRED_EDGE_LIST_NODE: { new (new_ptr) DEFERRED_EDGE_LIST_NODE; break; }
			case BlobType::FOREIGN_GRAPH_NODE: { new (new_ptr) FOREIGN_GRAPH_NODE; break; }
			case BlobType::FOREIGN_ENTITY_NODE: { new (new_ptr) FOREIGN_ENTITY_NODE; break; }
			case BlobType::FOREIGN_ATOMIC_ENTITY_NODE: { new (new_ptr) FOREIGN_ATOMIC_ENTITY_NODE; break; }
            case BlobType::FOREIGN_RELATION_EDGE: { new (new_ptr) FOREIGN_RELATION_EDGE; break; }
            case BlobType::NEXT_TX_EDGE: { new (new_ptr) NEXT_TX_EDGE; break; }
            default: {throw std::runtime_error("instantiate(BlobType bt, GraphData& gd) called for BlobType an unhandled case."); }
			}			
			move_head_forward(gd);
			return this_new_blob;
		}


		EZefRef instantiate(EZefRef src, BlobType bt, EZefRef trg, GraphData& gd) {
            // Without graph views, we must require all relations are created on the same graph as the UZRs themselves.
            if (graph_data(src) != &gd || graph_data(trg) != &gd)
                throw std::runtime_error(std::string("Not allowing an edge to be created between UZRs on a different graph.")
                                         + std::string("src:g=") + str(uid(*graph_data(src))) + ":ind=" + index(src) + ", "
                                         + std::string("trg:g=") + str(uid(*graph_data(trg))) + ":ind=" + index(trg) + ", "
                                         + std::string("gd=") + str(uid(gd)));
                                         
			using namespace blobs_ns;
            void * new_ptr = (void*)(std::uintptr_t(&gd) + gd.write_head * constants::blob_indx_step_in_bytes);
            MMap::ensure_or_alloc_range(new_ptr, max_basic_blob_size);
            *(BlobType*)new_ptr = bt;
			auto this_new_blob = EZefRef(new_ptr);

            auto common_behavior = [&src,&trg,&bt](auto & tmp) {
                new (&tmp) typename std::remove_reference<decltype(tmp)>::type;
				tmp.source_node_index = index(src);
				tmp.target_node_index = index(trg);
            };

			switch (bt) {
			case BlobType::RELATION_EDGE: { common_behavior(get<RELATION_EDGE>(this_new_blob)); break; }
			case BlobType::RAE_INSTANCE_EDGE: { common_behavior(get<RAE_INSTANCE_EDGE>(this_new_blob)); break; }
			case BlobType::INSTANTIATION_EDGE: { common_behavior(get<INSTANTIATION_EDGE>(this_new_blob)); break; }
			case BlobType::TO_DELEGATE_EDGE: { common_behavior(get<TO_DELEGATE_EDGE>(this_new_blob)); break; }
			case BlobType::DELEGATE_INSTANTIATION_EDGE: { common_behavior(get<DELEGATE_INSTANTIATION_EDGE>(this_new_blob)); break; }
			case BlobType::DELEGATE_RETIREMENT_EDGE: { common_behavior(get<DELEGATE_RETIREMENT_EDGE>(this_new_blob)); break; }
			// case BlobType::ASSIGN_TAG_NAME_EDGE: { common_behavior(get<ASSIGN_TAG_NAME_EDGE>(this_new_blob)); break; }
			case BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE: { common_behavior(get<NEXT_TAG_NAME_ASSIGNMENT_EDGE>(this_new_blob)); break; }
			case BlobType::ORIGIN_RAE_EDGE: { common_behavior(get<ORIGIN_RAE_EDGE>(this_new_blob)); break; }
			case BlobType::ORIGIN_GRAPH_EDGE: { common_behavior(get<ORIGIN_GRAPH_EDGE>(this_new_blob)); break; }
			case BlobType::FOREIGN_RELATION_EDGE: { common_behavior(get<FOREIGN_RELATION_EDGE>(this_new_blob)); break; }
			default: {throw std::runtime_error("instantiate(EZefRef src, BlobType bt, EZefRef trg, GraphData& gd) called for BlobType an unhandled case. BT: " + to_str(bt)); }
			}			

			move_head_forward(gd);
			append_edge_index(src, index(this_new_blob));
			append_edge_index(trg, -index(this_new_blob));
			return this_new_blob;
		}





		// look at the current write head: determine type of basic ZefRef there from first byte. Calculate actual
		// size of blob, including possible overflows and move write head in graph data forward to the next free index
		void move_head_forward(GraphData& gd) {
			auto num_indxs_to_move = num_blob_indexes_to_move(size_of_blob(
				EZefRef((void*)(std::uintptr_t(&gd) + gd.write_head * constants::blob_indx_step_in_bytes))
			));
			gd.write_head += num_indxs_to_move;
		}



//                                      _            __             _        _                             _   _                           
//                   ___ _ __ ___  __ _| |_ ___     / /   __ _  ___| |_     | |_ _ __ __ _ _ __  ___  __ _| |_(_) ___  _ __                
//    _____ _____   / __| '__/ _ \/ _` | __/ _ \   / /   / _` |/ _ \ __|    | __| '__/ _` | '_ \/ __|/ _` | __| |/ _ \| '_ \   _____ _____ 
//   |_____|_____| | (__| | |  __/ (_| | ||  __/  / /   | (_| |  __/ |_     | |_| | | (_| | | | \__ \ (_| | |_| | (_) | | | | |_____|_____|
//                  \___|_|  \___|\__,_|\__\___| /_/     \__, |\___|\__|     \__|_|  \__,_|_| |_|___/\__,_|\__|_|\___/|_| |_|              
//                                                       |___/                                             

		EZefRef get_or_create_and_get_tx(GraphData& gd) {
			// in case there is any transaction open, just return a EZefRef referencing that tx node
			if (gd.number_of_open_tx_sessions > 0 && gd.index_of_open_tx_node != 0)
				return EZefRef(gd.index_of_open_tx_node, gd);
				//return EZefRef((void*)(std::uintptr_t(&gd) + (gd.index_of_open_tx_node * constants::blob_indx_step_in_bytes)) );


			// ---- if we're here: crete new TX and connect to previous one ----			
			// create NEXT_TX_EDGE from the new tx_node to the previous tx_node			
			auto NEXT_TX_EDGE_uzr = instantiate(BT.NEXT_TX_EDGE, gd);
			auto& NEXT_TX_EDGE = get<blobs_ns::NEXT_TX_EDGE>(NEXT_TX_EDGE_uzr);
			
			// instantiate a new TX_EVENT_NODE
			EZefRef tx_event_node = internals::instantiate(BT.TX_EVENT_NODE, gd);
			NEXT_TX_EDGE.source_node_index = gd.latest_complete_tx;
			NEXT_TX_EDGE.target_node_index = index(tx_event_node);			

			//... double link ....
            internals::append_edge_index(tx_event_node, -index(NEXT_TX_EDGE_uzr));  //it's an incoming edge for the new tx node
            internals::append_edge_index(EZefRef(gd.latest_complete_tx, gd), index(NEXT_TX_EDGE_uzr));  //it's an outgoing edge for the previous tx node
			gd.index_of_open_tx_node = index(tx_event_node);
            EZefRef previous_tx{gd.latest_complete_tx, gd};
            int previous_time_slice;
            if(get<BlobType>(previous_tx) == BlobType::ROOT_NODE)
                previous_time_slice = 0;
            else
                previous_time_slice = *get<blobs_ns::TX_EVENT_NODE>(previous_tx).time_slice;

            get<blobs_ns::TX_EVENT_NODE>(tx_event_node).time_slice.value = previous_time_slice + 1;
            get<blobs_ns::TX_EVENT_NODE>(tx_event_node).time = Time{ std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count() * 1E-6 };  // rounded to seconds
			assign_uid(tx_event_node, make_random_uid());
			// gd.key_dict[internals::get_uid_as_hex_str(tx_event_node)] = index(tx_event_node);
			apply_action_TX_EVENT_NODE(gd, tx_event_node, true);
			return tx_event_node;
		}
		
		EZefRef get_or_create_and_get_tx(Graph& g) {
			return get_or_create_and_get_tx(g.my_graph_data());
		}

		EZefRef get_or_create_and_get_tx(EZefRef some_blob_to_specify_which_graph) { 
			return get_or_create_and_get_tx(graph_data(some_blob_to_specify_which_graph)); 
		}



//                            _     _ _                         _               _           _                                  
//                   __ _  __| | __| (_)_ __   __ _     ___  __| | __ _  ___   (_)_ __   __| | _____  _____  ___               
//    _____ _____   / _` |/ _` |/ _` | | '_ \ / _` |   / _ \/ _` |/ _` |/ _ \  | | '_ \ / _` |/ _ \ \/ / _ \/ __|  _____ _____ 
//   |_____|_____| | (_| | (_| | (_| | | | | | (_| |  |  __/ (_| | (_| |  __/  | | | | | (_| |  __/>  <  __/\__ \ |_____|_____|
//                  \__,_|\__,_|\__,_|_|_| |_|\__, |   \___|\__,_|\__, |\___|  |_|_| |_|\__,_|\___/_/\_\___||___/              
//                                            |___/               |___/                                                 

		EZefRef create_new_deferred_edge_list(GraphData& gd, edge_list_size_t edge_list_length, blob_index first_blob) {
			blobs_ns::DEFERRED_EDGE_LIST_NODE& el = get_next_free_writable_blob<blobs_ns::DEFERRED_EDGE_LIST_NODE>(gd);
            // MMap::ensure_or_alloc_range(&el, sizeof(blobs_ns::DEFERRED_EDGE_LIST_NODE) + edge_list_length*sizeof(int));
            MMap::ensure_or_alloc_range(&el, std::max(sizeof(blobs_ns::DEFERRED_EDGE_LIST_NODE) + edge_list_length*sizeof(blob_index),
                                                      blobs_ns::max_basic_blob_size));
            new (&el) blobs_ns::DEFERRED_EDGE_LIST_NODE;
			el.edges.local_capacity = edge_list_length;
			el.first_blob = first_blob;
			EZefRef uzr{(void*)&el};
            if(edge_list_end_offset(uzr) != 0) {
                throw std::runtime_error("Shouldn't be automatically enlarging edge lists anymore!");
                // std::cerr << "Warning, enlarging deferred edge list size to match the blob spacing." << std::endl;
                // std::cerr << "Offset: " << edge_list_end_offset(uzr) << std::endl;
                // if(edge_list_end_offset(uzr) % sizeof(blob_index) != 0)
                //     throw std::runtime_error("Unable to enlarge deferred edge list, because it isn't aligned to blob_index");
                // el.edges.local_capacity += (constants::blob_indx_step_in_bytes - edge_list_end_offset(uzr)) / sizeof(blob_index);
                // std::cerr << "Changed length: " << el.edges.local_capacity << std::endl;
                // std::cerr << "Changed offset: " << edge_list_end_offset(uzr) << std::endl;
                // assert(edge_list_end_offset(uzr) == 0);
            }

            // Fill the sentinel value in
            el.edges.indices[el.edges.local_capacity] = blobs_ns::sentinel_subsequent_index;
			move_head_forward(gd); // the size needs to be set first
			return uzr;
		}


		// should work for any ZefRef that has incoming / outgoing edges.
		// Depending on the ZefRef-type, the list of edges may be in a locally
		// different memory area of the struct. If the list is full, create a 
		// new DEFERRED_EDGE_LIST_NODE: enable this recursively.

		bool append_edge_index(EZefRef uzr, blob_index edge_index_to_append, bool prevent_new_edgelist_creation) {
            // Note: this assumes the edge index to be added is definitely new.
            // That is, it will always add the index provided to the end of the
            // list known by this blob. To maybe add if no present, then use the
            // "idempotent" version below.

			// the following lambda can mutate an existing ZefRef struct, but only by setting elements in s.edge_indexes
			// that have already been allocated and are zero before. This is the one exception to the append only structure.
			// prevent_new_edgelist_creation is a flag that is used in synchronization -> apply action blob.
            GraphData* gd = graph_data(uzr);
            if(edge_index_to_append >= gd->write_head) {
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                // throw std::runtime_error("Trying to append an edge index that's beyond the write_head!");
            }

            // Go directly to the end of the list

            // This will be an indirect reference into the middle of the blob at
            // a "last blob (width 16)" containing potentially 4 edge indices.
            blob_index last_blob_index = *last_edge_holding_blob(uzr);
            blob_index * last_blob;
            int subindex;
            if(last_blob_index == 0) {
                // This is a special case where there are no edges yet. Because
                // the start of the edge list may not be aligned with a blob,
                // then we need to be careful here and go directly to the start
                // of the edge list, instead of trying to interpret last_blob.
                uintptr_t direct_pointer = (uintptr_t)edge_indexes(uzr);
                last_blob = (blob_index*)(direct_pointer - (direct_pointer % constants::blob_indx_step_in_bytes));
                subindex = (blob_index*)direct_pointer - last_blob;
                // Need to set last_blob now, so that we can easily increment if necessary.
                *last_edge_holding_blob(uzr) = blob_index_from_ptr(last_blob);
            } else {
                last_blob = (blob_index*)ptr_from_blob_index(last_blob_index, *gd);
                // We may be jumping way ahead to an area we don't know about.
                Butler::ensure_or_get_range(last_blob, constants::blob_indx_step_in_bytes);
                static_assert(constants::blob_indx_step_in_bytes == 4*sizeof(blob_index));

                subindex = subindex_in_last_blob(last_blob);
                
                if(subindex == 3 && last_blob[subindex] == blobs_ns::sentinel_subsequent_index) {
                    if (!prevent_new_edgelist_creation) {
                        int new_edge_list_size = std::max(14, constants::new_appended_edge_list_growth_factor * total_edge_index_list_size_upper_limit(uzr));
                        // An edge list for a deferred edge list needs to be matched to the blob boundaries
                        size_t start_offset = offsetof(blobs_ns::DEFERRED_EDGE_LIST_NODE, edges) + offsetof(blobs_ns::DEFERRED_EDGE_LIST_NODE::deferred_edge_info, indices);
                        // This will overcompensate by an extra blob if it was already perfectly aligned... no big deal.
                        static_assert(constants::blob_indx_step_in_bytes == 4*sizeof(blob_index));
                        new_edge_list_size += 4 - (start_offset/sizeof(blob_index) + new_edge_list_size + 1) % 4;

                        EZefRef new_deferred_edge_list = create_new_deferred_edge_list(*gd, new_edge_list_size, index(uzr));
                        auto & new_details = get<blobs_ns::DEFERRED_EDGE_LIST_NODE>(new_deferred_edge_list);

                        // Update the subsequent index in the current blob.
                        auto new_index = index(new_deferred_edge_list);
                        last_blob[3] = new_index;

                        // Now we pretend that we didn't even create this blob, just by updating the last_blob and subindex "pointers"
                        uintptr_t direct_pointer = (uintptr_t)edge_indexes(new_deferred_edge_list);
                        last_blob = (blob_index*)(direct_pointer - (direct_pointer % constants::blob_indx_step_in_bytes));
                        // Note: subindex dosen't have to be 0, as the start of the edge list may not align with a blob
                        subindex = (blob_index*)direct_pointer - last_blob;
                        // Need to update the original source of last_blob
                        *last_edge_holding_blob(uzr) = blob_index_from_ptr(last_blob);
                    } else {
                        // False means we wanted to create a deferred edge list but got rejected.
                        return false;
                    }
                }
            }
            if(subindex == 3 && last_blob[subindex] != 0) {
                throw std::runtime_error("Shouldn't get here! subindex == 3 and last_blob[3] != 0");
            }

            last_blob[subindex] = edge_index_to_append;
            if(subindex == 3)
                (*last_edge_holding_blob(uzr))++;
            return true;
        }

		blob_index idempotent_append_edge_index(EZefRef uzr, blob_index edge_index_to_append) {
            // The comment below was for an extra parameter that has since been
            // removed. However, it will likely be added again, so I will leave
            // it in here as an idea.
            //
            // This version of append_edge_index uses a hint. The hint sets a
            // lower bound on where to start the search, i.e. for quicker
            // apply_actions_to_blobs behaviour. It *must* require that blobs
            // are added in the order that their edge indices will appear in the
            // lists.

			// the following lambda can mutate an existing ZefRef struct, but only by setting elements in s.edge_indexes
			// that have already been allocated and are zero before. This is the one exception to the append only structure.
			// prevent_new_edgelist_creation is a flag that is used in synchronization -> apply action blob.
            assert(edge_index_to_append != 0);

            GraphData * gd = graph_data(uzr);
            if(edge_index_to_append >= gd->write_head) {
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                std::cerr << "Trying to append an edge index that's beyond the write_head! " << edge_index_to_append << " >= " << gd->write_head.load() << std::endl;
                // throw std::runtime_error("Trying to append an edge index that's beyond the write_head!");
                return 0;
            }

            // First check to see if the edge is here.
            // Use std::find_if, but avoid allocating and copying to std::vector or similar. Use span to reference existing array and wrap for stl
            // don't include the last element. This would not point to an edge, but the next deferred_edge_list
            // ranges::span<blob_index> my_span(s.edges.indices, edge_indexes_capacity(s)-1);  // use std::span once compilers fully support C++20
            AllEdgeIndexes iterable{uzr, true};
            auto itr = iterable.begin();
            while(itr != iterable.end() && !(*itr == 0 || *itr == edge_index_to_append))
                itr++;
            // auto maybe_found = std::find_if(itr.begin(), itr.end(),   
            //                                 [edge_index_to_append](blob_index x)->bool { return x == 0 || x == edge_index_to_append; });

            if(itr != iterable.end() && *itr == edge_index_to_append)
                // Nothing to do, except return where we got up to for updating the hint in the caller.
                return index(itr.current_blob_pointed_to);
                
            assert(itr == iterable.end());
            if (*itr == blobs_ns::sentinel_subsequent_index) {
                // If we're here, it should be that the next thing to
                // apply_action to is the new deferred edge list which
                // will update the subsequent deferred edge list index.
                //
                // In other words, we don't have anything to do here.
                // But note that this only makes sense when apply
                // actions to updates, not when instantiating new blobs
                // (which doesn't hit this code).
            } else {
                // Otherwise, we fill in the new edge
                assert(*itr == 0);
                *itr = edge_index_to_append;

                // Update the original blob with the details. I think
                // this is technically not necessary, as we are applying
                // blobs and this information should already be on the
                // original blob.
                uintptr_t ptr = (uintptr_t)itr.ptr_to_current_edge_element;
                // We may be jumping way ahead to an area we don't know about.
                blob_index * last_blob = (blob_index*)(ptr - (ptr % constants::blob_indx_step_in_bytes));
                Butler::ensure_or_get_range(last_blob, 4);
                int subindex = (blob_index*)ptr - last_blob;
                (*last_edge_holding_blob(uzr)) = blob_index_from_ptr((void*)(ptr - (ptr % constants::blob_indx_step_in_bytes)));
                (*last_edge_holding_blob(uzr)) = blob_index_from_ptr((void*)(ptr - (ptr % constants::blob_indx_step_in_bytes)));
                if(subindex == 3)
                    (*last_edge_holding_blob(uzr))++;
            }
            return index(itr.current_blob_pointed_to);
        }


		BaseUID get_graph_uid(const GraphData& gd) {
			return get_blob_uid(EZefRef(constants::ROOT_NODE_blob_index, gd));
        }
		BaseUID get_graph_uid(const Graph& g) {
			return get_graph_uid(g.my_graph_data());
        }
		BaseUID get_graph_uid(const EZefRef& uzr) {
			return get_graph_uid(Graph(uzr));
        }

		BaseUID get_blob_uid(const EZefRef& uzr) {
            return blob_uid_ref(uzr);
        }


        void assign_uid(EZefRef uzr, BaseUID uid) {
            blob_uid_ref(uzr) = uid;
        }

	}
}
