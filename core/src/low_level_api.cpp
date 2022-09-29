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
#include "external_handlers.h"
// We reach up into select parts of the high_level api to implement the
// low_level api.
#include "ops_imperative.h"

namespace zefDB {
    
    //////////////////////////////////////
    // * AttributeEntityType

	AttributeEntityTypeStruct AET;

    // TODO: There seems to be something missing here with the string lookups - it's not the same pattern as ET,RT

    AttributeEntityType fallback_old_style_AET(const blobs_ns::ATTRIBUTE_ENTITY_NODE & ae) {
        return AttributeEntityType(ae.primitive_type);
    }

	AttributeEntityType AttributeEntityTypeStruct::operator() (const blobs_ns::ATTRIBUTE_ENTITY_NODE & ae) const {
        // The first edge is a REL_ENT_INSTANCE and the second must be the VALUE_TYPE_EDGE
        if(ae.edges.indices[1] <= 0) {
            // std::cerr << "Warning: Complex value type edge is missing" << std::endl;
            return fallback_old_style_AET(ae);
        }
        EZefRef maybe_edge{ae.edges.indices[1], *graph_data(&ae)};
        if(BT(maybe_edge) != BT.VALUE_TYPE_EDGE) {
            // std::cerr << "Warning: Complex value type edge is missing" << std::endl;
            return fallback_old_style_AET(ae);
        }

        EZefRef z_value_node = imperative::target(maybe_edge);
        auto & value_node = get<blobs_ns::VALUE_NODE>(z_value_node);

        return internals::value_from_node<AttributeEntityType>(value_node);
	}
	AttributeEntityType AttributeEntityTypeStruct::operator() (EZefRef uzr) const {
		if (get<BlobType>(uzr) != BlobType::ATTRIBUTE_ENTITY_NODE) throw std::runtime_error("AET(EZefRef uzr) called for a uzr which is not an atomic entity.");
        if(internals::is_delegate(uzr)) {
            // If this is a delegate, then it has no external value type, only
            // the primitive VRT. But casting it to an AET implies that this is
            // actually the logic type. So don't allow this
            throw std::runtime_error("Can't take an AET from a delegate attribute entity.");
        } else {
            return AET(get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(uzr));
        }
	}
	AttributeEntityType AttributeEntityTypeStruct::operator() (ZefRef zr) const {
		return AET(zr.blob_uzr);
	}

	AttributeEntityType::operator str () const {
        std::string temp = internals::get_string_name_from_value_rep_type(this->rep_type);
        if(_is_complex())
            temp += "[Complex type]";
        return temp;
    }

	std::ostream& operator << (std::ostream& o, const AttributeEntityType & aet) {
        o << "AET.";
		o << str(aet);
		return o;
	}

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
        else if (BT(uzr) == BT.ATTRIBUTE_ENTITY_NODE) {
            // We have to branch here as delegates which are atomic entities are
            // really value rep delegates... this is because of backwards
            // compatibility.
            if(is_delegate(uzr)) {
                os << " " << VRT(uzr);
            } else {
                os << " " << AET(uzr);
            }
        }
        else if (BT(uzr) == BT.VALUE_NODE)
            os << " " << VRT(uzr);
        else if (BT(uzr) == BT.TX_EVENT_NODE)
            os << " TX at slice=" << TimeSlice(uzr).value;
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



    std::ostream& operator<<(std::ostream& o, const EZefRefs& uzrs) {
        return o << "<EZefRefs with " << length(uzrs) << " items>";
    }
    std::ostream& operator<<(std::ostream& o, const EZefRefss& uzrss) {
        return o << "<EZefRefs with " << length(uzrss) << " items>";
    }





    

    namespace internals{

        template<class T>
        std::function<int(value_hash_t,blob_index)> create_compare_func_for_value_node(GraphData & gd, const T * value) {
            auto hash = value_hash(*value);
            return [value,&gd,hash](value_hash_t other_hash, blob_index other_indx) {
                if(other_hash != hash) {
                    return hash < other_hash ? -1 : +1;
                }

                EZefRef ezr_other{other_indx, gd};
                auto & other_node = get<blobs_ns::VALUE_NODE>(ezr_other);
                auto other_value = value_from_node<value_variant_t>(other_node);

                if(variant_eq(*value, other_value)) {
                    return 0;
                } else {
                    // Should be able to do better here instead of "always left"
                    // - however this will mess with the partial updates sent
                    // over the network. So instead we will go with "always
                    // left" for simplicity.
                    return -1;
                }
            };
        }


        template<>
        std::function<int(value_hash_t,blob_index)> create_compare_func_for_value_node(GraphData & gd, const value_variant_t * value) {
            return std::visit([&gd](const auto & x) {
                return create_compare_func_for_value_node(gd, &x);
            }, *value);
        }



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
            case BlobType::ATTRIBUTE_ENTITY_NODE: { new (new_ptr) ATTRIBUTE_ENTITY_NODE; break; }
            case BlobType::RELATION_EDGE: { new (new_ptr) RELATION_EDGE; break; }
            case BlobType::DEFERRED_EDGE_LIST_NODE: { new (new_ptr) DEFERRED_EDGE_LIST_NODE; break; }
            case BlobType::FOREIGN_GRAPH_NODE: { new (new_ptr) FOREIGN_GRAPH_NODE; break; }
            case BlobType::FOREIGN_ENTITY_NODE: { new (new_ptr) FOREIGN_ENTITY_NODE; break; }
            case BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE: { new (new_ptr) FOREIGN_ATTRIBUTE_ENTITY_NODE; break; }
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
                                         + std::string("src:g=") + str(get_graph_uid(src)) + ":ind=" + index(src) + ", "
                                         + std::string("trg:g=") + str(get_graph_uid(trg)) + ":ind=" + index(trg) + ", "
                                         + std::string("gd=") + str(get_graph_uid(gd)));
                                         
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
            case BlobType::VALUE_TYPE_EDGE: { common_behavior(get<VALUE_TYPE_EDGE>(this_new_blob)); break; }
            default: {throw std::runtime_error("instantiate(EZefRef src, BlobType bt, EZefRef trg, GraphData& gd) called for BlobType an unhandled case. BT: " + to_str(bt)); }
            }			

            move_head_forward(gd);
            append_edge_index(src, index(this_new_blob));
            append_edge_index(trg, -index(this_new_blob));
            return this_new_blob;
        }


        template<typename T>
        std::optional<EZefRef> search_value_node(const T & value, GraphData& gd) {
            auto ptr = gd.av_hash_lookup->get();
            value_variant_t value_var(std::in_place_type<T>, value);
            auto compare_func = internals::create_compare_func_for_value_node(gd, &value_var);
            auto preexisting = ptr->find_element(compare_func).second;
            if(preexisting != nullptr)
                return EZefRef{preexisting->val, gd};
            return {};
        }

        template std::optional<EZefRef> search_value_node(const bool & value, GraphData& gd);
        template std::optional<EZefRef> search_value_node(const int & value, GraphData& gd);
        template std::optional<EZefRef> search_value_node(const double & value, GraphData& gd);
        template std::optional<EZefRef> search_value_node(const str & value, GraphData& gd);
        template std::optional<EZefRef> search_value_node(const Time & value, GraphData& gd);
        template std::optional<EZefRef> search_value_node(const ZefEnumValue & value, GraphData& gd);
        template std::optional<EZefRef> search_value_node(const QuantityFloat & value, GraphData& gd);
        template std::optional<EZefRef> search_value_node(const QuantityInt & value, GraphData& gd);
        template std::optional<EZefRef> search_value_node(const SerializedValue & value, GraphData& gd);
        template std::optional<EZefRef> search_value_node(const AttributeEntityType & value, GraphData& gd);

        
        template<typename T>
        EZefRef instantiate_value_node(const T & value, GraphData& gd) {
            auto vrt = get_vrt_from_ctype(value);
            value_hash_t hash = value_hash(value);

            blobs_ns::VALUE_NODE& ent = internals::get_next_free_writable_blob<blobs_ns::VALUE_NODE>(gd);
            MMap::ensure_or_alloc_range(&ent, blobs_ns::max_basic_blob_size);
            ent.this_BlobType = BlobType::VALUE_NODE;
            ent.rep_type = vrt;

            char * data_buffer = internals::get_data_buffer(ent);
            internals::copy_to_buffer(data_buffer, ent.buffer_size_in_bytes, value);

            // The edges come after, and we need to prepare that structure
            auto & edges = blob_edge_info(ent);
            assert((uintptr_t)&edges % sizeof(blob_index) == 0);
            // We need to calculate the number of edges to use because of the
            // dynamic data buffer means it's impossible to precalculate the
            // number to align the end of the edge list to a blob.
            int edge_list_size = constants::default_local_edge_indexes_capacity_VALUE_NODE;
            static_assert(constants::blob_indx_step_in_bytes == 4*sizeof(blob_index));
            uintptr_t start_offset = (uintptr_t)&edges + offsetof(blobs_ns::edge_info, indices);
            // This will overcompensate by an extra blob if it was already perfectly aligned... no big deal.
            edge_list_size += 4 - (start_offset/sizeof(blob_index) + edge_list_size + 1) % 4;

            size_t edges_size = sizeof(blobs_ns::edge_info) + sizeof(blob_index)*(edge_list_size+1);
            MMap::ensure_or_alloc_range(&edges, edges_size);
            new(&edges) blobs_ns::edge_info(edge_list_size);

            internals::move_head_forward(gd);

            EZefRef z_ent((void*)&ent);
            internals::apply_action_VALUE_NODE(gd, z_ent, true);

            return z_ent;
        }

        template EZefRef instantiate_value_node(const bool & value, GraphData& g);
        template EZefRef instantiate_value_node(const int & value, GraphData& g);
        template EZefRef instantiate_value_node(const double & value, GraphData& g);
        template EZefRef instantiate_value_node(const str & value, GraphData& g);
        template EZefRef instantiate_value_node(const Time & value, GraphData& g);
        template EZefRef instantiate_value_node(const ZefEnumValue & value, GraphData& g);
        template EZefRef instantiate_value_node(const QuantityFloat & value, GraphData& g);
        template EZefRef instantiate_value_node(const QuantityInt & value, GraphData& g);
        template EZefRef instantiate_value_node(const SerializedValue & value, GraphData& g);
        template EZefRef instantiate_value_node(const AttributeEntityType & value, GraphData& g);






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
                // std::cerr << "Blob alignment: " << ((uintptr_t)&el) % sizeof(blob_index) << std::endl;
                // std::cerr << "length requested: " << edge_list_length << std::endl;
                // std::cerr << "Problem offset: " << edge_list_end_offset(uzr) << std::endl;
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

        bool is_root(EZefRef z) {
            return index(z) == constants::ROOT_NODE_blob_index;
        }
    
        bool is_delegate(EZefRef z) {
            // Note: internals::has_delegate is different to has_delegate below.
            if(!internals::has_delegate(BT(z)))
                return false;
            // we may additionally want to use (for efficiency) the spec. determined fact that if a rel_ent 
            // has an incoming edge of type BT.TO_DELEGATE_EDGE, this is always the first one in the edge list.
            return length(imperative::traverse_in_edge_multi(z, BT.TO_DELEGATE_EDGE)) == 1;
        };


        bool is_delegate_relation_group(EZefRef z) {
            if(!is_delegate(z) || BT(z) != BT.RELATION_EDGE)
                return false;

            return visit_blob_with_source_target([&z](auto & x) {
                return ((x.source_node_index == index(z)) &&
                        (x.target_node_index == index(z)));
            }, z);
        }


        bool has_delegate(EZefRef z) {
            return imperative::traverse_in_edge_multi(z, BT.RAE_INSTANCE_EDGE).len == 1;
        };


        template <typename T>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const T & value_to_be_assigned) {
            new(data_buffer_ptr) T(value_to_be_assigned);  // placement new: call copy ctor: copy assignment may not be defined
            buffer_size_in_bytes = sizeof(value_to_be_assigned);
        }

        template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const int & value_to_be_assigned);
        template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const double & value_to_be_assigned);
        template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const bool & value_to_be_assigned);
        template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, char const * const & value_to_be_assigned);
        template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const Time & value_to_be_assigned);
        template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const QuantityFloat & value_to_be_assigned);
        template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const QuantityInt & value_to_be_assigned);
        template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const ZefEnumValue & value_to_be_assigned);

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

        template<>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const AttributeEntityType & value) {
            if(value._is_complex()) {
                MMap::ensure_or_alloc_range(data_buffer_ptr, sizeof(enum_indx));
                *(enum_indx*)data_buffer_ptr = VRT._unspecified.value;
                unsigned int serialized_size;
                copy_to_buffer(data_buffer_ptr + sizeof(enum_indx), serialized_size, *value.complex_value);
                buffer_size_in_bytes = sizeof(enum_indx) + serialized_size;
            } else {
                buffer_size_in_bytes = sizeof(enum_indx);
                MMap::ensure_or_alloc_range(data_buffer_ptr, buffer_size_in_bytes);
                *(enum_indx*)data_buffer_ptr = value.rep_type.value;
            }
        }

        ////////////////////////////////
        // * Is compatible funcs

        // The flow here is:
        //
        // is_compatible -> pass_to_value_type_check
        //            or -> is_compatible_primitive -> is_compatible_rep_type

        template<typename T>
        bool is_compatible_rep_type(const ValueRepType & vrt);

        template<> bool is_compatible_rep_type<value_variant_t>(const ValueRepType & vrt) { return true; }
        template<> bool is_compatible_rep_type<bool>(const ValueRepType & vrt) { return vrt == VRT.Bool; }
        template<> bool is_compatible_rep_type<int>(const ValueRepType & vrt) { return vrt == VRT.Int || vrt == VRT.Float || vrt == VRT.Bool; }   // we can also assign an int to a bool
        template<> bool is_compatible_rep_type<double>(const ValueRepType & vrt) { return vrt == VRT.Float || vrt == VRT.Int; }
        template<> bool is_compatible_rep_type<str>(const ValueRepType & vrt) { return vrt == VRT.String; }
        template<> bool is_compatible_rep_type<const char*>(const ValueRepType & vrt) { return vrt == VRT.String; }
        template<> bool is_compatible_rep_type<Time>(const ValueRepType & vrt) { return vrt == VRT.Time; }
        template<> bool is_compatible_rep_type<SerializedValue>(const ValueRepType & vrt) { return vrt == VRT.Serialized; }
        template<> bool is_compatible_rep_type<AttributeEntityType>(const ValueRepType & vrt) { return vrt == VRT.Type; }

        template<> bool is_compatible_rep_type<ZefEnumValue>(const ValueRepType & vrt) {
            int offset = vrt.value % 16;
            if (offset != 1) return false;   // Enums encoded by an offset of 1
            return true;
        }
        template<> bool is_compatible_rep_type<QuantityFloat>(const ValueRepType & vrt) {
            int offset = vrt.value % 16;
            if (offset != 2) return false;   // QuantityFloat encoded by an offset of 2
            return true;		
        }
        template<> bool is_compatible_rep_type<QuantityInt>(const ValueRepType & vrt) {
            int offset = vrt.value % 16;		
            if (offset != 3) return false;   // QuantityInt encoded by an offset of 3
            return true;
        }

        template<typename T>
        bool is_compatible_primitive(const T & val, const ValueRepType & vrt) {
            return is_compatible_rep_type<T>(vrt); 
        }

        template<>
        bool is_compatible_primitive(const ZefEnumValue & en, const ValueRepType & vrt) {
            int offset = vrt.value % 16;
            return is_compatible_rep_type<ZefEnumValue>(vrt)
                && (ZefEnumValue{ (vrt.value - offset) }.enum_type() == en.enum_type());
        }
        template<>
        bool is_compatible_primitive(const QuantityFloat & q, const ValueRepType & vrt) {
            int offset = vrt.value % 16;
            return is_compatible_rep_type<QuantityFloat>(vrt)
                && ((vrt.value - offset) == q.unit.value);
        }
        template<>
        bool is_compatible_primitive(const QuantityInt & q, const ValueRepType & vrt) {
            int offset = vrt.value % 16;
            return is_compatible_rep_type<QuantityInt>(vrt)
                && ((vrt.value - offset) == q.unit.value);
        }

        template<typename T>
        bool is_compatible(const T & val, const AttributeEntityType & aet) {
            if(aet._is_complex())
                return internals::pass_to_value_type_check(val, *aet.complex_value);
            // This is a little out of place but is simpler.
            if(aet.rep_type == VRT.Any)
                return true;
            return is_compatible_primitive(val, aet.rep_type);
        }

        template bool is_compatible(const bool & val, const AttributeEntityType & aet);
        template bool is_compatible(const int & val, const AttributeEntityType & aet);
        template bool is_compatible(const double & val, const AttributeEntityType & aet);
        template bool is_compatible(const str & val, const AttributeEntityType & aet);
        template bool is_compatible(const Time & val, const AttributeEntityType & aet);
        template bool is_compatible(const SerializedValue & val, const AttributeEntityType & aet);
        template bool is_compatible(const ZefEnumValue & en, const AttributeEntityType & aet);
        template bool is_compatible(const QuantityFloat & q, const AttributeEntityType & aet);
        template bool is_compatible(const QuantityInt & q, const AttributeEntityType & aet);
        template bool is_compatible(const AttributeEntityType & val, const AttributeEntityType & aet);
        template<>
        bool is_compatible(const EZefRef & z, const AttributeEntityType & aet) {
            if(!is_zef_subtype(z, BT.VALUE_NODE))
                throw std::runtime_error("Got to is_compatible with a non-AVN EZefRef");
            auto & ent = get<blobs_ns::VALUE_NODE>(z);
            // In the future, this can just be a ZefValue pointer.
            auto val = value_from_node<value_variant_t>(ent);
            return std::visit([&aet](auto & x) {
                return is_compatible(x, aet);
            },
                val);
        }

        template<>
        bool is_compatible(const value_variant_t & val, const AttributeEntityType & aet) {
            return std::visit([&aet](auto & x) { return is_compatible(x, aet); },
                       val);
        }

        //////////////////////////////////////////////
        // * Obtaining raw values

        // use template specialization for the return value in the fct 'auto operator^ (ZefRef my_atomic_entity, T op) -> std::optional<decltype(op._x)>' below.
        // string values are saved as a char array. We could return a string_view, but for simplicity and pybind11, instantiate an std::string for now

        template<typename T>
        T value_from_ptr(const char * buf, unsigned int size, ValueRepType vrt);

        template<>
        str value_from_ptr<str>(const char * buf, unsigned int size, ValueRepType vrt) {
            if(vrt != VRT.String)
                throw std::runtime_error("Can't extract a string from anything other than a VRT.String");
            Butler::ensure_or_get_range(buf, size);
            return std::string(buf, size);
        }

        template<>
        SerializedValue value_from_ptr<SerializedValue>(const char * buf, unsigned int size, ValueRepType vrt) {
            if(vrt != VRT.Serialized)
                throw std::runtime_error("Can't extract a SerializedValue from anything other than a VRT.Serialized");
            Butler::ensure_or_get_range(buf, size);
            const char * cur = buf;
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
        double value_from_ptr<double>(const char * buf, unsigned int size, ValueRepType vrt) {
            // This needs to be specialised to convert, while allowing the other variants to avoid this unncessary check
            if (vrt == VRT.Float)
                return value_cast<double>(*(double*)(buf));
            else if(vrt == VRT.Int)
                return value_cast<double>(*(int*)(buf));
            else
                throw std::runtime_error("Can't extract a double from anything other than VRT.Float or VRT.Int");
        }
        template<>
        int value_from_ptr<int>(const char * buf, unsigned int size, ValueRepType vrt) {
            // This needs to be specialised to convert, while allowing the other variants to avoid this unncessary check
            if (vrt == VRT.Float)
                return value_cast<int>(*(double*)(buf));
            else if(vrt == VRT.Int)
                return value_cast<int>(*(int*)(buf));
            else if(vrt == VRT.Bool)
                return value_cast<int>(*(bool*)(buf));
            else
                throw std::runtime_error("Can't extract a int from anything other than VRT.Float, VRT.Int or VRT.Bool");
        }

        template<>
        ValueRepType value_from_ptr<ValueRepType>(const char * buf, unsigned int size, ValueRepType vrt) {
            if(vrt != VRT.Type)
                throw std::runtime_error("Not a VRT");
            return ValueRepType{*(enum_indx*)(buf)};
        }

        template<>
        AttributeEntityType value_from_ptr<AttributeEntityType>(const char * buf, unsigned int size, ValueRepType vrt) {
            if(vrt != VRT.Type)
                throw std::runtime_error("Not a type");

            // FIXME we may need to worry about unaligned accesses later on

            // Here we can use that we know how big the buffer is to determine
            // whether there's a single enum_index to identify a primitive VRT
            // or a complex one. However, this is not always available so let's
            // do it properly here too.

            // If the primitive VRT is zero, then it must be a complex type

            auto value_vrt = value_from_ptr<ValueRepType>(buf, sizeof(ValueRepType), VRT.Type);
            if(value_vrt == VRT._unspecified) {
                // This is a complex type which must be serialized (for now).

                // Need a better way to know what the size is that we are up to,
                // as this could be necessary in the future.
                auto serialized = value_from_ptr<SerializedValue>(buf + sizeof(ValueRepType), size - sizeof(ValueRepType), VRT.Serialized);
                return AttributeEntityType{serialized};
            } else {
                return AttributeEntityType{value_vrt};
            }
        }

        template<>
        value_variant_t value_from_ptr<value_variant_t>(const char * buf, unsigned int size, ValueRepType vrt) {
            // We get the actual value from the VRT and just return that unconverted
            switch (vrt.value) {
            case VRT.Float.value: { return value_from_ptr<double>(buf, size, vrt); }
            case VRT.Int.value: { return value_from_ptr<int>(buf, size, vrt); }
            case VRT.Bool.value: { return value_from_ptr<bool>(buf, size, vrt); }
            case VRT.String.value: { return value_from_ptr<std::string>(buf, size, vrt); }
            case VRT.Time.value: { return value_from_ptr<Time>(buf, size, vrt); }
            case VRT.Serialized.value: { return value_from_ptr<SerializedValue>(buf, size, vrt); }
            case VRT.Type.value: { return value_from_ptr<AttributeEntityType>(buf, size, vrt); }
            case VRT.Any.value: { throw std::runtime_error("Should never get to a point where a concrete value has a representation type of VRT.Any"); }
            default: {
                switch (vrt.value % 16) {
                case 1: { return value_from_ptr<ZefEnumValue>(buf, size, vrt); }
                case 2: { return value_from_ptr<QuantityFloat>(buf, size, vrt); }
                case 3: { return value_from_ptr<QuantityInt>(buf, size, vrt); }
                default: throw std::runtime_error("Return type not implemented.");
                }
            }
            }
        }

        // for contiguous POD types with compile-time determined size, we can use this template
        template <typename T>
        T value_from_ptr(const char * buf, unsigned int size, ValueRepType vrt) {
            return *(T*)(buf);  // project onto type
        }

        // template QuantityInt value_from_node<QuantityInt>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        // template QuantityFloat value_from_node<QuantityFloat>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        // template ZefEnumValue value_from_node<ZefEnumValue>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        // template Time value_from_node<Time>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        // template bool value_from_node<bool>(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);


        template<typename T, typename C>
        T value_from_node_(const C & aae_or_avn) {
            const char * data_buffer = get_data_buffer(aae_or_avn);
            unsigned int size = get_data_buffer_size(aae_or_avn);
            return value_from_ptr<T>(data_buffer, size, aae_or_avn.rep_type);
        }

        template<class T>
        T value_from_node(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae) {
            return value_from_node_<T>(aae);
        }
        template<class T>
        T value_from_node(const blobs_ns::VALUE_NODE& av) {
            return value_from_node_<T>(av);
        }

        template QuantityInt value_from_node<QuantityInt>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template QuantityFloat value_from_node<QuantityFloat>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template ZefEnumValue value_from_node<ZefEnumValue>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template Time value_from_node<Time>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template bool value_from_node<bool>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template str value_from_node<str>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template SerializedValue value_from_node<SerializedValue>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template double value_from_node<double>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template int value_from_node<int>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template AttributeEntityType value_from_node<AttributeEntityType>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template value_variant_t value_from_node<value_variant_t>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);

        template QuantityInt value_from_node<QuantityInt>(const blobs_ns::VALUE_NODE& av);
        template QuantityFloat value_from_node<QuantityFloat>(const blobs_ns::VALUE_NODE& av);
        template ZefEnumValue value_from_node<ZefEnumValue>(const blobs_ns::VALUE_NODE& av);
        template Time value_from_node<Time>(const blobs_ns::VALUE_NODE& av);
        template bool value_from_node<bool>(const blobs_ns::VALUE_NODE& av);
        template str value_from_node<str>(const blobs_ns::VALUE_NODE& av);
        template SerializedValue value_from_node<SerializedValue>(const blobs_ns::VALUE_NODE& av);
        template double value_from_node<double>(const blobs_ns::VALUE_NODE& av);
        template int value_from_node<int>(const blobs_ns::VALUE_NODE& av);
        template AttributeEntityType value_from_node<AttributeEntityType>(const blobs_ns::VALUE_NODE& aae);
        template value_variant_t value_from_node<value_variant_t>(const blobs_ns::VALUE_NODE& av);

    }
}
