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
	
	namespace internals {
		// ------------ factor out list monad lifting ---------------		
		inline auto lift_list_zrs_mv(std::function<ZefRef(ZefRef)> f) {
			return [f](ZefRefs&& zrs)->ZefRefs {    // don't pass the fct f by reference! It should be copied and be part of the lambda fct that is returned by value!
				std::transform(
					zrs._get_array_begin_const(),
					zrs._get_array_begin_const() + zrs.len,
					zrs._get_array_begin(),
					[&](const EZefRef& uzr) {return f(ZefRef{ uzr, zrs.reference_frame_tx }).blob_uzr; }
				);
				return zrs;
			};
		};


		// ------------ factor out list monad lifting ---------------		
		inline auto lift_list_zrs_cpy(std::function<ZefRef(ZefRef)> f) {
			return [f](const ZefRefs& zrs)->ZefRefs {    // don't pass the fct f by reference! It should be copied and be part of the lambda fct that is returned by value!
				auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
				std::transform(
					zrs._get_array_begin_const(),
					zrs._get_array_begin_const() + zrs.len,
					res._get_array_begin(),
					[&](const EZefRef& uzr) {return f(ZefRef{ uzr, zrs.reference_frame_tx }).blob_uzr; }
				);
				return res;
			};
		};





		// not required for now

		//inline auto lift_list2_zrs_cpy(std::function<ZefRefs(ZefRefs)> f2) {
		//	return [f2](const ZefRefss& zrss)->ZefRefss {    // don't pass the fct f by reference! It should be copied and be part of the lambda fct that is returned by value!
		//		GraphData& gd = zrss.len()==0 ? *(GraphData*)nullptr : 
		//							(zrss[0].len==0 ? *(GraphData*)nullptr : graph_data(zrss[0][0]));
		//		auto res = ZefRefss(zrss.len());
		//		std::transform(
		//			zrss.begin(),
		//			zrss.begin() + zrss.len(),
		//			res.begin(),
		//			f2
		//		);
		//		return res;
		//	};
		//};




		// ------------ factor out list monad lifting ---------------		
		inline auto lift_list_uzrs_mv(std::function<EZefRef(EZefRef)> f) {
			return [f](EZefRefs&& uzrs)->EZefRefs {    // don't pass the fct f by reference! It should be copied and be part of the lambda fct that is returned by value!
				for (auto& el : uzrs)
					el = f(el);
				return uzrs;
			};
		};


		// ------------ factor out list monad lifting ---------------		
		inline auto lift_list_uzrs_cpy(std::function<EZefRef(EZefRef)> f) {
			return [f](const EZefRefs& uzrs)->EZefRefs {    // don't pass the fct f by reference! It should be copied and be part of the lambda fct that is returned by value!
				EZefRefs res(uzrs);
				for (auto& el : res)
					el = f(el);
				return res;
			};
		};


		inline auto lift_zr(std::function<EZefRef(EZefRef)> f) {
			return [f](ZefRef zr)->ZefRef {    // don't pass the fct f by reference!				
				return ZefRef{ f(zr.blob_uzr), zr.tx };
			};
		}
	}


	
	namespace zefOps {




		//                        _                            
		//                       | |___  __                    
		//    _____ _____ _____  | __\ \/ /  _____ _____ _____ 
		//   |_____|_____|_____| | |_ >  <  |_____|_____|_____|
		//                        \__/_/\_|                    

		EZefRefs Tx::operator() (Graph& g) const {
			std::vector<EZefRef> all_txs;
			all_txs.reserve(16384);
			EZefRefs next_tx_candidates = g[constants::ROOT_NODE_blob_index] >> L[BT.NEXT_TX_EDGE];
			while (length(next_tx_candidates) == 1) {
				const EZefRef this_tx = next_tx_candidates | first;
				all_txs.push_back(this_tx);
				next_tx_candidates = this_tx >> L[BT.NEXT_TX_EDGE];
			}
			return EZefRefs(all_txs);
		}







		//                         _   _                                     
		//                        | |_(_)_ __ ___   ___                      
		//    _____ _____ _____   | __| | '_ ` _ \ / _ \   _____ _____ _____ 
		//   |_____|_____|_____|  | |_| | | | | | |  __/  |_____|_____|_____|
		//                         \__|_|_| |_| |_|\___|                     
		//                                                            

		Time TimeZefopStruct::operator() (EZefRef uzr) const {
			if (BT(uzr) != BT.TX_EVENT_NODE)
				throw std::runtime_error("time(uzr) called on uzr that is not a TX_EVENT_NODE.");
			return get<blobs_ns::TX_EVENT_NODE>(uzr).time;
		}
		
		Time TimeZefopStruct::operator() (ZefRef zr) const {
			return operator()(zr.blob_uzr);
		}
		
		Time TimeZefopStruct::operator() (Now op) const {
			return Time{ std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count() * 1E-6 };
		}


		//                                                                          
		//                         ___  ___  _   _ _ __ ___ ___                     
		//     _____ _____ _____  / __|/ _ \| | | | '__/ __/ _ \  _____ _____ _____ 
		//    |_____|_____|_____| \__ \ (_) | |_| | | | (_|  __/ |_____|_____|_____|
		//                        |___/\___/ \__,_|_|  \___\___|                    
		//                                                                       
        EZefRef Source::operator() (EZefRef uzr) const {
            return imperative::source(uzr);
        }
        EZefRefs Source::operator() (EZefRefs&& uzrs) const {
				// resuse the internal EZefRefs vector. e.g. auto res = std::move(my_uzr) | source
				// TODO: test this!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! is el below actually overwriting? !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				for (auto& el : uzrs)
					el = Source::operator()(el);
				return uzrs;
			}
        EZefRefs Source::operator() (const EZefRefs& uzrs) const {
				auto res = EZefRefs(
					uzrs.len,
					graph_data(uzrs)
				);
				std::transform(
					uzrs._get_array_begin_const(),
					uzrs._get_array_begin_const() + uzrs.len,
					res._get_array_begin(),
					[this](const EZefRef& uzr) {return Source::operator()(uzr); }
				);
				return res;
			}

        ZefRefs Source::operator() (ZefRefs&& zrs) const {
				std::transform(
					zrs._get_array_begin(),
					zrs._get_array_begin() + zrs.len,
					zrs._get_array_begin(),
					[this](const EZefRef& uzr) {return Source::operator()(uzr); }
				);
				return zrs;
			}
        ZefRefs Source::operator() (const ZefRefs& zrs) const {
				auto res = ZefRefs(
					zrs.len,
                    zrs.reference_frame_tx
				);
				std::transform(
					zrs._get_array_begin_const(),
					zrs._get_array_begin_const() + zrs.len,
					res._get_array_begin(),
					[this](const EZefRef& uzr) {return Source::operator()(uzr); }
				);
				return res;
			}



		ZefRef Source::operator() (ZefRef zr) const {
			return imperative::source(zr);
		}

		//                         _                       _                       
		//                        | |_ __ _ _ __ __ _  ___| |_                     
		//     _____ _____ _____  | __/ _` | '__/ _` |/ _ \ __|  _____ _____ _____ 
		//    |_____|_____|_____| | || (_| | | | (_| |  __/ |_  |_____|_____|_____|
		//                         \__\__,_|_|  \__, |\___|\__|                    
		//                                      |___/                          
        EZefRef Target::operator() (EZefRef uzr) const {
            return imperative::target(uzr);
        }

        EZefRefs Target::operator() (EZefRefs&& uzrs) const {
				for (auto& el : uzrs)
					el = Target::operator()(el);
				return uzrs;
			}

			EZefRefs Target::operator() (const EZefRefs& uzrs) const {
                return imperative::target(uzrs);
			}

			ZefRefs Target::operator() (ZefRefs&& zrs) const {
				std::transform(
					zrs._get_array_begin(),
					zrs._get_array_begin() + zrs.len,
					zrs._get_array_begin(),
					[this](const EZefRef& uzr) {return Target::operator()(uzr); }
				);
				return zrs;
			}
			ZefRefs Target::operator() (const ZefRefs& zrs) const {
                return imperative::target(zrs);
			}


		ZefRef Target::operator() (ZefRef zr) const {
			return imperative::target(zr);
		}




		//                               _                                      
		//                              (_)_ __  ___                            
		//    _____ _____ _____ _____   | | '_ \/ __|   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  | | | | \__ \  |_____|_____|_____|_____|
		//                              |_|_| |_|___/                           
		//                                                                      
    EZefRefs Ins::operator() (EZefRef uzr) const {
				GraphData* gd = graph_data(uzr);
				// allocate enough space: this may be more than the total number of edges
				// and specifically more than the in edges only
				if (!internals::has_edge_list(uzr)) {
					throw std::runtime_error("ins called on a EZefRef that does not have incoming or outgoing low level edges.");
					//return EZefRefs({});
				}
				auto res = EZefRefs(
					internals::total_edge_index_list_size_upper_limit(uzr),
					gd
				);
				EZefRef* pos_to_write_to = res._get_array_begin();
				int counter = 0;
				for (blob_index ind : AllEdgeIndexes(uzr)) {
					if (ind < 0) { *(pos_to_write_to++) = EZefRef(-ind, *gd); counter++; }
				}
				res.len = counter;
				if (res.delegate_ptr != nullptr) res.delegate_ptr->len = counter;
				return res;
			}



		ZefRefs Ins::operator() (ZefRef zr) const {
			return EZefRef(zr) | ins | filter[is_zefref_promotable_and_exists_at[zr.tx]] | to_zefref[zr.tx];
		}



		//                                           _                                  
		//                                ___  _   _| |_ ___                            
		//    _____ _____ _____ _____    / _ \| | | | __/ __|   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  | (_) | |_| | |_\__ \  |_____|_____|_____|_____|
		//                               \___/ \__,_|\__|___/                           
		//                                                                              
			EZefRefs Outs::operator() (EZefRef uzr) const {
				GraphData* gd = graph_data(uzr);
				// allocate enough space: this may be more than the total number of edges
				// and specifically more than the in edges only
				if (!internals::has_edge_list(uzr)) {
					throw std::runtime_error("outs called on a EZefRef that does not have incoming or outgoing low level edges.");
					//return EZefRefs({});
				}
				auto res = EZefRefs(
					internals::total_edge_index_list_size_upper_limit(uzr),
					gd
				);
				EZefRef* pos_to_write_to = res._get_array_begin();
				int counter = 0;
				for (blob_index ind : AllEdgeIndexes(uzr)) {
					if (ind > 0) { *(pos_to_write_to++) = EZefRef(ind, *gd); counter++; }
				}
				res.len = counter;
				if (res.delegate_ptr != nullptr) res.delegate_ptr->len = counter;
				return res;
			}

		ZefRefs Outs::operator() (ZefRef zr) const {
			return EZefRef(zr) | outs | filter[is_zefref_promotable_and_exists_at[zr.tx]] | to_zefref[zr.tx];
		}

		//                               _                              _                _                                  
		//                              (_)_ __  ___     __ _ _ __   __| |    ___  _   _| |_ ___                            
		//    _____ _____ _____ _____   | | '_ \/ __|   / _` | '_ \ / _` |   / _ \| | | | __/ __|   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  | | | | \__ \  | (_| | | | | (_| |  | (_) | |_| | |_\__ \  |_____|_____|_____|_____|
		//                              |_|_| |_|___/___\__,_|_| |_|\__,_|___\___/ \__,_|\__|___/                           
		//                                         |_____|              |_____|                                          
    EZefRefs InsAndOuts::operator() (EZefRef uzr) const {
				GraphData* gd = graph_data(uzr);
				// allocate enough space: this may be more than the total number of edges
				// and specifically more than the in edges only
				if (!internals::has_edge_list(uzr)) {
					throw std::runtime_error("ins_and_outs called on a EZefRef that does not have incoming or outgoing low level edges.");
					//return EZefRefs({});
				}
				auto res = EZefRefs(
					internals::total_edge_index_list_size_upper_limit(uzr),
					gd
				);
				EZefRef* pos_to_write_to = res._get_array_begin();
				int counter = 0;
				for (blob_index ind : AllEdgeIndexes(uzr)) {
					if (ind != 0) { *(pos_to_write_to++) = EZefRef(ind > 0 ? ind : -ind, *gd); counter++; }
				}
				res.len = counter;
				if (res.delegate_ptr != nullptr) res.delegate_ptr->len = counter;
				return res;
			}


		ZefRefs InsAndOuts::operator() (ZefRef zr) const {
			return EZefRef(zr) | ins_and_outs | filter[is_zefref_promotable_and_exists_at[zr.tx]] | to_zefref[zr.tx];
		}

        //////////////////////////////
        // * Connections

        bool HasIn::operator() (const EZefRef uzr) const {
            return std::visit(overloaded{
                    [](Sentinel x)->bool {throw std::runtime_error("has_in used without curried in relation"); return false; },
                    [&uzr](auto relation)->bool { 
                        return length(uzr < L[relation]) >= 1;
                    }
                }, relation);
        }
        bool HasIn::operator() (const ZefRef zr) const {
            return std::visit(overloaded{
                    [](Sentinel x)->bool {throw std::runtime_error("has_in used without curried in relation"); return false; },
                    [&zr](auto relation)->bool { 
                        return length(zr < L[relation]) >= 1;
                    }
                }, relation);
        }

        bool HasOut::operator() (const EZefRef uzr) const {
            return std::visit(overloaded{
                    [](Sentinel x)->bool {throw std::runtime_error("has_out used without curried in relation"); return false; },
                    [&uzr](auto relation)->bool { 
                        return length(uzr > L[relation]) >= 1;
                    }
                }, relation);
        }
        bool HasOut::operator() (const ZefRef zr) const {
            return std::visit(overloaded{
                    [](Sentinel x)->bool {throw std::runtime_error("has_out used without curried in relation"); return false; },
                    [&zr](auto relation)->bool { 
                        return length(zr > L[relation]) >= 1;
                    }
                }, relation);
        }


        bool has_relation(ZefRef a, ZefRef b) {
            return length(relations(a, b)) > 0;
        }
        bool has_relation(ZefRef a, RelationType rel, ZefRef b) {
            return length(relations(a, rel, b)) > 0;
        }
        ZefRef relation(ZefRef a, ZefRef b) {
            return relations(a,b) | only;
        }
        ZefRef relation(ZefRef a, RelationType rel, ZefRef b) {
            return relations(a, rel, b) | only;
        }
        ZefRefs relations(ZefRef a, ZefRef b) {
            return intersect(a | outs, b | ins);
        }
        ZefRefs relations(ZefRef a, RelationType rel, ZefRef b) {
            return intersect(a > L[rel], b < L[rel]);
        }

        bool has_relation(EZefRef a, EZefRef b) {
            return length(relations(a, b)) > 0;
        }
        bool has_relation(EZefRef a, RelationType rel, EZefRef b) {
            return length(relations(a, rel, b)) > 0;
        }
        EZefRef relation(EZefRef a, EZefRef b) {
            return relations(a, b) | only;
        }
        EZefRef relation(EZefRef a, RelationType rel, EZefRef b) {
            return relations(a, rel, b) | only;
        }
        EZefRefs relations(EZefRef a, EZefRef b) {
            return intersect(a | outs, b | ins);
        }
        EZefRefs relations(EZefRef a, RelationType rel, EZefRef b) {
            return intersect(a > L[rel], b < L[rel]);
        }


		//                                __ _ _ _                                       
		//                               / _(_) | |_ ___ _ __                            
		//    _____ _____ _____ _____   | |_| | | __/ _ \ '__|   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  |  _| | | ||  __/ |     |_____|_____|_____|_____|
		//                              |_| |_|_|\__\___|_|                              
		//                                                                             
			// -------------------- various functions to init filter object --------------------------
			Filter Filter::operator[] (EntityType my_entity_type) const {
				return Filter{
					not_in_activated,
					(std::function<bool(EZefRef)>)[my_entity_type](EZefRef uzr)->bool {
					return get<BlobType>(uzr) == BlobType::ENTITY_NODE &&
						get<blobs_ns::ENTITY_NODE>(uzr).entity_type == my_entity_type;
					},
					(std::function<bool(ZefRef)>)[my_entity_type](ZefRef zr)->bool {
					return get<BlobType>(zr) == BlobType::ENTITY_NODE &&
						get<blobs_ns::ENTITY_NODE>(zr).entity_type == my_entity_type;
					}
				};
			}

			Filter Filter::operator[] (RelationType my_relation_type) const {
				return Filter{
					not_in_activated,
					(std::function<bool(EZefRef)>)[my_relation_type](EZefRef uzr)->bool {
					return get<BlobType>(uzr) == BlobType::RELATION_EDGE &&
						get<blobs_ns::RELATION_EDGE>(uzr).relation_type == my_relation_type;
					},
					(std::function<bool(ZefRef)>)[my_relation_type](ZefRef zr)->bool {
					return get<BlobType>(zr) == BlobType::RELATION_EDGE &&
						get<blobs_ns::RELATION_EDGE>(zr).relation_type == my_relation_type;
					}
				};
			}

			Filter Filter::operator[] (AtomicEntityType aet) const {
				return Filter{
					not_in_activated,
					(std::function<bool(EZefRef)>)[aet](EZefRef uzr)->bool {
					return get<BlobType>(uzr) == BlobType::ATOMIC_ENTITY_NODE &&
						get<blobs_ns::ATOMIC_ENTITY_NODE>(uzr).my_atomic_entity_type == aet;
					},
					(std::function<bool(ZefRef)>)[aet](ZefRef zr)->bool {
					return get<BlobType>(zr) == BlobType::ATOMIC_ENTITY_NODE &&
						get<blobs_ns::ATOMIC_ENTITY_NODE>(zr).my_atomic_entity_type == aet;
					}
				};
			}

			Filter Filter::operator[] (BlobType my_BlobType) const {
				return Filter{
					not_in_activated,
					(std::function<bool(EZefRef)>)[my_BlobType](EZefRef uzr)->bool {
					return get<BlobType>(uzr) == my_BlobType;
					},
					(std::function<bool(ZefRef)>)[my_BlobType](ZefRef zr)->bool {  // skip the check here that the blob is a valid ZefRef compatible one
					return get<BlobType>(zr) == my_BlobType;
					}
				};
			}

			// at the time when the Filter struct is constructed, create the various predicate functions for which it may be used
			Filter Filter::operator[] (Tensor<RelationType, 1> my_relation_types) const {
				return Filter{
					not_in_activated,
					(std::function<bool(EZefRef)>)[my_relation_types](EZefRef uzr)mutable ->bool {   // we need the mutable here so long as our const iterators are not properly implemented. By default, value captured lambda parameters are const
						if (get<BlobType>(uzr) != BlobType::RELATION_EDGE) return false;
						for (auto rt : my_relation_types) {
							if (get<blobs_ns::RELATION_EDGE>(uzr).relation_type == rt) return true;
						}
						if (get<blobs_ns::RELATION_EDGE>(uzr).relation_type == RT.ZEF_Any) return true;
						return false;
					},
					(std::function<bool(ZefRef)>)[my_relation_types](ZefRef zr)mutable ->bool {
						if (get<BlobType>(zr) != BlobType::RELATION_EDGE) return false;
						for (auto rt : my_relation_types) {
							if (get<blobs_ns::RELATION_EDGE>(zr).relation_type == rt) return true;
						}
						if (get<blobs_ns::RELATION_EDGE>(zr).relation_type == RT.ZEF_Any) return true;
						return false;
					}
				};
			}
			
			
			Filter Filter::operator[] (Tensor<BlobType, 1> my_blob_types) const {
				return Filter{
					not_in_activated,
					(std::function<bool(EZefRef)>)[my_blob_types](EZefRef uzr)mutable ->bool {   // we need the mutable here so long as our const iterators are not properly implemented. By default, value captured lambda parameters are const						
						for (auto bt : my_blob_types) {
							if (get<BlobType>(uzr) == bt) return true;
						}						
						return false;
					},
					(std::function<bool(ZefRef)>)[my_blob_types](ZefRef zr)mutable ->bool {						
						for (auto bt : my_blob_types) {
							if (get<BlobType>(zr) == bt) return true;
						}						
						return false;
					}
				};
			}


			Filter Filter::operator[] (std::function<bool(EZefRef)> user_defined_predicate_fct) const {
				return Filter{ not_in_activated, user_defined_predicate_fct, {} };
			}

			Filter Filter::operator[] (std::function<bool(ZefRef)> user_defined_predicate_fct) const {
				return Filter{not_in_activated, {}, user_defined_predicate_fct };
			}

			Filter Filter::operator[] (NotIn my_not_in) const {
				return Filter{true, predicate_fct_uzefref, predicate_fct_zefref };
			}

			// -------------------- actual function called with list --------------------------
			EZefRefs Filter::operator() (const EZefRefs& candidate_list) const {
				if (!predicate_fct_uzefref) throw std::runtime_error("Filter operator() called, but the operator's predicate function was not set for EZefRefs.");
                if(not_in_activated)
                    return imperative::filter(candidate_list, [this](EZefRef uzr) { return !predicate_fct_uzefref(uzr); });
                else
                    return imperative::filter(candidate_list, predicate_fct_uzefref);
			}

			ZefRefs Filter::operator() (const ZefRefs& candidate_list) const {
				if (!predicate_fct_zefref) throw std::runtime_error("Filter operator() called, but the operator's predicate function was not set for ZefRefs.");
                if(not_in_activated)
                    return imperative::filter(candidate_list, [this](ZefRef zr) { return !predicate_fct_zefref(zr); });
                else
                    return imperative::filter(candidate_list, predicate_fct_zefref);
			}





        //                                          _                         
        //                           ___  ___  _ __| |_                       
        //    _____ _____ _____     / __|/ _ \| '__| __|    _____ _____ _____ 
        //   |_____|_____|_____|    \__ \ (_) | |  | |_    |_____|_____|_____|
        //                          |___/\___/|_|   \__|                      
        //        

            Sort Sort::operator[] (std::function<bool(ZefRef, ZefRef)> user_defined_ordering_fct) const {
                return Sort{ user_defined_ordering_fct };
            }

            Sort Sort::operator[] (std::function<bool(EZefRef, EZefRef)> user_defined_ordering_fct) const {
                return Sort{ user_defined_ordering_fct };
            }

            Sort Sort::operator[] (std::function<int(ZefRef)> user_defined_ordering_fct) const {
                // piggy-back off the operator above and convert to the right sorting fct on the fly here				
                return operator[]( [user_defined_ordering_fct](ZefRef z1, ZefRef z2)->bool { return user_defined_ordering_fct(z1) < user_defined_ordering_fct(z2); } );   // pass fct in by copy
			}

			Sort Sort::operator[] (std::function<int(EZefRef)> user_defined_ordering_fct) const {
				// piggy-back off the operator above and convert to the right sorting fct on the fly here
				return operator[]( [user_defined_ordering_fct](EZefRef z1, EZefRef z2)->bool { return user_defined_ordering_fct(z1) < user_defined_ordering_fct(z2); } );   // pass fct in by copy
			}

			ZefRefs Sort::operator() (ZefRefs zrs) const {				
				if (!std::holds_alternative<std::function<bool(ZefRef, ZefRef)>>(ordering_fct))
					throw std::runtime_error("sort(ZefRefs) called, but the ordering function curried into the sort struct is not of type 'std::function<bool(ZefRef, ZefRef)>'");
				std::vector<ZefRef> tmp;
				tmp.reserve(zrs.len);
				for (auto el : zrs)
					tmp.emplace_back(el);
				ranges::sort(tmp, std::get<std::function<bool(ZefRef, ZefRef)>>(ordering_fct));
				return ZefRefs(tmp, false, zrs.reference_frame_tx);
			}
			
			EZefRefs Sort::operator() (EZefRefs uzrs) const {
				if (!std::holds_alternative<std::function<bool(EZefRef, EZefRef)>>(ordering_fct))
					throw std::runtime_error("sort(EZefRefs) called, but the ordering function curried into the sort struct is not of type 'std::function<bool(EZefRef, EZefRef)>'");
				ranges::sort(ranges::span(uzrs._get_array_begin(), uzrs.len), std::get<std::function<bool(EZefRef, EZefRef)>>(ordering_fct));
				return uzrs;
			}
			









		//                                           _                                             
		//                               _   _ _ __ (_) __ _ _   _  ___                            
		//    _____ _____ _____ _____   | | | | '_ \| |/ _` | | | |/ _ \   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  | |_| | | | | | (_| | |_| |  __/  |_____|_____|_____|_____|
		//                               \__,_|_| |_|_|\__, |\__,_|\___|                           
		//                                                |_|                               

			EZefRefs Unique::operator() (EZefRefs zrs) const {		
				auto res = sort[([](EZefRef z1, EZefRef z2) { return index(z1) < index(z2); })](zrs);
				auto my_span = ranges::span(res._get_array_begin(), res.len);
				auto it = std::unique(my_span.begin(), my_span.end());
				res.len = it - my_span.begin();
				if (res.delegate_ptr != 0)
					res.delegate_ptr->len = res.len;
				return res;
			}			

			ZefRefs Unique::operator() (ZefRefs zrs) const {				
				auto res = sort(zrs);
				auto my_span = ranges::span(res._get_array_begin(), res.len);
				auto it = std::unique(my_span.begin(), my_span.end());
				res.len = it - my_span.begin();
				if (res.delegate_ptr != 0)   // beware: sometimes the indirected object is passed on out to python (?). Also need to set length there
					res.delegate_ptr->len = res.len;				
				return res;
			}






		//                           _       _                          _                           
		//                          (_)_ __ | |_ ___ _ __ ___  ___  ___| |_                         
		//    _____ _____ _____     | | '_ \| __/ _ \ '__/ __|/ _ \/ __| __|      _____ _____ _____ 
		//   |_____|_____|_____|    | | | | | ||  __/ |  \__ \  __/ (__| |_      |_____|_____|_____|
		//                          |_|_| |_|\__\___|_|  |___/\___|\___|\__|                        
		//                                                                                    

			ZefRefs Intersect::operator() (const ZefRefs& zrs1, const ZefRefs& zrs2) const {
				if (length(zrs1) != 0 && length(zrs2) != 0 && zrs1.reference_frame_tx != zrs2.reference_frame_tx)
					throw std::runtime_error("'intersect' zefop called on two ZefRefs with different reference frames. This does not make sense.");

				auto zrs1_unique = unique(zrs1);
				auto zrs2_unique = unique(zrs2);
				int new_len = std::min(zrs1_unique.len, zrs2_unique.len);
				ZefRefs res(new_len, length(zrs1) > 0 ? zrs1.reference_frame_tx : zrs2.reference_frame_tx);
				auto my_span = ranges::span(res._get_array_begin(), new_len);
				auto it = std::set_intersection(
					zrs1_unique._get_array_begin_const(),
					zrs1_unique._get_array_begin_const() + zrs1.len,
					zrs2_unique._get_array_begin_const(),
					zrs2_unique._get_array_begin_const() + zrs2.len,
					my_span.begin(),
					[](EZefRef z1, EZefRef z2) {return index(z1) < index(z2); }
				);
				res.len = it - my_span.begin();
                if(res.delegate_ptr != nullptr)
                    res.delegate_ptr->len = res.len;
				return res;
			}

			EZefRefs Intersect::operator() (const EZefRefs& uzrs1, const EZefRefs& uzrs2) const {
				auto uzrs1_unique = unique(uzrs1);
				auto uzrs2_unique = unique(uzrs2);
				int new_len = std::min(uzrs1_unique.len, uzrs2_unique.len);
				auto res = EZefRefs(new_len);
				auto my_span = ranges::span(res._get_array_begin(), new_len);
				auto it = std::set_intersection(
					uzrs1_unique._get_array_begin_const(),
					uzrs1_unique._get_array_begin_const() + uzrs1.len,
					uzrs2_unique._get_array_begin_const(),
					uzrs2_unique._get_array_begin_const() + uzrs2.len,
					my_span.begin(),
					[](EZefRef z1, EZefRef z2) {return index(z1) < index(z2); }
				);
				res.len = it - my_span.begin();
                if(res.delegate_ptr != nullptr)
                    res.delegate_ptr->len = res.len;
				return res;
			}

        //////////////////////////////
        // * Concatenate

			ZefRefs Concatenate::operator() (const ZefRefs& zrs1, const ZefRefs& zrs2) const {
				if (length(zrs1) != 0 && length(zrs2) != 0 && zrs1.reference_frame_tx != zrs2.reference_frame_tx)
					throw std::runtime_error("'concatenate' zefop called on two ZefRefs with different reference frames. This does not make sense.");

                int new_len = length(zrs1) + length(zrs2);
				ZefRefs res(new_len, length(zrs1) > 0 ? zrs1.reference_frame_tx : zrs2.reference_frame_tx);

                EZefRef* ptr_to_write_to = res._get_array_begin();
                for (const auto & zr : zrs1)
                    *(ptr_to_write_to++) = zr.blob_uzr;
                for (const auto & zr : zrs2)
                    *(ptr_to_write_to++) = zr.blob_uzr;

				return res;
			}
			EZefRefs Concatenate::operator() (const EZefRefs& uzrs1, const EZefRefs& uzrs2) const {
                int new_len = length(uzrs1) + length(uzrs2);
				EZefRefs res(new_len);

                EZefRef* ptr_to_write_to = res._get_array_begin();
                for (const auto & uzr : uzrs1)
                    *(ptr_to_write_to++) = uzr;
                for (const auto & uzr : uzrs2)
                    *(ptr_to_write_to++) = uzr;

				return res;
			}

        //////////////////////////////
        // * SetUnion

			ZefRefs SetUnion::operator() (const ZefRefs& zrs1, const ZefRefs& zrs2) const {
                return concatenate(zrs1 | unique, zrs2 | unique) | unique;
			}
			EZefRefs SetUnion::operator() (const EZefRefs& uzrs1, const EZefRefs& uzrs2) const {
                return concatenate(uzrs1 | unique, uzrs2 | unique) | unique;
			}

        //////////////////////////////
        // * Without

			ZefRefs Without::operator() (const ZefRefs& original, const ZefRefs& to_remove) const {
                if ((original | tx) != (to_remove | tx))
                    throw std::runtime_error("without used between ZefRefs of different time slices");
                // This is inefficient but easy to implement
                // TODO: Faster version
                return original | filter[([&to_remove](ZefRef z) {return !contains2(to_remove, z);})];
			}
			EZefRefs Without::operator() (const EZefRefs& original, const EZefRefs& to_remove) const {
                // This is inefficient but easy to implement
                // TODO: Faster version
                return original | filter[([&to_remove](EZefRef z) {return !contains2(to_remove, z);})];
			}

			ZefRefs Without::operator() (const ZefRefs& original) const {
                return std::visit(overloaded{
                        [](Sentinel x)->ZefRefs {throw std::runtime_error("without used without curried in ZefRefs"); },
                        [&original,this](ZefRefs zrs)->ZefRefs {return Without::operator()(original, zrs);},
                        [](EZefRefs x)->ZefRefs {throw std::runtime_error("without used between ZefRefs and EZefRefs"); }
                    }, removing);
			}
			EZefRefs Without::operator() (const EZefRefs& original) const {
                return std::visit(overloaded{
                        [](Sentinel x)->EZefRefs {throw std::runtime_error("without used without curried in EZefRefs"); },
                        [&original,this](EZefRefs uzrs)->EZefRefs {return Without::operator()(original, uzrs);},
                        [](ZefRefs x)->EZefRefs {throw std::runtime_error("without used between EZefRefs and ZefRefs"); }
                    }, removing);
			}













		//                                __ _       _   _                                        
		//                               / _| | __ _| |_| |_ ___ _ __                             
		//    _____ _____ _____ _____   | |_| |/ _` | __| __/ _ \ '_ \    _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  |  _| | (_| | |_| ||  __/ | | |  |_____|_____|_____|_____|
		//                              |_| |_|\__,_|\__|\__\___|_| |_|                           
		//                                                                                       
 			// EZefRefs operator() (const EZefRefss& uzrss) {
    EZefRefs Flatten::operator() (EZefRefss& uzrss) const {
                // FIXME: better approach than vector creation.
 				size_t len_to_reserve = 0;

                for (const auto& uzrs : uzrss) {
                    len_to_reserve += length(uzrs);
                }
 
                std::vector<EZefRef> vec;
                vec.reserve(len_to_reserve);
                // FIXME: Can't use const here because of iterator spec
                for (auto& uzrs : uzrss) {
                    for(const auto& uzr : uzrs)
                        vec.push_back(uzr);
 				}

                EZefRefs res(vec);

 				return res;
 			}

 			// ZefRefs operator() (const ZefRefss& zrss) {
    ZefRefs Flatten::operator() (ZefRefss& zrss) const {
                // FIXME: better approach than vector creation.
 				size_t len_to_reserve = 0;
                EZefRef common_tx(nullptr);

                for (const auto& zrs : zrss) {
                    len_to_reserve += length(zrs);
                    if (zrs.reference_frame_tx.blob_ptr != nullptr) {
                        if(common_tx.blob_ptr == nullptr)
                            common_tx = zrs.reference_frame_tx;
                        else if(common_tx != zrs.reference_frame_tx)
                            throw std::runtime_error("Not all ZefRefs have the same tx. Can't flatten.");
                    }
                }
 
                ZefRefs res(len_to_reserve, common_tx);
                EZefRef* ptr_to_write_to = res._get_array_begin();

                for (auto& zrs : zrss) {
                    for(const auto& zr : zrs) {
                        *(ptr_to_write_to++) = zr.blob_uzr;
#ifdef ZEF_DEBUG
                        assert(zr.tx == common_tx);
#endif
                    }
                }

 				return res;
 			}






		//                         __ _          _                       
		//                        / _(_)_ __ ___| |_                     
		//    _____ _____ _____  | |_| | '__/ __| __|  _____ _____ _____ 
		//   |_____|_____|_____| |  _| | |  \__ \ |_  |_____|_____|_____|
		//                       |_| |_|_|  |___/\__|                    
		//                                                               






		//                        _           _                       
		//                       | | __ _ ___| |_                     
		//    _____ _____ _____  | |/ _` / __| __|  _____ _____ _____ 
		//   |_____|_____|_____| | | (_| \__ \ |_  |_____|_____|_____|
		//                       |_|\__,_|___/\__|                    
		//                                                        







		//                                     _                            
		//                          ___  _ __ | |_   _                      
		//    _____ _____ _____    / _ \| '_ \| | | | |   _____ _____ _____ 
		//   |_____|_____|_____|  | (_) | | | | | |_| |  |_____|_____|_____|
		//                         \___/|_| |_|_|\__, |                     
		//                                       |___/                      

	// TODO
	// distinguish between                  zrss | only     and   zrss | lift[only]

	//ZefRefs Only::operator() (const ZefRefss& zrss) const {
	//	throw std::runtime_error("not implemented only ZefRefss");

	//	internals::lift_list2_zrs_cpy(*this)(zrss);

	//	return zrss[0];
	//}



		// future implementation option: 
		// zz = [
//					[a, b, c, d],
//					[g, t, a]
//				]
//
//			zz | lift[first]    # results in[a, g]
//			zz | first          # results in[a, b, c, d]



		// ------------ factor out list monad lifting ---------------		
	inline auto lift_list_zrs(std::function<ZefRef(ZefRefs)> f) {
		return [f](const ZefRefss& zrss)->ZefRefs {    // don't pass the fct f by reference! It should be copied and be part of the lambda fct that is returned by value!			
			auto res = ZefRefs(zrss.len(), zrss.reference_frame_tx);
			std::transform(
				zrss.begin(),
				zrss.end(),
				res._get_array_begin(),
				[&](const ZefRefs& zrs) { return f(zrs).blob_uzr; }
			);
			return res;
		};
	};
	


		// ------------ factor out list monad lifting ---------------		
	inline auto lift_list_uzrs(std::function<EZefRef(EZefRefs)> f) {
		return [f](const EZefRefss& zrss)->EZefRefs {    // don't pass the fct f by reference! It should be copied and be part of the lambda fct that is returned by value!
			// GraphData& gd = zrss.len == 0 ? *(GraphData*)nullptr : graph_data(zrss[0]);
			auto res = EZefRefs(zrss.len());
			std::transform(
				zrss.begin(),
				zrss.end(),
				res._get_array_begin(),
				[&](const EZefRefs& zrs) { return f(zrs); }
			);			
			return res;
		};
	};



        EternalUID Uid::operator() (const EZefRef uzr) const {
            // Special handling for foreign RAEs. We want invertibility for
            // g[uid(z)] to be true, which means returning the foreign
            // EternalUID in that case.
            if(internals::is_foreign_rae_blob(BT(uzr))) {
                if(BT(uzr) == BT.FOREIGN_GRAPH_NODE)
                    return EternalUID(internals::get_blob_uid(uzr),
                                      internals::get_blob_uid(uzr));
                else
                    return EternalUID(internals::get_blob_uid(uzr),
                                      internals::get_blob_uid(uzr >> BT.ORIGIN_GRAPH_EDGE));
            }
            return EternalUID(internals::get_blob_uid(uzr),
                              internals::get_graph_uid(uzr));
        }
        ZefRefUID Uid::operator() (const ZefRef zr) const {
            // Note that it makes no sense to have a foreign blob as a
            // ZefRef. In fact it is downright confusing - does the graph
            // UID belong to the current graph or the foreign graph? So
            // reject this outright.
            if(internals::is_foreign_rae_blob(BT(zr)))
                throw std::runtime_error("Cannot get the ZefRefUID of a ZefRef pointing at a foreign RAE blob. You should convert to an EZefRef first.");
            return ZefRefUID(internals::get_blob_uid(zr | to_ezefref),
                             internals::get_blob_uid(zr | tx | to_ezefref),
                             internals::get_graph_uid(zr | to_ezefref));

        }






        //                       __     __         _                     _     _  __ _           _      ____  _                   _                           
		//                       \ \   / /_ _ _ __(_) ___  _   _ ___    | |   (_)/ _| |_ ___  __| |    / ___|| |_ _ __ _   _  ___| |_ ___                     
		//    _____ _____ _____   \ \ / / _` | '__| |/ _ \| | | / __|   | |   | | |_| __/ _ \/ _` |    \___ \| __| '__| | | |/ __| __/ __|  _____ _____ _____ 
		//   |_____|_____|_____|   \ V / (_| | |  | | (_) | |_| \__ \   | |___| |  _| ||  __/ (_| |     ___) | |_| |  | |_| | (__| |_\__ \ |_____|_____|_____|
		//                          \_/ \__,_|_|  |_|\___/ \__,_|___/   |_____|_|_|  \__\___|\__,_|    |____/ \__|_|   \__,_|\___|\__|___/                    
		//                                                                                                                                               

	// There is no use in overloading move sematics for the following
	ZefRefs LiftedOnly_::operator() (const ZefRefss& zrss) const { return lift_list_zrs(only)(zrss); }
	EZefRefs LiftedOnly_::operator() (const EZefRefss& uzrss) const { return lift_list_uzrs(only)(uzrss); }

		// There is no use in overloading move sematics for the following
	ZefRefs LiftedFirst_::operator() (const ZefRefss& zrss) const { return lift_list_zrs(first)(zrss); }
	EZefRefs LiftedFirst_::operator() (const EZefRefss& uzrss) const { return lift_list_uzrs(first)(uzrss); }

		// There is no use in overloading move sematics for the following
	ZefRefs LiftedLast_::operator() (const ZefRefss& zrss) const { return lift_list_zrs(last)(zrss); }
	EZefRefs LiftedLast_::operator() (const EZefRefss& uzrss) const { return lift_list_uzrs(last)(uzrss); }






		//                                   _     _                _                        
		//                          _____  _(_)___| |_ ___     __ _| |_                      
		//    _____ _____ _____    / _ \ \/ / / __| __/ __|   / _` | __|   _____ _____ _____ 
		//   |_____|_____|_____|  |  __/>  <| \__ \ |_\__ \  | (_| | |_   |_____|_____|_____|
		//                         \___/_/\_\_|___/\__|___/___\__,_|\__|                     
		//                                               |_____|                             
    ExistsAt ExistsAt::operator[] (EZefRef tx_node) const {
				// assert(get<BlobType>(tx_node) == BlobType::TX_EVENT_NODE);
				switch (get<BlobType>(tx_node)) {
				case BlobType::TX_EVENT_NODE: {
                    return ExistsAt{ get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice };
                }
                case BlobType::ROOT_NODE: {
                    return ExistsAt{ 0 };
                }
                default:
                    throw std::runtime_error("Tx passed in not a TX_EVENT_NODE");
                }
			}
    ExistsAt ExistsAt::operator[] (TimeSlice ts_number) const {
				assert(*ts_number > 0);
				return ExistsAt{ ts_number };
			}

			// this operator should only be used for EZefRefs that can be promoted to ZefRefs: AtomicEntity, Entity, Relation, TX_Event
    bool ExistsAt::operator() (EZefRef rel_ent) const {
        return imperative::exists_at(rel_ent, time_slice);
    }

    bool ExistsAt::operator() (ZefRef rel_ent) const {
        return (*this)(rel_ent | to_ezefref);
    }


			// TODO: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! return a vector of bool values (e.g. std::vector<bool> ) or a ZefRefs with vlaue nodes  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			//EZefRefs operator() (const EZefRefs& uzrs) const {
			//	auto res = EZefRefs(
			//		uzrs.len,
			//		[&uzrs]()->GraphData& {return uzrs.len == 0 ? *(GraphData*)nullptr : graph_data(uzrs[0]); }()
			//	);
			//	std::transform(
			//		uzrs._get_array_begin_const(),
			//		uzrs._get_array_begin_const() + uzrs.len,
			//		res._get_array_begin(),
			//		[this](EZefRef uzr) {return ExistsAt::operator()(uzr); }
			//	);
			//	return res;
			//}

			// N.b. for a ZefRef it only makes sense for it to refer to a EZefRef at a given graph-time point, iff it exists there
			// i.e. the time slice baked into a ZefRef does not add any value when asking whether the EZefRef exists at a given time




		//                         _                    __           __                     
		//                        | |_ ___     _______ / _|_ __ ___ / _|                    
		//    _____ _____ _____   | __/ _ \   |_  / _ \ |_| '__/ _ \ |_   _____ _____ _____ 
		//   |_____|_____|_____|  | || (_) |   / /  __/  _| | |  __/  _| |_____|_____|_____|
		//                         \__\___/___/___\___|_| |_|  \___|_|                      
		//                               |_____|                                                                
        ToZefRef ToZefRef::operator[] (EZefRef _reference_tx) const {
            if (BT(_reference_tx) != BT.TX_EVENT_NODE)  {
                std::cerr << "Bad tx: index: " << index(_reference_tx) << " : z = " << _reference_tx << std::endl;
                print_backtrace();
                throw std::runtime_error("the EZefRef curried into zefop 'to_zefref[...]' as a reference frame has to be a BT.TX_EVENT_NODE, but was not.");
            }
            return ToZefRef{ _reference_tx, _allow_terminated_relent_promotion };
        }

	ZefRef ToZefRef::operator() (EZefRef uzr_to_promote) const {
				if (reference_tx.blob_ptr == nullptr)
					throw std::runtime_error("to_zefref(uzr) called, but no reference frame was set.");

				#ifdef ZEF_DEBUG
				is_promotable_to_zefref(uzr_to_promote, reference_tx);
				if (!_allow_terminated_relent_promotion && !exists_at[reference_tx](uzr_to_promote))
					throw std::runtime_error("to_zefref called to promote a EZefRef that does not exist at the time slice specified.");
				#endif // ZEF_DEBUG
				return ZefRef{ uzr_to_promote, reference_tx };
			}

    ZefRef ToZefRef::operator() (ZefRef zr_to_promote) const {
				if (reference_tx.blob_ptr == nullptr)
					throw std::runtime_error("to_zefref(uzr) called, but no reference frame was set.");
				// make sure we catch the case of throwing an error if we want to promote a ZefRef to a different time slice where it does not exist
				if (!_allow_terminated_relent_promotion && !exists_at[reference_tx](zr_to_promote.blob_uzr))
					throw std::runtime_error("to_zefref called to promote a ZefRef that does not exist at the newly specified reference tx. ZR:" + to_str(zr_to_promote));
				return ToZefRef::operator()(zr_to_promote.blob_uzr);  
			}

    ZefRefs ToZefRef::operator() (const EZefRefs& uzrs_to_promote) const {
				#ifdef ZEF_DEBUG		
				if (reference_tx.blob_ptr == nullptr)
					throw std::runtime_error("to_zefref(uzr) called, but no reference frame was set.");				
				std::for_each(
					uzrs_to_promote._get_array_begin_const(),
					uzrs_to_promote._get_array_begin_const() + uzrs_to_promote.len,
					[this](EZefRef uzr) {  
						// wtf C++ language committee!!? This is one more reason why Rust will win in the long run: 'reference_tx' is a member variable of 
						// the struct and visible in this scope. Why can't a lambda function in this scope capture it? Why is it automatically 
						// in the scope of the lambda. Why can the lambda capture a copy (not by copy)? Who comes up with this shit?
						is_promotable_to_zefref(uzr, reference_tx);
						if (!_allow_terminated_relent_promotion && !exists_at[reference_tx](uzr))
							throw std::runtime_error("to_zefref called to promote a EZefRefs, at least one of which does not exist at the time slice specified. This is often an unintended user error and hence this message. You can promote a terminated relent to a ZefRef: use 'z | to_zefref[allow_promotion_of_termianted_relents][my_tx]' if you really want this.");
					}
				);
				#endif // ZEF_DEBUG
				auto res = ZefRefs(
					uzrs_to_promote.len,
                    reference_tx
				);
				// even in a ZefRefs struct, the various elements are stored as a contiguous list of EZefRefs. The reference tx is stored only once
				std::memcpy(
					res._get_array_begin(),
					uzrs_to_promote._get_array_begin_const(),
					uzrs_to_promote.len * sizeof(EZefRef)
				);

				return res;
			}
    ZefRefs ToZefRef::operator() (ZefRefs&& zrs) const { zrs.reference_frame_tx = reference_tx; return zrs; }
	ZefRefs ToZefRef::operator() (const ZefRefs& zrs) const { ZefRefs res = zrs; res.reference_frame_tx = reference_tx; return res; }


		//                  _                  __           __                                    _        _     _                         _              _     _                _                 
		//                 (_)___     _______ / _|_ __ ___ / _|   _ __  _ __ ___  _ __ ___   ___ | |_ __ _| |__ | | ___     __ _ _ __   __| |    _____  _(_)___| |_ ___     __ _| |_               
		//    _____ _____  | / __|   |_  / _ \ |_| '__/ _ \ |_   | '_ \| '__/ _ \| '_ ` _ \ / _ \| __/ _` | '_ \| |/ _ \   / _` | '_ \ / _` |   / _ \ \/ / / __| __/ __|   / _` | __|  _____ _____ 
		//   |_____|_____| | \__ \    / /  __/  _| | |  __/  _|  | |_) | | | (_) | | | | | | (_) | || (_| | |_) | |  __/  | (_| | | | | (_| |  |  __/>  <| \__ \ |_\__ \  | (_| | |_  |_____|_____|
		//                 |_|___/___/___\___|_| |_|  \___|_|____| .__/|_|  \___/|_| |_| |_|\___/ \__\__,_|_.__/|_|\___|___\__,_|_| |_|\__,_|___\___/_/\_\_|___/\__|___/___\__,_|\__|              
		//                      |_____|                    |_____|_|                                                  |_____|              |_____|                    |_____|                        
    IsZefRefPromotableAndExistsAt IsZefRefPromotableAndExistsAt::operator[] (EZefRef tx_node) const {
				assert(get<BlobType>(tx_node) == BlobType::TX_EVENT_NODE);
				return IsZefRefPromotableAndExistsAt{ get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice };
			}
    IsZefRefPromotableAndExistsAt IsZefRefPromotableAndExistsAt::operator[] (TimeSlice ts_number) const {
				assert(*ts_number > 0);
				return IsZefRefPromotableAndExistsAt{ ts_number };
			}

			// this operator should only be used for EZefRefs that can be promoted to ZefRefs: AtomicEntity, Entity, Relation, TX_Event
    bool IsZefRefPromotableAndExistsAt::operator() (EZefRef rel_ent) const {				
				switch (get<BlobType>(rel_ent)) {
				case BlobType::RELATION_EDGE: {
					blobs_ns::RELATION_EDGE& x = get<blobs_ns::RELATION_EDGE>(rel_ent);
					return time_slice >= x.instantiation_time_slice
						&& (x.termination_time_slice.value == 0 || time_slice < x.termination_time_slice);
				}
				case BlobType::ENTITY_NODE: {
					blobs_ns::ENTITY_NODE& x = get<blobs_ns::ENTITY_NODE>(rel_ent);
					return time_slice >= x.instantiation_time_slice
						&& (x.termination_time_slice.value == 0 || time_slice < x.termination_time_slice);
				}
				case BlobType::ATOMIC_ENTITY_NODE: {
					blobs_ns::ATOMIC_ENTITY_NODE& x = get<blobs_ns::ATOMIC_ENTITY_NODE>(rel_ent);
					return time_slice >= x.instantiation_time_slice
						&& (x.termination_time_slice.value == 0 || time_slice < x.termination_time_slice);
				}
				case BlobType::TX_EVENT_NODE: {
					return time_slice >= get<blobs_ns::TX_EVENT_NODE>(rel_ent).time_slice;
				}
				default: { return false; }				
				}
			}



		//                        _                            
		//                       | |___  __                    
		//    _____ _____ _____  | __\ \/ /  _____ _____ _____ 
		//   |_____|_____|_____| | |_ >  <  |_____|_____|_____|
		//                        \__/_/\_\                    .
		                                     
		// Given the time_slice we want, return the corresponding tx on the graph curried in.
		// Start with the latest tx and iteratively walk back the linked list of tx's until time_slice value is found.		
		EZefRef Tx::operator[] (TimeSlice ts) const {
			if (!linked_graph_data_maybe.has_value()) throw std::runtime_error("Tx(time_slice) called, but a GraphData reference is not curried into the 'tx' struct instance.");
			GraphData& gd = *linked_graph_data_maybe.value();			
			auto tx_candidate = EZefRef(gd.latest_complete_tx, gd);
			if (ts.value < 0 || ts > get<blobs_ns::TX_EVENT_NODE>(tx_candidate).time_slice) throw std::runtime_error("Tx requested for TimeSlice in Tx(time_slice) invalid: time_slice value out of range.");
			// iterative traverse list of tx's backwards (in tx time order)
			while (index(tx_candidate) > constants::ROOT_NODE_blob_index) {
				if (get<blobs_ns::TX_EVENT_NODE>(tx_candidate).time_slice == ts) return tx_candidate;
				tx_candidate = tx_candidate << BT.NEXT_TX_EDGE;
			}
			throw std::runtime_error("Matching tx not found on graph in Tx(time_slice) operator.");
			//return EZefRef(42, gd);  // in case a compiler complains
		}



		//                             _      _                  _                         
		//                          __| | ___| | ___  __ _  __ _| |_ ___                   
		//     _____ _____ _____   / _` |/ _ \ |/ _ \/ _` |/ _` | __/ _ \   _____ _____ _____ 
		//    |_____|_____|_____| | (_| |  __/ |  __/ (_| | (_| | ||  __/  |_____|_____|_____|
		//                         \__,_|\___|_|\___|\__, |\__,_|\__\___|                  
		//                                           |___/                            
        EZefRef DelegateOp::operator() (const EZefRef uzr) const {
            return imperative::delegate(uzr);
        }
        std::optional<EZefRef> DelegateOp::operator() (const Graph & g) const {
            GraphData& gd = g.my_graph_data();
            return std::visit(overloaded {
                    [&](const Sentinel & et) -> std::optional<EZefRef> {
                        throw std::runtime_error("Can't request delegates of a graph without being more specific");
                    },
                    [&](const auto & thing) -> std::optional<EZefRef> {
                        return imperative::delegate(g, thing);
                    }
                }, param);
        }

        DelegateOp DelegateOp::operator[](const EntityType& et) const {
            return DelegateOp{et};
        }
        DelegateOp DelegateOp::operator[](const AtomicEntityType& aet) const {
            return DelegateOp{aet};
        }
        // Delegate DelegateOp::operator[](const RelationType& rt) const {
        //     return Delegate{rt};
        // }
//             Delegate operator[](EntityType et_from, RelationType rt, EntityType et_to) {
// //                 std::string key = "Delegate.RT." + get_all_active_graph_data_tracker().relation_type_names.at(rel.relation_type_indx);
// // "Delegate.(Delegate.ET.ClientConnection>Delegate.RT.PrimaryInstance>Delegate.ET.ZefGraph)"
//                 std::string key = "not implemented!";
//                 return DelegateOp::Typed{key};
//             }

    EZefRefs DelegateOp::operator() (EZefRefs&& uzrs) const { return internals::lift_list_uzrs_mv(*this)(std::move(uzrs)); }
    EZefRefs DelegateOp::operator() (const EZefRefs& uzrs) const { return internals::lift_list_uzrs_cpy(*this)(uzrs); }

    ZefRef DelegateOp::operator() (ZefRef zr) const { return internals::lift_zr(*this)(zr); }
    ZefRefs DelegateOp::operator() (ZefRefs&& zrs) const { return internals::lift_list_zrs_mv(internals::lift_zr(*this))(std::move(zrs)); }
    ZefRefs DelegateOp::operator() (const ZefRefs& zrs) const { return internals::lift_list_zrs_cpy(internals::lift_zr(*this))(zrs); }





		//                        _           _              _   _       _   _                _                            
		//                       (_)_ __  ___| |_ __ _ _ __ | |_(_) __ _| |_(_) ___  _ __    | |___  __                    
		//    _____ _____ _____  | | '_ \/ __| __/ _` | '_ \| __| |/ _` | __| |/ _ \| '_ \   | __\ \/ /  _____ _____ _____ 
		//   |_____|_____|_____| | | | | \__ \ || (_| | | | | |_| | (_| | |_| | (_) | | | |  | |_ >  <  |_____|_____|_____|
		//                       |_|_| |_|___/\__\__,_|_| |_|\__|_|\__,_|\__|_|\___/|_| |_|___\__/_/\_\.                   
		//                                                                               |_____|                     
    EZefRef InstantiationTx::operator() (EZefRef uzr) const {
				return (uzr < BT.RAE_INSTANCE_EDGE) << L[BT.INSTANTIATION_EDGE] | only;  // also count the cloning operation of cloned relents as instantiation txs
				//return (uzr < BT.RAE_INSTANCE_EDGE) << BT.INSTANTIATION_EDGE;
			}

    EZefRefs InstantiationTx::operator() (EZefRefs&& uzrs) const { return internals::lift_list_uzrs_mv(*this)(std::move(uzrs)); }
    EZefRefs InstantiationTx::operator() (const EZefRefs& uzrs) const { return internals::lift_list_uzrs_cpy(*this)(uzrs); }

    ZefRef InstantiationTx::operator() (ZefRef zr) const { return internals::lift_zr(*this)(zr); }
    ZefRefs InstantiationTx::operator() (ZefRefs&& zrs) const { return internals::lift_list_zrs_mv(internals::lift_zr(*this))(std::move(zrs)); }
    ZefRefs InstantiationTx::operator() (const ZefRefs& zrs) const { return internals::lift_list_zrs_cpy(internals::lift_zr(*this))(zrs); }



		//                        _                      _             _   _                _                            
		//                       | |_ ___ _ __ _ __ ___ (_)_ __   __ _| |_(_) ___  _ __    | |___  __                    
		//    _____ _____ _____  | __/ _ \ '__| '_ ` _ \| | '_ \ / _` | __| |/ _ \| '_ \   | __\ \/ /  _____ _____ _____ 
		//   |_____|_____|_____| | ||  __/ |  | | | | | | | | | | (_| | |_| | (_) | | | |  | |_ >  <  |_____|_____|_____|
		//                        \__\___|_|  |_| |_| |_|_|_| |_|\__,_|\__|_|\___/|_| |_|___\__/_/\_\.                 
		//                                                                             |_____|       
    EZefRef TerminationTx::operator() (EZefRef uzr) {
				EZefRefs termination_edges = (uzr < BT.RAE_INSTANCE_EDGE) << L[BT.TERMINATION_EDGE];   // deal with the case that this has not been terminated: return the root node as sentinel
				return termination_edges.len == 0 ?
					EZefRef(constants::ROOT_NODE_blob_index, *graph_data(uzr)) :
					termination_edges[0];
			}

    EZefRefs TerminationTx::operator() (EZefRefs&& uzrs) { return internals::lift_list_uzrs_mv(*this)(std::move(uzrs)); }
    EZefRefs TerminationTx::operator() (const EZefRefs& uzrs) { return internals::lift_list_uzrs_cpy(*this)(uzrs); }

    ZefRef TerminationTx::operator() (ZefRef zr) { 
        EZefRef termination_tx_maybe = (*this)(zr | to_ezefref);
        if(index(termination_tx_maybe) == constants::ROOT_NODE_blob_index){
            return ZefRef(termination_tx_maybe, zr.tx);
        }
        // If the termination occurred after the reference frame, this seems like it hasn't happened
        if(time_slice(termination_tx_maybe) > time_slice(zr.tx)){
            return ZefRef(EZefRef(constants::ROOT_NODE_blob_index, *graph_data(termination_tx_maybe)), zr.tx);
        }
        return ZefRef(termination_tx_maybe, zr.tx);
    }

    ZefRefs TerminationTx::operator() (ZefRefs&& zrs) { return internals::lift_list_zrs_mv(internals::lift_zr(*this))(std::move(zrs)); }
    ZefRefs TerminationTx::operator() (const ZefRefs& zrs) { return internals::lift_list_zrs_cpy(internals::lift_zr(*this))(zrs); }







	//                                   _                             _                                  _      _                                
	//                       __   ____ _| |_   _  ___     __ _ ___ ___(_) __ _ _ __  _ __ ___   ___ _ __ | |_   | |___  _____                     
	//    _____ _____ _____  \ \ / / _` | | | | |/ _ \   / _` / __/ __| |/ _` | '_ \| '_ ` _ \ / _ \ '_ \| __|  | __\ \/ / __|  _____ _____ _____ 
	//   |_____|_____|_____|  \ V / (_| | | |_| |  __/  | (_| \__ \__ \ | (_| | | | | | | | | |  __/ | | | |_   | |_ >  <\__ \ |_____|_____|_____|
	//                         \_/ \__,_|_|\__,_|\___|___\__,_|___/___/_|\__, |_| |_|_| |_| |_|\___|_| |_|\__|___\__/_/\_\___/                    
	//                                              |_____|              |___/                              |_____|                               

	EZefRefs ValueAssignmentTxs::operator() (EZefRef uzrs) const { return (uzrs < BT.RAE_INSTANCE_EDGE) << L[BT.ATOMIC_VALUE_ASSIGNMENT_EDGE]; }
	
	ZefRefs ValueAssignmentTxs::operator() (ZefRef zrs) const { 
		ZefRef frame = zrs | tx; 
		auto frame_ts = frame | time_slice; 
		return  ((
			zrs 
			| to_ezefref) 
			< BT.RAE_INSTANCE_EDGE) 
			<< L[BT.ATOMIC_VALUE_ASSIGNMENT_EDGE] 
			| filter[([frame_ts](EZefRef z) { return (z | time_slice) <= frame_ts; })] 
			| to_zefref[frame]; 
	}




	//                        _   _                   _                       _                     
	//                       | |_(_)_ __ ___   ___   | |_ _ __ __ ___   _____| |                    
	//    _____ _____ _____  | __| | '_ ` _ \ / _ \  | __| '__/ _` \ \ / / _ \ |  _____ _____ _____ 
	//   |_____|_____|_____| | |_| | | | | | |  __/  | |_| | | (_| |\ V /  __/ | |_____|_____|_____|
	//                        \__|_|_| |_| |_|\___|___\__|_|  \__,_| \_/ \___|_|                    
	//                                           |_____|                                  

	EZefRef TimeTravel::traverse_time(EZefRef start_tx) const {
		auto frame = start_tx;
		if (std::holds_alternative<int>(param)) {
			int n = std::get<int>(param);
			if (n >= 0) {
				for (int c = 0; c < n; c++)
					frame = frame >> BT.NEXT_TX_EDGE;
			}
			else {
				for (int c = 0; c < -n; c++)
					frame = frame << BT.NEXT_TX_EDGE;
			}
			return frame;
		}
		else if (std::holds_alternative<Sentinel>(param)) {
			throw std::runtime_error("no parameter curried into 'time_travel' zefop at the time of use.");
		}
		throw std::runtime_error("We should not have landed here: in TimeTravel::operator()");
	}

	ZefRef TimeTravel::operator() (ZefRef z) const { return z | to_zefref[traverse_time(z | tx | to_ezefref)];	}
	ZefRefs TimeTravel::operator() (const ZefRefs& zrs) const { return zrs | to_zefref[traverse_time(zrs | tx | to_ezefref)]; }
	ZefRefss TimeTravel::operator()  (const ZefRefss& zrss) const {
		throw std::runtime_error("time_travel not implemented yet for EZefRefss");
		//const ZefRefss& zrs) const { return zrs | to_zefref[traverse_time(zrs | tx | to_ezefref)];     // TODO!!!!!!!!!!!!!!!!!!!
		return zrss;
	}





	//                                                                
	//                        _ __   _____      __                    
	//    _____ _____ _____  | '_ \ / _ \ \ /\ / /  _____ _____ _____ 
	//   |_____|_____|_____| | | | | (_) \ V  V /  |_____|_____|_____|
	//                       |_| |_|\___/ \_/\_/                      
	//                                                                

	Time Now::operator() () const {
		return Time{ std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count() * 1E-6 };
	}

    ZefRef Now::operator() (const Graph& g) const {
				GraphData& gd = g.my_graph_data();
				EZefRef tx = (gd.number_of_open_tx_sessions > 0 && gd.open_tx_thread == std::this_thread::get_id()) ?
					EZefRef(gd.index_of_open_tx_node, g) :
					EZefRef(gd.latest_complete_tx, g);
                return ZefRef(tx,tx);
			}

    ZefRef Now::operator() (EZefRef uzr) const {
				// tasks::apply_immediate_updates_from_zm();
				// EZefRef latest_tx = operator()(Graph(graph_data(uzr))) | to_ezefref;
        // EZefRef latest_tx = operator()(Graph(uzr)) | to_ezefref;
        Graph g(uzr);
        EZefRef latest_tx = operator()(g) | to_ezefref;
                if (!is_promotable_to_zefref(uzr, latest_tx)) throw std::runtime_error("Zefop 'now' called on EZefRef that cannot be promoted to a ZefRef.");

				if (!imperative::exists_at_now(uzr))
					throw std::runtime_error("'now(EZefRef)' called on a EZefRef that does not exist at the latest time slice. You can opt in to allow representing terminated RAEs by providing the flag 'z | now[allow_tombstone]' ");

				return ZefRef(uzr, latest_tx);
			}

    ZefRef Now::operator() (ZefRef zr) const {
				return zr | to_zefref[Graph(zr.tx) | now];  // keep the reference graph: only move to the newest slice in the reference frame!
			}
    //ZefRefs Now::operator() (ZefRefs&& zrs) {  TODO.... }
	ZefRefs Now::operator() (const ZefRefs& zrs) { 
		// tasks::apply_immediate_updates_from_zm();  
		ZefRefs res = zrs; 
		res.reference_frame_tx = Graph(res.reference_frame_tx) | now | to_ezefref; return res;	
	}
	} // zefOps namespace


    //////////////////////////////
    // * Instances

    // Template the implementation out but keep it as a separate function name
    // to manually specify what types are implemented (below in Instances::pure).
    template<class T>
    ZefRefs Instances_pure_helper(EZefRef tx, T type) {
        // This version ignores any local curried in data. It is the "pure function"
        if (!(tx <= BT.TX_EVENT_NODE)) {
            throw std::runtime_error("Instances(tx, type) should be called with a TX as the first argument.");
        }
        
        std::optional<EZefRef> del = Graph(tx) | delegate[type];
        
        if(!del)
            return ZefRefs(0, tx);
        return Instances::pure(tx, *del);
    }

    ZefRefs Instances::pure(EZefRef tx, EntityType et) {
        return Instances_pure_helper(tx, et);
    }
    ZefRefs Instances::pure(EZefRef tx, AtomicEntityType aet) {
        return Instances_pure_helper(tx, aet);
    }
    ZefRefs Instances::pure(EZefRef tx, Instances::Sentinel aet) {
        return Instances::pure(tx);
    }

    ZefRefs Instances::pure(EZefRef tx) {
        // This version ignores any local curried in data. It is the "pure function"

        if (!(tx <= BT.TX_EVENT_NODE)) {
            throw std::runtime_error("Instances(uzr) should be called with a TX as the first argument.");
        }

        return (internals::all_raes(Graph(tx))
                | filter[std::function<bool(EZefRef)>(exists_at[tx])]
                | to_zefref[tx]);
    }

    ZefRefs Instances::pure(EZefRef tx, EZefRef delegate) {
        if (!(tx <= BT.TX_EVENT_NODE)) {
            throw std::runtime_error("Instances(tx, type) should be called with a TX as the first argument.");
        }

        return ((delegate < BT.TO_DELEGATE_EDGE) >> L[BT.RAE_INSTANCE_EDGE]) | filter[std::function<bool(EZefRef)>(exists_at[tx])] | to_zefref[tx];
    }

    ZefRefs Instances::pure(ZefRef tx_or_delegate) {
        if (tx_or_delegate <= BT.TX_EVENT_NODE) {
            return pure(EZefRef(tx_or_delegate));
        } else {
            if (!internals::has_delegate(BT(tx_or_delegate))) throw std::runtime_error("Instances(zr) called for a blob type where no delegate exists.");
            auto tx1 = EZefRef(tx_or_delegate | tx);
            return pure(tx1, EZefRef(tx_or_delegate));
        }
    }

    // The functions below provide checks on the curried info but otherwise just call into pure(...)
    
    // pipe a delegate zr through: use the reference frame from the delegate   my_delegate_zr | instances
    ZefRefs Instances::operator() (ZefRef zr) const {
        if (zr <= BT.TX_EVENT_NODE) {
            return (*this)(EZefRef(zr));
        } else {
            if (!internals::has_delegate(BT(zr))) throw std::runtime_error("Instances(zr) called for a blob type where no delegate exists.");
            if (!std::holds_alternative<Sentinel>(ref_frame_data)) throw std::runtime_error("An additional reference frame was curried into 'my_zr | instances[some_ref_frame_info]'. Please be more precise which reference frame to use.");
            auto tx1 = zr | tx;
            return pure(tx1, EZefRef(zr));
        }
    }

            // pipe a delegate through, but determine the reference frame from the instance zefop
    ZefRefs Instances::operator() (EZefRef uzr) const {
        if (uzr <= BT.TX_EVENT_NODE) {
            if (!std::holds_alternative<Sentinel>(ref_frame_data)) throw std::runtime_error("An additional reference frame was curried into 'instances'. Please be more precise which reference frame to use.");
            return std::visit([&uzr](auto curried_in_type) { return pure(uzr, curried_in_type); },
                              curried_in_type);
        } else {
            if (!internals::has_delegate(BT(uzr))) throw std::runtime_error("Instances(zr) called for a blob type where no delegate exists.");

            EZefRef ref_frame_tx = std::visit(overloaded{
                    [](Sentinel x)->EZefRef {throw std::runtime_error("a reference frame has to be specified and curried into the 'instance' zefop when piping through a delegate EZefRef."); },
                    [](EZefRef tx)->EZefRef { return tx; },
                    [](ZefRef tx)->EZefRef { return tx | to_ezefref; },
                    [](Now now_op)->EZefRef { throw std::runtime_error("in 'my_uzr_delegate | instance[now]'  it is not sufficient to specify the time slice via 'now', since the reference graph is not clear. Specify a reference tx in this case in the instance zefop."); } 
                }, ref_frame_data);
            return pure(ref_frame_tx, uzr);
        }
    }

    ZefRefs Instances::operator() (const Graph& g) const {
        if (std::holds_alternative<Sentinel>(ref_frame_data)) throw std::runtime_error("g | Instances was called, but no reference frame was set in the latter op.");

        EZefRef ref_frame_tx = std::visit(overloaded{
                [](Sentinel x)->EZefRef {throw std::runtime_error("g | Instances was called, but no reference frame was set in the latter op."); },
                [](EZefRef tx)->EZefRef { return tx; },
                [](ZefRef tx)->EZefRef { return tx | to_ezefref; },
                [&g](Now now_op)->EZefRef { return g | now_op | to_ezefref; }    //get the very latest tx (even the open one if a Transaction is open) of the current graph g
            }, ref_frame_data);

        return std::visit([&ref_frame_tx](auto curried_in_type) { return pure(ref_frame_tx, curried_in_type); },
                          curried_in_type);
    }
		









	//                                 __  __           _           _                       
	//                           __ _ / _|/ _| ___  ___| |_ ___  __| |                      
	//    _____ _____ _____     / _` | |_| |_ / _ \/ __| __/ _ \/ _` |    _____ _____ _____ 
	//   |_____|_____|_____|   | (_| |  _|  _|  __/ (__| ||  __/ (_| |   |_____|_____|_____|
	//                          \__,_|_| |_|  \___|\___|\__\___|\__,_|                      
	//           
	ZefRefs Affected::operator() (EZefRef my_tx) const {
		if (BT(my_tx) != BT.TX_EVENT_NODE)
			throw std::runtime_error("The EZefRef passed to the zefop 'affected' has to be a transaction, but was not: " + to_str(my_tx));
		return my_tx
			| outs
			| filter[([](EZefRef z)->bool {
				return find_element<BlobType>({ BT.INSTANTIATION_EDGE, BT.ATOMIC_VALUE_ASSIGNMENT_EDGE, BT.TERMINATION_EDGE }, BT(z));
				})]
			| target
			| target
			| unique
			| to_zefref[allow_terminated_relent_promotion][my_tx];
	}



	//                          _           _              _   _       _           _                       
	//                         (_)_ __  ___| |_ __ _ _ __ | |_(_) __ _| |_ ___  __| |                      
	//    _____ _____ _____    | | '_ \/ __| __/ _` | '_ \| __| |/ _` | __/ _ \/ _` |    _____ _____ _____ 
	//   |_____|_____|_____|   | | | | \__ \ || (_| | | | | |_| | (_| | ||  __/ (_| |   |_____|_____|_____|
	//                         |_|_| |_|___/\__\__,_|_| |_|\__|_|\__,_|\__\___|\__,_|                      
	//                                                                                                    
	ZefRefs Instantiated::operator() (EZefRef my_tx) const {
		if (BT(my_tx) != BT.TX_EVENT_NODE)
			throw std::runtime_error("The EZefRef passed to the zefop 'instantiated' has to be a transaction, but was not: " + to_str(my_tx));
		return my_tx
			| outs
			| filter[std::function<bool(EZefRef)>([](EZefRef z)->bool {
				return BT(z) == BT.INSTANTIATION_EDGE;
				})]
			| target
			| target
			| to_zefref[allow_terminated_relent_promotion][my_tx];
	}



	//                          _                      _             _           _                       
	//                         | |_ ___ _ __ _ __ ___ (_)_ __   __ _| |_ ___  __| |                      
	//    _____ _____ _____    | __/ _ \ '__| '_ ` _ \| | '_ \ / _` | __/ _ \/ _` |    _____ _____ _____ 
	//   |_____|_____|_____|   | ||  __/ |  | | | | | | | | | | (_| | ||  __/ (_| |   |_____|_____|_____|
	//                          \__\___|_|  |_| |_| |_|_|_| |_|\__,_|\__\___|\__,_|                      
	//                                                                                          
	ZefRefs Terminated::operator() (EZefRef my_tx) const {
		if (BT(my_tx) != BT.TX_EVENT_NODE)
			throw std::runtime_error("The EZefRef passed to the zefop 'terminated' has to be a transaction, but was not: " + to_str(my_tx));
		return my_tx
			| outs
			| filter[std::function<bool(EZefRef)>([](EZefRef z)->bool {
				return BT(z) == BT.TERMINATION_EDGE;
				})]
			| target
			| target
			| to_zefref[allow_terminated_relent_promotion][my_tx];
	}


	//                                     _                             _                      _                       
	//                         __   ____ _| |_   _  ___     __ _ ___ ___(_) __ _ _ __   ___  __| |                      
	//    _____ _____ _____    \ \ / / _` | | | | |/ _ \   / _` / __/ __| |/ _` | '_ \ / _ \/ _` |    _____ _____ _____ 
	//   |_____|_____|_____|    \ V / (_| | | |_| |  __/  | (_| \__ \__ \ | (_| | | | |  __/ (_| |   |_____|_____|_____|
	//                           \_/ \__,_|_|\__,_|\___|___\__,_|___/___/_|\__, |_| |_|\___|\__,_|                      
	//                                                |_____|              |___/                                        
	ZefRefs ValueAssigned::operator() (EZefRef my_tx) const {
		if (BT(my_tx) != BT.TX_EVENT_NODE)
			throw std::runtime_error("The EZefRef passed to the zefop 'value_assigned' has to be a transaction, but was not: " + to_str(my_tx));
		return my_tx
			| outs
			| filter[std::function<bool(EZefRef)>([](EZefRef z)->bool {
				return BT(z) == BT.ATOMIC_VALUE_ASSIGNMENT_EDGE;
				})]
			| target
			| target
			| to_zefref[allow_terminated_relent_promotion][my_tx];
	}






	//                                     _                   _ _                                 
	//                           ___ _   _| |__  ___  ___ _ __(_) |__   ___                        
	//    _____ _____ _____     / __| | | | '_ \/ __|/ __| '__| | '_ \ / _ \     _____ _____ _____ 
	//   |_____|_____|_____|    \__ \ |_| | |_) \__ \ (__| |  | | |_) |  __/    |_____|_____|_____|
	//                          |___/\__,_|_.__/|___/\___|_|  |_|_.__/ \___|                       
	//                                                                                          

	Subscription Subscribe::operator() (ZefRef z) const {
		// Function to register the subsciption to observable z in ref graph of z.
		// The owning Subscribe object should contain all the info required: relation_direction, callback fct and, in future, execution context
		// A ZefRef is passed in: the graph to which the callback/subscription is atatched is taken from the ZR's reference frame

		using namespace zefOps;
		if (std::holds_alternative<Sentinel>(relation_direction))
			throw std::runtime_error("subscribe called, but no subscription type was set in 'subscribe' zefop." + to_str(z));

		if (std::holds_alternative<OnValueAssignment>(relation_direction) && BT(z)!=BT.ATOMIC_ENTITY_NODE)
			throw std::runtime_error("subscribe called for 'OnValueAssignment', but the ZefRef is not an AE:" + to_str(z));

		if(!bool(callback_fct))
			throw std::runtime_error("no callback functionw as set in 'subscribe' zefop");

	// ---------------- checks passed --------------------

		auto get_latest_or_create_and_get = [](TagString tag_name, EntityType et, Graph& gg)->ZefRef {
			if (!gg.contains(tag_name)) {
				auto res = instantiate(et, gg);
				tag(res, tag_name.s);
				return res;
			}
			return gg[tag_name] | now;
		};


		auto g_to_register_callback = Graph(z);
		auto& obs = g_to_register_callback.my_graph_data().observables;
		if (!bool(obs))
			// obs.emplace(); // init if this is empty (allows fast check when tx closes)
            obs = std::make_unique<ZefObservables>(); // init if this is empty (allows fast check when tx closes)
		
		auto& g_observables = *(*obs).g_observables;  // deref twice: for optional<ZefObservables> and unique_ptr
		auto& callbacks_and_refcount = (*obs).callbacks_and_refcount;
		{			
			auto keep_open_tx = Transaction(g_observables);
			if (std::holds_alternative<OnValueAssignment>(relation_direction)) {				
				auto z_uid = uid(z|to_ezefref);
				// any observed relent is merged into g_observables. 
				// Although the clone has a different uid, the ancestor's uid is also contained in g_observables for the clone edge.
				if (!g_observables.contains(z_uid)) { // if z was never tracked in any context before					
					// merge(EZefRefs({ EZefRef(z) }), (g_to_register_callback | now), g_observables);
                    {
                        Transaction{g_observables};
                        internals::merge_atomic_entity_(g_observables, AET(z), z_uid.blob_uid, z_uid.graph_uid);
                    }
				}
				ZefRef relent_proxy = internals::local_entity(g_observables[z_uid]) | now;
				auto val_assignment_list = relent_proxy >> L[RT.OnValueAssignment];
				// if there is no outgoing ZEF_OnValueAssignment edge, create one
				if (length(val_assignment_list) == 0) {
					instantiate(
						relent_proxy,
						RT.OnValueAssignment,
						instantiate(ET.List, g_observables),
						g_observables
					);
				}
				BaseUID subscription_uid = make_random_uid();
				ZefRef z_subs = instantiate(
					relent_proxy >> RT.OnValueAssignment,
					RT.ListElement,
					instantiate(ET.Subscription, g_observables, subscription_uid),   // specify the uid to be shared with the dict
					g_observables
				);

				// the uid on the graph is the dict key, so we can match up in both directions
				callbacks_and_refcount[subscription_uid] = ZefObservables::DictElement{
					*callback_fct,		// if we're here, we know this is set
					1, // The base Subscription ctor doesn't register a ref count, so we must do it here ourselves.
                    _keep_alive.value
				};

				return Subscription { obs, subscription_uid };
			}
			// ###################################################  observe structural changes ##############################################################
			else {
				bool is_out_relation = (std::holds_alternative<OnInstantiation>(relation_direction) && std::get<OnInstantiation>(relation_direction).is_outgoing) ||
					(std::holds_alternative<OnTermination>(relation_direction) && std::get<OnTermination>(relation_direction).is_outgoing);
				bool is_instantiation = std::holds_alternative<OnInstantiation>(relation_direction);
				RelationType relation_type = std::visit(overloaded{
					[](OnInstantiation op)->RelationType {if (!bool(op.rt)) throw std::runtime_error("rel not set in OnInstantiation used in 'subscribe'"); return *op.rt; },
					[](OnTermination op)->RelationType {if (!bool(op.rt)) throw std::runtime_error("rel not set in OnTermination used in 'subscribe'"); return *op.rt; },

					[](auto x)->RelationType { return RT._unspecified; } // never reached, bute required to compile
					}, relation_direction);

				TagString callback_list_key{"CallbackList." + to_str(internals::make_hash(EZefRef(z), relation_type, is_out_relation, is_instantiation))};

				ZefRef callback_list_ent = g_observables.contains(callback_list_key) ? (g_observables[callback_list_key] | now) : [&]()->ZefRef {   // get the callback node if it already exists and is found by the unique has. else create it
					TagString ins_or_term_tagname{is_instantiation ? "MonitoredRelInstantiations" : "MonitoredRelTerminations"};
					auto proxy = get_latest_or_create_and_get(ins_or_term_tagname, ET.Proxy, g_observables);


					auto get_or_create_monitored_rel_list_entity = [&g_observables, &proxy, &ins_or_term_tagname](RelationType rt, bool is_out_rel)->ZefRef {
						auto candidates = is_out_rel ? proxy >> L[rt] : proxy << L[rt];
						if (length(candidates) == 1)
							return candidates | first;
						if (is_out_rel) {
							ZefRef zz = instantiate(ET.MonitoredRelationList, g_observables);
							tag(zz, ins_or_term_tagname.s+"."+str(rt)+".Out");
							instantiate(
								proxy,
								rt,
								zz,
								g_observables
							);
							return zz;
						}
						else {
							ZefRef zz = instantiate(ET.MonitoredRelationList, g_observables);
							tag(zz, ins_or_term_tagname.s + "." + str(rt) + ".In");
							instantiate(
								zz,
								rt,
								proxy,
								g_observables
							);
							return zz;
						}
					};

					ZefRef monitored_rel_list_entity = std::visit(overloaded{
						[&g_observables, &get_or_create_monitored_rel_list_entity](OnInstantiation op)->ZefRef { return get_or_create_monitored_rel_list_entity(*op.rt, op.is_outgoing); },
						[&g_observables, &get_or_create_monitored_rel_list_entity](OnTermination op)->ZefRef { return get_or_create_monitored_rel_list_entity(*op.rt, op.is_outgoing); },
						[&g_observables](auto x)->ZefRef { return g_observables[42] | now; } // never reached
						}, relation_direction);

					// we do not need to look for the 'the callback_list_ent', this does definitely not exist (we would have found it from the composite_hash in the dict otherwise)
					ZefRef new_callback_list = instantiate(ET.CallbackList, g_observables);
					tag(new_callback_list, callback_list_key);   // in addition to the uid, tag this witht he special composite hash
					instantiate(monitored_rel_list_entity, RT.ListElement, new_callback_list, g_observables);
					return new_callback_list;
				}();


				BaseUID subscription_uid = make_random_uid();
				ZefRef z_subs = instantiate(
					callback_list_ent,
					RT.ListElement,
					instantiate(ET.Subscription, g_observables, subscription_uid),   // specify the uid to be shared with the dict
					g_observables
				);
				// the uid on the graph is the dict key, so we can match up in both directions
				callbacks_and_refcount[subscription_uid] = ZefObservables::DictElement{
					*callback_fct,		// if we're here, we know this is set
					1, // The base Subscription ctor doesn't register a ref count, so we must do it here ourselves.
                    _keep_alive.value
				};
				return Subscription( obs, subscription_uid );
			}		
		}		
	}



	Subscription Subscribe::operator() (EZefRef z) const { return operator()(z | zefOps::now); }  // TODO: rather check that no time slice info is required from z above and have the long implementation be in here



	Subscription Subscribe::operator() (Graph z) const {
		using namespace zefOps;
		if (!std::holds_alternative<Sentinel>(relation_direction))
			throw std::runtime_error("'g ^ subscribe[subscr_kind]' called, but 'subscr_kind' can't be set at this stage when subscribing to an entire graph. \
				The callback will be triggered on each closing transaction. In future, this syntax for subscribing is likely to change to \
				'my_aet ^ on_value_assignment ^ subscribe[my_callback]' to give more control over separating the event firing predicate function from what follows." + to_str(z));

		if (!bool(callback_fct))
			throw std::runtime_error("no callback functionw as set in 'subscribe' zefop called to subscribe on graph");

		auto g_to_register_callback = Graph(z);
		auto& obs = g_to_register_callback.my_graph_data().observables;
		if (!bool(obs))
			// obs.emplace(); // init if this is empty (allows fast check when tx closes)
            obs = std::make_shared<ZefObservables>(); // init if this is empty (allows fast check when tx closes)

		auto& g_observables = *(*obs).g_observables;  // deref twice: for optional<ZefObservables> and unique_ptr
		auto& callbacks_and_refcount = (*obs).callbacks_and_refcount;
		{
			auto keep_open_tx = Transaction(g_observables);
			if (!g_observables.contains("GraphSubscriptions"))
				tag(instantiate(ET.GraphSubscriptions, g_observables), "GraphSubscriptions");

			BaseUID subscription_uid = make_random_uid();
			instantiate(
                        g_observables[TagString("GraphSubscriptions")] | now,
                        RT.ListElement,
                        instantiate(ET.Subscription, g_observables, subscription_uid),
                        g_observables
			);
			// the uid on the graph is the dict key, so we can match up in both directions
			callbacks_and_refcount[subscription_uid] = ZefObservables::DictElement{
				*callback_fct,		// if we're here, we know this is set
                1, // The base Subscription ctor doesn't register a ref count, so we must do it here ourselves.
                _keep_alive.value
            };
			return Subscription(obs, subscription_uid);
		}
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




	// namespace internals {		


	// 	str zeroth_order_name_from_nth_order_delegate(EZefRef nth_order_delegate, int n) {
	// 		// this relies on first_order_delegate being a first order delegate. This fct does not check!
	// 		switch (get<BlobType>(nth_order_delegate)) {
	// 		case  BlobType::ENTITY_NODE: return std::string("ET.") + str(nth_order_delegate | ET);
	// 		case  BlobType::ATOMIC_ENTITY_NODE: return std::string("AET.") + str(nth_order_delegate | AET);
	// 		//case  BlobType::ATOMIC_VALUE_NODE: return std::string("AVT.") + to_str(first_order_delegate | AVT);
	// 		case  BlobType::RELATION_EDGE: return "(" + type_name_(nth_order_delegate | source, -n) + ">RT." + str(nth_order_delegate | RT) + ">" + type_name_(nth_order_delegate | target, -n) + ")";
	// 		default: {
	// 			print("Reached invalid case in zeroth_order_name_from_first_order_delegate.  first_order_delegate passed in as EZefRef:\n ");
	// 			print(nth_order_delegate);
	// 			throw std::runtime_error("\nInvalid case reached in zeroth_order_name_from_first_order_delegate that should not be reached.");
	// 			}
	// 		}
	// 	}


	// 	std::tuple<int, EZefRef> delegate_order_and_first_order_delegate_node(EZefRef z, int order_shift) {
	// 		// for any geven relent, calculates the delegate order m and finds the first order delegate.
	// 		// the name can subsequently be extracted from the first order delegate only and the 'Delegate.' is prepended m times
	// 		// can be used on instances or any delegate.
	// 		// special treatment for relations where there is an intermediate delegate group
	// 		EZefRef first_in_edge = z | ins | first;
	// 		if (BT(first_in_edge) == BT.RAE_INSTANCE_EDGE) 
	// 			return std::make_tuple(0, first_in_edge | source | target);		// the case where this is a pure instance: order 0 and walk to delegate
			
	// 		// once we're here, we know that we are dealing with a delegate and we can just traverse backwards to the first order delegate and keep track of the steps
	// 		if (BT(first_in_edge) != BT.TO_DELEGATE_EDGE) {
    //             print_backtrace();
    //             std::cerr << first_in_edge << std::endl;
	// 			throw std::runtime_error("unexpected traversal pattern in delegate_order_and_first_order_delegate_node: extepected a BT.TO_DELEGATE_EDGE");
    //         }
			
	// 		EZefRef potential_root = BT(z) != BT.RELATION_EDGE ?
	// 			first_in_edge | source :
	// 			(first_in_edge | source) << BT.TO_DELEGATE_EDGE;   // for relations: walk one further and jump of the delegate group
			
	// 		if (is_root(potential_root)) 
	// 			return std::make_tuple(1 + order_shift, z);
	// 		else
	// 			return delegate_order_and_first_order_delegate_node(first_in_edge | source, order_shift + 1);			
	// 	}



	// } // internals


    // NOTE!!!!
    // NOTE!!!!
    // NOTE!!!!
    // NOTE!!!!
    //
    // This was commented out because the type_name relies on ordering of edges
    // in ins/outs of the low level blobs, but the newer delegate things can
    // break that.

 	// str type_name_(EZefRef uzr, int delegate_order_shift) {
	// 	// type_name_(uzr, 0) is just the normal typename, which includes exactly the correct number of 'Delegate.' strings prefixed for uzr.
	// 	// The parameter 'delegate_order_shift' allows to artifically increase (delegate_order_shift>0) or descrese this number by passing an argument
	// 	using namespace internals;		
	// 	// catch the case of a delegate group case immediately		
	// 	if (is_delegate_group(uzr)) 
	// 		return str("DelegateGroup.RT.") + str(uzr | RT);

	// 	// if this a pure instance, don't traverse to the first order delegate. This function may be used in a 
	// 	// call stack where this path is not signed up yet. In that case, just infer the name directly and tell the function that it is a zeroth order delegate.
	// 	if (BT(uzr | ins | first) == BT.RAE_INSTANCE_EDGE) 
	// 		return zeroth_order_name_from_nth_order_delegate(uzr, 0);  // in this branch we know that uzr is of delegate order 0.
		
	// 	auto [order, first_order_delegate] = delegate_order_and_first_order_delegate_node(uzr, 0);		
	// 	return (order + delegate_order_shift) * str("Delegate.") + zeroth_order_name_from_nth_order_delegate(first_order_delegate, 1);
 	// }
	
	// std::string type_name(EZefRef uzr) { return type_name_(uzr, 0); }
	// std::string type_name(ZefRef zr) { return type_name_(zr.blob_uzr, 0); }











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

    namespace imperative {
        //////////////////////////////
        // * retire

		void retire(EZefRef uzr) {
            // This is only for the delegates and is deliberately separate from
            // termination of instances in order to make this harder to
            // accidentally do.


            // We are also going to allow retiring delegates that no longer have
            // any alive instances, or higher-order instances. This means any
            // relation connected to the delegate (e.g. metadata) must be
            // terminated before this is called.

            if(!is_delegate(uzr))
                throw std::runtime_error("Can only retire delegates not" + to_str(uzr) + ".");

            Graph g(uzr);
            // All of these checks must happen while we have write role so that we don't check against mutating data.
            auto & gd = g.my_graph_data();
            LockGraphData lock{&gd};

            if(!exists_at_now(uzr))
                throw std::runtime_error("Delegate is already retired");
            
            for(auto & parent : uzr >> L[BT.TO_DELEGATE_EDGE]) {
                if(exists_at_now(parent))
                    throw std::runtime_error("Can't retire a delegate when it has a parent higher-order delegate.");
            }

            for(auto & rel : uzr | ins_and_outs) {
                if(BT(rel) == BT.RELATION_EDGE && exists_at_now(rel))
                    throw std::runtime_error("Can't retire a delegate when it is the source/target of another instance/delegate.");
            }

            for(auto & instance : (uzr < BT.TO_DELEGATE_EDGE) >> L[BT.RAE_INSTANCE_EDGE]) {
                if(exists_at_now(instance))
                    throw std::runtime_error("Can't retire a delegate when it has existing instances.");
            }

            // Finally we can retire this delegate!
            Transaction transaction{gd};
            EZefRef tx_node = internals::get_or_create_and_get_tx(gd);
            auto retire_uzr = internals::instantiate(tx_node, BT.DELEGATE_RETIREMENT_EDGE, uzr < BT.TO_DELEGATE_EDGE, g);
        }
        //////////////////////////////
        // * exists_at

		bool exists_at(EZefRef uzr, TimeSlice ts) {
            if(is_delegate(uzr)) {
                // Although the node itself might have some information, we
                // should check the sequence of DELEGATE_INSTANTIATION_EDGE and
                // DELEGATE_RETIREMENT_EDGE to determine if we are in a valid
                // timeslice for the delegate.

                EZefRef to_del = uzr < BT.TO_DELEGATE_EDGE;

                TimeSlice dummy(-1);
                TimeSlice latest_instantiation_edge = dummy;
                TimeSlice latest_retirement_edge = dummy;

                for(auto & it : to_del << L[BT.DELEGATE_INSTANTIATION_EDGE]) {
                    // `it` should always be a TX.
                    TimeSlice this_ts = time_slice(it);
                    if(this_ts <= ts && this_ts > latest_instantiation_edge)
                        latest_instantiation_edge = this_ts;
                }

                for(auto & it : to_del << L[BT.DELEGATE_RETIREMENT_EDGE]) {
                    // `it` should always be a TX.
                    TimeSlice this_ts = time_slice(it);
                    if(this_ts <= ts && this_ts > latest_retirement_edge)
                        latest_retirement_edge = this_ts;
                }

                if(latest_instantiation_edge == dummy)
                    return false;
                if(latest_retirement_edge == dummy)
                    return true;
                return (latest_instantiation_edge > latest_retirement_edge);
            }

            switch (get<BlobType>(uzr)) {
            case BlobType::RELATION_EDGE: {
                blobs_ns::RELATION_EDGE& x = get<blobs_ns::RELATION_EDGE>(uzr);
                return ts >= x.instantiation_time_slice
                    && (x.termination_time_slice.value == 0 || ts < x.termination_time_slice);
            }
            case BlobType::ENTITY_NODE: {
                blobs_ns::ENTITY_NODE& x = get<blobs_ns::ENTITY_NODE>(uzr);
                return ts >= x.instantiation_time_slice
                    && (x.termination_time_slice.value == 0 || ts < x.termination_time_slice);
            }
            case BlobType::ATOMIC_ENTITY_NODE: {
                blobs_ns::ATOMIC_ENTITY_NODE& x = get<blobs_ns::ATOMIC_ENTITY_NODE>(uzr);
                return ts >= x.instantiation_time_slice
                    && (x.termination_time_slice.value == 0 || ts < x.termination_time_slice);
            }
            case BlobType::TX_EVENT_NODE: {
                return ts >= get<blobs_ns::TX_EVENT_NODE>(uzr).time_slice;
            }
            case BlobType::ROOT_NODE: {
                return true;
            }
            default: {throw std::runtime_error("ExistsAt() called on a EZefRef that cannot be promoted to a ZefRef and where asking this question makes no sense."); return false; }
            }
        };
        bool exists_at(EZefRef uzr, EZefRef tx) {
            if (get<BlobType>(tx) != BlobType::TX_EVENT_NODE)
                throw std::runtime_error("ExistsAt() called with a tx that is not a TX_EVENT_NODE.");
            return exists_at(uzr, get<blobs_ns::TX_EVENT_NODE>(tx).time_slice);
        }
        bool exists_at(EZefRef uzr, ZefRef tx) {
            return exists_at(uzr, tx.blob_uzr);
        }
		bool exists_at(ZefRef zr, TimeSlice ts) {
            return exists_at(zr.blob_uzr, ts);
        }
		bool exists_at(ZefRef zr, EZefRef tx) {
            return exists_at(zr.blob_uzr, tx);
        }
		bool exists_at(ZefRef zr, ZefRef tx) {
            return exists_at(zr.blob_uzr, tx);
        }

        //////////////////////////////
        // * exists_at_now

        // This is an optimised version that uses only the latest edge in the
        // RAE_INSTANCE_EDGE or TO_DELEGATE_EDGE in order to determine whether
        // the RAE/delegate is alive.
        //
        // This relies on the instantiations/terminations being sorted chronologically.
		bool exists_at_now(EZefRef uzr) {
            if(is_delegate(uzr)) {
                EZefRef to_del = internals::get_TO_DELEGATE_EDGE(uzr);
                EZefRef last_edge(internals::last_set_edge_index(to_del), *graph_data(uzr));

                if (get<BlobType>(last_edge) == BlobType::DELEGATE_RETIREMENT_EDGE) return false;
                assert(get<BlobType>(last_edge) == BlobType::DELEGATE_INSTANTIATION_EDGE
                       || get<BlobType>(last_edge) == BlobType::RAE_INSTANCE_EDGE);
                return true;
            }

            if(get<BlobType>(uzr) == BlobType::ROOT_NODE)
                return true;

            if(get<BlobType>(uzr) == BlobType::TX_EVENT_NODE)
                return true;

            // For RAEs
            internals::assert_is_this_a_rae(uzr);
			EZefRef this_re_ent_inst = internals::get_RAE_INSTANCE_EDGE(uzr);
			blob_index last_index = internals::last_set_edge_index(this_re_ent_inst);
			EZefRef last_in_edge_on_scenario_node(last_index, *graph_data(uzr));

			if (get<BlobType>(last_in_edge_on_scenario_node) == BlobType::TERMINATION_EDGE) return false;
            assert(get<BlobType>(last_in_edge_on_scenario_node) == BlobType::INSTANTIATION_EDGE
                   || get<BlobType>(last_in_edge_on_scenario_node) == BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE
                   || get<BlobType>(last_in_edge_on_scenario_node) == BlobType::ORIGIN_RAE_EDGE);
            return true;
        }


        //////////////////////////////
        // * to_frame

        ZefRef to_frame(EZefRef uzr, EZefRef tx, bool allow_terminated) {
            if (!allow_terminated && !exists_at(uzr, tx))
                throw std::runtime_error("to_frame called to promote a EZefRef that does not exist at the time slice specified.");
            return ZefRef{ uzr, tx };
        }
        ZefRef to_frame(ZefRef zr, EZefRef tx, bool allow_terminated) {
            return to_frame(zr.blob_uzr, tx, allow_terminated);
        }
        ZefRef to_frame(EZefRef uzr, ZefRef tx, bool allow_terminated) {
            return to_frame(uzr, tx.blob_uzr, allow_terminated);
        }
        ZefRef to_frame(ZefRef zr, ZefRef tx, bool allow_terminated) {
            return to_frame(zr.blob_uzr, tx.blob_uzr, allow_terminated);
        }

        ZefRefs to_frame(EZefRefs uzrs, EZefRef tx, bool allow_terminated) {
            if(!allow_terminated) {
                for(const auto & uzr : uzrs) {
                    if(!exists_at(uzr, tx))
                        throw std::runtime_error("to_frame called to promote a EZefRef that does not exist at the time slice specified.");
                }
            }
            auto res = ZefRefs(uzrs.len, tx);
            // even in a ZefRefs struct, the various elements are stored as a
            // contiguous list of EZefRefs. The reference tx is stored only once
            // TODO: THIS SEEMS WRONG IF DEFFERRED LISTS ARE USED
            // TODO: THIS SEEMS WRONG IF DEFFERRED LISTS ARE USED
            // TODO: THIS SEEMS WRONG IF DEFFERRED LISTS ARE USED
            // TODO: THIS SEEMS WRONG IF DEFFERRED LISTS ARE USED
            // TODO: THIS SEEMS WRONG IF DEFFERRED LISTS ARE USED
            std::memcpy(
                    res._get_array_begin(),
                    uzrs._get_array_begin_const(),
                    uzrs.len * sizeof(EZefRef)
            );
            return res;
        }
        ZefRefs to_frame(ZefRefs zrs, EZefRef tx, bool allow_terminated) {
            return to_frame(to_ezefref(zrs), tx, allow_terminated);
        }
        ZefRefs to_frame(EZefRefs uzrs, ZefRef tx, bool allow_terminated) {
            return to_frame(uzrs, tx.blob_uzr, allow_terminated);
        }
        ZefRefs to_frame(ZefRefs zrs, ZefRef tx, bool allow_terminated) {
            return to_frame(to_ezefref(zrs), tx.blob_uzr, allow_terminated);
        }

        //////////////////////////////
        // * target


        EZefRef target(EZefRef uzr) {
            if(!internals::has_source_target_node(uzr))
                throw std::runtime_error(" 'target(my_uzef_ref)' called for a uzr where a target node is not defined.");
            blob_index target_index = internals::target_node_index(uzr);
            return EZefRef(target_index, *graph_data(uzr));
        }
        ZefRef target(ZefRef zr) {
			return to_frame(target(EZefRef(zr)), zr.tx, true);
        }

        EZefRefs target(const EZefRefs& uzrs) {
            auto res = EZefRefs(
                    uzrs.len,
                    graph_data(uzrs)
            );
            std::transform(
                    uzrs._get_array_begin_const(),
                    uzrs._get_array_begin_const() + uzrs.len,
                    res._get_array_begin(),
                    [](EZefRef uzr) { return target(uzr); }
            );
            return res;
        }

        ZefRefs target(const ZefRefs& zrs) {
            auto res = ZefRefs(
                    zrs.len,
                    zrs.reference_frame_tx
            );
            std::transform(
                    zrs._get_array_begin_const(),
                    zrs._get_array_begin_const() + zrs.len,
                    res._get_array_begin(),
                    [](EZefRef uzr) { return target(uzr); }
            );
            return res;
        }

        //////////////////////////////
        // * source


        EZefRef source(EZefRef uzr) {
            if(!internals::has_source_target_node(uzr))
                throw std::runtime_error(" 'source(my_uzef_ref)' called for a uzr where a source node is not defined.");
            blob_index src_index = internals::source_node_index(uzr);
            return EZefRef(src_index, *graph_data(uzr));
        }
        ZefRef source(ZefRef zr) {
            return to_frame(source(EZefRef(zr)), zr.tx, true);
        }

        EZefRefs source(const EZefRefs& uzrs) {
            auto res = EZefRefs(
                    uzrs.len,
                    graph_data(uzrs)
            );
            std::transform(
                    uzrs._get_array_begin_const(),
                    uzrs._get_array_begin_const() + uzrs.len,
                    res._get_array_begin(),
                    [](EZefRef uzr) { return source(uzr); }
            );
            return res;
        }

        ZefRefs source(const ZefRefs& zrs) {
            auto res = ZefRefs(
                    zrs.len,
                    zrs.reference_frame_tx
            );
            std::transform(
                    zrs._get_array_begin_const(),
                    zrs._get_array_begin_const() + zrs.len,
                    res._get_array_begin(),
                    [](EZefRef uzr) { return source(uzr); }
            );
            return res;
        }


        //////////////////////////////
        // * Traversal

        std::optional<EZefRef> make_optional(EZefRefs zrs) {
            if (length(zrs) == 0)
                return std::optional<EZefRef>{};
            else if (length(zrs) == 1)
                return std::optional<EZefRef>(zrs | only);
            else
                throw std::runtime_error("More than one item found for O_Class");
        }
        std::optional<ZefRef> make_optional(ZefRefs zrs) {
            if (length(zrs) == 0)
                return std::optional<ZefRef>{};
            else if (length(zrs) == 1)
                return std::optional<ZefRef>(zrs | only);
            else
                throw std::runtime_error("More than one item found for O_Class");
        }
        EZefRef traverse_out_edge(EZefRef node, BlobType bt_or_rt) {
            try {
                return only(filter(outs(node), bt_or_rt));
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_out_node(EZefRef node, BlobType bt_or_rt) {
            try {
                return only(target(filter(outs(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_in_edge(EZefRef node, BlobType bt_or_rt) {
            try {
                return only(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_in_node(EZefRef node, BlobType bt_or_rt) {
            try {
                return only(source(filter(ins(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_out_edge_multi(EZefRef node, BlobType bt_or_rt) {
            try {
                return filter(outs(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_out_node_multi(EZefRef node, BlobType bt_or_rt) {
            try {
                return target(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_in_edge_multi(EZefRef node, BlobType bt_or_rt) {
            try {
                return filter(ins(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_in_node_multi(EZefRef node, BlobType bt_or_rt) {
            try {
                return source(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_out_edge_optional(EZefRef node, BlobType bt_or_rt) {
            try {
                return make_optional(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_out_node_optional(EZefRef node, BlobType bt_or_rt) {
            try {
                return make_optional(target(filter(outs(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_in_edge_optional(EZefRef node, BlobType bt_or_rt) {
            try {
                return make_optional(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_in_node_optional(EZefRef node, BlobType bt_or_rt) {
            try {
                return make_optional(source(filter(ins(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }

        EZefRef traverse_out_edge(EZefRef node, RelationType bt_or_rt) {
            try {
                return only(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_out_node(EZefRef node, RelationType bt_or_rt) {
            try {
                return only(target(filter(outs(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_in_edge(EZefRef node, RelationType bt_or_rt) {
            try {
                return only(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRef traverse_in_node(EZefRef node, RelationType bt_or_rt) {
            try {
                return only(source(filter(ins(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_out_edge_multi(EZefRef node, RelationType bt_or_rt) {
            try {
                return filter(outs(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_out_node_multi(EZefRef node, RelationType bt_or_rt) {
            try {
                return target(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_in_edge_multi(EZefRef node, RelationType bt_or_rt) {
            try {
                return filter(ins(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        EZefRefs traverse_in_node_multi(EZefRef node, RelationType bt_or_rt) {
            try {
                return source(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_out_edge_optional(EZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_out_node_optional(EZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(target(filter(outs(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_in_edge_optional(EZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<EZefRef> traverse_in_node_optional(EZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(source(filter(ins(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }

        ZefRef traverse_out_edge(ZefRef node, RelationType rt) {
            ZefRefs this_outs = filter(outs(node), rt); 
            if(length(this_outs) == 0) {
                int this_time_slice = time_slice(tx(node));
                EZefRefs all_outs = filter(outs(to_ezefref(node)), rt);
                if(length(all_outs) > 0) {
                    throw std::runtime_error("There are no " + to_str(rt) + " found in the outgoing relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + " however there were outgoing " + to_str(rt) + " found in other time slices.\nHint: you may have wanted to change the reference frame via `z | now` or `z | in_frame[...]`.");
                }
                ZefRefs all_ins = filter(ins(node), rt);
                if(length(all_ins) > 0) {
                    throw std::runtime_error("There was no " + to_str(rt) + " found in the outgoing relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + " however there were incoming " + to_str(rt) + " found.\nHint: you may have wanted to use < or << instead.");
                }
                throw std::runtime_error("There was no " + to_str(rt) + " found in the outgoing relations on node " + to_str(node) + ".");
            } else if(length(this_outs) >= 2) {
                int this_time_slice = time_slice(tx(node));
                throw std::runtime_error("There are more than one " + to_str(rt) + " found in the outgoing relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + ".\nHint: you may have wanted to use > L[" + to_str(rt) + "] or >> L[" + to_str(rt) + "] instead.");
            }

            try {
                return only(this_outs);
            } catch (const std::runtime_error& error) {
                throw std::runtime_error("Unexpected error while traversing outwards along " + to_str(rt) + " of " + to_str(node) + ":\n" + error.what()); 
            }
        }
        ZefRef traverse_out_node(ZefRef node, RelationType rt) {
            try {
                return target(traverse_out_edge(node, rt));
            } catch (const std::runtime_error& error) {
                throw std::runtime_error("Unexpected error while traversing outwards along " + to_str(rt) + " of " + to_str(node) + ":\n" + error.what()); 
            }
        }
        ZefRef traverse_in_edge(ZefRef node, RelationType rt) {
            ZefRefs this_ins = filter(ins(node), rt); 
            if(length(this_ins) == 0) {
                int this_time_slice = time_slice(tx(node));
                EZefRefs all_ins = filter(ins(to_ezefref(node)), rt);
                if(length(all_ins) > 0) {
                    throw std::runtime_error("There are no " + to_str(rt) + " found in the incoming relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + " however there were incoming " + to_str(rt) + " found in other time slices.\nHint: you may have wanted to change the reference frame via `z | now` or `z | in_frame[...]`.");
                }
                ZefRefs all_outs = filter(outs(node), rt);
                if(length(all_outs) > 0) {
                    throw std::runtime_error("There was no " + to_str(rt) + " found in the incoming relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + " however there were outgoing " + to_str(rt) + " found.\nHint: you may have wanted to use > or >> instead.");
                }
                throw std::runtime_error("There was no " + to_str(rt) + " found in the incoming relations on node " + to_str(node) + ".");
            } else if(length(this_ins) >= 2) {
                int this_time_slice = time_slice(tx(node));
                throw std::runtime_error("There are more than one " + to_str(rt) + " found in the incoming relations on node " + to_str(node) + " in time slice " + to_str(this_time_slice) + ".\nHint: you may have wanted to use < L[" + to_str(rt) + "] or << L[" + to_str(rt) + "] instead.");
            }

            try {
                return only(this_ins);
            } catch (const std::runtime_error& error) {
                throw std::runtime_error("Unexpected error while traversing inwards along " + to_str(rt) + " of " + to_str(node) + ":\n" + error.what()); 
            }
        }
        ZefRef traverse_in_node(ZefRef node, RelationType rt) {
            try {
                return source(traverse_in_edge(node, rt));
            } catch (const std::runtime_error& error) {
                throw std::runtime_error("Unexpected error while traversing inwards along " + to_str(rt) + " of " + to_str(node) + ":\n" + error.what()); 
            }
        }
        ZefRefs traverse_out_edge_multi(ZefRef node, RelationType bt_or_rt) {
            try {
                return filter(outs(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        ZefRefs traverse_out_node_multi(ZefRef node, RelationType bt_or_rt) {
            try {
                return target(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        ZefRefs traverse_in_edge_multi(ZefRef node, RelationType bt_or_rt) {
            try {
                return filter(ins(node), bt_or_rt); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        ZefRefs traverse_in_node_multi(ZefRef node, RelationType bt_or_rt) {
            try {
                return source(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_multi " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<ZefRef> traverse_out_edge_optional(ZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(filter(outs(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<ZefRef> traverse_out_node_optional(ZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(target(filter(outs(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_out_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<ZefRef> traverse_in_edge_optional(ZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(filter(ins(node), bt_or_rt)); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_edge_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }
        std::optional<ZefRef> traverse_in_node_optional(ZefRef node, RelationType bt_or_rt) {
            try {
                return make_optional(source(filter(ins(node), bt_or_rt))); 
            }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("Unable to traverse_in_node_optional " + to_str(node) + " along " + to_str(bt_or_rt)); 
            }
        }

        EZefRefs traverse_out_edge(const EZefRefs& nodes, BlobType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_out_edge(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_out_edge_multi(const EZefRefs& uzrs, BlobType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_out_edge_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_out_node(const EZefRefs& nodes, BlobType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_out_node(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_out_node_multi(const EZefRefs& uzrs, BlobType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_out_node_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_in_edge(const EZefRefs& nodes, BlobType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_in_edge(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_in_edge_multi(const EZefRefs& uzrs, BlobType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_in_edge_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_in_node(const EZefRefs& nodes, BlobType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_in_node(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_in_node_multi(const EZefRefs& uzrs, BlobType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_in_node_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_out_edge(const EZefRefs& nodes, RelationType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_out_edge(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_out_edge_multi(const EZefRefs& uzrs, RelationType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_out_edge_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_out_node(const EZefRefs& nodes, RelationType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_out_node(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_out_node_multi(const EZefRefs& uzrs, RelationType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_out_node_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_in_edge(const EZefRefs& nodes, RelationType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_in_edge(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_in_edge_multi(const EZefRefs& uzrs, RelationType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_in_edge_multi(el, bt_or_rt));
            return res; 
        }

        EZefRefs traverse_in_node(const EZefRefs& nodes, RelationType bt_or_rt) {
            EZefRefs res(nodes);
            for (auto& el : res)
                el = traverse_in_node(el, bt_or_rt);
            return res; 
        }
        EZefRefss traverse_in_node_multi(const EZefRefs& uzrs, RelationType bt_or_rt) {
            EZefRefss res(length(uzrs));
            for (auto el : uzrs)
                res.v.emplace_back(traverse_in_node_multi(el, bt_or_rt));
            return res; 
        }

        ZefRefs traverse_out_edge(const ZefRefs& zrs, RelationType rt) {
            auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
            std::transform( zrs._get_array_begin_const(), zrs._get_array_begin_const() + zrs.len, res._get_array_begin(),
                            [&](const EZefRef& uzr) {
                                return traverse_out_edge(ZefRef{uzr, zrs.reference_frame_tx} , rt).blob_uzr; 
                            });
            return res; 
        }
        ZefRefss traverse_out_edge_multi(const ZefRefs& zrs, RelationType rt) {
            ZefRefss res(length(zrs));
            for (auto el : zrs)
                res.v.emplace_back(traverse_out_edge_multi(el, rt));
            res.reference_frame_tx = zrs.reference_frame_tx;
            return res; 
        }

        ZefRefs traverse_out_node(const ZefRefs& zrs, RelationType rt) {
            auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
            std::transform( zrs._get_array_begin_const(), zrs._get_array_begin_const() + zrs.len, res._get_array_begin(),
                            [&](const EZefRef& uzr) {
                                return traverse_out_node(ZefRef{uzr, zrs.reference_frame_tx} , rt).blob_uzr; 
                            });
            return res; 
        }
        ZefRefss traverse_out_node_multi(const ZefRefs& zrs, RelationType rt) {
            ZefRefss res(length(zrs));
            for (auto el : zrs)
                res.v.emplace_back(traverse_out_node_multi(el, rt));
            res.reference_frame_tx = zrs.reference_frame_tx;
            return res; 
        }

        ZefRefs traverse_in_edge(const ZefRefs& zrs, RelationType rt) {
            auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
            std::transform( zrs._get_array_begin_const(), zrs._get_array_begin_const() + zrs.len, res._get_array_begin(),
                            [&](const EZefRef& uzr) {
                                return traverse_in_edge(ZefRef{uzr, zrs.reference_frame_tx} , rt).blob_uzr; 
                            });
            return res; 
        }
        ZefRefss traverse_in_edge_multi(const ZefRefs& zrs, RelationType rt) {
            ZefRefss res(length(zrs));
            for (auto el : zrs)
                res.v.emplace_back(traverse_in_edge_multi(el, rt));
            res.reference_frame_tx = zrs.reference_frame_tx;
            return res; 
        }

        ZefRefs traverse_in_node(const ZefRefs& zrs, RelationType rt) {
            auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
            std::transform( zrs._get_array_begin_const(), zrs._get_array_begin_const() + zrs.len, res._get_array_begin(),
                            [&](const EZefRef& uzr) {
                                return traverse_in_node(ZefRef{uzr, zrs.reference_frame_tx} , rt).blob_uzr; 
                            });
            return res; 
        }
        ZefRefss traverse_in_node_multi(const ZefRefs& zrs, RelationType rt) {
            ZefRefss res(length(zrs));
            for (auto el : zrs)
                res.v.emplace_back(traverse_in_node_multi(el, rt));
            res.reference_frame_tx = zrs.reference_frame_tx;
            return res; 
        }




        // -------------------------------------------------------------------------------
        //////////////////////////////
        // * filter

        EZefRefs filter(const EZefRefs& input, const std::function<bool(EZefRef)>& pred) {
            // std::cerr << "Filtering using input length of: " << input.len << std::endl;
            EZefRefs res(
                length(input),
                // graph_data_ptr(input)
                graph_data(input)
            );
            const EZefRef* read_it = input._get_array_begin_const();
            auto write_it = res._get_array_begin();
            int count = 0;
            for (auto it = read_it; it != read_it + input.len; ++it) {
                if (pred(*it)) {    // this is the same function as xor (predicate, not_in_activated_flag)
                    *(write_it++) = *it;
                    count++;
                }
            }
            res.len = count;				
            if (res.delegate_ptr != nullptr)
                res.delegate_ptr->len = count;
            return res;
        }
        ZefRefs filter(const ZefRefs& input, const std::function<bool(ZefRef)>& pred) {
            auto res = ZefRefs(
                    input.len,
                    input.reference_frame_tx
            );

            const EZefRef* read_it = input._get_array_begin_const();
            EZefRef* write_it = res._get_array_begin();
            int count = 0;
            for (auto it = read_it; it != read_it + input.len; ++it) {
                if (pred(ZefRef(*it, input.reference_frame_tx))) {
                    *(write_it++) = *it;
                    count++;
                }
            }
            res.len = count;
            if (res.delegate_ptr != nullptr)
                res.delegate_ptr->len = count;
            return res;
        }


        ZefRefs filter(const ZefRefs& zrs, EntityType et) {
            return filter(zrs, [&et](ZefRef zr) {
                return (BT(zr) == BT.ENTITY_NODE &&
                        (ET(zr) == et || ET(zr) == ET.ZEF_Any));
            });
        }
        ZefRefs filter(const ZefRefs& zrs, BlobType bt) {
            return filter(zrs, [&bt](ZefRef zr) {
                return BT(zr) == bt;
            });
        }
        ZefRefs filter(const ZefRefs& zrs, RelationType rt) {
            return filter(zrs, [&rt](ZefRef zr) {
                return (BT(zr) == BT.RELATION_EDGE &&
                        (RT(zr) == rt || RT(zr) == RT.ZEF_Any));
            });
        }
        ZefRefs filter(const ZefRefs& zrs, AtomicEntityType aet) {
            return filter(zrs, [&aet](ZefRef zr) {
                return (BT(zr) == BT.ATOMIC_ENTITY_NODE && AET(zr) == aet);
            });
        }

        EZefRefs filter(const EZefRefs& uzrs, EntityType et) {
            return filter(uzrs, [&et](EZefRef uzr) {
                return (BT(uzr) == BT.ENTITY_NODE &&
                        (ET(uzr) == et || ET(uzr) == ET.ZEF_Any));
            });
        }
        EZefRefs filter(const EZefRefs& uzrs, BlobType bt) {
            return filter(uzrs, [&bt](EZefRef uzr) {
                return BT(uzr) == bt;
            });
        }
        EZefRefs filter(const EZefRefs& uzrs, RelationType rt) {
            return filter(uzrs, [&rt](EZefRef uzr) {
                return (BT(uzr) == BT.RELATION_EDGE &&
                        (RT(uzr) == rt || RT(uzr) == RT.ZEF_Any));
            });
        }
        EZefRefs filter(const EZefRefs& uzrs, AtomicEntityType aet) {
            return filter(uzrs, [&aet](EZefRef uzr) {
                return (BT(uzr) == BT.ATOMIC_ENTITY_NODE && AET(uzr) == aet);
            });
        }

        //////////////////////////////
        // * terminate

    
        // terminate any entity or relation
        void terminate(EZefRef my_rel_ent) {
            GraphData& gd = *graph_data(my_rel_ent);
            if (!gd.is_primary_instance)
                throw std::runtime_error("'terminate' called for a graph which is not a primary instance. This is not allowed. Shame on you!");
            if (is_delegate(my_rel_ent))
                throw std::runtime_error("Terminate called on a delegate. This is not allowed. At most, delegates may be tagged as 'disabled' in the future.");

            // tasks::apply_immediate_updates_from_zm();
            // TODO: check whether locally owned!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            using namespace internals;
            assert_is_this_a_rae(my_rel_ent);
            if (is_terminated(my_rel_ent))
                throw std::runtime_error("Terminate called on already terminated entity or relation.");

            auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);		
            EZefRef tx_node = get_or_create_and_get_tx(gd);

            // execute zefhook before the rel_ent is terminated
            ZefRef my_rel_ent_now = to_frame(my_rel_ent, tx_node);

            EZefRef RAE_INSTANCE_EDGE = get_RAE_INSTANCE_EDGE(my_rel_ent);
            blobs_ns::TERMINATION_EDGE& my_termination_edge = get_next_free_writable_blob<blobs_ns::TERMINATION_EDGE>(gd);
            MMap::ensure_or_alloc_range(&my_termination_edge, blobs_ns::max_basic_blob_size);
            my_termination_edge.this_BlobType = BlobType::TERMINATION_EDGE;
            switch (get<BlobType>(my_rel_ent)) {
            case BlobType::ENTITY_NODE: {get<blobs_ns::ENTITY_NODE>(my_rel_ent).termination_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice; break; }
            case BlobType::ATOMIC_ENTITY_NODE: {get<blobs_ns::ATOMIC_ENTITY_NODE>(my_rel_ent).termination_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice; break; }
            case BlobType::RELATION_EDGE: {get<blobs_ns::RELATION_EDGE>(my_rel_ent).termination_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx_node).time_slice; break; }
            default: {throw std::runtime_error("termiate called on a EZefRef pointing to a blob type where the concept of termination makes no sense."); }
            }
            move_head_forward(gd);
            my_termination_edge.source_node_index = index(tx_node);
            my_termination_edge.target_node_index = index(RAE_INSTANCE_EDGE);
            blob_index this_termination_edge_index = index(EZefRef((void*)&my_termination_edge));
            append_edge_index(tx_node, this_termination_edge_index);
            append_edge_index(RAE_INSTANCE_EDGE, -this_termination_edge_index);
            //terminate all edges that have not been terminated yet
            for (EZefRef ed : my_rel_ent | ins_and_outs)
                // Note: this can't be done in a filter until filters are lazily evaluated.
                if(ed | is_zefref_promotable_and_exists_at[tx_node])
                    terminate(ed);
        }
        void terminate(ZefRef my_rel_ent) {
            terminate(EZefRef(my_rel_ent));
        }

        void terminate(EZefRefs uzrs) {
            // This will check before terminating each EZefRef whether it currently
            // exists. e.g. a problem can occur if terminating an entity also
            // terminates a relation that is in the list.
            GraphData * gd_ptr = graph_data(uzrs);
            if(!gd_ptr)
                return;
            GraphData& gd = *gd_ptr;
            auto my_tx_to_keep_this_open_in_this_fct = Transaction(gd);		
            EZefRef tx_node = internals::get_or_create_and_get_tx(gd);

            for (EZefRef uzr : uzrs)
                // Note: this can't be done in a filter until filters are lazily evaluated.
                if(uzr | is_zefref_promotable_and_exists_at[tx_node])
                    terminate(uzr);
        }
        void terminate(ZefRefs zrs) {
            terminate(zrs | to_ezefref);
        }

        //////////////////////////////
        // * delegate

        EZefRef delegate(EZefRef uzr) {
            // return the delegate RelEnt for a given instance
            return target(traverse_in_node(uzr, BT.RAE_INSTANCE_EDGE));
        }
        EZefRef delegate(ZefRef zr) {
            return delegate(to_ezefref(zr));
        }
        std::optional<EZefRef> delegate(const Graph & g, EntityType et) {
            GraphData& gd = g.my_graph_data();
            EZefRef root_ezr{constants::ROOT_NODE_blob_index, gd};
            auto temp = filter(traverse_out_node_multi(root_ezr, BT.TO_DELEGATE_EDGE), et);
            if(length(temp) == 0)
                return {};
            return temp | only;
        }

        std::optional<EZefRef> delegate(const Graph & g, AtomicEntityType aet) {
            GraphData& gd = g.my_graph_data();
            EZefRef root_ezr{constants::ROOT_NODE_blob_index, gd};
            auto temp = filter(traverse_out_node_multi(root_ezr, BT.TO_DELEGATE_EDGE), aet);
            if(length(temp) == 0)
                return {};
            return temp | only;
        }

        std::optional<EZefRef> delegate_to_ezr(const DelegateEntity & d, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};
            EZefRef z = root;
            for(int i = 0 ; i < order ; i++) {
                EZefRefs opts = filter(traverse_out_node_multi(z, BT.TO_DELEGATE_EDGE), d.et);
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(BT.ENTITY_NODE, gd);
                        get<blobs_ns::ENTITY_NODE>(new_z).entity_type = d.et;
                        get<blobs_ns::ENTITY_NODE>(new_z).instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx).time_slice;
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                        z = new_z;
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const DelegateAtomicEntity & d, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};
            EZefRef z = root;
            for(int i = 0 ; i < order ; i++) {
                EZefRefs opts = filter(traverse_out_node_multi(z, BT.TO_DELEGATE_EDGE), d.aet);
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(BT.ATOMIC_ENTITY_NODE, gd);
                        get<blobs_ns::ATOMIC_ENTITY_NODE>(new_z).my_atomic_entity_type = d.aet;
                        get<blobs_ns::ATOMIC_ENTITY_NODE>(new_z).instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx).time_slice;
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                        z = new_z;
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
            }

            return z;
        }

        bool is_delegate_relation_group(EZefRef z) {
            return (z <= RT) && (source(z) == z) && (target(z) == z);
        }

        std::optional<EZefRef> delegate_to_ezr(const DelegateRelationGroup & d, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};
            EZefRef z = root;
            for(int i = 0 ; i < order ; i++) {
                EZefRefs opts = filter(traverse_out_node_multi(z, BT.TO_DELEGATE_EDGE), d.rt);
                opts = filter(opts, is_delegate_relation_group);
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(BT.RELATION_EDGE, gd);
                        get<blobs_ns::RELATION_EDGE>(new_z).relation_type = d.rt;
                        get<blobs_ns::RELATION_EDGE>(new_z).instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx).time_slice;
                        get<blobs_ns::RELATION_EDGE>(new_z).source_node_index = index(new_z);
                        get<blobs_ns::RELATION_EDGE>(new_z).target_node_index = index(new_z);
                        internals::append_edge_index(new_z, index(new_z));
                        internals::append_edge_index(new_z, -index(new_z));
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                        z = new_z;
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const DelegateRelationTriple & d, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};

            std::optional<EZefRef> d_src = delegate_to_ezr(*(d.source), g, create, 1);
            std::optional<EZefRef> d_trg = delegate_to_ezr(*(d.target), g, create, 1);

            auto rel_group = delegate_to_ezr(DelegateRelationGroup{d.rt}, 1, g, create);
            if(!rel_group)
                return {};
            EZefRef z = *rel_group;
            for(int i = 0 ; i < order ; i++) {
                if(!d_src || !d_trg)
                    return {};
                EZefRefs opts = filter(traverse_out_node_multi(z, BT.TO_DELEGATE_EDGE), d.rt);
                opts = filter(opts, [&](EZefRef z) { return source(z) == *d_src && target(z) == *d_trg; });
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(*d_src, BT.RELATION_EDGE, *d_trg, gd);
                        get<blobs_ns::RELATION_EDGE>(new_z).relation_type = d.rt;
                        get<blobs_ns::RELATION_EDGE>(new_z).instantiation_time_slice = get<blobs_ns::TX_EVENT_NODE>(tx).time_slice;
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                        z = new_z;
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
                // Don't create these on the last loop, as that's unnecessary
                if(i < order-1) {
                    d_src = delegate_to_ezr(delegate_of(*d_src), g, create);
                    d_trg = delegate_to_ezr(delegate_of(*d_trg), g, create);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const DelegateTX & d, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};

            EZefRef z = root;
            for(int i = 0 ; i < order ; i++) {
                EZefRefs opts = filter(traverse_out_node_multi(root, BT.TO_DELEGATE_EDGE),
                                       [&](EZefRef z) { return BT(z) == BT.TX_EVENT_NODE; });
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(BT.TX_EVENT_NODE, gd);
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const DelegateRoot & d, int order, Graph g, bool create) {
            auto & gd = g.my_graph_data();
            EZefRef root{constants::ROOT_NODE_blob_index, gd};

            EZefRef z = root;
            for(int i = 0 ; i < order ; i++) {
                EZefRefs opts = filter(traverse_out_node_multi(root, BT.TO_DELEGATE_EDGE),
                                       [&](EZefRef z) { return BT(z) == BT.ROOT_NODE; });
                if(length(opts) == 0) {
                    if(create) {
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef new_z = internals::instantiate(BT.ROOT_NODE, gd);
                        EZefRef new_to_delegate_edge = internals::instantiate(z, BT.TO_DELEGATE_EDGE, new_z, gd);
                        internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, new_to_delegate_edge, gd);
                    } else
                        return {};
                } else {
                    z = only(opts);
                }
            }

            return z;
        }

        std::optional<EZefRef> delegate_to_ezr(const Delegate & d, Graph g, bool create, int order_diff) {
            auto & gd = g.my_graph_data();

            int actual_order = d.order + order_diff;
            
            if(actual_order == 0) {
                throw std::runtime_error("Can't obtain EZefRef of a delegate of order 0");
            }
            // To try and combine all additions into one transaction. Will do two attempts in the case of creating.
            std::optional<EZefRef> res =  std::visit([&](auto & x) { return delegate_to_ezr(x, actual_order, g, false); },
                                                     d.item);
            if(!res) {
                if(create) {
                    auto tx = Transaction(gd);
                    res =  std::visit([&](auto & x) { return delegate_to_ezr(x, actual_order, g, true); },
                                      d.item);
                    return res;
                } else 
                    return {};
            } else {
                if(!exists_at_now(*res)) {
                    if(create) {
                        // Assign new instantiation edges all the way down.
                        EZefRef tx = internals::get_or_create_and_get_tx(gd);
                        EZefRef cur_d = *res;
                        while(!exists_at_now(cur_d)) {
                            internals::instantiate(tx, BT.DELEGATE_INSTANTIATION_EDGE, cur_d < BT.TO_DELEGATE_EDGE, gd);
                            cur_d = cur_d << BT.TO_DELEGATE_EDGE;
                        }
                    } else {
                        return {};
                    }
                }
                return res;
            }
        }

        Delegate delegate_rep(EZefRef ezr) {
            if(ezr <= ET
               || ezr <= AET
               || is_delegate_relation_group(ezr)
               || BT(ezr) == BT.TX_EVENT_NODE
               || BT(ezr) == BT.ROOT_NODE) {
                int order = 0;
                EZefRef cur = ezr;
                while(cur | has_in[BT.TO_DELEGATE_EDGE]) {
                    order++;
                    cur = cur << BT.TO_DELEGATE_EDGE;
                }
                // Note: we may end up with order 0 at the end here - this is
                // acceptable, and means this is an instance!
                if(ezr <= ET)
                    return Delegate{order, DelegateEntity{ET(ezr)}};
                if(ezr <= AET)
                    return Delegate{order, DelegateAtomicEntity{AET(ezr)}};
                if(ezr <= RT)
                    return Delegate{order, DelegateRelationGroup{RT(ezr)}};
                if(BT(ezr) == BT.TX_EVENT_NODE)
                    return Delegate{order, DelegateTX{}};
                if(BT(ezr) == BT.ROOT_NODE)
                    return Delegate{order, DelegateRoot{}};
            }

            if(ezr <= RT) {
                // A relation but not a delegate group
                Delegate src = delegate_rep(source(ezr));
                Delegate trg = delegate_rep(target(ezr));

                int order = 0;
                EZefRef cur = ezr;
                while(cur | has_in[BT.TO_DELEGATE_EDGE] && !is_delegate_relation_group(cur)) {
                    order++;
                    src.order--;
                    trg.order--;
                    cur = cur << BT.TO_DELEGATE_EDGE;
                }

                return Delegate{order, DelegateRelationTriple{RT(ezr), src, trg}};
            }

            throw std::runtime_error("Don't know how to get delegate from " + to_str(ezr));
        }
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




	}





	bool is_compatible(bool _, AtomicEntityType aet, bool extended_type_check=false) { return aet == AET.Bool ; }
	bool is_compatible(int _, AtomicEntityType aet, bool extended_type_check=false) { return aet == AET.Int || aet == AET.Float || aet == AET.Bool; }   // we can also assign an int to a bool
	bool is_compatible(double _, AtomicEntityType aet, bool extended_type_check=false) { return aet == AET.Float || aet == AET.Int; }
	bool is_compatible(str _, AtomicEntityType aet, bool extended_type_check=false) { return aet == AET.String; }
	bool is_compatible(const char* _, AtomicEntityType aet, bool extended_type_check=false) { return aet == AET.String; }
	bool is_compatible(Time _, AtomicEntityType aet, bool extended_type_check=false) { return aet == AET.Time; }
	bool is_compatible(SerializedValue _, AtomicEntityType aet, bool extended_type_check=false) { return aet == AET.Serialized; }

	bool is_compatible(ZefEnumValue en, AtomicEntityType aet, bool extended_type_check=false) {
		int offset = aet.value % 16;
		if (offset != 1) return false;   // Enums encoded by an offset of 1
		if (!extended_type_check) return true;
		return (ZefEnumValue{ (aet.value - offset) }.enum_type() == en.enum_type());   // the enum type of the value to be assigned has to agree with the enum type specified in the AET
	}
	bool is_compatible(QuantityFloat q, AtomicEntityType aet, bool extended_type_check=false) {
		int offset = aet.value % 16;
		if (offset != 2) return false;   // QuantityFloat encoded by an offset of 2
		if (!extended_type_check) return true;		
		return ((aet.value - offset) == q.unit.value);   // units agree
	}
	bool is_compatible(QuantityInt q, AtomicEntityType aet, bool extended_type_check=false) {
		int offset = aet.value % 16;		
		if (offset != 3) return false;   // QuantityInt encoded by an offset of 3
		if (!extended_type_check) return true;
		return ((aet.value - offset) == q.unit.value);   // units agree
	}

    namespace imperative {

        //TODO: in python bindings when assigning a bool, the art needs to be set to 'Int'?
        template <typename T>
        void __assign_value(EZefRef my_atomic_entity, T value_to_be_assigned) {
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
            if (!is_compatible(value_to_be_assigned, AET(my_atomic_entity), true))
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

        template<>
        void assign_value(EZefRef my_atomic_entity, bool value_to_be_assigned) {
            __assign_value(my_atomic_entity, value_to_be_assigned);
        }
        template<>
        void assign_value(EZefRef my_atomic_entity, int value_to_be_assigned) {
            __assign_value(my_atomic_entity, value_to_be_assigned);
        }
        template<>
        void assign_value(EZefRef my_atomic_entity, double value_to_be_assigned) {
            __assign_value(my_atomic_entity, value_to_be_assigned);
        }
        template<>
        void assign_value(EZefRef my_atomic_entity, str value_to_be_assigned) {
            __assign_value(my_atomic_entity, value_to_be_assigned);
        }
        template<>
        void assign_value(EZefRef my_atomic_entity, const char* value_to_be_assigned) {
            __assign_value(my_atomic_entity, value_to_be_assigned);
        }
        template<>
        void assign_value(EZefRef my_atomic_entity, Time value_to_be_assigned) {
            __assign_value(my_atomic_entity, value_to_be_assigned);
        }
        template<>
        void assign_value(EZefRef my_atomic_entity, SerializedValue value_to_be_assigned) {
            __assign_value(my_atomic_entity, value_to_be_assigned);
        }
        template<>
        void assign_value(EZefRef my_atomic_entity, ZefEnumValue value_to_be_assigned) {
            __assign_value(my_atomic_entity, value_to_be_assigned);
        }
        template<>
        void assign_value(EZefRef my_atomic_entity, QuantityFloat value_to_be_assigned) {
            __assign_value(my_atomic_entity, value_to_be_assigned);
        }
        template<>
        void assign_value(EZefRef my_atomic_entity, QuantityInt value_to_be_assigned) {
            __assign_value(my_atomic_entity, value_to_be_assigned);
        }
    }
    


	namespace internals {
		
		struct Sentinel{};  // sentinel value

		// an instance of this type is stored within each atomic types struct: _Float, _Double, ...
		// In case this is specified (not 'void'), this indicates that the time slice specified by this struct should be used to obtain the value
		using AtomicTypeVariant = std::variant<
			Sentinel,   // if nothing is set: use the time slice specified by the zefref
			TimeSlice,
			EZefRef,   // in case a transaction is specified
			Now
		>;

		inline std::ostream& operator<< (std::ostream& o, AtomicTypeVariant atv) {
			o << "<";
			std::visit(overloaded{
				[&o](Sentinel x)->void { o << "not set"; },
				[&o](TimeSlice x)->void { o << "TimeSlice specified: " << x.value; },
				[&o](EZefRef x)->void { o << "EZefRef specified as ref tx: " << x; },
				[&o](Now x)->void { o << "'Now' specified"; }
			}, atv);
			o << ">";
			return o;
		}


		/*[[[cog
		import cog
		atomic_type_names = [
			['Float','double', ' = 0.0'],			
			['Int','int', ' = 0'],
			['Bool','bool', ' = false'],
			['String','std::string', ' = ""'],
			['Time','zefDB::Time', ' = Time{0.0}'],
			['Enum','ZefEnumValue', ' {}'],
			['QuantityFloat','QuantityFloat', ' {0.0, EN.Unit._undefined}'],
			['QuantityInt','QuantityInt', ' {0, EN.Unit._undefined}'],
		]

		for n in atomic_type_names:
			cog.outl(f'struct _{n[0]} {{')
			cog.outl(f'    AtomicTypeVariant time_slice_override = Sentinel();')
			cog.outl(f'    {n[1]} _x{n[2]};  // this is here only to automatically extract the type in the generic function ^. No useful info is actually ever stored here')
			cog.outl(f'    _{n[0]} operator[](TimeSlice ts) const {{ return _{n[0]}{{ AtomicTypeVariant(ts) }}; }}    // returns a new object of this very type with the TimeSlice info baked in')
			cog.outl(f'    _{n[0]} operator[](EZefRef tx) const {{ return _{n[0]}{{ AtomicTypeVariant(tx) }}; }}   // returns a new object of this very type with the tx info baked in')
			cog.outl(f'    _{n[0]} operator[](Now la) const {{ return _{n[0]}{{ AtomicTypeVariant(la) }}; }}')
			cog.outl(f'}};')
			cog.outl(f'')
		]]]*/
		struct _Float {
		    AtomicTypeVariant time_slice_override = Sentinel();
		    double _x = 0.0;  // this is here only to automatically extract the type in the generic function ^. No useful info is actually ever stored here
		    _Float operator[](TimeSlice ts) const { return _Float{ AtomicTypeVariant(ts) }; }    // returns a new object of this very type with the TimeSlice info baked in
		    _Float operator[](EZefRef tx) const { return _Float{ AtomicTypeVariant(tx) }; }   // returns a new object of this very type with the tx info baked in
		    _Float operator[](Now la) const { return _Float{ AtomicTypeVariant(la) }; }
		};

		struct _Int {
		    AtomicTypeVariant time_slice_override = Sentinel();
		    int _x = 0;  // this is here only to automatically extract the type in the generic function ^. No useful info is actually ever stored here
		    _Int operator[](TimeSlice ts) const { return _Int{ AtomicTypeVariant(ts) }; }    // returns a new object of this very type with the TimeSlice info baked in
		    _Int operator[](EZefRef tx) const { return _Int{ AtomicTypeVariant(tx) }; }   // returns a new object of this very type with the tx info baked in
		    _Int operator[](Now la) const { return _Int{ AtomicTypeVariant(la) }; }
		};

		struct _Bool {
		    AtomicTypeVariant time_slice_override = Sentinel();
		    bool _x = false;  // this is here only to automatically extract the type in the generic function ^. No useful info is actually ever stored here
		    _Bool operator[](TimeSlice ts) const { return _Bool{ AtomicTypeVariant(ts) }; }    // returns a new object of this very type with the TimeSlice info baked in
		    _Bool operator[](EZefRef tx) const { return _Bool{ AtomicTypeVariant(tx) }; }   // returns a new object of this very type with the tx info baked in
		    _Bool operator[](Now la) const { return _Bool{ AtomicTypeVariant(la) }; }
		};

		struct _String {
		    AtomicTypeVariant time_slice_override = Sentinel();
		    std::string _x = "";  // this is here only to automatically extract the type in the generic function ^. No useful info is actually ever stored here
		    _String operator[](TimeSlice ts) const { return _String{ AtomicTypeVariant(ts) }; }    // returns a new object of this very type with the TimeSlice info baked in
		    _String operator[](EZefRef tx) const { return _String{ AtomicTypeVariant(tx) }; }   // returns a new object of this very type with the tx info baked in
		    _String operator[](Now la) const { return _String{ AtomicTypeVariant(la) }; }
		};

		struct _Time {
		    AtomicTypeVariant time_slice_override = Sentinel();
		    zefDB::Time _x = Time{0.0};  // this is here only to automatically extract the type in the generic function ^. No useful info is actually ever stored here
		    _Time operator[](TimeSlice ts) const { return _Time{ AtomicTypeVariant(ts) }; }    // returns a new object of this very type with the TimeSlice info baked in
		    _Time operator[](EZefRef tx) const { return _Time{ AtomicTypeVariant(tx) }; }   // returns a new object of this very type with the tx info baked in
		    _Time operator[](Now la) const { return _Time{ AtomicTypeVariant(la) }; }
		};

		struct _Serialized {
		    AtomicTypeVariant time_slice_override = Sentinel();
		    zefDB::SerializedValue _x;  // this is here only to automatically extract the type in the generic function ^. No useful info is actually ever stored here
		    _Serialized operator[](TimeSlice ts) const { return _Serialized{ AtomicTypeVariant(ts) }; }    // returns a new object of this very type with the SerializedSlice info baked in
		    _Serialized operator[](EZefRef tx) const { return _Serialized{ AtomicTypeVariant(tx) }; }   // returns a new object of this very type with the tx info baked in
		    _Serialized operator[](Now la) const { return _Serialized{ AtomicTypeVariant(la) }; }
		};

		struct _Enum {
		    AtomicTypeVariant time_slice_override = Sentinel();
		    ZefEnumValue _x {};  // this is here only to automatically extract the type in the generic function ^. No useful info is actually ever stored here
		    _Enum operator[](TimeSlice ts) const { return _Enum{ AtomicTypeVariant(ts) }; }    // returns a new object of this very type with the TimeSlice info baked in
		    _Enum operator[](EZefRef tx) const { return _Enum{ AtomicTypeVariant(tx) }; }   // returns a new object of this very type with the tx info baked in
		    _Enum operator[](Now la) const { return _Enum{ AtomicTypeVariant(la) }; }
		};

		struct _QuantityFloat {
		    AtomicTypeVariant time_slice_override = Sentinel();
		    QuantityFloat _x {0.0, EN.Unit._undefined};  // this is here only to automatically extract the type in the generic function ^. No useful info is actually ever stored here
		    _QuantityFloat operator[](TimeSlice ts) const { return _QuantityFloat{ AtomicTypeVariant(ts) }; }    // returns a new object of this very type with the TimeSlice info baked in
		    _QuantityFloat operator[](EZefRef tx) const { return _QuantityFloat{ AtomicTypeVariant(tx) }; }   // returns a new object of this very type with the tx info baked in
		    _QuantityFloat operator[](Now la) const { return _QuantityFloat{ AtomicTypeVariant(la) }; }
		};

		struct _QuantityInt {
		    AtomicTypeVariant time_slice_override = Sentinel();
		    QuantityInt _x {0, EN.Unit._undefined};  // this is here only to automatically extract the type in the generic function ^. No useful info is actually ever stored here
		    _QuantityInt operator[](TimeSlice ts) const { return _QuantityInt{ AtomicTypeVariant(ts) }; }    // returns a new object of this very type with the TimeSlice info baked in
		    _QuantityInt operator[](EZefRef tx) const { return _QuantityInt{ AtomicTypeVariant(tx) }; }   // returns a new object of this very type with the tx info baked in
		    _QuantityInt operator[](Now la) const { return _QuantityInt{ AtomicTypeVariant(la) }; }
		};

		//[[[end]]]

	}

	namespace zefOps {
	


		struct Value {
			internals::AtomicTypeVariant time_slice_override = internals::Sentinel();   // for Python we can do my_uzr | value[now]. This struct should have the capability to have this curried in

			const internals::_Float Float{};
			const internals::_Int Int{};
			const internals::_Bool Bool{};
			const internals::_String String{};
			const internals::_Time Time{};
			const internals::_Time Serialized{};
			const internals::_Enum Enum{};
			const internals::_QuantityFloat QuantityFloat{};
			const internals::_QuantityInt QuantityInt{};
			
			Value(internals::AtomicTypeVariant time_slice_override_) : 
				time_slice_override(time_slice_override_),
				Float(internals::_Float{ time_slice_override_ }),
				Int(internals::_Int{ time_slice_override_ }),
				Bool(internals::_Bool{ time_slice_override_ }),
				String(internals::_String{ time_slice_override_ }),
				Time(internals::_Time{ time_slice_override_ }),
				Serialized(internals::_Time{ time_slice_override_ }),
				Enum(internals::_Enum{ time_slice_override_ }),
				QuantityFloat(internals::_QuantityFloat{ time_slice_override_ }),
				QuantityInt(internals::_QuantityInt{ time_slice_override_ })
			{}
			Value() {}

			Value operator[](TimeSlice ts) const { return Value{ internals::AtomicTypeVariant(ts) }; }    // returns a new object of this very type with the TimeSlice info baked in
			Value operator[](EZefRef tx) const { return Value{ internals::AtomicTypeVariant(tx) }; }   // returns a new object of this very type with the tx info baked in
			Value operator[](Now la) const { return Value{ internals::AtomicTypeVariant(la) }; }
		};
		const Value value;

	}




	namespace internals {
		// use template specialization for the return value in the fct 'auto operator^ (ZefRef my_atomic_entity, T op) -> std::optional<decltype(op._x)>' below.
		// string values are saved as a char array. We could return a string_view, but for simplicity and pybind11, instantiate an std::string for now
		inline auto get_final_value_for_op_hat(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, internals::_String op, AtomicEntityType aet)->std::optional<decltype(op._x)> {
            Butler::ensure_or_get_range(aae.data_buffer, aae.buffer_size_in_bytes);
            return std::string(aae.data_buffer, aae.buffer_size_in_bytes);
		}

		inline auto get_final_value_for_op_hat(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, internals::_Serialized op, AtomicEntityType aet)->std::optional<decltype(op._x)> {
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

		inline std::optional<double> get_final_value_for_op_hat(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, internals::_Float op, AtomicEntityType aet) {
            // This needs to be specialised to convert, while allowing the other variants to avoid this unncessary check
            if (aet == AET.Float)
                return cast_it_for_fucks_sake(*(double*)(aae.data_buffer), double());
            else// if(aet == AET.Int)
                return cast_it_for_fucks_sake(*(int*)(aae.data_buffer), double());
		}
		inline std::optional<int> get_final_value_for_op_hat(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, internals::_Int op, AtomicEntityType aet) {
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
		auto get_final_value_for_op_hat(blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae, T op, AtomicEntityType aet)->std::optional<decltype(op._x)> {
			return *(decltype(op._x)*)(aae.data_buffer);  // project onto type
		}

	}















	namespace zefOps {
		// for ZefRefs:  1) it is recommended to NOT specify any time info in the 'value' zefop and the time info is taken from the zefref tx
		//               2) if time info is specified in the value zefop, this overrides the overrides the ZefRefs
		// for EZefRefs: 1) time info HAS to be specified in the 'value' zefop (where else would it get it from). An error is thrown if not
		template <typename T>
		auto operator| (ZefRef my_atomic_entity, T op) -> std::optional<decltype(op._x)> {
			using namespace internals;
			// check that the type T corresponds to the type of atomic entity of the zefRef
			if (get<BlobType>(my_atomic_entity.blob_uzr) != BlobType::ATOMIC_ENTITY_NODE) 
				throw std::runtime_error("ZefRef | value.something called for a ZefRef not pointing to an ATOMIC_ENTITY_NODE blob.");			
            AtomicEntityType aet = get<blobs_ns::ATOMIC_ENTITY_NODE>(my_atomic_entity.blob_uzr).my_atomic_entity_type;
			if (!is_compatible(op._x, aet))
				throw std::runtime_error("ZefRef | value." + to_str(op._x) + " called, but the specified return type does not agree with the type of the ATOMIC_ENTITY_NODE pointed to (" + to_str(get<blobs_ns::ATOMIC_ENTITY_NODE>(my_atomic_entity.blob_uzr).my_atomic_entity_type) + ")");

			GraphData& gd = *graph_data(my_atomic_entity);			
			EZefRef ref_tx = std::holds_alternative<internals::Sentinel>(op.time_slice_override) ?
				my_atomic_entity.tx :   // use the reference frame baked into the ZefRef if none is specified in the 'value' zefop
				std::visit(overloaded{
					[&gd](internals::Sentinel ts)->EZefRef {return EZefRef(0, gd); },   // never used, but required for completion
					[&gd](TimeSlice ts)->EZefRef {return tx[gd][ts]; },  // determine the tx event for the specified time slice here: before this it is not know for which graph the tx should be determined
					[](EZefRef uzr)->EZefRef { return uzr; },   // if the tx is directly specified
					[&gd](Now latest_op)->EZefRef { return EZefRef(gd.latest_complete_tx, gd); }
					}, op.time_slice_override);
			
			if (!exists_at[ref_tx](my_atomic_entity.blob_uzr))
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
			else return internals::get_final_value_for_op_hat(get<blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE>(result_candidate_edge), op, aet);
		}
		

		// 

		template <typename T>
		auto operator| (EZefRef my_atomic_entity, T op) -> std::optional<decltype(op._x)> {
			using namespace internals;
			// check that the type T corresponds to the type of atomic entity of the zefRef
			if (get<BlobType>(my_atomic_entity) != BlobType::ATOMIC_ENTITY_NODE) 
				throw std::runtime_error("EZefRef | value.something called for a ZefRef not pointing to an ATOMIC_ENTITY_NODE blob.");			
            AtomicEntityType aet = get<blobs_ns::ATOMIC_ENTITY_NODE>(my_atomic_entity).my_atomic_entity_type;
			if (!is_compatible(op._x, aet))
				throw std::runtime_error("EZefRef | value.something called, but the specified return type does not agree with the type of the ATOMIC_ENTITY_NODE pointed to.");

			GraphData& gd = *graph_data(my_atomic_entity);			
			EZefRef ref_tx = std::visit(overloaded{
					[&gd](internals::Sentinel ts)->EZefRef {throw std::runtime_error("Time slice info need to be set inside 'value' zefop when calling on a EZefRef"); return EZefRef(0, gd); },   // never used, but required for completion
					[&gd](TimeSlice ts)->EZefRef {return tx[gd][ts]; },  // determine the tx event for the specified time slice here: before this it is not know for which graph the tx should be determined
					[](EZefRef uzr)->EZefRef { return uzr; },   // if the tx is directly specified
					[&gd](Now latest_op)->EZefRef { return EZefRef(gd.latest_complete_tx, gd); }
				}, op.time_slice_override);

			if (!exists_at[ref_tx](my_atomic_entity))
				throw std::runtime_error("EZefRef | value.something called, but the rel_ent pointed to does not exists in the reference frame tx specified.");

			auto tx_time_slice = [](EZefRef uzr)->TimeSlice { return get<blobs_ns::TX_EVENT_NODE>(uzr).time_slice; };
			TimeSlice ref_time_slice = tx_time_slice(ref_tx);
			auto result_candidate_edge = EZefRef(nullptr);
			// Ranges don't work for AllEdgeIndexes class and we want this part to be lazy, do it the ugly imperative way for now
			// This is one of the critical parts where we want lazy evaluation.
			// Enter the pyramid of death. 
			for (auto ind : AllEdgeIndexes(my_atomic_entity < BT.RAE_INSTANCE_EDGE)) {
				if (ind < 0) {
					auto incoming_val_assignment_edge = EZefRef(-ind, gd);
					if (get<BlobType>(incoming_val_assignment_edge) == BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE) {
						if (tx_time_slice(incoming_val_assignment_edge | source) <= ref_time_slice) result_candidate_edge = incoming_val_assignment_edge;
						else break;
					}
				}
			}
			if (result_candidate_edge.blob_ptr == nullptr) return {};  // no assignment edge was found
			else return internals::get_final_value_for_op_hat(get<blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE>(result_candidate_edge), op, aet);
		}




		// we want to be able to specify time in different ways:	
		//		1)  my_ent | value<int> // implicitly that of the zefref
		//		1b)  my_ent_uzefref | value<int>    // we do not know which time slice to use!
		//		2)  my_ent | value<int>(Time(1570923843.1))
		//		3)  my_ent | value<int>(Time(05-08-2019, 17:00:00))  // at least from Python?
		//		4)  my_ent | value<int>(TimeSlice(2342))


		//template <typename T>
		//struct value {
		//	value<T>() {}
		//	value<T>(int x) {}  // if we want to pass additional args, e.g. auto x = my_atomic_ent|value<int>(now)
		//	//struct V{};
		//	//template <typename T>
		//	//V<T> operator() (){}

		//};
		////constexpr Value value;




		//template <typename T>
		//std::optional<T> operator| (ZefRef my_atomic_entity, value<T> op) {
		//	return 12.1;
		//}

	}   // namespace zefOps

    namespace imperative {
        value_ret_t value(ZefRef ae) {
            if (get<BlobType>(ae.blob_uzr) != BlobType::ATOMIC_ENTITY_NODE) throw std::runtime_error("'value(zefref)' called for a zefref which is not an atomic entity.");
            auto aet = get<blobs_ns::ATOMIC_ENTITY_NODE>(ae.blob_uzr).my_atomic_entity_type.value;
            switch (aet) {
            case AET.Float.value: { return ae | internals::_Float{}; }
            case AET.Int.value: { return ae | internals::_Int{}; }
            case AET.Bool.value: { return ae | internals::_Bool{}; }
            case AET.String.value: { return ae | internals::_String{}; }
            case AET.Time.value: { return ae | internals::_Time{}; }
            case AET.Serialized.value: { return ae | internals::_Serialized{}; }
            default: {
                switch (aet % 16) {
                case 1: { return ae | internals::_Enum{}; }
                case 2: { return ae | internals::_QuantityFloat{}; }
                case 3: { return ae | internals::_QuantityInt{}; }
                default: throw std::runtime_error("Return type not implemented.");
                }
            }
            }
        }
        value_ret_t value(EZefRef ae, EZefRef tx) {
            return value(to_frame(ae, tx));
        }
        value_ret_t value(ZefRef ae, EZefRef tx) {
            return value(to_frame(ae, tx));
        }
        value_ret_t value(EZefRef ae, ZefRef tx) {
            return value(to_frame(ae, tx));
        }
        value_ret_t value(ZefRef ae, ZefRef tx) {
            return value(to_frame(ae, tx));
        }

        std::vector<value_ret_t> value(ZefRefs zrs) {
            std::vector<value_ret_t> res;
            res.reserve(length(zrs));
            std::transform( 
                zrs.begin(),
                zrs.end(),
                std::back_inserter(res),
                [](const ZefRef& zr) { return value(zr); }
            );
            return res;
        }

        std::vector<value_ret_t> value(EZefRefs uzrs, EZefRef tx) {
            return value(to_frame(uzrs, tx));
        }
        std::vector<value_ret_t> value(ZefRefs zrs, EZefRef tx) {
            return value(to_frame(zrs, tx));
        }
        std::vector<value_ret_t> value(EZefRefs uzrs, ZefRef tx) {
            return value(to_frame(uzrs, tx));
        }
        std::vector<value_ret_t> value(ZefRefs zrs, ZefRef tx) {
            return value(to_frame(zrs, tx));
        }
    }

}
