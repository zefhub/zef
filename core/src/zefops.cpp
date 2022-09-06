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


// This is starting to separate out the old zefops style that was in
// high_level_api.cpp


#include "high_level_api.h"
#include "zefops.h"
#include "ops_imperative.h"
#include <iterator>
#include <unordered_set>

namespace zefDB {
	AttributeEntityType operator| (EZefRef uzr, const AttributeEntityTypeStruct& AET_) {return AET_(uzr);}
	AttributeEntityType operator| (ZefRef zr, const AttributeEntityTypeStruct& AET_) {return AET_(zr);}

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
            return imperative::ins(uzr);
        }

		ZefRefs Ins::operator() (ZefRef zr) const {
			return imperative::ins(zr);
		}



		//                                           _                                  
		//                                ___  _   _| |_ ___                            
		//    _____ _____ _____ _____    / _ \| | | | __/ __|   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  | (_) | |_| | |_\__ \  |_____|_____|_____|_____|
		//                               \___/ \__,_|\__|___/                           
		//                                                                              
        EZefRefs Outs::operator() (EZefRef uzr) const {
            return imperative::outs(uzr);
        }

		ZefRefs Outs::operator() (ZefRef zr) const {
            return imperative::outs(zr);
		}

		//                               _                              _                _                                  
		//                              (_)_ __  ___     __ _ _ __   __| |    ___  _   _| |_ ___                            
		//    _____ _____ _____ _____   | | '_ \/ __|   / _` | '_ \ / _` |   / _ \| | | | __/ __|   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  | | | | \__ \  | (_| | | | | (_| |  | (_) | |_| | |_\__ \  |_____|_____|_____|_____|
		//                              |_|_| |_|___/___\__,_|_| |_|\__,_|___\___/ \__,_|\__|___/                           
		//                                         |_____|              |_____|                                          
        EZefRefs InsAndOuts::operator() (EZefRef uzr) const {
            return imperative::ins_and_outs(uzr);
        }


		ZefRefs InsAndOuts::operator() (ZefRef zr) const {
            return imperative::ins_and_outs(zr);
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

			// Filter Filter::operator[] (AttributeEntityType aet) const {
			// 	return Filter{
			// 		not_in_activated,
			// 		(std::function<bool(EZefRef)>)[aet](EZefRef uzr)->bool {
			// 		return get<BlobType>(uzr) == BlobType::ATTRIBUTE_ENTITY_NODE &&
			// 			get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(uzr).my_atomic_entity_type == aet;
			// 		},
			// 		(std::function<bool(ZefRef)>)[aet](ZefRef zr)->bool {
			// 		return get<BlobType>(zr) == BlobType::ATTRIBUTE_ENTITY_NODE &&
			// 			get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(zr).my_atomic_entity_type == aet;
			// 		}
			// 	};
			// }

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
				case BlobType::ATTRIBUTE_ENTITY_NODE: {
					blobs_ns::ATTRIBUTE_ENTITY_NODE& x = get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(rel_ent);
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
        DelegateOp DelegateOp::operator[](const ValueRepType& vrt) const {
            return DelegateOp{vrt};
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

        EZefRefs ValueAssignmentTxs::operator() (EZefRef uzrs) const { return (uzrs < BT.RAE_INSTANCE_EDGE) << L[BT.ATOMIC_VALUE_ASSIGNMENT_EDGE, BT.ATTRIBUTE_VALUE_ASSIGNMENT_EDGE]; }
	
	ZefRefs ValueAssignmentTxs::operator() (ZefRef zrs) const { 
		ZefRef frame = zrs | tx; 
		auto frame_ts = frame | time_slice; 
		return (((zrs 
                 | to_ezefref) 
                < BT.RAE_INSTANCE_EDGE)
                << L[BT.ATOMIC_VALUE_ASSIGNMENT_EDGE, BT.ATTRIBUTE_VALUE_ASSIGNMENT_EDGE]
                | filter[([frame_ts](EZefRef z) { return (z | time_slice) <= frame_ts; })] 
                | to_zefref[frame]
                );
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
        return imperative::now(g.my_graph_data());
    }

    ZefRef Now::operator() (EZefRef uzr) const {
        return imperative::now(uzr);
    }

    ZefRef Now::operator() (ZefRef zr) const {
        return imperative::now(zr);
    }
    //ZefRefs Now::operator() (ZefRefs&& zrs) {  TODO.... }
	ZefRefs Now::operator() (const ZefRefs& zrs) { 
		// tasks::apply_immediate_updates_from_zm();  
		ZefRefs res = zrs; 
		res.reference_frame_tx = Graph(res.reference_frame_tx) | now | to_ezefref; return res;	
	}
    //////////////////////////////
    // * Instances

    // Template the implementation out but keep it as a separate function name
    // to manually specify what types are implemented (below in Instances::pure).
    template<class T>
    ZefRefs Instances_pure_helper(EZefRef tx, T type) {
        // This version ignores any local curried in data. It is the "pure function"
        if (!(is_zef_subtype(tx, BT.TX_EVENT_NODE))) {
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
    ZefRefs Instances::pure(EZefRef tx, ValueRepType vrt) {
        return Instances_pure_helper(tx, vrt);
    }
    ZefRefs Instances::pure(EZefRef tx, Instances::Sentinel) {
        return Instances::pure(tx);
    }

    ZefRefs Instances::pure(EZefRef tx) {
        // This version ignores any local curried in data. It is the "pure function"

        if (!is_zef_subtype(tx, BT.TX_EVENT_NODE)) {
            throw std::runtime_error("Instances(uzr) should be called with a TX as the first argument.");
        }

        return (all_raes(Graph(tx))
                | filter[std::function<bool(EZefRef)>(exists_at[tx])]
                | to_zefref[tx]);
    }

    ZefRefs Instances::pure(EZefRef tx, EZefRef delegate) {
        if (!is_zef_subtype(tx, BT.TX_EVENT_NODE)) {
            throw std::runtime_error("Instances(tx, type) should be called with a TX as the first argument.");
        }

        return ((delegate < BT.TO_DELEGATE_EDGE) >> L[BT.RAE_INSTANCE_EDGE]) | filter[std::function<bool(EZefRef)>(exists_at[tx])] | to_zefref[tx];
    }

    ZefRefs Instances::pure(ZefRef tx_or_delegate) {
        if (is_zef_subtype(tx_or_delegate, BT.TX_EVENT_NODE)) {
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
        if (is_zef_subtype(zr, BT.TX_EVENT_NODE)) {
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
        if (is_zef_subtype(uzr, BT.TX_EVENT_NODE)) {
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
			>> L[BT.ATOMIC_VALUE_ASSIGNMENT_EDGE, BT.ATTRIBUTE_VALUE_ASSIGNMENT_EDGE]
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

		if (std::holds_alternative<OnValueAssignment>(relation_direction) && BT(z)!=BT.ATTRIBUTE_ENTITY_NODE)
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
	


    }
}