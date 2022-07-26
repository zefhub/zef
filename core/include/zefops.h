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


// This is a collection of things which are the old zefops style from
// high_level_api.h

#pragma once

#include "export_statement.h"

#include "high_level_api.h"

namespace zefDB {

	

	namespace zefOps {

        //////////////////////////////////////////////////
        // * Stuff from other files

        LIBZEF_DLL_EXPORTED AttributeEntityType operator| (EZefRef uzr, const AttributeEntityTypeStruct& AET_); 
        LIBZEF_DLL_EXPORTED AttributeEntityType operator| (ZefRef zr, const AttributeEntityTypeStruct& AET_);


		//                        _                            
		//                       | |___  __                    
		//    _____ _____ _____  | __\ \/ /  _____ _____ _____ 
		//   |_____|_____|_____| | |_ >  <  |_____|_____|_____|
		//                        \__/_/\_|                    
		                                     
		struct LIBZEF_DLL_EXPORTED Tx {
			std::optional<GraphData*> linked_graph_data_maybe = {};

			EZefRefs operator() (Graph& g) const;
			ZefRef operator() (ZefRef zr) const { return ZefRef{zr.tx,zr.tx}; }
			ZefRef operator() (const ZefRefs& zrs) const { return ZefRef{zrs.reference_frame_tx, zrs.reference_frame_tx}; }
			
			Tx operator[] (Graph& g) const { return Tx {&g.my_graph_data()}; };
			Tx operator[] (GraphData& gd) const { return Tx {&gd}; };
			EZefRef operator[] (TimeSlice ts) const;  // get the tx given a time slice and a graph
		};
		constexpr Tx tx;
		inline EZefRefs operator| (Graph& g, Tx op) { return tx(g); }
		inline ZefRef operator| (ZefRef zr, Tx op) { return tx(zr); }
		inline ZefRef operator| (const ZefRefs& zrs, Tx op) { return tx(zrs); }


		//                         _   _                                     
		//                        | |_(_)_ __ ___   ___                      
		//    _____ _____ _____   | __| | '_ ` _ \ / _ \   _____ _____ _____ 
		//   |_____|_____|_____|  | |_| | | | | | |  __/  |_____|_____|_____|
		//                         \__|_|_| |_| |_|\___|                     
		//                                                            
		// use as my_tx_zr | time

        struct Now;

		struct LIBZEF_DLL_EXPORTED TimeZefopStruct {
			Time operator() (EZefRef uzr) const;   // acts on a tx
			Time operator() (ZefRef zr) const;

			Time operator() (Now op) const;
		};
		const TimeZefopStruct time;

		inline Time operator| (EZefRef zr, TimeZefopStruct op) { return op(zr); }
		inline Time operator| (ZefRef uzr, TimeZefopStruct op) { return op(uzr); }




		//                                                                          
		//                         ___  ___  _   _ _ __ ___ ___                     
		//     _____ _____ _____  / __|/ _ \| | | | '__/ __/ _ \  _____ _____ _____ 
		//    |_____|_____|_____| \__ \ (_) | |_| | | | (_|  __/ |_____|_____|_____|
		//                        |___/\___/ \__,_|_|  \___\___|                    
		//                                                                       
		struct LIBZEF_DLL_EXPORTED Source {
			EZefRef operator() (EZefRef uzr) const ;
			EZefRefs operator() (EZefRefs&& uzrs) const ;
			EZefRefs operator() (const EZefRefs& uzrs) const ;

			ZefRef operator() (ZefRef zr) const;
			ZefRefs operator() (ZefRefs&& zrs) const ;
			ZefRefs operator() (const ZefRefs& zrs) const ;
		};
		constexpr Source source;

		inline EZefRef operator| (EZefRef uzr, const Source& op) { return op(uzr); }
		inline EZefRefs operator| (EZefRefs&& uzrs, const Source& op) { return op(std::move(uzrs)); }
		inline EZefRefs operator| (const EZefRefs& uzrs, const Source& op) { return op(uzrs); }

		inline ZefRef operator| (ZefRef zr, const Source& op) { return op(zr); }
		inline ZefRefs operator| (ZefRefs&& zrs, const Source& op) { return op(std::move(zrs)); }
		inline ZefRefs operator| (const ZefRefs& zrs, const Source& op) { return op(zrs); }







		//                         _                       _                       
		//                        | |_ __ _ _ __ __ _  ___| |_                     
		//     _____ _____ _____  | __/ _` | '__/ _` |/ _ \ __|  _____ _____ _____ 
		//    |_____|_____|_____| | || (_| | | | (_| |  __/ |_  |_____|_____|_____|
		//                         \__\__,_|_|  \__, |\___|\__|                    
		//                                      |___/                          
		struct LIBZEF_DLL_EXPORTED Target {
			EZefRef operator() (EZefRef uzr) const ;

			EZefRefs operator() (EZefRefs&& uzrs) const ;

			EZefRefs operator() (const EZefRefs& uzrs) const ;

			ZefRef operator() (ZefRef zr) const;
			ZefRefs operator() (ZefRefs&& zrs) const ;
			ZefRefs operator() (const ZefRefs& zrs) const ;

		};
		constexpr Target target;

		inline EZefRef operator| (EZefRef uzr, const Target& op) { return op(uzr); }
		inline EZefRefs operator| (EZefRefs&& uzrs, const Target& op) { return op(std::move(uzrs)); }
		inline EZefRefs operator| (const EZefRefs& uzrs, const Target& op) { return op(uzrs); }

		inline ZefRef operator| (ZefRef zr, const Target& op) { return op(zr); }
		inline ZefRefs operator| (ZefRefs&& zrs, const Target& op) { return op(std::move(zrs)); }
		inline ZefRefs operator| (const ZefRefs& zrs, const Target& op) { return op(zrs); }




		//                               _                                      
		//                              (_)_ __  ___                            
		//    _____ _____ _____ _____   | | '_ \/ __|   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  | | | | \__ \  |_____|_____|_____|_____|
		//                              |_|_| |_|___/                           
		//                                                                      
		struct LIBZEF_DLL_EXPORTED Ins {
			EZefRefs operator() (EZefRef uzr) const ;
			ZefRefs operator() (ZefRef zr) const;
		};
		constexpr Ins ins;
		inline EZefRefs operator| (EZefRef uzr, Ins op) { return op(uzr); }
		inline ZefRefs operator| (ZefRef zr, Ins op) { return op(zr); }






		//                                           _                                  
		//                                ___  _   _| |_ ___                            
		//    _____ _____ _____ _____    / _ \| | | | __/ __|   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  | (_) | |_| | |_\__ \  |_____|_____|_____|_____|
		//                               \___/ \__,_|\__|___/                           
		//                                                                              
		struct LIBZEF_DLL_EXPORTED Outs {
			EZefRefs operator() (EZefRef uzr) const ;
			ZefRefs operator() (ZefRef zr) const;
		};
		constexpr Outs outs;
		inline EZefRefs operator| (EZefRef uzr, Outs op) { return op(uzr); }
		inline ZefRefs operator| (ZefRef zr, Outs op) { return op(zr); }





		//                               _                              _                _                                  
		//                              (_)_ __  ___     __ _ _ __   __| |    ___  _   _| |_ ___                            
		//    _____ _____ _____ _____   | | '_ \/ __|   / _` | '_ \ / _` |   / _ \| | | | __/ __|   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  | | | | \__ \  | (_| | | | | (_| |  | (_) | |_| | |_\__ \  |_____|_____|_____|_____|
		//                              |_|_| |_|___/___\__,_|_| |_|\__,_|___\___/ \__,_|\__|___/                           
		//                                         |_____|              |_____|                                          
		struct LIBZEF_DLL_EXPORTED InsAndOuts {
			EZefRefs operator() (EZefRef uzr) const ;

			ZefRefs operator() (ZefRef zr) const;
		};
		constexpr InsAndOuts ins_and_outs;
		inline EZefRefs operator| (EZefRef uzr, InsAndOuts op) { return op(uzr); }
		inline ZefRefs operator| (ZefRef zr, InsAndOuts op) { return op(zr); }


        //////////////////////////////
        // * Connections

        struct LIBZEF_DLL_EXPORTED HasIn {
            struct Sentinel {};
            using type_variant = std::variant<Sentinel, RelationType, BlobType>;

            type_variant relation{ Sentinel {} };  

            bool operator() (const EZefRef uzr) const;
            bool operator() (const ZefRef zr) const;

            HasIn operator[](const RelationType& rt) const {
                return HasIn{rt};
            };
            HasIn operator[](const BlobType& bt) const {
                return HasIn{bt};
            };
        };
        constexpr HasIn has_in;
        inline bool operator| (EZefRef uzr, HasIn op) { return op(uzr); }
        inline bool operator| (ZefRef zr, HasIn op) { return op(zr); }

        struct LIBZEF_DLL_EXPORTED HasOut {
            struct Sentinel {};
            using type_variant = std::variant<Sentinel, RelationType, BlobType>;

            type_variant relation{ Sentinel {} };  

            bool operator() (const EZefRef uzr) const;
            bool operator() (const ZefRef zr) const;

            HasOut operator[](const RelationType& rt) const {
                return HasOut{rt};
            };
            HasOut operator[](const BlobType& bt) const {
                return HasOut{bt};
            };
        };
        constexpr HasOut has_out;
        inline bool operator| (EZefRef uzr, HasOut op) { return op(uzr); }
        inline bool operator| (ZefRef zr, HasOut op) { return op(zr); }

        LIBZEF_DLL_EXPORTED bool has_relation(ZefRef a, ZefRef b);
        LIBZEF_DLL_EXPORTED bool  has_relation(ZefRef a, RelationType rel, ZefRef b);
        LIBZEF_DLL_EXPORTED ZefRef relation(ZefRef a, ZefRef b);
        LIBZEF_DLL_EXPORTED ZefRef relation(ZefRef a, RelationType rel, ZefRef b);
        LIBZEF_DLL_EXPORTED ZefRefs relations(ZefRef a, ZefRef b);
        LIBZEF_DLL_EXPORTED ZefRefs relations(ZefRef a, RelationType rel, ZefRef b);

        LIBZEF_DLL_EXPORTED bool has_relation(EZefRef a, EZefRef b);
        LIBZEF_DLL_EXPORTED bool  has_relation(EZefRef a, RelationType rel, EZefRef b);
        LIBZEF_DLL_EXPORTED EZefRef relation(EZefRef a, EZefRef b);
        LIBZEF_DLL_EXPORTED EZefRef relation(EZefRef a, RelationType rel, EZefRef b);
        LIBZEF_DLL_EXPORTED EZefRefs relations(EZefRef a, EZefRef b);
        LIBZEF_DLL_EXPORTED EZefRefs relations(EZefRef a, RelationType rel, EZefRef b);


        //                                     _       _                           
		//                         _ __   ___ | |_    (_)_ __                      
		//     _____ _____ _____  | '_ \ / _ \| __|   | | '_ \   _____ _____ _____ 
		//    |_____|_____|_____| | | | | (_) | |_    | | | | | |_____|_____|_____|
		//                        |_| |_|\___/ \__|___|_|_| |_|                    
		//                                       |_____|                           
		struct LIBZEF_DLL_EXPORTED NotIn {
		};
		constexpr NotIn not_in;




		//                                __ _ _ _                                       
		//                               / _(_) | |_ ___ _ __                            
		//    _____ _____ _____ _____   | |_| | | __/ _ \ '__|   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  |  _| | | ||  __/ |     |_____|_____|_____|_____|
		//                              |_| |_|_|\__\___|_|                              
		//                                                                             
		struct LIBZEF_DLL_EXPORTED Filter {
			bool not_in_activated = false;
			std::function<bool(EZefRef)> predicate_fct_uzefref = {};
			std::function<bool(ZefRef)> predicate_fct_zefref = {};

			// -------------------- various functions to init filter object --------------------------
			Filter operator[] (EntityType my_entity_type) const ;
			Filter operator[] (RelationType my_relation_type) const ;
			// Filter operator[] (AttributeEntityType my_entity_type) const ;
			Filter operator[] (BlobType my_BlobType) const ;			
			Filter operator[] (Tensor<RelationType, 1> my_relation_types) const;
			Filter operator[] (Tensor<BlobType, 1> my_blob_types) const;
			Filter operator[] (std::function<bool(EZefRef)> user_defined_predicate_fct) const ;
			Filter operator[] (std::function<bool(ZefRef)> user_defined_predicate_fct) const ;
			Filter operator[] (NotIn my_not_in) const ;

			// -------------------- actual function called with list --------------------------
			EZefRefs operator() (const EZefRefs& candidate_list) const ;
			ZefRefs operator() (const ZefRefs& candidate_list) const ;
		};
		const Filter filter;
		inline EZefRefs operator| (const EZefRefs& uzrs, const Filter& op) { return op(uzrs); }
		inline ZefRefs operator| (const ZefRefs& zrs, const Filter& op) { return op(zrs); }









		//                                          _                         
		//                           ___  ___  _ __| |_                       
		//    _____ _____ _____     / __|/ _ \| '__| __|    _____ _____ _____ 
		//   |_____|_____|_____|    \__ \ (_) | |  | |_    |_____|_____|_____|
		//                          |___/\___/|_|   \__|                      
		//                                                                    
		struct LIBZEF_DLL_EXPORTED Sort {		
			std::variant<
				//internals::Sentinel,
				std::function<bool(EZefRef, EZefRef)>,
				std::function<bool(ZefRef, ZefRef)> 
			> ordering_fct = std::function<bool(ZefRef, ZefRef)>([](ZefRef z1, ZefRef z2) { return index(z1) < index(z2); });

			//std::function<bool(ZefRef, ZefRef)> user_defined_ordering_fct = [](ZefRef z1, ZefRef z2) { return index(z1) < index(z2); };

			// -------------------- various functions to init sort object --------------------------			
			Sort operator[] (std::function<bool(ZefRef, ZefRef)> user_defined_ordering_fct) const;
			Sort operator[] (std::function<bool(EZefRef, EZefRef)> user_defined_ordering_fct) const;

			Sort operator[] (std::function<int(ZefRef)> user_defined_ordering_fct) const;
			Sort operator[] (std::function<int(EZefRef)> user_defined_ordering_fct) const;
			// -------------------- actual function called with list --------------------------
			ZefRefs operator() (ZefRefs zrs) const;
			EZefRefs operator() (EZefRefs zrs) const;
			//ZefRefs operator() (ZefRefs&& zrs) const; //TODO
		};
		const Sort sort;
		inline ZefRefs operator| (const ZefRefs zrs, const Sort& op) { return op(zrs); }
		inline EZefRefs operator| (const EZefRefs zrs, const Sort& op) { return op(zrs); }
		//inline ZefRefs operator| (ZefRefs&& zrs, const Sort& op) { return op(std::move(zrs)); }






		//                                           _                                             
		//                               _   _ _ __ (_) __ _ _   _  ___                            
		//    _____ _____ _____ _____   | | | | '_ \| |/ _` | | | |/ _ \   _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  | |_| | | | | | (_| | |_| |  __/  |_____|_____|_____|_____|
		//                               \__,_|_| |_|_|\__, |\__,_|\___|                           
		//                                                |_|                               

		struct LIBZEF_DLL_EXPORTED Unique {
			//ZefRefs operator() (const ZefRefs& zrs) const;
			EZefRefs operator() (EZefRefs uzrs) const;   // pass by value: fct needs to make copy in any case
			ZefRefs operator() (ZefRefs zrs) const;   // pass by value: fct needs to make copy in any case
			//ZefRefs operator() (ZefRefs&& zrs) const;   // TODO
		};
		constexpr Unique unique;		
		inline EZefRefs operator| (const EZefRefs& zrs, const Unique& op) { return op(zrs); }
		inline ZefRefs operator| (const ZefRefs& zrs, const Unique& op) { return op(zrs); }





		//                           _       _                          _                           
		//                          (_)_ __ | |_ ___ _ __ ___  ___  ___| |_                         
		//    _____ _____ _____     | | '_ \| __/ _ \ '__/ __|/ _ \/ __| __|      _____ _____ _____ 
		//   |_____|_____|_____|    | | | | | ||  __/ |  \__ \  __/ (__| |_      |_____|_____|_____|
		//                          |_|_| |_|\__\___|_|  |___/\___|\___|\__|                        
		//                                                                                    

		struct LIBZEF_DLL_EXPORTED Intersect {
			//EZefRefs operator() (EZefRefs zrs) const;
			//ZefRefs operator() (ZefRefs zrs) const;
			ZefRefs operator() (const ZefRefs& zrs1, const ZefRefs& zrs2) const;
			EZefRefs operator() (const EZefRefs& uzrs1, const EZefRefs& uzrs2) const;
		};
		const Intersect intersect;
		//inline EZefRefs operator| (const EZefRefs& zrs, const Intersect& op) { return op(zrs); }
		//inline ZefRefs operator| (const ZefRefs& zrs, const Intersect& op) { return op(zrs); }

        //////////////////////////////
        // * SetUnion

		struct LIBZEF_DLL_EXPORTED SetUnion {
			ZefRefs operator() (const ZefRefs& zrs1, const ZefRefs& zrs2) const;
			EZefRefs operator() (const EZefRefs& uzrs1, const EZefRefs& uzrs2) const;
		};
		const SetUnion set_union;

        //////////////////////////////
        // * Concatenate

		struct LIBZEF_DLL_EXPORTED Concatenate {
			ZefRefs operator() (const ZefRefs& zrs1, const ZefRefs& zrs2) const;
			EZefRefs operator() (const EZefRefs& uzrs1, const EZefRefs& uzrs2) const;
		};
		const Concatenate concatenate;

        //////////////////////////////
        // * Without

		struct LIBZEF_DLL_EXPORTED Without {
            // TODO: Allow this to have the second argument curried in
            struct Sentinel {};
            using type_variant = std::variant<Sentinel, ZefRefs, EZefRefs>;

            type_variant removing{ Sentinel {} };  

            Without operator[](const ZefRefs& zrs) const {return Without{zrs};};
            Without operator[](const EZefRefs& uzrs) const {return Without{uzrs};};

			ZefRefs operator() (const ZefRefs& original, const ZefRefs& to_remove) const;
			EZefRefs operator() (const EZefRefs& original, const EZefRefs& to_remove) const;
			ZefRefs operator() (const ZefRefs& original) const;
			EZefRefs operator() (const EZefRefs& original) const;
		};
		const Without without;
		inline ZefRefs operator| (const ZefRefs& zrs, const Without& op) { return op(zrs); }
		inline EZefRefs operator| (const EZefRefs& uzrs, const Without& op) { return op(uzrs); }





		//                                __ _       _   _                                        
		//                               / _| | __ _| |_| |_ ___ _ __                             
		//    _____ _____ _____ _____   | |_| |/ _` | __| __/ _ \ '_ \    _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|  |  _| | (_| | |_| ||  __/ | | |  |_____|_____|_____|_____|
		//                              |_| |_|\__,_|\__|\__\___|_| |_|                           
		//                                                                                       
		struct LIBZEF_DLL_EXPORTED Flatten {
 			// EZefRefs operator() (const EZefRefss& uzrss) {
            EZefRefs operator() (EZefRefss& uzrss) const ;

 			// ZefRefs operator() (const ZefRefss& zrss) {
            ZefRefs operator() (ZefRefss& zrss) const ;


		};
		constexpr Flatten flatten;
		// inline EZefRefs operator| (const EZefRefss& uzrss, Flatten fl) { return fl(uzrss); }
		inline EZefRefs operator| (EZefRefss& uzrss, const Flatten fl) { return fl(uzrss); }
		inline ZefRefs operator| (ZefRefss& zrss, const Flatten fl) { return fl(zrss); }


		//                                                _       _                  _   _                         
		//                         __ _ ___ ___  ___ _ __| |_    | | ___ _ __   __ _| |_| |__                      
		//    _____ _____ _____   / _` / __/ __|/ _ \ '__| __|   | |/ _ \ '_ \ / _` | __| '_ \   _____ _____ _____ 
		//   |_____|_____|_____| | (_| \__ \__ \  __/ |  | |_    | |  __/ | | | (_| | |_| | | | |_____|_____|_____|
		//                        \__,_|___/___/\___|_|   \__|___|_|\___|_| |_|\__, |\__|_| |_|                    
		//                                                  |_____|            |___/                               
		struct LIBZEF_DLL_EXPORTED AssertLength {
			int length = 0;
			AssertLength operator[] (int _length) const {
				if (_length < 0) throw std::runtime_error("AssertLength[len] initialized with negative length, which is invalid");
				return AssertLength{ _length };
			}

			EZefRefs operator() (EZefRefs&& uzrs) const {
				if (uzrs.len != length) {
                    std::stringstream ss;
                    ss << "length of EZefRefs passed to AssertLength (" << uzrs.len << ") does not agree with expected length (" << length << ")";
                    throw std::runtime_error(ss.str());
                }
                return uzrs;
			}

			EZefRefs operator() (const EZefRefs& uzrs) const {
				if (uzrs.len != length) {
                    std::stringstream ss;
                    ss << "length of EZefRefs passed to AssertLength (" << uzrs.len << ") does not agree with expected length (" << length << ")";
                    throw std::runtime_error(ss.str());
                }
                return uzrs;
			}

			ZefRefs operator() (const ZefRefs& zrs) const {
				if (zrs.len != length) {
                    std::stringstream ss;
                    ss << "length of EZefRefs passed to AssertLength (" << zrs.len << ") does not agree with expected length (" << length << ")";
                    throw std::runtime_error(ss.str());
                }
                return zrs;
			}

			ZefRefs operator() (ZefRefs&& zrs) const {
				if (zrs.len != length) {
                    std::stringstream ss;
                    ss << "length of EZefRefs passed to AssertLength (" << zrs.len << ") does not agree with expected length (" << length << ")";
                    throw std::runtime_error(ss.str());
                }
                return zrs;
			}
		};
		constexpr AssertLength assert_length;
		inline EZefRefs operator| (const EZefRefs& uzrs, AssertLength al) { return al(uzrs); }
		inline ZefRefs operator| (const ZefRefs& zrs, AssertLength al) { return al(zrs); }



		//                         __ _          _                       
		//                        / _(_)_ __ ___| |_                     
		//    _____ _____ _____  | |_| | '__/ __| __|  _____ _____ _____ 
		//   |_____|_____|_____| |  _| | |  \__ \ |_  |_____|_____|_____|
		//                       |_| |_|_|  |___/\__|                    
		//                                                               
		struct LIBZEF_DLL_EXPORTED First {
			EZefRef operator() (const EZefRefs& uzrs) const {
                if(uzrs.len == 0)
                    throw std::runtime_error("Cannot get first item of empty EZefRefs");
				return uzrs[0];
			}

			ZefRef operator() (const ZefRefs& zrs) const {
                if(zrs.len == 0)
                    throw std::runtime_error("Cannot get first item of empty ZefRefs");
				return zrs[0];
			}
		};
		constexpr First first;
		inline EZefRef operator| (const EZefRefs& uzrs, First f) { return f(uzrs); }
		inline ZefRef operator| (const ZefRefs& zrs, First f) { return f(zrs); }








		//                        _           _                       
		//                       | | __ _ ___| |_                     
		//    _____ _____ _____  | |/ _` / __| __|  _____ _____ _____ 
		//   |_____|_____|_____| | | (_| \__ \ |_  |_____|_____|_____|
		//                       |_|\__,_|___/\__|                    
		//                                                        
		struct LIBZEF_DLL_EXPORTED Last {
			EZefRef operator() (const EZefRefs& uzrs) const {
                if(uzrs.len == 0)
                    throw std::runtime_error("Cannot get last item of empty EZefRefs");
				return uzrs[uzrs.len - 1];
			}

			ZefRef operator() (const ZefRefs& zrs) const {
                if(zrs.len == 0)
                    throw std::runtime_error("Cannot get last item of empty ZefRefs");
				return zrs[zrs.len - 1];
			}
		};
		constexpr Last last;
		inline EZefRef operator| (const EZefRefs& uzrs, Last la) { return la(uzrs); }
		inline ZefRef operator| (const ZefRefs& zrs, Last la) { return la(zrs); }



		//                                     _                            
		//                          ___  _ __ | |_   _                      
		//    _____ _____ _____    / _ \| '_ \| | | | |   _____ _____ _____ 
		//   |_____|_____|_____|  | (_) | | | | | |_| |  |_____|_____|_____|
		//                         \___/|_| |_|_|\__, |                     
		//                                       |___/                      
		struct LIBZEF_DLL_EXPORTED Only {
			EZefRef operator() (const EZefRefs& uzrs) const { 
				if (uzrs.len != 1)
					throw std::runtime_error("Only(EZefRefs zs) request, but length was " + to_str(uzrs.len));
				return uzrs | first; 
			}
			ZefRef operator() (const ZefRefs& zrs) const { 
				if (zrs.len != 1)
					throw std::runtime_error("Only(ZefRefs zs) request, but length was " + to_str(zrs.len));
				return zrs | first; 
			}

			//ZefRefs operator() (const ZefRefss& zrs) const;   TODO
		};
		constexpr Only only;
		inline EZefRef operator| (const EZefRefs& uzrs, Only f) { return f(uzrs); }
		inline ZefRef operator| (const ZefRefs& zrs, Only f) { return f(zrs); }




		//                                      _                                          
		//                           ___  _ __ | |_   _     ___  _ __                      
		//    _____ _____ _____     / _ \| '_ \| | | | |   / _ \| '__|   _____ _____ _____ 
		//   |_____|_____|_____|   | (_) | | | | | |_| |  | (_) | |     |_____|_____|_____|
		//                          \___/|_| |_|_|\__, |___\___/|_|                        
		//                                        |___/_____|                              
		struct LIBZEF_DLL_EXPORTED OnlyOr {
			std::variant<
				Sentinel,
				std::function<ZefRef(ZefRefs)>,
				std::function<EZefRef(EZefRefs)>
			> fct_to_computer_alternative = Sentinel();


			OnlyOr operator[] (std::function<ZefRef(ZefRefs)> compute_alternative) const { return OnlyOr{ compute_alternative }; }
			OnlyOr operator[] (std::function<EZefRef(EZefRefs)> compute_alternative) const { return OnlyOr{ compute_alternative }; }

			ZefRef operator() (const ZefRefs& zrs) const {
				if (!std::holds_alternative<std::function<ZefRef(ZefRefs)>>(fct_to_computer_alternative))
					throw std::runtime_error("only_or called on a ZefRefs, but the function to calc the alternative provided on the fly was not of type ZefRefs->ZefRef");
				return length(zrs) == 1 ? zrs | first : std::get<std::function<ZefRef(ZefRefs)>>(fct_to_computer_alternative)(zrs);
			}

			//ZefRefs operator() (const ZefRefss& zrs) const;   TODO
		};
		const OnlyOr only_or;
		//inline EZefRef operator| (const EZefRefs& uzrs, OnlyOr f) { return f(uzrs); }
		inline ZefRef operator| (const ZefRefs& zrs, OnlyOr f) { return f(zrs); }



		//                             _                           _                       
		//                         ___| | ___ _ __ ___   ___ _ __ | |_                     
		//    _____ _____ _____   / _ \ |/ _ \ '_ ` _ \ / _ \ '_ \| __|  _____ _____ _____ 
		//   |_____|_____|_____| |  __/ |  __/ | | | | |  __/ | | | |_  |_____|_____|_____|
		//                        \___|_|\___|_| |_| |_|\___|_| |_|\__|                    
		//                                                                               
		struct LIBZEF_DLL_EXPORTED Element {
			const int n = 0;    // negative means: count from back

			Element operator[] (int n_) const {
				return Element{ n_ };
			}

			EZefRef operator() (const EZefRefs& uzrs) const {
				int ll = length(uzrs);
				if (abs(n) >= ll) { throw std::runtime_error("index n out of range in 'element[n]' for ZefRefs"); }
				return n >= 0 ? uzrs[n] : uzrs[ll+n];		// if n is negative: count from back. zrs | element[-1]   is the very last one
			}

			ZefRef operator() (const ZefRefs& uzrs) const {
				int ll = length(uzrs);
				if (abs(n) >= ll) { throw std::runtime_error("index n out of range in 'element[n]' for EZefRefs"); }
				return n >= 0 ? uzrs[n] : uzrs[ll+n];		// if n is negative: count from back. zrs | element[-1]   is the very last one
			}
		};
		const Element element;



		//                          _        _                             
		//                         | |_ __ _| | _____                      
		//    _____ _____ _____    | __/ _` | |/ / _ \   _____ _____ _____ 
		//   |_____|_____|_____|   | || (_| |   <  __/  |_____|_____|_____|
		//                          \__\__,_|_|\_\___|                     
		//                                                                 
		struct LIBZEF_DLL_EXPORTED Take {
			const int number_of_elements = 0;  //TODO: negative numbers indicate: take from the back

			Take operator[] (int number_of_elements) const {				
				return Take{ number_of_elements };
			}

			EZefRefs operator() (const EZefRefs& uzrs) const {
				auto u_len = length(uzrs);
				EZefRefs uzrs_cpy(uzrs);    	// TODO: this is silly and inefficient. Only done, because we don't have const iterators implemented.
				if (number_of_elements >= 0) {
					if (uzrs_cpy.delegate_ptr == nullptr) {
						uzrs_cpy.len = std::min(number_of_elements, u_len);
					}
					else {
						uzrs_cpy.len = std::min(number_of_elements, u_len);   //  TODO: this can be removed once all other zefops look at the right length. independent of SSO
						uzrs_cpy.delegate_ptr->len = std::min(number_of_elements, u_len);
					}
					return uzrs_cpy;
				}
				else {
					std::vector<EZefRef> tmp;
					tmp.reserve(-number_of_elements);
					for (auto it = std::begin(uzrs_cpy) + std::max(0, u_len - (-number_of_elements)); it != std::end(uzrs_cpy); it++)
						tmp.emplace_back(*it);
					return EZefRefs(tmp);
				}
			}


			// todo: check the r-value versions below at some point, for now make sure the other ones are working without any problems. !!!!!!!!!!!
			//EZefRefs operator() (EZefRefs&& uzrs) const {
			//	if (number_of_elements >= 0) {
			//		uzrs.len = std::min(number_of_elements, uzrs.len);
			//		return uzrs;
			//	}
			//	else {
			//		return operator() (uzrs);   // uzrs is an l-value in here and this will bind to the function above
			//	}
			//}

			ZefRefs operator() (const ZefRefs& zrs) const {
				auto u_len = length(zrs);
				ZefRefs zrs_cpy(zrs);			// TODO: this is silly and inefficient. Only done, because we don't have const iterators implemented.
				if (number_of_elements >= 0) {
					if (zrs_cpy.delegate_ptr == nullptr){
						zrs_cpy.len = std::min(number_of_elements, u_len);
					}
					else {
						zrs_cpy.len = std::min(number_of_elements, u_len);   //  TODO: this can be removed once all other zefops look at the right length. independent of SSO
						zrs_cpy.delegate_ptr->len = std::min(number_of_elements, u_len);
					}
					return zrs_cpy;
				}
				else {
					std::vector<ZefRef> tmp;
					tmp.reserve(-number_of_elements);
					for (auto it = std::begin(zrs_cpy) + std::max(0, u_len - (-number_of_elements)); it != std::end(zrs_cpy); it++)
						tmp.emplace_back(*it);
					return ZefRefs(tmp);
				}
			}

			//ZefRefs operator() (ZefRefs&& zrs) const {
			//	if (number_of_elements >= 0) {
			//		zrs.len = std::min(number_of_elements, zrs.len);
			//		return zrs;
			//	}
			//	else {
			//		return operator() (zrs);   // zrs is an l-value in here and this will bind to the function above
			//	}
			//}
		};
		constexpr Take take;
		inline EZefRefs operator| (const EZefRefs& uzrs, Take f) { return f(uzrs); }
		inline ZefRefs operator| (const ZefRefs& zrs, Take f) { return f(zrs); }





		//                                   _     _                _                        
		//                          _____  _(_)___| |_ ___     __ _| |_                      
		//    _____ _____ _____    / _ \ \/ / / __| __/ __|   / _` | __|   _____ _____ _____ 
		//   |_____|_____|_____|  |  __/>  <| \__ \ |_\__ \  | (_| | |_   |_____|_____|_____|
		//                         \___/_/\_\_|___/\__|___/___\__,_|\__|                     
		//                                               |_____|                             
		struct LIBZEF_DLL_EXPORTED ExistsAt {
			TimeSlice time_slice = { 0 };

			ExistsAt operator[] (EZefRef tx_node) const ;
			ExistsAt operator[] (ZefRef tx_node) const { return operator[](tx_node.blob_uzr); };
			ExistsAt operator[] (TimeSlice ts_number) const ;

			// this operator should only be used for EZefRefs that can be promoted to ZefRefs: AtomicEntity, Entity, Relation, TX_Event
			bool operator() (EZefRef rel_ent) const ;
			bool operator() (ZefRef rel_ent) const ;


			// TODO: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! return a vector of bool values (e.g. std::vector<bool> ) or a ZefRefs with vlaue nodes  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			//EZefRefs operator() (const EZefRefs& uzrs) const {

			// N.b. for a ZefRef it only makes sense for it to refer to a EZefRef at a given graph-time point, iff it exists there
			// i.e. the time slice baked into a ZefRef does not add any value when asking whether the EZefRef exists at a given time
		};
		constexpr ExistsAt exists_at;
		inline bool operator| (const EZefRef& uzr, ExistsAt f) { return f(uzr); }
		inline bool operator| (const ZefRef& zr, ExistsAt f) { return f(zr); }




		struct LIBZEF_DLL_EXPORTED AllowTerminatedRelentPromotion {};
		constexpr AllowTerminatedRelentPromotion allow_terminated_relent_promotion;

		//                         _                    __           __                     
		//                        | |_ ___     _______ / _|_ __ ___ / _|                    
		//    _____ _____ _____   | __/ _ \   |_  / _ \ |_| '__/ _ \ |_   _____ _____ _____ 
		//   |_____|_____|_____|  | || (_) |   / /  __/  _| | |  __/  _| |_____|_____|_____|
		//                         \__\___/___/___\___|_| |_|  \___|_|                      
		//                               |_____|                                                                
		struct LIBZEF_DLL_EXPORTED ToZefRef {
			EZefRef reference_tx = EZefRef{ nullptr };
			bool _allow_terminated_relent_promotion = false;

			ToZefRef operator[] (EZefRef _reference_tx) const ;
			ToZefRef operator[] (ZefRef _reference_tx) const { return operator[](_reference_tx.blob_uzr); };			
			ToZefRef operator[] (AllowTerminatedRelentPromotion op) const { return ToZefRef{ reference_tx, true }; };

			ZefRef operator() (EZefRef uzr_to_promote) const ;

			ZefRef operator() (ZefRef zr_to_promote) const ;

			ZefRefs operator() (const EZefRefs& uzrs_to_promote) const ;
			ZefRefs operator() (ZefRefs&& zrs) const;
			ZefRefs operator() (const ZefRefs& zrs) const;
		};
		const ToZefRef to_zefref;  //TODO: modify EZefRef that one can create 'constexpr my_uzefref' and make this constexpr too
		inline ZefRef operator| (EZefRef uzr, ToZefRef op) { return op(uzr); }
		inline ZefRefs operator| (const EZefRefs& uzrs, ToZefRef op) { return op(uzrs); }

		inline ZefRef operator| (ZefRef zr, ToZefRef op) { return op(zr); }
		inline ZefRefs operator| (ZefRefs&& zrs, ToZefRef op) { return op(std::move(zrs)); }
		inline ZefRefs operator| (const ZefRefs& zrs, ToZefRef op) { return op(zrs); }


		inline std::ostream& operator<< (std::ostream& os, const ToZefRef& op) {
			os << "ToZefRef(reference_tx="<< op.reference_tx << ", allow_terminated_relent_promotion="<< op._allow_terminated_relent_promotion<< ")";
			return os;
		}



		//                         _                         __           __                     
		//                        | |_ ___    _   _ _______ / _|_ __ ___ / _|                    
		//    _____ _____ _____   | __/ _ \  | | | |_  / _ \ |_| '__/ _ \ |_   _____ _____ _____ 
		//   |_____|_____|_____|  | || (_) | | |_| |/ /  __/  _| | |  __/  _| |_____|_____|_____|
		//                         \__\___/___\__,_/___\___|_| |_|  \___|_|                      
		//                               |_____|                                                 
		struct LIBZEF_DLL_EXPORTED ToEZefRef {
			EZefRef operator() (ZefRef zr) const { return zr.blob_uzr; }
			EZefRef operator() (EZefRef uzr) const { return uzr; }   // idempotent
			EZefRefs operator() (const EZefRefs& uzrs) const { return uzrs; }
			EZefRefs operator() (EZefRefs&& uzrs) const { return uzrs; }

			EZefRefs operator() (const ZefRefs& zrs) const {
				int len = zrs.delegate_ptr == nullptr ? zrs.len : zrs.delegate_ptr->len;
				auto res = EZefRefs(
					len,
					graph_data(zrs)
				);
				// even in a ZefRefs struct, the various elements are stored as a contiguous list of EZefRefs. The reference tx is stored only once
				std::memcpy(
					res._get_array_begin(),
					zrs._get_array_begin_const(),
					len * sizeof(EZefRef)
				);
				return res;
			}
		};
		constexpr ToEZefRef to_ezefref;
		inline EZefRef operator| (ZefRef zr, ToEZefRef op) { return to_ezefref(zr); }
		inline EZefRef operator| (EZefRef zr, ToEZefRef op) { return to_ezefref(zr); }
		inline EZefRefs operator| (const EZefRefs& zrs, ToEZefRef op) { return to_ezefref(zrs); }
		inline EZefRefs operator| (EZefRefs&& zrs, ToEZefRef op) { return to_ezefref(zrs); }
		
		inline EZefRefs operator| (const ZefRefs& zrs, ToEZefRef op) { return to_ezefref(zrs); }






		//                  _                  __           __                                    _        _     _                         _              _     _                _                 
		//                 (_)___     _______ / _|_ __ ___ / _|   _ __  _ __ ___  _ __ ___   ___ | |_ __ _| |__ | | ___     __ _ _ __   __| |    _____  _(_)___| |_ ___     __ _| |_               
		//    _____ _____  | / __|   |_  / _ \ |_| '__/ _ \ |_   | '_ \| '__/ _ \| '_ ` _ \ / _ \| __/ _` | '_ \| |/ _ \   / _` | '_ \ / _` |   / _ \ \/ / / __| __/ __|   / _` | __|  _____ _____ 
		//   |_____|_____| | \__ \    / /  __/  _| | |  __/  _|  | |_) | | | (_) | | | | | | (_) | || (_| | |_) | |  __/  | (_| | | | | (_| |  |  __/>  <| \__ \ |_\__ \  | (_| | |_  |_____|_____|
		//                 |_|___/___/___\___|_| |_|  \___|_|____| .__/|_|  \___/|_| |_| |_|\___/ \__\__,_|_.__/|_|\___|___\__,_|_| |_|\__,_|___\___/_/\_\_|___/\__|___/___\__,_|\__|              
		//                      |_____|                    |_____|_|                                                  |_____|              |_____|                    |_____|                        
		struct LIBZEF_DLL_EXPORTED IsZefRefPromotableAndExistsAt {
			TimeSlice time_slice = { 0 };

			IsZefRefPromotableAndExistsAt operator[] (EZefRef tx_node) const ;
			IsZefRefPromotableAndExistsAt operator[] (TimeSlice ts_number) const ;

			// this operator should only be used for EZefRefs that can be promoted to ZefRefs: AtomicEntity, Entity, Relation, TX_Event
			bool operator() (EZefRef rel_ent) const ;

			// N.b. for a ZefRef it only makes sense for it to refer to a EZefRef at a given graph-time point, iff it exists there
		};
		constexpr IsZefRefPromotableAndExistsAt is_zefref_promotable_and_exists_at;
		inline bool operator| (EZefRef uzr, IsZefRefPromotableAndExistsAt op) { return op(uzr); }





		//                  _                  __           __                                    _        _     _    
		//                 (_)___     _______ / _|_ __ ___ / _|   _ __  _ __ ___  _ __ ___   ___ | |_ __ _| |__ | | ___ 
		//    _____ _____  | / __|   |_  / _ \ |_| '__/ _ \ |_   | '_ \| '__/ _ \| '_ ` _ \ / _ \| __/ _` | '_ \| |/ _ \  _____ _____ _____ 
		//   |_____|_____| | \__ \    / /  __/  _| | |  __/  _|  | |_) | | | (_) | | | | | | (_) | || (_| | |_) | |  __/ |_____|_____|_____|
		//                 |_|___/___/___\___|_| |_|  \___|_|____| .__/|_|  \___/|_| |_| |_|\___/ \__\__,_|_.__/|_|\___|
		//                      |_____|                    |_____|_|                                                  
		struct LIBZEF_DLL_EXPORTED IsZefRefPromotable {
			bool operator() (EZefRef rel_ent) const {
                return zefDB::find_element<BlobType>({
                        BlobType::RELATION_EDGE,
                        BlobType::ENTITY_NODE,
                        BlobType::ATTRIBUTE_ENTITY_NODE,
                        BlobType::VALUE_NODE,
                        BlobType::TX_EVENT_NODE,
                        BlobType::ROOT_NODE,
                    },
                    get<BlobType>(rel_ent)				
                    );
			}
		};
		constexpr IsZefRefPromotable is_zefref_promotable;
		inline bool operator| (EZefRef uzr, IsZefRefPromotable op) { return is_zefref_promotable(uzr); }



		//                              _     _                     
		//                        _   _(_) __| |                    
		//    _____ _____ _____  | | | | |/ _` |  _____ _____ _____ 
		//   |_____|_____|_____| | |_| | | (_| | |_____|_____|_____|
		//                        \__,_|_|\__,_|                    
		//                                                        

		struct LIBZEF_DLL_EXPORTED Uid {
			BaseUID operator() (const Graph& g) const { return internals::get_graph_uid(g.my_graph_data()); }
			BaseUID operator() (const GraphData& gd) const { return internals::get_graph_uid(gd); }
			EternalUID operator() (const EZefRef uzr) const;
			ZefRefUID operator() (const ZefRef zr) const;
		};
		constexpr Uid uid;
		inline BaseUID operator| (const Graph& g, Uid x) { return x(g); }
		inline BaseUID operator| (GraphData& gd, Uid x) { return x(gd); }
		inline EternalUID operator| (EZefRef uzr, Uid x) { return x(uzr); }
		inline ZefRefUID operator| (ZefRef zr, Uid x) { return x(zr); }
		





		//                       __     __         _                     _     _  __ _           _      ____  _                   _                           
		//                       \ \   / /_ _ _ __(_) ___  _   _ ___    | |   (_)/ _| |_ ___  __| |    / ___|| |_ _ __ _   _  ___| |_ ___                     
		//    _____ _____ _____   \ \ / / _` | '__| |/ _ \| | | / __|   | |   | | |_| __/ _ \/ _` |    \___ \| __| '__| | | |/ __| __/ __|  _____ _____ _____ 
		//   |_____|_____|_____|   \ V / (_| | |  | | (_) | |_| \__ \   | |___| |  _| ||  __/ (_| |     ___) | |_| |  | |_| | (__| |_\__ \ |_____|_____|_____|
		//                          \_/ \__,_|_|  |_|\___/ \__,_|___/   |_____|_|_|  \__\___|\__,_|    |____/ \__|_|   \__,_|\___|\__|___/                    
		//                                                                                                                                               

		// not used as a zefop, no singlton, but return type of lift[only]
		struct LIBZEF_DLL_EXPORTED LiftedOnly_ {
			EZefRef operator() (const EZefRefs& uzrs) const { return only(uzrs); }
			ZefRef operator() (const ZefRefs& zrs) const { return only(zrs); }
			
			ZefRefs operator() (const ZefRefss& zrss) const;
			EZefRefs operator() (const EZefRefss& uzrss) const;
		};
		inline EZefRef operator| (const EZefRefs& uzrs, LiftedOnly_ f) { return only(uzrs); }
		inline ZefRef operator| (const ZefRefs& zrs, LiftedOnly_ f) { return only(zrs); }
		inline ZefRefs operator| (const ZefRefss& zrss, LiftedOnly_ f) { return LiftedOnly_{}(zrss); }
		inline EZefRefs operator| (const EZefRefss& uzrss, LiftedOnly_ f) { return LiftedOnly_{}(uzrss); }
		

		struct LIBZEF_DLL_EXPORTED LiftedFirst_ {
			EZefRef operator() (const EZefRefs& uzrs) const { return first(uzrs); }
			ZefRef operator() (const ZefRefs& zrs) const { return first(zrs); }
			
			ZefRefs operator() (const ZefRefss& zrss) const;
			EZefRefs operator() (const EZefRefss& uzrss) const;
		};
		inline EZefRef operator| (const EZefRefs& uzrs, LiftedFirst_ f) { return first(uzrs); }
		inline ZefRef operator| (const ZefRefs& zrs, LiftedFirst_ f) { return first(zrs); }
		inline ZefRefs operator| (const ZefRefss& zrss, LiftedFirst_ f) { return LiftedFirst_{}(zrss); }
		inline EZefRefs operator| (const EZefRefss& uzrss, LiftedFirst_ f) { return LiftedFirst_{}(uzrss); }
						

		struct LIBZEF_DLL_EXPORTED LiftedLast_ {
			EZefRef operator() (const EZefRefs& uzrs) const { return last(uzrs); }
			ZefRef operator() (const ZefRefs& zrs) const { return last(zrs); }
			
			ZefRefs operator() (const ZefRefss& zrss) const;
			EZefRefs operator() (const EZefRefss& uzrss) const;
		};
		inline EZefRef operator| (const EZefRefs& uzrs, LiftedLast_ f) { return last(uzrs); }
		inline ZefRef operator| (const ZefRefs& zrs, LiftedLast_ f) { return last(zrs); }
		inline ZefRefs operator| (const ZefRefss& zrss, LiftedLast_ f) { return LiftedLast_{}(zrss); }
		inline EZefRefs operator| (const EZefRefss& uzrss, LiftedLast_ f) { return LiftedLast_{}(uzrss); }






		//                        _     _  __ _                       
		//                       | |   (_)/ _| |_                     
		//    _____ _____ _____  | |   | | |_| __|  _____ _____ _____ 
		//   |_____|_____|_____| | |___| |  _| |_  |_____|_____|_____|
		//                       |_____|_|_|  \__|                    
		//                                                       

		struct LIBZEF_DLL_EXPORTED Lift {
			LiftedOnly_ operator[] (Only op) const { return LiftedOnly_{}; }
			LiftedFirst_ operator[] (First op) const { return LiftedFirst_{}; }
			LiftedLast_ operator[] (Last op) const  { return LiftedLast_{}; }
		};
		constexpr Lift lift;




		//                       __   __                        
		//                       \ \ / /__                      
		//    _____ _____ _____   \ V / _ \   _____ _____ _____ 
		//   |_____|_____|_____|   | | (_) | |_____|_____|_____|
		//                         |_|\___/                     
		//                                                      
		struct LIBZEF_DLL_EXPORTED Yo {
			void operator() () const;
			void operator() (str command) const;
		};
		constexpr Yo yo;

		// my_zr >> RT.UsedBy >> yo  //show all outgoing
		// my_zr >> RT.UsedBy >> yo[ET.Machine]		// find some machine nearby




		//                                                  _                                                   
		//                                                 | |                                                  
		//    _____ _____ _____ _____ _____ _____ _____    | |        _____ _____ _____ _____ _____ _____ _____ 
		//   |_____|_____|_____|_____|_____|_____|_____|   | |___    |_____|_____|_____|_____|_____|_____|_____|
		//                                                 |_____|                                              
		//                                                                                                    	
		// a wrapper class to create an interface for lists in zef: e.g. my_product >> L[RT.OperationIn]  returns a list of ZefRef
		struct LIBZEF_DLL_EXPORTED L_Class {
			const std::variant<
				RelationType,
				BlobType,
				Tensor<RelationType, 1>,
				Tensor<BlobType, 1>
                > data = {};
 
			
			L_Class operator[] (RelationType rt) const { return L_Class{ rt }; }
			L_Class operator[] (BlobType bt) const { return L_Class{ bt }; }
			L_Class operator[] (Tensor<RelationType, 1> rts) const { return L_Class{ rts }; }  // allows us to write L[RT.IfPreviousIs, RT.IfNextIs]			
			L_Class operator[] (Tensor<BlobType, 1> bts) const { return L_Class{ bts }; }  // allows us to write L[BT.REL_ENT_INSTANCE, BT.CLONE]			
            L_Class() = default;
		};
		const L_Class L;




		//                               ___                            
		//                              / _ |                           
		//    _____ _____ _____ _____  | | | |  _____ _____ _____ _____ 
		//   |_____|_____|_____|_____| | |_| | |_____|_____|_____|_____|
		//                              \___/                           
		//                                                             
		// Similar to the L class but is a one-or-nothing result, using an optional
		struct LIBZEF_DLL_EXPORTED O_Class {
			const std::variant<
				RelationType,
				BlobType,
				Tensor<RelationType, 1>,
				Tensor<BlobType, 1>
                > data = {};
 
			
			O_Class operator[] (RelationType rt) const { return O_Class{ rt }; }
			O_Class operator[] (BlobType bt) const { return O_Class{ bt }; }
			O_Class operator[] (Tensor<RelationType, 1> rts) const { return O_Class{ rts }; }  // allows us to write L[RT.IfPreviousIs, RT.IfNextIs]			
			O_Class operator[] (Tensor<BlobType, 1> bts) const { return O_Class{ bts }; }  // allows us to write L[BT.RAE_INSTANCE_EDGE, BT.CLONE]			
		};
		const O_Class O;

		

		//                               ____        _       _                               
		//                              | __ )  __ _| |_ ___| |__                            
		//     _____ _____ _____ _____  |  _ \ / _` | __/ __| '_ \   _____ _____ _____ _____ 
		//    |_____|_____|_____|_____| | |_) | (_| | || (__| | | | |_____|_____|_____|_____|
		//                              |____/ \__,_|\__\___|_| |_|                          
		//                                                                                   

		// used as a parameter to be curried into the Q[batch["my_batch_id42"]]  to signal which queued 
		// functions should be executed together within one transaction.
		struct LIBZEF_DLL_EXPORTED Batch {
			const std::optional<str> batch_id = {};		// for now the batch id is any string

			Batch operator[] (str batch_id) const { return Batch{ batch_id }; }
		};
		const Batch batch;



		//                               ___                            
		//                              / _ |                           
		//    _____ _____ _____ _____  | | | |  _____ _____ _____ _____ 
		//   |_____|_____|_____|_____| | |_| | |_____|_____|_____|_____|
		//                              \__\_|                          
		//                                                             
		struct LIBZEF_DLL_EXPORTED Q_Class {
			// queue a task to be executed later in a separate transaction.
			// the function takes the graph to which the data should be written. This is passed in by the queueing system 
			
			// TODO!!!!!!!!!!!!!:       g | Q[inter_graph_linking[true]][my_fct]
			
			Batch batch_info = Batch();
			double execution_priority = 0.5;
			std::function<void(Graph)> fct = {};	// we don't need an std::optional here: std::function supports casting to bool

			Q_Class operator[] (Batch batch_info_) const { return Q_Class{ batch_info_, execution_priority,  fct }; }
			Q_Class operator[] (double execution_priority_) const { return Q_Class{ batch_info, execution_priority_,  fct }; }
			Q_Class operator[] (std::function<void(Graph)> fct_) const { return Q_Class{ batch_info, execution_priority,  fct_ }; }
		};
		const Q_Class Q;

		inline Graph operator| (Graph g, Q_Class q) {
			if (!bool(q.fct)) throw std::runtime_error("A function of type (Graph)->void needs to be assigned to the Q operator before q-ing it on a graph!");
			internals::q_function_on_graph(q.fct, q.execution_priority, g.my_graph_data());
			return g; 
		}





	}  // namespace zefOps

	// -------------- remain within the same level of liftedness ----------------------
	

	using namespace zefOps;
	
	// ------------------------------- EZefRefs -------------------------------------
	inline EZefRef operator> (EZefRef uzr, BlobType bt) {
		try {
			return uzr | outs | filter[bt] | only;
		}
		catch (const std::runtime_error& error) {
			throw std::runtime_error(str("error traversing > from UZR of type ") + to_str("") );
		}
	}

	inline EZefRef operator>> (EZefRef uzr, BlobType bt) {
		return uzr | outs | filter[bt] | only | target;
	}

	inline EZefRef operator< (EZefRef uzr, BlobType bt) {
		return uzr | ins | filter[bt] | only;
	}

	inline EZefRef operator<< (EZefRef uzr, BlobType bt) {
		return uzr | ins | filter[bt] | only | source;
	}
		
	









	// -------------------------------------------------------------------------------

	inline EZefRef operator> (EZefRef uzr, RelationType rt) {
	try{
		return uzr | zefOps::outs | filter[rt] | only;
	}
	catch (const std::runtime_error& error) {
		throw std::runtime_error(str("error traversing > RT." + to_str(rt) + " from UZR with uid ") + to_str(uzr | uid));
	}
	}

	inline EZefRef operator>> (EZefRef uzr, RelationType rt) {
	try{
		return uzr | outs | filter[rt] | only | target;
	}
	catch (const std::runtime_error& error) {
		throw std::runtime_error(str("error traversing >> RT." + to_str(rt) + " from UZR with uid ") + to_str(uzr | uid));
	}
	}

	inline EZefRef operator< (EZefRef uzr, RelationType rt) {
	try{
			return uzr | ins | filter[rt] | only;
	}
	catch (const std::runtime_error& error) {
		throw std::runtime_error(str("error traversing < RT." + to_str(rt) + " from UZR with uid ") + to_str(uzr | uid));
	}
	}

	inline EZefRef operator<< (EZefRef uzr, RelationType rt) {
	try{
		return uzr | ins | filter[rt] | only | source;
	}
	catch (const std::runtime_error& error) {
		throw std::runtime_error(str("error traversing << RT." + to_str(rt) + " from UZR with uid ") + to_str(uzr | uid));
	}
	}



	



	inline EZefRefs operator> (EZefRefs&& uzrs, RelationType rt) {
		for (auto& el : uzrs) el = el > rt;
		return uzrs;
	}
	inline EZefRefs operator> (const EZefRefs& uzrs, RelationType rt) {
		EZefRefs res(uzrs);
		for (auto& el : res) el = el > rt;
		return res;
	}	

	inline EZefRefs operator>> (EZefRefs&& uzrs, RelationType rt) {
		for (auto& el : uzrs) el = el >> rt;
		return uzrs;
	}
	inline EZefRefs operator>> (const EZefRefs& uzrs, RelationType rt) {
		EZefRefs res(uzrs);
		for (auto& el : res) el = el >> rt;
		return res;
	}	

	inline EZefRefs operator< (EZefRefs&& uzrs, RelationType rt) {
		for (auto& el : uzrs) el = el < rt;
		return uzrs;
	}
	inline EZefRefs operator< (const EZefRefs& uzrs, RelationType rt) {
		EZefRefs res(uzrs);
		for (auto& el : res) el = el < rt;
		return res;
	}

	inline EZefRefs operator<< (EZefRefs&& uzrs, RelationType rt) {
		for (auto& el : uzrs) el = el << rt;
		return uzrs;
	}
	inline EZefRefs operator<< (const EZefRefs& uzrs, RelationType rt) {
		EZefRefs res(uzrs);
		for (auto& el : res) el = el << rt;
		return res;
	}









	// ------------------------------- ZefRefs -------------------------------------
	inline ZefRef operator> (ZefRef zr, RelationType rt) {
	try{
			return zr | outs | filter[rt] | only;
	}
	catch (const std::runtime_error& error) {
		throw std::runtime_error(str("error traversing > RT." + to_str(rt) + " from ZR with uid ") + to_str(zr | uid));
	}
	}

	inline ZefRef operator>> (ZefRef zr, RelationType rt) {
	try{
		return zr | outs | filter[rt] | only | target;
	}
	catch (const std::runtime_error& error) {
		throw std::runtime_error(str("error traversing >> RT." + to_str(rt) + " from ZR with uid ") + to_str(zr|uid));
	}
	}

	inline ZefRef operator< (ZefRef zr, RelationType rt) {
	try{
		return zr | ins | filter[rt] | only;
	}
	catch (const std::runtime_error& error) {
		throw std::runtime_error(str("error traversing < RT." + to_str(rt) + " from ZR with uid ") + to_str(zr | uid));
	}
	}

	inline ZefRef operator<< (ZefRef zr, RelationType rt) {
	try{
		return zr | ins | filter[rt] | only | source;
	}
	catch (const std::runtime_error& error) {
		throw std::runtime_error(str("error traversing << RT." + to_str(rt) + " from ZR with uid ") + to_str(zr | uid));
	}
	}





	inline ZefRefs operator> (ZefRefs&& zrs, RelationType rt) {
		std::transform(
			zrs._get_array_begin_const(),
			zrs._get_array_begin_const() + zrs.len,
			zrs._get_array_begin(),
			[&](const EZefRef& uzr) {return (ZefRef{ uzr, zrs.reference_frame_tx } > rt).blob_uzr; }
		);
		return zrs;
	}
	inline ZefRefs operator> (const ZefRefs& zrs, RelationType rt) {
		auto res = ZefRefs(zrs.len,	zrs.reference_frame_tx);
		std::transform(
			zrs._get_array_begin_const(),
			zrs._get_array_begin_const() + zrs.len,
			res._get_array_begin(),
			[&](const EZefRef& uzr) {return (ZefRef{uzr, zrs.reference_frame_tx } > rt).blob_uzr; }
		);
		return res;
	}

	inline ZefRefs operator>> (ZefRefs&& zrs, RelationType rt) {
		std::transform(
			zrs._get_array_begin_const(),
			zrs._get_array_begin_const() + zrs.len,
			zrs._get_array_begin(),
			[&](const EZefRef& uzr) {return (ZefRef{ uzr, zrs.reference_frame_tx } >> rt).blob_uzr; }
		);
		return zrs;
	}
	inline ZefRefs operator>> (const ZefRefs& zrs, RelationType rt) {
		auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
		std::transform(
			zrs._get_array_begin_const(),
			zrs._get_array_begin_const() + zrs.len,
			res._get_array_begin(),
			[&](const EZefRef& uzr) {return (ZefRef{ uzr, zrs.reference_frame_tx } >> rt).blob_uzr; }
		);
		return res;
	}

	inline ZefRefs operator< (ZefRefs&& zrs, RelationType rt) {
		std::transform(
			zrs._get_array_begin_const(),
			zrs._get_array_begin_const() + zrs.len,
			zrs._get_array_begin(),
			[&](const EZefRef& uzr) {return (ZefRef{ uzr, zrs.reference_frame_tx } < rt).blob_uzr; }
		);
		return zrs;
	}
	inline ZefRefs operator< (const ZefRefs& zrs, RelationType rt) {
		auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
		std::transform(
			zrs._get_array_begin_const(),
			zrs._get_array_begin_const() + zrs.len,
			res._get_array_begin(),
			[&](const EZefRef& uzr) {return (ZefRef{ uzr, zrs.reference_frame_tx } < rt).blob_uzr; }
		);
		return res;
	}

	inline ZefRefs operator<< (ZefRefs&& zrs, RelationType rt) {
		std::transform(
			zrs._get_array_begin_const(),
			zrs._get_array_begin_const() + zrs.len,
			zrs._get_array_begin(),
			[&](const EZefRef& uzr) {return (ZefRef{ uzr, zrs.reference_frame_tx } << rt).blob_uzr; }
		);
		return zrs;
	}
	inline ZefRefs operator<< (const ZefRefs& zrs, RelationType rt) {
		auto res = ZefRefs(zrs.len, zrs.reference_frame_tx);
		std::transform(
			zrs._get_array_begin_const(),
			zrs._get_array_begin_const() + zrs.len,
			res._get_array_begin(),
			[&](const EZefRef& uzr) {return (ZefRef{ uzr, zrs.reference_frame_tx } << rt).blob_uzr; }
		);
		return res;
	}
















	inline EZefRefs operator> (EZefRef uzr, const L_Class& my_L) {		
		return std::visit([uzr](const auto& var)->EZefRefs { return uzr | outs | filter[var]; }, my_L.data);
	}

	inline EZefRefs operator>> (EZefRef uzr, L_Class my_L) {
		return std::visit([uzr](const auto& var)->EZefRefs { return uzr | outs | filter[var] | target; }, my_L.data);
	}

	inline EZefRefs operator< (EZefRef uzr, L_Class my_L) {
		return std::visit([uzr](const auto& var)->EZefRefs { return uzr | ins | filter[var]; }, my_L.data);
	}

	inline EZefRefs operator<< (EZefRef uzr, L_Class my_L) {
		return std::visit([uzr](const auto& var)->EZefRefs { return uzr | ins | filter[var] | source; }, my_L.data);
	}


    inline auto list_to_optional(EZefRefs zrs) {
        if (length(zrs) == 0)
            return std::optional<EZefRef>{};
        else if (length(zrs) == 1)
            return std::optional<EZefRef>(zrs | only);
        else
            throw std::runtime_error("More than one item found for O_Class");
    }

	inline std::optional<EZefRef> operator> (EZefRef uzr, const O_Class& my_O) {		
		return std::visit([uzr](const auto& var)->std::optional<EZefRef> { return list_to_optional(uzr > L[var]); }, my_O.data);
	}

	inline std::optional<EZefRef> operator>> (EZefRef uzr, O_Class my_O) {
		return std::visit([uzr](const auto& var)->std::optional<EZefRef> { return list_to_optional(uzr >> L[var]); }, my_O.data);
	}

	inline std::optional<EZefRef> operator< (EZefRef uzr, O_Class my_O) {
		return std::visit([uzr](const auto& var)->std::optional<EZefRef> { return list_to_optional(uzr < L[var]); }, my_O.data);
	}

	inline std::optional<EZefRef> operator<< (EZefRef uzr, O_Class my_O) {
		return std::visit([uzr](const auto& var)->std::optional<EZefRef> { return list_to_optional(uzr << L[var]); }, my_O.data);
	}





	// ------------------------------- ZefRefs -------------------------------------
		
	inline ZefRefs operator> (ZefRef zr, L_Class my_L) {
		return std::visit([zr](const auto& var)->ZefRefs { return zr | outs | filter[var]; }, my_L.data);
	}

	inline ZefRefs operator>> (ZefRef zr, L_Class my_L) {
		return std::visit([zr](const auto& var)->ZefRefs { return zr | outs | filter[var] | target; }, my_L.data);
	}

	inline ZefRefs operator< (ZefRef zr, L_Class my_L) {
		return std::visit([zr](const auto& var)->ZefRefs { return zr | ins | filter[var]; }, my_L.data);
	}

	inline ZefRefs operator<< (ZefRef zr, L_Class my_L) {
		return std::visit([zr](const auto& var)->ZefRefs { return zr | ins | filter[var] | source; }, my_L.data);
	}


    inline auto list_to_optional(ZefRefs zrs) {
        if (length(zrs) == 0)
            return std::optional<ZefRef>{};
        else if (length(zrs) == 1)
            return std::optional<ZefRef>(zrs | only);
        else
            throw std::runtime_error("More than one item found for O_Class");
    }

	inline std::optional<ZefRef> operator> (ZefRef zr, const O_Class& my_O) {		
		return std::visit([zr](const auto& var)->std::optional<ZefRef> { return list_to_optional(zr > L[var]); }, my_O.data);
	}

	inline std::optional<ZefRef> operator>> (ZefRef zr, O_Class my_O) {
		return std::visit([zr](const auto& var)->std::optional<ZefRef> { return list_to_optional(zr >> L[var]); }, my_O.data);
	}

	inline std::optional<ZefRef> operator< (ZefRef zr, O_Class my_O) {
		return std::visit([zr](const auto& var)->std::optional<ZefRef> { return list_to_optional(zr < L[var]); }, my_O.data);
	}

	inline std::optional<ZefRef> operator<< (ZefRef zr, O_Class my_O) {
		return std::visit([zr](const auto& var)->std::optional<ZefRef> { return list_to_optional(zr << L[var]); }, my_O.data);
	}





	// TODO: !!!!!!!!!!!! make operators below accept "const EZefRefs& uzrs": requires change in iterators? !!!!!!!!!!!!!!!!

	inline EZefRefss operator> (EZefRefs& uzrs, L_Class my_L) {
		EZefRefss res(length(uzrs));
		for (auto el : uzrs) res.v.emplace_back(el > my_L);		
		return res;
	}

	inline EZefRefss operator>> (EZefRefs& uzrs, L_Class my_L) {
		EZefRefss res(length(uzrs));
		for (auto el : uzrs) res.v.emplace_back(el >> my_L);
		return res;
	}
	
	inline EZefRefss operator< (EZefRefs& uzrs, L_Class my_L) {
		EZefRefss res(length(uzrs));
		for (auto el : uzrs) res.v.emplace_back(el < my_L);
		return res;
	}

	inline EZefRefss operator<< (EZefRefs& uzrs, L_Class my_L) {
		EZefRefss res(length(uzrs));
		for (auto el : uzrs) res.v.emplace_back(el << my_L);
		return res;
	}






    
	inline ZefRefss operator> (ZefRefs zrs, L_Class my_L) {
		ZefRefss res(length(zrs));
		for (auto el : zrs) res.v.emplace_back(el > my_L);
		res.reference_frame_tx = zrs.reference_frame_tx;
		return res;
	}

	inline ZefRefss operator>> (ZefRefs zrs, L_Class my_L) {
		ZefRefss res(length(zrs));
		for (auto el : zrs) res.v.emplace_back(el >> my_L);
		res.reference_frame_tx = zrs.reference_frame_tx;
		return res;
	}
	
	inline ZefRefss operator< (ZefRefs zrs, L_Class my_L) {
		ZefRefss res(length(zrs));
		for (auto el : zrs) res.v.emplace_back(el < my_L);
		res.reference_frame_tx = zrs.reference_frame_tx;
		return res;
	}

	inline ZefRefss operator<< (ZefRefs zrs, L_Class my_L) {
		ZefRefss res(length(zrs));
		for (auto el : zrs) res.v.emplace_back(el << my_L);
		res.reference_frame_tx = zrs.reference_frame_tx;
		return res;
	}



    


    namespace zefOps {


        //                             _      _                  _                         
		//                          __| | ___| | ___  __ _  __ _| |_ ___                   
		//     _____ _____ _____   / _` |/ _ \ |/ _ \/ _` |/ _` | __/ _ \   _____ _____ _____ 
		//    |_____|_____|_____| | (_| |  __/ |  __/ (_| | (_| | ||  __/  |_____|_____|_____|
		//                         \__,_|\___|_|\___|\__, |\__,_|\__\___|                  
		//                                           |___/                            
		struct LIBZEF_DLL_EXPORTED DelegateOp {
			using type_variant = std::variant<Sentinel, EntityType, ValueRepType>;
			type_variant param = Sentinel{};			

			EZefRef operator() (const EZefRef uzr) const ;
            std::optional<EZefRef> operator() (const Graph & g) const ;

            DelegateOp operator[](const EntityType& et) const ;
            DelegateOp operator[](const ValueRepType& aet) const ;
            // DelegateOp operator[](const RelationType& rt) const ;
//             DelegateOp operator[](EntityType et_from, RelationType rt, EntityType et_to) {
// //                 std::string key = "Delegate.RT." + get_all_active_graph_data_tracker().relation_type_names.at(rel.relation_type_indx);
// // "Delegate.(Delegate.ET.ClientConnection>Delegate.RT.PrimaryInstance>Delegate.ET.ZefGraph)"
//                 std::string key = "not implemented!";
//                 return DelegateOp::Typed{key};
//             }

            EZefRefs operator() (EZefRefs&& uzrs) const; 
			EZefRefs operator() (const EZefRefs& uzrs) const; 

			ZefRef operator() (ZefRef zr) const; 
			ZefRefs operator() (ZefRefs&& zrs) const; 
			ZefRefs operator() (const ZefRefs& zrs) const; 
		};
		constexpr DelegateOp delegate;
		inline EZefRef operator| (EZefRef uzr, DelegateOp op) { return op(uzr); }
		inline EZefRefs operator| (EZefRefs&& uzrs, DelegateOp op) { return op(std::move(uzrs)); }
		inline EZefRefs operator| (const EZefRefs& uzrs, DelegateOp op) { return op(uzrs); }

		inline ZefRef operator| (ZefRef zr, DelegateOp op) { return op(zr); }
		inline ZefRefs operator| (ZefRefs&& zrs, DelegateOp op) { return op(std::move(zrs)); }
		inline ZefRefs operator| (const ZefRefs& zrs, DelegateOp op) { return op(zrs); }

        inline std::optional<EZefRef> operator| (const Graph& g, DelegateOp op) { return op(g); }


		//                        _           _              _   _       _   _                _                            
		//                       (_)_ __  ___| |_ __ _ _ __ | |_(_) __ _| |_(_) ___  _ __    | |___  __                    
		//    _____ _____ _____  | | '_ \/ __| __/ _` | '_ \| __| |/ _` | __| |/ _ \| '_ \   | __\ \/ /  _____ _____ _____ 
		//   |_____|_____|_____| | | | | \__ \ || (_| | | | | |_| | (_| | |_| | (_) | | | |  | |_ >  <  |_____|_____|_____|
		//                       |_|_| |_|___/\__\__,_|_| |_|\__|_|\__,_|\__|_|\___/|_| |_|___\__/_/\_\.                   
		//                                                                               |_____|                     
		struct LIBZEF_DLL_EXPORTED InstantiationTx {
			EZefRef operator() (EZefRef uzr) const;

			EZefRefs operator() (EZefRefs&& uzrs) const;
			EZefRefs operator() (const EZefRefs& uzrs) const;

			ZefRef operator() (ZefRef zr) const;
			ZefRefs operator() (ZefRefs&& zrs) const;
			ZefRefs operator() (const ZefRefs& zrs) const;
		};
		constexpr InstantiationTx instantiation_tx;
		inline EZefRef operator| (EZefRef uzr, InstantiationTx op) { return op(uzr); }
		inline EZefRefs operator| (EZefRefs&& uzrs, InstantiationTx op) { return op(std::move(uzrs)); }
		inline EZefRefs operator| (const EZefRefs& uzrs, InstantiationTx op) { return op(uzrs); }

		inline ZefRef operator| (ZefRef zr, InstantiationTx op) { return op(zr); }
		inline ZefRefs operator| (ZefRefs&& zrs, InstantiationTx op) { return op(std::move(zrs)); }
		inline ZefRefs operator| (const ZefRefs& zrs, InstantiationTx op) { return op(zrs); }





		//                        _                      _             _   _                _                            
		//                       | |_ ___ _ __ _ __ ___ (_)_ __   __ _| |_(_) ___  _ __    | |___  __                    
		//    _____ _____ _____  | __/ _ \ '__| '_ ` _ \| | '_ \ / _` | __| |/ _ \| '_ \   | __\ \/ /  _____ _____ _____ 
		//   |_____|_____|_____| | ||  __/ |  | | | | | | | | | | (_| | |_| | (_) | | | |  | |_ >  <  |_____|_____|_____|
		//                        \__\___|_|  |_| |_| |_|_|_| |_|\__,_|\__|_|\___/|_| |_|___\__/_/\_\.                 
		//                                                                             |_____|       
		struct LIBZEF_DLL_EXPORTED TerminationTx {
			EZefRef operator() (EZefRef uzr) ;

			EZefRefs operator() (EZefRefs&& uzrs); 
			EZefRefs operator() (const EZefRefs& uzrs); 

			ZefRef operator() (ZefRef zr); 
			ZefRefs operator() (ZefRefs&& zrs); 
			ZefRefs operator() (const ZefRefs& zrs); 
		};
		constexpr TerminationTx termination_tx;
		inline EZefRef operator| (EZefRef uzr, TerminationTx op) { return op(uzr); }
		inline EZefRefs operator| (EZefRefs&& uzrs, TerminationTx op) { return op(std::move(uzrs)); }
		inline EZefRefs operator| (const EZefRefs& uzrs, TerminationTx op) { return op(uzrs); }

		inline ZefRef operator| (ZefRef zr, TerminationTx op) { return op(zr); }
		inline ZefRefs operator| (ZefRefs&& zrs, TerminationTx op) { return op(std::move(zrs)); }
		inline ZefRefs operator| (const ZefRefs& zrs, TerminationTx op) { return op(zrs); }





		//                                   _                             _                                  _      _                                
		//                       __   ____ _| |_   _  ___     __ _ ___ ___(_) __ _ _ __  _ __ ___   ___ _ __ | |_   | |___  _____                     
		//    _____ _____ _____  \ \ / / _` | | | | |/ _ \   / _` / __/ __| |/ _` | '_ \| '_ ` _ \ / _ \ '_ \| __|  | __\ \/ / __|  _____ _____ _____ 
		//   |_____|_____|_____|  \ V / (_| | | |_| |  __/  | (_| \__ \__ \ | (_| | | | | | | | | |  __/ | | | |_   | |_ >  <\__ \ |_____|_____|_____|
		//                         \_/ \__,_|_|\__,_|\___|___\__,_|___/___/_|\__, |_| |_|_| |_| |_|\___|_| |_|\__|___\__/_/\_\___/                    
		//                                              |_____|              |___/                              |_____|                               

		struct LIBZEF_DLL_EXPORTED ValueAssignmentTxs {
			EZefRefs operator() (EZefRef uzrs) const;
			ZefRefs operator() (ZefRef zrs) const;    // This should only list value assigments that are know at the time of the reference frame tx of the zr
		};
		constexpr ValueAssignmentTxs value_assignment_txs;
		inline EZefRefs operator| (EZefRef uzr, ValueAssignmentTxs op) { return value_assignment_txs(uzr); }
		inline ZefRefs operator| (ZefRef zr, ValueAssignmentTxs op) { return value_assignment_txs(zr); }





		//                        _   _                   _                       _                     
		//                       | |_(_)_ __ ___   ___   | |_ _ __ __ ___   _____| |                    
		//    _____ _____ _____  | __| | '_ ` _ \ / _ \  | __| '__/ _` \ \ / / _ \ |  _____ _____ _____ 
		//   |_____|_____|_____| | |_| | | | | | |  __/  | |_| | | (_| |\ V /  __/ | |_____|_____|_____|
		//                        \__|_|_| |_| |_|\___|___\__|_|  \__,_| \_/ \___|_|                    
		//                                           |_____|                                  
		struct LIBZEF_DLL_EXPORTED TimeTravel {
			// z | time_travel[-5]       # go 5 transactions / time slices backwards in time
			// z | time_travel[2]        # go 2 transactions / time slices forward in time
			// z | time_travel["2020-5-8 14:52:34 (+0800)"]        # go to an absolute date
			// z | time_travel["yesterday"]
			// z | time_travel[my_tx]		# where tx is a EZefRef
			// z | time_travel[my_tx]		# where tx is a ZefRef   - should we allow this? Go to the blob_ptr, not the reference frame!
			// z | time_travel[-3*days]		# duration: QuantityFloat(x, EN.unit.seconds)

			using type_variant = std::variant<Sentinel, int, EZefRef, str, QuantityFloat>;
			type_variant param = Sentinel{};			

			TimeTravel operator[] (int delta_n) const { return TimeTravel{ delta_n }; };
			TimeTravel operator[] (str s) const { throw std::runtime_error("time_travel['date literal'] not implemented yet"); return TimeTravel{ s }; };
			TimeTravel operator[] (EZefRef my_tx) const { throw std::runtime_error("time_travel[EZefRef] not implemented yet"); return TimeTravel{ my_tx }; };
			TimeTravel operator[] (QuantityFloat some_duration) const { throw std::runtime_error("time_travel[QuantityFloat duration] not implemented yet"); return TimeTravel{ some_duration }; };

			ZefRef operator() (ZefRef z) const;
			ZefRefs operator() (const ZefRefs& zrs) const;
			//ZefRefs operator() (ZefRefs&& z) const;   // TODO
			ZefRefss operator() (const ZefRefss& zrs) const;
			//ZefRefss operator() (ZefRefss&& z) const;   // TODO

		private:
			EZefRef traverse_time(EZefRef start_tx) const;
		};
		const TimeTravel time_travel;
		inline ZefRef operator| (ZefRef zr, const TimeTravel& op) { return op(zr); }
		inline ZefRefs operator| (const ZefRefs& zrs, const TimeTravel& op) { return op(zrs); }
		inline ZefRefss operator| (const ZefRefss& zrs, const TimeTravel& op) { return op(zrs); }
		






	
		//                        _   _                        _ _                              
		//                       | |_(_)_ __ ___   ___     ___| (_) ___ ___                     
		//    _____ _____ _____  | __| | '_ ` _ \ / _ \   / __| | |/ __/ _ \  _____ _____ _____ 
		//   |_____|_____|_____| | |_| | | | | | |  __/   \__ \ | | (_|  __/ |_____|_____|_____|
		//                        \__|_|_| |_| |_|\___|___|___/_|_|\___\___|                    
		//                                           |_____|                                    
		
		// This struct is already defined in blobs.h. Use this as the class like a zefop. Only overload op here
		// struct TimeSlice {};
		constexpr TimeSlice time_slice{0};
        inline TimeSlice operator| (EZefRef uzr, TimeSlice op) { return TimeSlice(uzr); }
		inline TimeSlice operator| (ZefRef zr, TimeSlice op) { return TimeSlice(zr); }

		//                                                                
		//                        _ __   _____      __                    
		//    _____ _____ _____  | '_ \ / _ \ \ /\ / /  _____ _____ _____ 
		//   |_____|_____|_____| | | | | (_) \ V  V /  |_____|_____|_____|
		//                       |_| |_|\___/ \_/\_/                      
		//                                                                
		struct LIBZEF_DLL_EXPORTED Now{
			Time operator() () const ;

			ZefRef operator() (const Graph& g) const ;
			ZefRef operator() (EZefRef uzr) const ;

			ZefRef operator() (ZefRef zr) const ;
            // Note: ZefRefs operator() (EZefRefs& uzrs)   is not implemented on purpose: which ref tx to use if [] is passed? Throwing then could lead to subtle bugs for the zefdb user
            //ZefRefs operator() (ZefRefs&& zrs); 
            ZefRefs operator() (const ZefRefs& zrs); 
            ZefRefs operator() (const EZefRefs& zrs) { throw std::runtime_error("EZefRefs uzrs | now called: Please be more specific, this may lead to subtle bugs if EZefRefs from different graphs are mixed and which graph to use as a reference frame. Use 'uzrs | to_zefref[g|now]."); }    // which reference graph to use? This may be misleading with view graphs. Force the user to write to_zefref[g|now]
		};
		constexpr Now now;
		inline ZefRef operator| (const Graph& g, Now op) { return op(g); }  // g | now  returns the now tx on g
		inline ZefRef operator| (EZefRef uzr, Now op) { return op(uzr); }  // uzr | now  returns the a ZefRef with the now tx of the owning graph baked in as reference
		inline ZefRef operator| (ZefRef zr, Now op) { return op(zr); }  // uzr | now  returns the a ZefRef with the now tx of the owning graph baked in as reference
				
		// inline ZefRefs operator| (ZefRefs&& zrs, Now op) { return op(zrs); } TODO
		inline ZefRefs operator| (const ZefRefs& zrs, Now op) { return op(zrs); }
		inline ZefRefs operator| (const EZefRefs& uzrs, Now op) { return op(uzrs); }



	}



    namespace zefOps {


        //                               _           _                                                       
        //                              (_)_ __  ___| |_ __ _ _ __   ___ ___  ___                            
        //    _____ _____ _____ _____   | | '_ \/ __| __/ _` | '_ \ / __/ _ \/ __|   _____ _____ _____ _____ 
        //   |_____|_____|_____|_____|  | | | | \__ \ || (_| | | | | (_|  __/\__ \  |_____|_____|_____|_____|
        //                              |_|_| |_|___/\__\__,_|_| |_|\___\___||___/                           
        //             
        struct LIBZEF_DLL_EXPORTED Instances {
            // ----------------------- we want to be able to do       'g | instances[now][ET.Machine]'
            // to get all instances currently alive. This needs a different type after currying in the ref tx that the return type can be resolved to be a ZefRefs at compile time
            struct Sentinel {};
            using type_variant = std::variant<Sentinel, EntityType, ValueRepType>;

            std::variant<Sentinel, EZefRef, ZefRef, Now> ref_frame_data{ Sentinel {} };  
            type_variant curried_in_type = type_variant{ Sentinel{} };   // which type should be returned? e.g.     g | instances[ET.Machine] ...

            Instances operator[] (EntityType my_et) const { return Instances{ ref_frame_data, my_et }; };
            Instances operator[] (ValueRepType my_vrt) const { return Instances{ ref_frame_data, my_vrt }; };
            // Instances operator[] (Zuple zuple) const { return Instances{ ref_frame_tx, my_aet }; };   TODO
            Instances operator[] (EZefRef ref_frame_tx) const {
                if (BT(ref_frame_tx) != BT.TX_EVENT_NODE)
                    throw std::runtime_error("non-tx type EZefRef passed into instances[...]");
                return Instances{ ref_frame_tx, curried_in_type };
            }
            Instances operator[] (ZefRef ref_frame_tx) const { return operator[](ref_frame_tx.blob_uzr); }
            Instances operator[] (Now now_op) const {
                return Instances{ now_op, curried_in_type };
            }
            ZefRefs operator() (const Graph& g) const;
            ZefRefs operator() (ZefRef zr) const;
            ZefRefs operator() (EZefRef uzr) const;

            static ZefRefs pure(EZefRef tx);
            static ZefRefs pure(EZefRef tx, EntityType et);
            static ZefRefs pure(EZefRef tx, ValueRepType aet);
            static ZefRefs pure(EZefRef tx, Sentinel s);
            static ZefRefs pure(EZefRef tx, EZefRef delegate);

            // template<class ...ARGS>
            // static ZefRefs pure(ZefRef tx, ARGS && ...args) { return pure(EZefRef(tx), args...); }

            // Do this explicitly for easier bindings
            static ZefRefs pure(ZefRef tx_or_delegate);
            static ZefRefs pure(ZefRef tx, EntityType et) { return pure(EZefRef(tx), et); }
            static ZefRefs pure(ZefRef tx, ValueRepType aet) { return pure(EZefRef(tx), aet); };
            static ZefRefs pure(ZefRef tx, Sentinel s) { return pure(EZefRef(tx), s); };
            static ZefRefs pure(ZefRef tx, EZefRef delegate) { return pure(EZefRef(tx), delegate); };
        };                       
        const Instances instances;
                                 
        inline ZefRefs operator| (ZefRef zr, Instances op) { return op(zr); }
        inline ZefRefs operator| (EZefRef uzr, Instances op) { return op(uzr); }
        inline ZefRefs operator| (const Graph& g, Instances op) { return op(g); }
                                 
                                 
                                 
                                 
                                 
                                 
                                 
        //                         _           _                                    _                        _                     
        //                        (_)_ __  ___| |_ __ _ _ __   ___ ___  ___     ___| |_ ___ _ __ _ __   __ _| |                    
        //    _____ _____ _____   | | '_ \/ __| __/ _` | '_ \ / __/ _ \/ __|   / _ \ __/ _ \ '__| '_ \ / _` | |  _____ _____ _____ 
        //   |_____|_____|_____|  | | | | \__ \ || (_| | | | | (_|  __/\__ \  |  __/ ||  __/ |  | | | | (_| | | |_____|_____|_____|
        //                        |_|_| |_|___/\__\__,_|_| |_|\___\___||___/___\___|\__\___|_|  |_| |_|\__,_|_|                    
        //                                                                |_____|                                                  
                                 
                                 
        struct LIBZEF_DLL_EXPORTED InstancesEternal {
            Instances::type_variant curried_in_type = Instances::Sentinel{} ;
                                 
            EZefRefs operator() (EZefRef uzr) const { 
                if (!internals::has_delegate(BT(uzr))) throw std::runtime_error("InstancesEternal(uzr) called for a blob type where no delegate exists."); 
                return (uzr < BT.TO_DELEGATE_EDGE) >> L[BT.RAE_INSTANCE_EDGE]; 
            }                    
                                 
            EZefRefs operator() (const Graph& g) const;
                                 
            InstancesEternal operator[] (EntityType my_et) const { return InstancesEternal{ my_et }; };
            InstancesEternal operator[] (ValueRepType my_aet) const { return InstancesEternal{ my_aet }; };
            //InstancesEternal operator[] (EntityType my_et) const { return InstancesEternal{ my_et }; };
                                 
            //EZefRefss operator() (const EZefRefs& uzrs)  TODO			
            //ZefRefss operator() (EZefRefs& uzrs)  TODO
        };                       
        const InstancesEternal instances_eternal;
                                 
        inline EZefRefs operator| (const Graph& g, InstancesEternal op) { return op(g); }
        inline EZefRefs operator| (EZefRef uzr, InstancesEternal op) { return op(uzr); }
                                 
                                 
        inline EZefRefs InstancesEternal::operator() (const Graph& g) const {			
            // If the delegate doesn't exist, an exception will be thrown. Use this to return an empty list
            return std::visit(overloaded{
                    [&g](Instances::Sentinel sent) { return all_raes(g); },
                    [&g](auto tp) {
                        std::optional<EZefRef> temp = g | delegate[tp];
                        if(!temp) 
                            return EZefRefs(0, &g.my_graph_data());
                        return (*temp) | instances_eternal;
                    }
                }, curried_in_type);
        }                        
                                 
                                 
                                 
                                 
                                 
                                 
                                 
        //                                 __  __           _           _                       
        //                           __ _ / _|/ _| ___  ___| |_ ___  __| |                      
        //    _____ _____ _____     / _` | |_| |_ / _ \/ __| __/ _ \/ _` |    _____ _____ _____ 
        //   |_____|_____|_____|   | (_| |  _|  _|  __/ (__| ||  __/ (_| |   |_____|_____|_____|
        //                          \__,_|_| |_|  \___|\___|\__\___|\__,_|                      
        //                               
        // use as all_rel_ents_affected_by_my_tx = my_tx | affected
        struct LIBZEF_DLL_EXPORTED Affected {
            ZefRefs operator() (EZefRef my_tx) const;
            ZefRefs operator() (ZefRef my_tx) const { return operator() (my_tx | to_ezefref); }
        };                       
        constexpr Affected affected;
        inline ZefRefs operator| (EZefRef tx_uzr, Affected op) { return op(tx_uzr); }
        inline ZefRefs operator| (ZefRef tx_zr, Affected op) { return op(tx_zr); }
                                 
                                 
                                 
        //                          _           _              _   _       _           _                       
        //                         (_)_ __  ___| |_ __ _ _ __ | |_(_) __ _| |_ ___  __| |                      
        //    _____ _____ _____    | | '_ \/ __| __/ _` | '_ \| __| |/ _` | __/ _ \/ _` |    _____ _____ _____ 
        //   |_____|_____|_____|   | | | | \__ \ || (_| | | | | |_| | (_| | ||  __/ (_| |   |_____|_____|_____|
        //                         |_|_| |_|___/\__\__,_|_| |_|\__|_|\__,_|\__\___|\__,_|                      
        //                                                                                                    
                                 
        struct LIBZEF_DLL_EXPORTED Instantiated {
            ZefRefs operator() (EZefRef my_tx) const;
            ZefRefs operator() (ZefRef my_tx) const { return operator() (my_tx | to_ezefref); }
        };                       
        constexpr Instantiated instantiated;
        inline ZefRefs operator| (EZefRef z, Instantiated op) { return op(z); }
        inline ZefRefs operator| (ZefRef tx_zr, Instantiated op) { return op(tx_zr); }
                                 
                                 
        //                          _                      _             _           _                       
        //                         | |_ ___ _ __ _ __ ___ (_)_ __   __ _| |_ ___  __| |                      
        //    _____ _____ _____    | __/ _ \ '__| '_ ` _ \| | '_ \ / _` | __/ _ \/ _` |    _____ _____ _____ 
        //   |_____|_____|_____|   | ||  __/ |  | | | | | | | | | | (_| | ||  __/ (_| |   |_____|_____|_____|
        //                          \__\___|_|  |_| |_| |_|_|_| |_|\__,_|\__\___|\__,_|                      
        //                                                                                          
                                 
        struct LIBZEF_DLL_EXPORTED Terminated {
            ZefRefs operator() (EZefRef my_tx) const;
            ZefRefs operator() (ZefRef my_tx) const { return operator() (my_tx | to_ezefref); }
        };                       
        constexpr Terminated terminated;
        inline ZefRefs operator| (EZefRef z, Terminated op) { return op(z); }
        inline ZefRefs operator| (ZefRef tx_zr, Terminated op) { return op(tx_zr); }
                                 
                                 
                                 
                                 
        //                                     _                             _                      _                       
        //                         __   ____ _| |_   _  ___     __ _ ___ ___(_) __ _ _ __   ___  __| |                      
        //    _____ _____ _____    \ \ / / _` | | | | |/ _ \   / _` / __/ __| |/ _` | '_ \ / _ \/ _` |    _____ _____ _____ 
        //   |_____|_____|_____|    \ V / (_| | | |_| |  __/  | (_| \__ \__ \ | (_| | | | |  __/ (_| |   |_____|_____|_____|
        //                           \_/ \__,_|_|\__,_|\___|___\__,_|___/___/_|\__, |_| |_|\___|\__,_|                      
        //                                                |_____|              |___/                                        
        struct LIBZEF_DLL_EXPORTED ValueAssigned {
            ZefRefs operator() (EZefRef my_tx) const;
            ZefRefs operator() (ZefRef my_tx) const { return operator() (my_tx | to_ezefref); }
        };                       
        constexpr ValueAssigned value_assigned;
        inline ZefRefs operator| (EZefRef z, ValueAssigned op) { return op(z); }
        inline ZefRefs operator| (ZefRef tx_zr, ValueAssigned op) { return op(tx_zr); }
                                 
                                 
                                 
                                 
                                 
                                 
        // used to indicate to not cancel a subscription when the subscription object is terminated
        struct LIBZEF_DLL_EXPORTED KeepAlive {
            bool value = true;       
            KeepAlive operator[] (bool val) const { return KeepAlive{ val }; };
        };                           
        const KeepAlive keep_alive;  
                                 
                                 
        struct LIBZEF_DLL_EXPORTED Outgoing {};
        constexpr Outgoing outgoing; 
        inline std::ostream& operator<< (std::ostream& os, const Outgoing& op) { os << "zefop:Outgoing"; return os; }
                                 
        struct LIBZEF_DLL_EXPORTED Incoming {};
        constexpr Incoming incoming; 
        inline std::ostream& operator<< (std::ostream& os, const Incoming& op) { os << "zefop:Incoming"; return os; }
                                 
                                 
                                 
        struct LIBZEF_DLL_EXPORTED OnValueAssignment {};
        constexpr OnValueAssignment on_value_assignment;
                                 
                                 
        struct LIBZEF_DLL_EXPORTED OnInstantiation {
            const bool is_outgoing = true;
            const std::optional<RelationType> rt = {};
            OnInstantiation operator[] (Outgoing outgoing_op) const { return OnInstantiation{ true, rt }; };
            OnInstantiation operator[] (Incoming incoming_op) const { return OnInstantiation{ false, rt }; };
            OnInstantiation operator[] (RelationType my_rt) const { return OnInstantiation{ is_outgoing, my_rt }; };
        };                           
        const OnInstantiation on_instantiation;
                                 
                                 
        struct LIBZEF_DLL_EXPORTED OnTermination {
            const bool is_outgoing = true;
            const std::optional<RelationType> rt = {};
            OnTermination operator[] (Outgoing outgoing_op) const { return OnTermination{ true, rt }; };
            OnTermination operator[] (Incoming incoming_op) const { return OnTermination{ false, rt }; };
            OnTermination operator[] (RelationType my_rt) const { return OnTermination{ is_outgoing, my_rt }; };
        };                           
        const OnTermination on_termination;
                                 
                                 
                                 
        inline std::ostream& operator<< (std::ostream& os, const OnValueAssignment& op) { os << "OnValueAssignment"; return os; }
        inline std::ostream& operator<< (std::ostream& os, const KeepAlive& op) { os << "KeepAlive["<<(op.value ? "true" : "false") << "]"; return os; }
        inline std::ostream& operator<< (std::ostream& os, const OnInstantiation& op) {
            os << "OnInstantiation[";
            os << "[" << (op.is_outgoing ? "outgoing" : "incoming") << "]";
            if (op.rt)               
                os << *op.rt;        
            os << "]";               
            return os;               
        }                            
        inline std::ostream& operator<< (std::ostream& os, const OnTermination& op) {
            os << "OnTermination[";  
            os << "[" << (op.is_outgoing ? "outgoing" : "incoming") << "]";
            if (op.rt)               
                os << *op.rt;        
            os << "]";               
            return os;               
        }                            
                                 
                                 
                                 
        // e.g. my_aet_zr | subscribe[on_out_instantiation[RT.ListElement]][my_callback_fct][keep_alive]
        struct LIBZEF_DLL_EXPORTED Subscribe {
            const std::variant<      
            Sentinel,            
            OnValueAssignment,   
            OnInstantiation,     
            OnTermination        
            > relation_direction = Sentinel{};
            KeepAlive _keep_alive = keep_alive[true];
            const std::optional<std::function<void(ZefRef)>> callback_fct = {};
                                 
                                 
            Subscribe operator[] (OnValueAssignment op) const { return Subscribe{op, _keep_alive, callback_fct }; };
            Subscribe operator[] (OnInstantiation op) const { return Subscribe{op, _keep_alive, callback_fct }; };
            Subscribe operator[] (OnTermination op) const { return Subscribe{op, _keep_alive, callback_fct }; };
                                 
            Subscribe operator[] (KeepAlive op) const { return Subscribe{ relation_direction, op, callback_fct }; };
            Subscribe operator[] (std::function<void(ZefRef)> fct) const { return Subscribe{ relation_direction, _keep_alive, fct }; };
                                 
            // only allow subscriptions on ZefRefs: the subscription is added to the reference frame graph
            Subscription operator() (ZefRef zr) const;
            Subscription operator() (EZefRef uzr) const;
            Subscription operator() (Graph zr) const;
        };                           
        const Subscribe subscribe;	 
        inline Subscription operator| (ZefRef zr, const Subscribe& op) { return op(zr); }
        inline Subscription operator| (EZefRef uzr, const Subscribe& op) { return op(uzr); }
        inline Subscription operator| (Graph g, const Subscribe& op) { return op(g); }
                                 
                                 
        inline std::ostream& operator<< (std::ostream& os, const Subscribe& op) {
            os << "Subscribe[relation_direction=";
            std::visit([&os](auto x) { os << x; }, op.relation_direction);
            os << ", " << op._keep_alive;
            os << ", callback_fct=" << (bool(op.callback_fct) ? "" : str("not ")) << str("assigned]");
		                         
            return os;               
        }                            
                                 
                                 
                                 
                                 
	} // zefOps namespace        
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
	                             
                                 
                                 
                                 
                                 
                                 
                                 
                                 
                                 
}