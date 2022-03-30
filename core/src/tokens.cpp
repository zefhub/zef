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

#include "tokens.h"
#include "zefref.h"
#include "zwitch.h"
#include "butler/butler.h"
#include "low_level_api.h"


namespace zefDB {

	namespace internals {
		EntityType get_entity_type(EZefRef uzr){ return ET(uzr);}  // exposed that via pybind that it can be used in the 'EntityTypeStruct' defined in the python __init__ .py
		RelationType get_relation_type(EZefRef uzr){ return RT(uzr);}
		BlobType get_blob_type(EZefRef uzr){ return BT(uzr);}
		AtomicEntityType get_atomic_entity_type(EZefRef uzr){ return AET(uzr);}
	}


    //////////////////////////////
    // * BlobType

	std::ostream& operator << (std::ostream& o, BlobType bl) {		
		switch (bl) {
		/*[[[cog
		import cog
		for bl in all_blob_type_names:
			cog.outl(f'case BlobType::{bl}: {{o << "{bl}"; break; }}')
		]]]*/
		case BlobType::_unspecified: {o << "_unspecified"; break; }
		case BlobType::ROOT_NODE: {o << "ROOT_NODE"; break; }
		case BlobType::TX_EVENT_NODE: {o << "TX_EVENT_NODE"; break; }
		case BlobType::RAE_INSTANCE_EDGE: {o << "RAE_INSTANCE_EDGE"; break; }
		case BlobType::TO_DELEGATE_EDGE: {o << "TO_DELEGATE_EDGE"; break; }
		case BlobType::NEXT_TX_EDGE: {o << "NEXT_TX_EDGE"; break; }
		case BlobType::ENTITY_NODE: {o << "ENTITY_NODE"; break; }
		case BlobType::ATOMIC_ENTITY_NODE: {o << "ATOMIC_ENTITY_NODE"; break; }
		case BlobType::ATOMIC_VALUE_NODE: {o << "ATOMIC_VALUE_NODE"; break; }
		case BlobType::RELATION_EDGE: {o << "RELATION_EDGE"; break; }
		case BlobType::DELEGATE_INSTANTIATION_EDGE: {o << "DELEGATE_INSTANTIATION_EDGE"; break; }
		case BlobType::DELEGATE_RETIREMENT_EDGE: {o << "DELEGATE_RETIREMENT_EDGE"; break; }
		case BlobType::INSTANTIATION_EDGE: {o << "INSTANTIATION_EDGE"; break; }
		case BlobType::TERMINATION_EDGE: {o << "TERMINATION_EDGE"; break; }
		case BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE: {o << "ATOMIC_VALUE_ASSIGNMENT_EDGE"; break; }
		case BlobType::DEFERRED_EDGE_LIST_NODE: {o << "DEFERRED_EDGE_LIST_NODE"; break; }
		case BlobType::ASSIGN_TAG_NAME_EDGE: {o << "ASSIGN_TAG_NAME_EDGE"; break; }
		case BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE: {o << "NEXT_TAG_NAME_ASSIGNMENT_EDGE"; break; }
		case BlobType::FOREIGN_GRAPH_NODE: {o << "FOREIGN_GRAPH_NODE"; break; }
		case BlobType::ORIGIN_RAE_EDGE: {o << "ORIGIN_RAE_EDGE"; break; }
		case BlobType::ORIGIN_GRAPH_EDGE: {o << "ORIGIN_GRAPH_EDGE"; break; }
		case BlobType::FOREIGN_ENTITY_NODE: {o << "FOREIGN_ENTITY_NODE"; break; }
		case BlobType::FOREIGN_ATOMIC_ENTITY_NODE: {o << "FOREIGN_ATOMIC_ENTITY_NODE"; break; }
		case BlobType::FOREIGN_RELATION_EDGE: {o << "FOREIGN_RELATION_EDGE"; break; }
		//[[[end]]]
		}
		return o;
	}



	BlobType BlobTypeStruct::operator() (EZefRef uzr) const {
		return get<BlobType>(uzr);
	}
	BlobType BlobTypeStruct::operator() (ZefRef zr) const {
		return get<BlobType>(zr.blob_uzr);
	}
	
	BlobType operator| (EZefRef uzr, const BlobTypeStruct& BT_) {return BT_(uzr);}
	BlobType operator| (ZefRef zr, const BlobTypeStruct& BT_) {return BT_(zr);}

//                        _____       _   _ _        _____                                           
//                       | ____|_ __ | |_(_) |_ _   |_   _|   _ _ __   ___  ___                      
//    _____ _____ _____  |  _| | '_ \| __| | __| | | || || | | | '_ \ / _ \/ __|   _____ _____ _____ 
//   |_____|_____|_____| | |___| | | | |_| | |_| |_| || || |_| | |_) |  __/\__ \  |_____|_____|_____|
//                       |_____|_| |_|\__|_|\__|\__, ||_| \__, | .__/ \___||___/                     
//                                              |___/     |___/|_|                         

	EntityType EntityTypeStruct::operator() (const std::string& string_input) const {
        auto maybe_et = global_token_store().ET_from_string(string_input);
        if(maybe_et)
            return *maybe_et;
        // Doesn't exist, need to create, synchronising with upstream
        return internals::get_global_entity_type_from_string(string_input);
	};


    EntityType::operator str() const {
        return internals::get_string_name_from_entity_type(*this);
    }
    
    std::ostream& operator << (std::ostream& o, EntityType et) {		
        // o << "ET(" << et.entity_type_indx << ")";
        o << "ET." << str(et);
		return o;
	}

	EntityType EntityTypeStruct::operator() (EZefRef uzr) const {
		if (get<BlobType>(uzr) != BlobType::ENTITY_NODE) throw std::runtime_error("Entity type requested from a EZefRef that is not of type 'ENTITY_NODE'. This does not make sense.");
        return get<blobs_ns::ENTITY_NODE>(uzr).entity_type;
    }
    EntityType EntityTypeStruct::operator() (ZefRef zr) const {
		return operator()(EZefRef(zr));
	}
    EntityTypeStruct::operator str() const {
        return "EntityTypeStruct(global)";
    }

	EntityType operator| (EZefRef uzr, const EntityTypeStruct& ET_) {return ET_(uzr);}
	EntityType operator| (ZefRef zr, const EntityTypeStruct& ET_) {return ET_(zr);}



//                        ____      _       _   _           _____                                           
//                       |  _ \ ___| | __ _| |_(_) ___  _ _|_   _|   _ _ __   ___  ___                      
//    _____ _____ _____  | |_) / _ \ |/ _` | __| |/ _ \| '_ \| || | | | '_ \ / _ \/ __|   _____ _____ _____ 
//   |_____|_____|_____| |  _ <  __/ | (_| | |_| | (_) | | | | || |_| | |_) |  __/\__ \  |_____|_____|_____|
//                       |_| \_\___|_|\__,_|\__|_|\___/|_| |_|_| \__, | .__/ \___||___/                     
//                                                               |___/|_|                           


	RelationType RelationTypeStruct::operator() (const std::string& string_input) const {
		return internals::get_relation_type_from_string(string_input);
	};	

	RelationType RelationTypeStruct::operator() (const char* char_array_input) const {
		return operator() (str(char_array_input));
	};



    RelationType::operator str() const {
        return internals::get_string_name_from_relation_type(*this);
    }

	std::ostream& operator << (std::ostream& o, RelationType rt) {
        o << "RT.";
		switch (rt.relation_type_indx) {
#include "blobs.cpp.RT.gen"
        default: { o << internals::get_string_name_from_relation_type(rt); }		
		}
		return o;
	}


    RelationType RelationTypeStruct::operator() (EZefRef uzr) const {
		if (get<BlobType>(uzr) != BlobType::RELATION_EDGE) throw std::runtime_error("Relation type requested from a EZefRef that is not of type 'RELATION_EDGE'. This does not make sense.");
		return get<blobs_ns::RELATION_EDGE>(uzr).relation_type;
	}
	RelationType RelationTypeStruct::operator() (ZefRef zr) const {
		if (get<BlobType>(zr.blob_uzr) != BlobType::RELATION_EDGE) throw std::runtime_error("Relation type requested from a ZefRef that is not of type 'RELATION_EDGE'. This does not make sense.");
		return get<blobs_ns::RELATION_EDGE>(zr.blob_uzr).relation_type;
	}

	RelationType operator| (EZefRef uzr, const RelationTypeStruct& RT_) {return RT_(uzr);}
	RelationType operator| (ZefRef zr, const RelationTypeStruct& RT_) {return RT_(zr);}

    //////////////////////////////
    // * Keyword

	Keyword KeywordStruct::operator() (const std::string& string_input) const {
		return internals::get_keyword_from_string(string_input);
	};	

	Keyword KeywordStruct::operator() (const char* char_array_input) const {
		return operator() (str(char_array_input));
	};



    Keyword::operator str() const {
        return internals::get_string_name_from_keyword(*this);
    }

	std::ostream& operator << (std::ostream& o, Keyword kw) {
        o << "KW.";
		switch (kw.indx) {
#include "blobs.cpp.KW.gen"
        default: { o << internals::get_string_name_from_keyword(kw); }		
		}
		return o;
	}



    //////////////////////////////////////
    // * AtomicEntityType

    // TODO: There seems to be something missing here with the string lookups - it's not the same pattern as ET,RT

	AtomicEntityType AtomicEntityTypeStruct::operator() (EZefRef uzr) const {
		if (get<BlobType>(uzr) != BlobType::ATOMIC_ENTITY_NODE) throw std::runtime_error("AET(EZefRef uzr) calles for a uzr which is not an atomic entity.");
		return get<blobs_ns::ATOMIC_ENTITY_NODE>(uzr).my_atomic_entity_type;
	}
	AtomicEntityType AtomicEntityTypeStruct::operator() (ZefRef zr) const {
		return AET(zr.blob_uzr);
	}

	AtomicEntityType::operator str () const {
		return internals::get_string_name_from_atomic_entity_type(*this);
	}
	std::ostream& operator << (std::ostream& o, AtomicEntityType aet) {
        o << "AET.";
		o << str(aet);
		return o;
	}

	AtomicEntityType operator| (EZefRef uzr, const AtomicEntityTypeStruct& AET_) {return AET_(uzr);}
	AtomicEntityType operator| (ZefRef zr, const AtomicEntityTypeStruct& AET_) {return AET_(zr);}


    //////////////////////////////
    // * ZefEnumValue

	


    enum_indx round_down_mod16(enum_indx x) {
        return x - (x % 16);
    }

    ZefEnumValue ZefEnumStruct::operator() (const std::string& enum_type, const std::string& enum_val) const {
        // call with EN("MachineStatus", "IDLE")
        return internals::get_enum_value_from_string(enum_type, enum_val);
    }    

	std::ostream& operator<<(std::ostream& os, ZefEnumValue zef_enum_val) {
		auto sp = internals::get_enum_string_pair(zef_enum_val);
		os << "EN." << sp.first << "." << sp.second;
		return os;
	}

	std::string ZefEnumValue::enum_type() { return internals::get_enum_string_pair(*this).first; }
	std::string ZefEnumValue::enum_value() { return  internals::get_enum_string_pair(*this).second; }

    //////////////////////////////
    // * TokenStore

	TokenStore& global_token_store() {
        // if new graphs are created often, don't always reallocate this vector
        static TokenStore* ptr_to_singleton = new TokenStore();
        return *ptr_to_singleton;
	}

    TokenStore::TokenStore() {
        init_defaults();
    }
    TokenStore::TokenStore(MMap::FileGraph & fg) {
        init_defaults();
    }

    std::optional<EntityType> TokenStore::ET_from_string(const std::string & s) {
        return ETs.maybe_at(s);
    }
    EntityType TokenStore::ET_from_string_failhard(const std::string & s) {
        return RTs.at(s);
    }

    EntityType TokenStore::add_ET_from_string(const std::string & s) {
        if(!Butler::butler_is_master)
            throw std::runtime_error("Can't add tokens from string when we aren't master");
        // First, try the quickest version
        std::optional<EntityType> et = ET_from_string(s);
        if(et)
            return *et;

        // Otherwise, fallback to creating it ourselves, although we have to
        // consider that someone else may have jumped in (this is slower, but it
        // is a rare scenario)

        // TODO: This should become generalised so we can switch out the
        // implementation of the maps. It'd be nice to only have one lock (for
        // appending) but to do that with a shared_mutex means special logic (as
        // there's no shared_recursive_mutex).
        //
        // Could do a double-layer of mutexes, but this can easily lead to deadlocks.
        std::unique_lock lock(ETs.m);
        // Double check for race conditions
        if(ETs.map.contains(s))
            return ETs.map.at(s);
            
        EntityType new_et = ETs.map.generate_unused_random_number();

        ETs.map.insert(new_et.entity_type_indx, s);
        return new_et;
    }

    std::optional<std::string> TokenStore::string_from_ET(const EntityType & et) {
        return ETs.maybe_at(et.entity_type_indx);
    }
    std::string TokenStore::string_from_ET_failhard(const EntityType & et) {
        return ETs.at(et.entity_type_indx);
    }

    std::optional<RelationType> TokenStore::RT_from_string(const std::string & s) {
        return RTs.maybe_at(s);
    }
    RelationType TokenStore::RT_from_string_failhard(const std::string & s) {
        return RTs.at(s);
    }

    RelationType TokenStore::add_RT_from_string(const std::string & s) {
        if(!Butler::butler_is_master)
            throw std::runtime_error("Can't add tokens from string when we aren't master");
        // As we are doing something more complicated here, manually manage the mutex usage
        // First, try the quickest version
        std::optional<RelationType> rt = RT_from_string(s);
        if(rt)
            return *rt;

        // Otherwise, fallback to creating it ourselves, although we have to
        // consider that someone else may have jumped in (this is slower, but it
        // is a rare scenario)

        // TODO: This should become generalised so we can switch out the
        // implementation of the maps. It'd be nice to only have one lock (for
        // appending) but to do that with a shared_mutex means special logic (as
        // there's no shared_recursive_mutex).
        //
        // Could do a double-layer of mutexes, but this can easily lead to deadlocks.
        std::unique_lock lock(RTs.m);
        // Double check for race conditions
        if(RTs.map.contains(s))
            return RTs.map.at(s);
            
        RelationType new_rt = RTs.map.generate_unused_random_number();

        RTs.map.insert(new_rt.relation_type_indx, s);
        return new_rt;
    }

    std::optional<std::string> TokenStore::string_from_RT(const RelationType & rt) {
        return RTs.maybe_at(rt.relation_type_indx);
    }
    std::string TokenStore::string_from_RT_failhard(const RelationType & rt) {
        return RTs.at(rt.relation_type_indx);
    }

    std::optional<Keyword> TokenStore::KW_from_string(const std::string & s) {
        return KWs.maybe_at(s);
    }
    Keyword TokenStore::KW_from_string_failhard(const std::string & s) {
        return KWs.at(s);
    }

    Keyword TokenStore::add_KW_from_string(const std::string & s) {
        if(!Butler::butler_is_master)
            throw std::runtime_error("Can't add tokens from string when we aren't master");
        // As we are doing something more complicated here, manually manage the mutex usage
        // First, try the quickest version
        std::optional<Keyword> kw = KW_from_string(s);
        if(kw)
            return *kw;

        // Otherwise, fallback to creating it ourselves, although we have to
        // consider that someone else may have jumped in (this is slower, but it
        // is a rare scenario)

        // TODO: This should become generalised so we can switch out the
        // implementation of the maps. It'd be nice to only have one lock (for
        // appending) but to do that with a shared_mutex means special logic (as
        // there's no shared_recursive_mutex).
        //
        // Could do a double-layer of mutexes, but this can easily lead to deadlocks.
        std::unique_lock lock(KWs.m);
        // Double check for race conditions
        if(KWs.map.contains(s))
            return KWs.map.at(s);
            
        Keyword new_kw = KWs.map.generate_unused_random_number();

        KWs.map.insert(new_kw.indx, s);
        return new_kw;
    }

    std::optional<std::string> TokenStore::string_from_KW(const Keyword & kw) {
        return KWs.maybe_at(kw.indx);
    }
    std::string TokenStore::string_from_KW_failhard(const Keyword & kw) {
        return KWs.at(kw.indx);
    }

    std::optional<ZefEnumValue> TokenStore::EN_from_string(const string_pair & s) {
        return ENs.maybe_at(s);
    }
    ZefEnumValue TokenStore::EN_from_string_failhard(const string_pair & s) {
        return ENs.at(s);
    }

    ZefEnumValue TokenStore::add_EN_from_string(const string_pair & s) {
        if(!Butler::butler_is_master)
            throw std::runtime_error("Can't add tokens from string when we aren't master");
        // As we are doing somrthing more complicated here, manually manage the mutex usage
        // First, try the quickest version
        std::optional<ZefEnumValue> en = EN_from_string(s);
        if(en)
            return *en;

        // Otherwise, fallback to creating it ourselves, although we have to
        // consider that someone else may have jumped in (this is slower, but it
        // is a rare scenario)

        // TODO: This should become generalised so we can switch out the
        // implementation of the maps. It'd be nice to only have one lock (for
        // appending) but to do that with a shared_mutex means special logic (as
        // there's no shared_recursive_mutex).
        //
        // Could do a double-layer of mutexes, but this can easily lead to deadlocks.
        std::unique_lock lock(ENs.m);
        // Double check for race conditions
        if(ENs.map.contains(s))
            return ENs.map.at(s);
            
        ZefEnumValue new_en = ENs.map.generate_unused_random_number();

        ENs.map.insert(new_en.value, s);
        return new_en;
    }

    std::optional<string_pair> TokenStore::string_from_EN(const ZefEnumValue & en) {
        return ENs.maybe_at(en.value);
    }
    string_pair TokenStore::string_from_EN_failhard(const ZefEnumValue & en) {
        return ENs.at(en.value);
    }

	void TokenStore::init_ETs() {
#include "blobs.cpp.ETnames.gen"
	}
	void TokenStore::init_RTs() {
#include "blobs.cpp.RTnames.gen"
	}

    void TokenStore::init_KWs() {
#include "blobs.cpp.KWnames.gen"
    }

    void TokenStore::init_ENs() {
#include "blobs.cpp.ENnames.gen"
    }



    void TokenStore::init_defaults() {
        init_ETs();
        init_RTs();
        init_KWs();
        init_ENs();
    }


    //////////////////////////////////////////////////////
    // * Query upstream functions


    namespace internals {
        // we need a hash fct that can be evaluated constexpr, i.e. at compile time to avoid ifs or unoredered map lookup
        EntityType get_global_entity_type_from_string(const std::string& entity_type_str){
            // TODO: This needs to change, but for now just getting code to work, so
            // this looks up the global token store as a hardcoded thing.
            auto & tokens = global_token_store();
            auto get_et = [&tokens,&entity_type_str]() { return tokens.ET_from_string(entity_type_str); };
            auto et = get_et();
            
            if(et)
                return *et;

            if(zwitch.developer_output())
                std::cerr << "Did not find ET in global tokens: '" << entity_type_str << "'." << std::endl;

            auto butler = Butler::get_butler();
            auto response =
                butler->msg_push_timeout<Messages::TokenQueryResponse>(
                                                                       Messages::TokenQuery{
                                                                           Messages::TokenQuery::ET,
                                                                           {entity_type_str},
                                                                           {},
                                                                           true,
                                                                           false
                                                                       });
            if(!response.generic.success) {
                throw std::runtime_error("Failed to tokenize new ET: " + response.generic.reason);
            }

            auto & [r_name, r_indx] = response.pairs[0];
            assert(entity_type_str == r_name);

            return EntityType(r_indx);
        }		

        RelationType get_relation_type_from_string(const std::string& relation_type_str){
            auto & tokens = global_token_store();
            auto get_rt = [&tokens,&relation_type_str]() { return tokens.RT_from_string(relation_type_str); };
            auto rt = get_rt();
            
            if(rt)
                return *rt;

            auto butler = Butler::get_butler();
            auto response =
                butler->msg_push_timeout<Messages::TokenQueryResponse>(
                                                                           Messages::TokenQuery{
                                                                               Messages::TokenQuery::RT,
                                                                               {relation_type_str},
                                                                               {},
                                                                               true,
                                                                               false
                                                                           });
            if(!response.generic.success) {
                throw std::runtime_error("Failed to tokenize new RT: " + response.generic.reason);
            }

            auto & [r_name, r_indx] = response.pairs[0];
            assert(relation_type_str == r_name);

            return RelationType(r_indx);
        }		

        Keyword get_keyword_from_string(const std::string& keyword_str){
            auto & tokens = global_token_store();
            auto get_kw = [&tokens,&keyword_str]() { return tokens.KW_from_string(keyword_str); };
            auto kw = get_kw();
            
            if(kw)
                return *kw;

            auto butler = Butler::get_butler();
            auto response =
                butler->msg_push_timeout<Messages::TokenQueryResponse>(
                                                                           Messages::TokenQuery{
                                                                               Messages::TokenQuery::KW,
                                                                               {keyword_str},
                                                                               {},
                                                                               true,
                                                                               false
                                                                           });
            if(!response.generic.success) {
                throw std::runtime_error("Failed to tokenize new KW: " + response.generic.reason);
            }

            auto & [r_name, r_indx] = response.pairs[0];
            assert(keyword_str == r_name);

            return Keyword(r_indx);
        }		

        AtomicEntityType get_aet_from_enum_type_name_string(const std::string& enum_type_name_str) {
            switch (hash_char_array(enum_type_name_str.data())) {
#include "graph.cpp.AETenumfromstring.gen"
            default:
                return AtomicEntityType{ get_enum_value_from_string(enum_type_name_str, "").value + 1 };
            }
        }


        AtomicEntityType get_aet_from_quantity_float_name_string(const std::string& unit_enum_value_str) {
            using namespace std::chrono;
            switch (hash_char_array(unit_enum_value_str.data())) {
#include "graph.cpp.AETquantityfloatfromstring.gen"
            default:
                return AtomicEntityType{ get_enum_value_from_string("Unit", unit_enum_value_str).value + 2 };
            }
        }


        AtomicEntityType get_aet_from_quantity_int_name_string(const std::string& unit_enum_value_str) {
            using namespace std::chrono;
            switch (hash_char_array(unit_enum_value_str.data())) {
#include "graph.cpp.AETquantityintfromstring.gen"
            default:
                return AtomicEntityType{ get_enum_value_from_string("Unit", unit_enum_value_str).value + 3 };
            }
        }

        ZefEnumValue get_enum_value_from_string(const std::string& enum_type, const std::string& enum_val) {
            using namespace std::chrono;
            // tasks::apply_immediate_updates_from_zm();
            const std::string enum_type_and_val = enum_type + "." + enum_val;
            switch (hash_char_array(enum_type_and_val.data())) {
#include "zef_enums.cpp.enumvalfromstring.gen"
            default: {
                auto & tokens = global_token_store();
                auto get_en = [&tokens,&enum_type,&enum_val]() { return tokens.EN_from_string({enum_type,enum_val}); };
                auto en = get_en();

                if (en)
                    return *en;

                auto butler = Butler::get_butler();
                std::string full_name = enum_type + "." + enum_val;
                auto response =
                    butler->msg_push_timeout<Messages::TokenQueryResponse>(
                                                                           Messages::TokenQuery{
                                                                               Messages::TokenQuery::EN,
                                                                               {full_name},
                                                                               {},
                                                                               true,
                                                                               false
                                                                           });
                if(!response.generic.success) {
                    throw std::runtime_error("Failed to tokenize new EN: " + response.generic.reason);
                }

                auto & [r_name, r_indx] = response.pairs[0];
                assert(full_name == r_name);

                return ZefEnumValue(r_indx);
            }
            }
        }



        std::string get_string_name_from_entity_type(EntityType et) {
            auto maybe_str = global_token_store().string_from_ET(et);
            if(maybe_str)
                return *maybe_str;

            auto& indx = et.entity_type_indx;

            auto butler = Butler::get_butler();
            auto response = butler->msg_push_timeout<Messages::TokenQueryResponse>(Messages::TokenQuery{
                    Messages::TokenQuery::ET,
                    {},
                    {indx},
                    false,
                    false
                });
            if(!response.generic.success) {
                if(zwitch.debug_allow_unknown_tokens())
                    return "_UNK" + to_str(indx);
                throw std::runtime_error("Failed to identify ET(" + to_str(indx) + "): " + response.generic.reason);
            }

            auto & [r_name,r_indx] = response.pairs[0];
            assert(indx == r_indx);

            return r_name;
        }
    
        std::string get_string_name_from_relation_type(RelationType rt) {
            auto maybe_str = global_token_store().string_from_RT(rt);
            if(maybe_str)
                return *maybe_str;
            else {
                auto& indx = rt.relation_type_indx;
                auto butler = Butler::get_butler();
                auto response = butler->msg_push_timeout<Messages::TokenQueryResponse>(Messages::TokenQuery{
                        Messages::TokenQuery::RT,
                        {},
                        {indx},
                        false,
                        false
                    });
                if(!response.generic.success) {
                    if(zwitch.debug_allow_unknown_tokens())
                        return "_UNK" + to_str(indx);
                    throw std::runtime_error("Failed to identify RT(" + to_str(indx) + "): " + response.generic.reason);
                }

                auto & [r_name,r_indx] = response.pairs[0];
                assert(indx == r_indx);

                return r_name;
            }
        }

        std::string get_string_name_from_keyword(Keyword kw) {
            auto maybe_str = global_token_store().string_from_KW(kw);
            if(maybe_str)
                return *maybe_str;
            else {
                auto& indx = kw.indx;
                auto butler = Butler::get_butler();
                auto response = butler->msg_push_timeout<Messages::TokenQueryResponse>(Messages::TokenQuery{
                        Messages::TokenQuery::KW,
                        {},
                        {indx},
                        false,
                        false
                    });
                if(!response.generic.success) {
                    if(zwitch.debug_allow_unknown_tokens())
                        return "_UNK" + to_str(indx);
                    throw std::runtime_error("Failed to identify KW(" + to_str(indx) + "): " + response.generic.reason);
                }

                auto & [r_name,r_indx] = response.pairs[0];
                assert(indx == r_indx);

                return r_name;
            }
        }

        string_pair get_enum_string_pair(ZefEnumValue en) {
            auto& indx = en.value;

            auto& names_dict = global_token_store().ENs;
            
            if(names_dict.contains(indx))
                return names_dict.at(indx);
            else {
                auto butler = Butler::get_butler();
                auto response = butler->msg_push_timeout<Messages::TokenQueryResponse>(Messages::TokenQuery{
                        Messages::TokenQuery::EN,
                        {},
                        {indx},
                        false,
                        false
                    });
                if(!response.generic.success) {
                    if(zwitch.debug_allow_unknown_tokens())
                        return string_pair{"_UNK", "_UNK" + to_str(indx)};
                    throw std::runtime_error("Failed to identify EN(" + to_str(indx) + "): " + response.generic.reason);
                }

                auto & [r_name,r_indx] = response.pairs[0];
                assert(indx == r_indx);

                return names_dict.at(indx);
            }
        }

        std::string get_string_name_from_atomic_entity_type(AtomicEntityType aet) {
            // TODO: See above
            switch (aet.value) {
            case AET._unspecified.value: {return "_unspecified"; break; }
            case AET.String.value: {return "String"; break; }
            case AET.Bool.value: {return "Bool"; break; }
            case AET.Float.value: {return "Float"; break; }
            case AET.Int.value: {return "Int"; break; }
            case AET.Time.value: {return "Time"; break; }
            case AET.Serialized.value: {return "Serialized"; break; }

#include "graph.cpp.stringfromAET.gen"
                // if we reach here, it may still be a known type. It was just not known at compile time
            default: {
                // deal with dictionary lookup here, e.g. for enum types, QuantityFloat and others that can be extended at runtime.
                ZefEnumValue base_en{round_down_mod16(aet.value)};
                switch (aet.value % 16) {
                case 1: return "Enum." + base_en.enum_type();
                case 2: return "QuantityFloat." + base_en.enum_value();
                case 3: return "QuantityInt." + base_en.enum_value();
                default: throw std::runtime_error("In fct get_string_name_from_atomic_entity_type: invalid AET Enum subtype");
                };
            }
            }
		
        }

        string_pair split_enum_string(const std::string & name) {
            int sep = name.find('.');
            std::string enum_type = name.substr(0, sep);
            std::string enum_value = name.substr(sep+1);
            return string_pair{enum_type, enum_value};
        }


        ZefEnumValue get_unit_from_aet(const AtomicEntityType & aet) {
            if(!(aet <= AET.QuantityFloat || aet <= AET.QuantityInt))
                throw std::runtime_error("AET is not a type with units.");
            int offset = aet.value % 16;
            return ZefEnumValue{ (aet.value - offset) };
        }
        std::string get_enum_type_from_aet(const AtomicEntityType & aet) {
            if(!(aet <= AET.Enum))
                throw std::runtime_error("AET is not an enum.");
            int offset = aet.value % 16;
            return ZefEnumValue{ (aet.value - offset) }.enum_type();
        }
    }


    DelegateRelationTriple::DelegateRelationTriple(RelationType rt, Delegate source, Delegate target)
            : rt(rt),
              source(std::make_shared<Delegate>(source)),
              target(std::make_shared<Delegate>(target)) {}
    DelegateRelationTriple::DelegateRelationTriple(RelationType rt, std::shared_ptr<Delegate> source, std::shared_ptr<Delegate> target)
        : rt(rt),
          source(source),
          target(target) {}

    // This is implemented in high_level_api.cpp
    //Delegate delegate_of(EZefRef ezr);
    Delegate delegate_of(ZefRef zr) {
        return delegate_of(zr.blob_uzr);
    }

    Delegate delegate_of(EntityType et) {
        return Delegate{1, DelegateEntity{et}};
    }
    Delegate delegate_of(AtomicEntityType aet) {
        return Delegate{1, DelegateAtomicEntity{aet}};
    }
    Delegate delegate_of(RelationType rt) {
        return Delegate{1, DelegateRelationGroup{rt}};
    }
    Delegate delegate_of(const Delegate & d) {
        return Delegate{d.order+1, d.item};
    }

    std::ostream & operator<<(std::ostream & o, const DelegateEntity & d) {
        return o << "d" << d.et;
    }
    std::ostream & operator<<(std::ostream & o, const DelegateAtomicEntity & d) {
        return o << "d" << d.aet;
    }
    std::ostream & operator<<(std::ostream & o, const DelegateRelationGroup & d) {
        return o << "d" << d.rt;
    }
    std::ostream & operator<<(std::ostream & o, const DelegateTX & d) {
        return o << "dTX";
    }
    std::ostream & operator<<(std::ostream & o, const DelegateRoot & d) {
        return o << "dRoot";
    }
    std::ostream & operator<<(std::ostream & o, const DelegateRelationTriple & d) {
        o << "{" << *d.source << ">" << "d" << d.rt << ">" << *d.target << "}";
        return o;
    }

    std::ostream & operator<<(std::ostream & o, const Delegate & d) {
        o << "D" << d.order << "(";
        std::visit([&](auto & x) {
            o << x;
        }, d.item);
        o << ")";
        return o;
    }


}
