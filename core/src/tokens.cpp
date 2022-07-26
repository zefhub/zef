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
#include "zef_config.h"


namespace zefDB {

	namespace internals {
		EntityType get_entity_type(EZefRef uzr){ return ET(uzr); }  // exposed that via pybind that it can be used in the 'EntityTypeStruct' defined in the python __init__ .py
		RelationType get_relation_type(EZefRef uzr){ return RT(uzr); }
		BlobType get_blob_type(EZefRef uzr){ return BT(uzr);}
		AttributeEntityType get_atomic_entity_type(EZefRef uzr){ return AET(uzr); }
	}


    //////////////////////////////
    // * BlobType

	std::ostream& operator << (std::ostream& o, BlobType bl) {		
        switch (bl) {
#define SHOW_BLOB(x) case BlobType::x: {o << #x; break; }
            SHOW_BLOB(_unspecified)
            SHOW_BLOB(ROOT_NODE)
            SHOW_BLOB(TX_EVENT_NODE)
            SHOW_BLOB(RAE_INSTANCE_EDGE)
            SHOW_BLOB(TO_DELEGATE_EDGE)
            SHOW_BLOB(NEXT_TX_EDGE)
            SHOW_BLOB(ENTITY_NODE)
            SHOW_BLOB(ATTRIBUTE_ENTITY_NODE)
            SHOW_BLOB(VALUE_NODE)
            SHOW_BLOB(RELATION_EDGE)
            SHOW_BLOB(DELEGATE_INSTANTIATION_EDGE)
            SHOW_BLOB(DELEGATE_RETIREMENT_EDGE)
            SHOW_BLOB(INSTANTIATION_EDGE)
            SHOW_BLOB(TERMINATION_EDGE)
            SHOW_BLOB(ATOMIC_VALUE_ASSIGNMENT_EDGE)
            SHOW_BLOB(DEFERRED_EDGE_LIST_NODE)
            SHOW_BLOB(ASSIGN_TAG_NAME_EDGE)
            SHOW_BLOB(NEXT_TAG_NAME_ASSIGNMENT_EDGE)
            SHOW_BLOB(FOREIGN_GRAPH_NODE)
            SHOW_BLOB(ORIGIN_RAE_EDGE)
            SHOW_BLOB(ORIGIN_GRAPH_EDGE)
            SHOW_BLOB(FOREIGN_ENTITY_NODE)
            SHOW_BLOB(FOREIGN_ATTRIBUTE_ENTITY_NODE)
            SHOW_BLOB(FOREIGN_RELATION_EDGE)
            SHOW_BLOB(VALUE_TYPE_EDGE)
            SHOW_BLOB(VALUE_EDGE)
            SHOW_BLOB(ATTRIBUTE_VALUE_ASSIGNMENT_EDGE)
#undef SHOW_BLOB
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
    // * VRT

	ValueRepType ValueRepTypeStruct::operator() (EZefRef uzr) const {
		if (get<BlobType>(uzr) == BlobType::ATTRIBUTE_ENTITY_NODE)
            return get<blobs_ns::ATTRIBUTE_ENTITY_NODE>(uzr).primitive_type;
        else if(get<BlobType>(uzr) == BlobType::VALUE_NODE)
            return get<blobs_ns::VALUE_NODE>(uzr).rep_type;
        else
            throw std::runtime_error("VRT(EZefRef uzr) called for a uzr which is not an atomic entity or value.");
	}
	ValueRepType ValueRepTypeStruct::operator() (ZefRef zr) const {
		return VRT(zr.blob_uzr);
	}

	ValueRepType::operator str () const {
        return internals::get_string_name_from_value_rep_type(*this);
    }

	std::ostream& operator << (std::ostream& o, ValueRepType vrt) {
        o << "VRT.";
		o << str(vrt);
		return o;
	}


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


    //////////////////////////////
    // * TokenStore

	TokenStore& global_token_store() {
        // if new graphs are created often, don't always reallocate this vector
        static TokenStore* ptr_to_singleton = new TokenStore();
        return *ptr_to_singleton;
	}

    TokenStore::TokenStore() {
        init_defaults();

        load_cached_tokens();
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


    std::filesystem::path get_cached_tokens_path() {
        std::string path = std::get<std::string>(get_config_var("tokens.cachePath"));
        if(starts_with(path, "$CONFIG/"))
            return zefdb_config_path() / path.substr(strlen("$CONFIG/"));
        return path;
    }

    void TokenStore::save_cached_tokens() {
        try {
            std::filesystem::path cache_path = get_cached_tokens_path();
            if(cache_path == "")
                return;
            auto lock_path = cache_path;
            lock_path += ".lock";
            FileLock file_lock(lock_path);

            json j{
                {"ETs", ETs.all_entries_as_list()},
                {"RTs", RTs.all_entries_as_list()},
                {"ENs", ENs.all_entries_as_list()},
                {"KWs", KWs.all_entries_as_list()},
            };

            if(zwitch.developer_output())
                std::cerr << "Going to save tokens to: " << cache_path << std::endl;
            std::ofstream file(cache_path);
            file << j;
            if(!file.good())
                throw std::runtime_error("Error in writing file.");
        } catch(const std::exception & e) {
            std::cerr << "Error while trying to save cached tokens: " << e.what() << std::endl;
        }
    }

    void TokenStore::load_cached_tokens() {
        try {
            std::filesystem::path cache_path = get_cached_tokens_path();
            if(cache_path == "")
                return;
            auto lock_path = cache_path;
            lock_path += ".lock";
            FileLock file_lock(lock_path);

            if(zwitch.developer_output())
                std::cerr << "Going to attempt loading cached tokens from: " << cache_path << std::endl;
            
            if(!std::filesystem::exists(cache_path))
                return;

            std::ifstream file(cache_path);
            json j;
            file >> j;

            auto j_ETs = j["ETs"].get<std::vector<std::pair<token_value_t,std::string>>>();
            auto j_RTs = j["RTs"].get<std::vector<std::pair<token_value_t,std::string>>>();
            auto j_KWs = j["KWs"].get<std::vector<std::pair<token_value_t,std::string>>>();
            auto j_ENs = j["ENs"].get<std::vector<std::tuple<token_value_t,std::string,std::string>>>();

            for(auto & it : j_ETs)
                force_add_entity_type(it.first, it.second);
            for(auto & it : j_RTs)
                force_add_relation_type(it.first, it.second);
            for(auto & it : j_KWs)
                force_add_keyword(it.first, it.second);
            for(auto & it : j_ENs)
                force_add_enum_type(std::get<0>(it), std::get<1>(it) + "." + std::get<2>(it));
        } catch(const std::exception & e) {
            std::cerr << "Error while trying to load cached tokens: " << e.what() << std::endl;
        }
    }

    // TODO: Replace these with the individual type updates, which will occur in new messages from zefhub.
    template<typename T1, typename T2>
    void update_bidirection_name_map(thread_safe_bidirectional_map<T1,T2>& map_to_update, const T1 & indx, const T2 & name) {			
        auto prior_name = map_to_update.maybe_at(indx);
        auto prior_indx = map_to_update.maybe_at(name);
        if(prior_name && *prior_name != name)
            throw std::runtime_error("Force adding token disagrees with existing token: " + *prior_name);
        else if(prior_indx && *prior_indx != indx)
            throw std::runtime_error("Force adding token disagrees with existing token: " + to_str(*prior_indx));
        else
            map_to_update.insert(indx, name);
    }


    void update_zef_enum_bidirectional_map(thread_safe_zef_enum_bidirectional_map& map_to_update, const enum_indx & indx, const std::string& name) {
        auto [enum_type,enum_value] = internals::split_enum_string(name);
        auto pair = std::make_pair(enum_type, enum_value);
        auto prior_pair = map_to_update.maybe_at(indx);
        auto prior_indx = map_to_update.maybe_at(pair);
        if(prior_pair && *prior_pair != pair)
            throw std::runtime_error("Force adding EN disagrees with existing token: " + std::get<0>(*prior_pair) + "." + std::get<1>(*prior_pair));
        else if(prior_indx && *prior_indx != indx)
            throw std::runtime_error("Force adding EN disagrees with existing token: " + to_str(*prior_indx));
        else
            map_to_update.insert(indx, pair);
    }




    void TokenStore::force_add_entity_type(const token_value_t & indx, const std::string & name) {
        update_bidirection_name_map(ETs, indx, name);
    }

    void TokenStore::force_add_relation_type(const token_value_t & indx, const std::string & name) {
        update_bidirection_name_map(RTs, indx, name);
    }

    void TokenStore::force_add_keyword(const token_value_t & indx, const std::string & name) {
        update_bidirection_name_map(KWs, indx, name);
    }

    void TokenStore::force_add_enum_type(const enum_indx & indx, const std::string& name) {
        update_zef_enum_bidirectional_map(ENs, indx, name);
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

        ValueRepType get_vrt_from_enum_type_name_string(const std::string& enum_type_name_str) {
            switch (hash_char_array(enum_type_name_str.data())) {
#include "graph.cpp.AETenumfromstring.gen"
            default:
                return ValueRepType{ get_enum_value_from_string(enum_type_name_str, "").value + 1 };
            }
        }


        ValueRepType get_vrt_from_quantity_float_name_string(const std::string& unit_enum_value_str) {
            using namespace std::chrono;
            switch (hash_char_array(unit_enum_value_str.data())) {
#include "graph.cpp.AETquantityfloatfromstring.gen"
            default:
                return ValueRepType{ get_enum_value_from_string("Unit", unit_enum_value_str).value + 2 };
            }
        }


        ValueRepType get_vrt_from_quantity_int_name_string(const std::string& unit_enum_value_str) {
            using namespace std::chrono;
            switch (hash_char_array(unit_enum_value_str.data())) {
#include "graph.cpp.AETquantityintfromstring.gen"
            default:
                return ValueRepType{ get_enum_value_from_string("Unit", unit_enum_value_str).value + 3 };
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

        std::string get_string_name_from_value_rep_type(ValueRepType vrt) {
            // TODO: See above
            switch (vrt.value) {
            case VRT._unspecified.value: {return "_unspecified"; break; }
            case VRT.String.value: {return "String"; break; }
            case VRT.Bool.value: {return "Bool"; break; }
            case VRT.Float.value: {return "Float"; break; }
            case VRT.Int.value: {return "Int"; break; }
            case VRT.Time.value: {return "Time"; break; }
            case VRT.Serialized.value: {return "Serialized"; break; }
            case VRT.Any.value: {return "Any"; break; }
            case VRT.Type.value: {return "Type"; break; }

#include "graph.cpp.stringfromAET.gen"
                // if we reach here, it may still be a known type. It was just not known at compile time
            default: {
                // deal with dictionary lookup here, e.g. for enum types, QuantityFloat and others that can be extended at runtime.
                ZefEnumValue base_en{round_down_mod16(vrt.value)};
                switch (vrt.value % 16) {
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


        ZefEnumValue get_unit_from_vrt(const ValueRepType & vrt) {
            if(!is_zef_subtype(vrt, VRT.QuantityFloat) && !is_zef_subtype(vrt, VRT.QuantityInt))
                throw std::runtime_error("VRT is not a type with units.");
            int offset = vrt.value % 16;
            return ZefEnumValue{ (vrt.value - offset) };
        }
        std::string get_enum_type_from_vrt(const ValueRepType & vrt) {
            if(!is_zef_subtype(vrt, VRT.Enum))
                throw std::runtime_error("VRT is not an enum.");
            int offset = vrt.value % 16;
            return ZefEnumValue{ (vrt.value - offset) }.enum_type();
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
        return Delegate{1, et};
    }
    Delegate delegate_of(ValueRepType vrt) {
        return Delegate{1, vrt};
    }
    Delegate delegate_of(RelationType rt) {
        return Delegate{1, rt};
    }
    Delegate delegate_of(const Delegate & d) {
        return Delegate{d.order+1, d.item};
    }

    void show_wrapper(std::ostream & o, const Delegate & d, bool outer) {
        if(outer || d.order > 0)
            o << "D" << d.order;
        std::visit(overloaded {
                [&](const DelegateRelationTriple & x) {
                    o << x;
                },
                [&](auto & x) {
                    if(outer || d.order > 0)
                        o << "(" << x << ")";
                    else
                        o << x;
                }}, d.item);
    }

    std::ostream & operator<<(std::ostream & o, const DelegateTX & d) {
        return o << "TX";
    }
    std::ostream & operator<<(std::ostream & o, const DelegateRoot & d) {
        return o << "Root";
    }
    std::ostream & operator<<(std::ostream & o, const DelegateRelationTriple & d) {
        o << "{";
        show_wrapper(o, *d.source, false);
        o << "," << d.rt << ",";
        show_wrapper(o, *d.target, false);
        o << "}";
        return o;
    }

    std::ostream & operator<<(std::ostream & o, const Delegate & d) {
        show_wrapper(o, d, true);
        return o;
    }


}
