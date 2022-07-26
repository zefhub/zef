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
#include "append_structures.h"
#include "butler/threadsafe_map.h"
#include "scalars.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace zefDB {
    struct EntityType;
    struct RelationType;
    struct Keyword;
    struct ValueRepType;
    struct ZefEnumValue;

    namespace internals {
        LIBZEF_DLL_EXPORTED EntityType get_global_entity_type_from_string(const std::string& entity_type_str);
        LIBZEF_DLL_EXPORTED RelationType get_relation_type_from_string(const std::string& relation_type_str);
        LIBZEF_DLL_EXPORTED Keyword get_keyword_from_string(const std::string& keyword_str);
        LIBZEF_DLL_EXPORTED ValueRepType get_vrt_from_enum_type_name_string(const std::string& enum_type_name_str);
        LIBZEF_DLL_EXPORTED ValueRepType get_vrt_from_quantity_float_name_string(const std::string& unit_enum_value_str);
        LIBZEF_DLL_EXPORTED ValueRepType get_vrt_from_quantity_int_name_string(const std::string& unit_enum_value_str);
        LIBZEF_DLL_EXPORTED ZefEnumValue get_enum_value_from_string(const std::string& enum_type, const std::string& enum_val);

        LIBZEF_DLL_EXPORTED std::string get_string_name_from_entity_type(EntityType et) ;
        LIBZEF_DLL_EXPORTED std::string get_string_name_from_relation_type(RelationType rt) ;
        LIBZEF_DLL_EXPORTED std::string get_string_name_from_keyword(Keyword kw) ;
        LIBZEF_DLL_EXPORTED std::string get_string_name_from_value_rep_type(ValueRepType vrt);
        LIBZEF_DLL_EXPORTED string_pair get_enum_string_pair(ZefEnumValue en);

        LIBZEF_DLL_EXPORTED string_pair split_enum_string(const std::string & name);

        LIBZEF_DLL_EXPORTED ZefEnumValue get_unit_from_vrt(const ValueRepType & aet);
        LIBZEF_DLL_EXPORTED std::string get_enum_type_from_vrt(const ValueRepType & aet);
    }

    //////////////////////////////
    // * BlobType


	enum class BlobType : unsigned char {
		_unspecified,
		ROOT_NODE,
		TX_EVENT_NODE,
		RAE_INSTANCE_EDGE,
		TO_DELEGATE_EDGE,
		NEXT_TX_EDGE,
		ENTITY_NODE,
		ATTRIBUTE_ENTITY_NODE,
		VALUE_NODE,
		RELATION_EDGE,
		DELEGATE_INSTANTIATION_EDGE,
		DELEGATE_RETIREMENT_EDGE,
		INSTANTIATION_EDGE,
		TERMINATION_EDGE,
		ATOMIC_VALUE_ASSIGNMENT_EDGE,
		DEFERRED_EDGE_LIST_NODE,
		ASSIGN_TAG_NAME_EDGE,
		NEXT_TAG_NAME_ASSIGNMENT_EDGE,
		FOREIGN_GRAPH_NODE,
		ORIGIN_RAE_EDGE,
		ORIGIN_GRAPH_EDGE,
		FOREIGN_ENTITY_NODE,
		FOREIGN_ATTRIBUTE_ENTITY_NODE,
		FOREIGN_RELATION_EDGE,
        VALUE_TYPE_EDGE,
		VALUE_EDGE,
		ATTRIBUTE_VALUE_ASSIGNMENT_EDGE,
		_last_blobtype,
	};

	struct LIBZEF_DLL_EXPORTED BlobTypeStruct {
		static constexpr BlobType _unspecified = BlobType::_unspecified;
		static constexpr BlobType ROOT_NODE = BlobType::ROOT_NODE;
		static constexpr BlobType TX_EVENT_NODE = BlobType::TX_EVENT_NODE;
		static constexpr BlobType RAE_INSTANCE_EDGE = BlobType::RAE_INSTANCE_EDGE;
		static constexpr BlobType TO_DELEGATE_EDGE = BlobType::TO_DELEGATE_EDGE;
		static constexpr BlobType NEXT_TX_EDGE = BlobType::NEXT_TX_EDGE;
		static constexpr BlobType ENTITY_NODE = BlobType::ENTITY_NODE;
		static constexpr BlobType ATTRIBUTE_ENTITY_NODE = BlobType::ATTRIBUTE_ENTITY_NODE;
		static constexpr BlobType VALUE_NODE = BlobType::VALUE_NODE;
		static constexpr BlobType RELATION_EDGE = BlobType::RELATION_EDGE;
		static constexpr BlobType DELEGATE_INSTANTIATION_EDGE = BlobType::DELEGATE_INSTANTIATION_EDGE;
		static constexpr BlobType DELEGATE_RETIREMENT_EDGE = BlobType::DELEGATE_RETIREMENT_EDGE;
		static constexpr BlobType INSTANTIATION_EDGE = BlobType::INSTANTIATION_EDGE;
		static constexpr BlobType TERMINATION_EDGE = BlobType::TERMINATION_EDGE;
		static constexpr BlobType ATOMIC_VALUE_ASSIGNMENT_EDGE = BlobType::ATOMIC_VALUE_ASSIGNMENT_EDGE;
		static constexpr BlobType DEFERRED_EDGE_LIST_NODE = BlobType::DEFERRED_EDGE_LIST_NODE;
		static constexpr BlobType ASSIGN_TAG_NAME_EDGE = BlobType::ASSIGN_TAG_NAME_EDGE;
		static constexpr BlobType NEXT_TAG_NAME_ASSIGNMENT_EDGE = BlobType::NEXT_TAG_NAME_ASSIGNMENT_EDGE;
		static constexpr BlobType FOREIGN_GRAPH_NODE = BlobType::FOREIGN_GRAPH_NODE;
		static constexpr BlobType ORIGIN_RAE_EDGE = BlobType::ORIGIN_RAE_EDGE;
		static constexpr BlobType ORIGIN_GRAPH_EDGE = BlobType::ORIGIN_GRAPH_EDGE;
		static constexpr BlobType FOREIGN_ENTITY_NODE = BlobType::FOREIGN_ENTITY_NODE;
		static constexpr BlobType FOREIGN_ATTRIBUTE_ENTITY_NODE = BlobType::FOREIGN_ATTRIBUTE_ENTITY_NODE;
		static constexpr BlobType FOREIGN_RELATION_EDGE = BlobType::FOREIGN_RELATION_EDGE;
		static constexpr BlobType VALUE_TYPE_EDGE = BlobType::VALUE_TYPE_EDGE;
		static constexpr BlobType VALUE_EDGE = BlobType::VALUE_EDGE;
		static constexpr BlobType ATTRIBUTE_VALUE_ASSIGNMENT_EDGE = BlobType::ATTRIBUTE_VALUE_ASSIGNMENT_EDGE;

		BlobType operator() (EZefRef uzr) const;
		BlobType operator() (ZefRef zr) const;
	};
	LIBZEF_DLL_EXPORTED BlobType operator| (EZefRef uzr, const BlobTypeStruct& BT_);
	LIBZEF_DLL_EXPORTED BlobType operator| (ZefRef zr, const BlobTypeStruct& BT_);
	constexpr BlobTypeStruct BT;  //singleton instance to address enum value as BT.ROOT_NODE


	LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& o, BlobType bl);



//                        _____       _   _ _        _____                                           
//                       | ____|_ __ | |_(_) |_ _   |_   _|   _ _ __   ___  ___                      
//    _____ _____ _____  |  _| | '_ \| __| | __| | | || || | | | '_ \ / _ \/ __|   _____ _____ _____ 
//   |_____|_____|_____| | |___| | | | |_| | |_| |_| || || |_| | |_) |  __/\__ \  |_____|_____|_____|
//                       |_____|_| |_|\__|_|\__|\__, ||_| \__, | .__/ \___||___/                     
//                                              |___/     |___/|_|                         

	// acts like an enum, saves data like an enum (as a token_value_t=ushort), but is a struct
	struct LIBZEF_DLL_EXPORTED EntityType {		
		constexpr EntityType(token_value_t n = 0) : entity_type_indx(n) {};
		token_value_t entity_type_indx;
		bool operator== (const EntityType& rhs) const { return entity_type_indx == rhs.entity_type_indx; }
		bool operator!= (const EntityType& rhs) const { return entity_type_indx != rhs.entity_type_indx; }
        operator str() const;
	};
	LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& o, EntityType et);

    inline void to_json(json& j, const EntityType & et) {
        j = json{
            {"zef_type", "EntityType"},
            {"entity_type_indx", et.entity_type_indx}
        };
    }

    inline void from_json(const json& j, EntityType & et) {
        assert(j["zef_type"].get<std::string>() == "EntityType");
        j.at("entity_type_indx").get_to(et.entity_type_indx);
    }


	// use Python COG to genereate these? https://www.python.org/about/success/cog/
	// domain specific nodes to shorten node and edge type lookups. This list should be append-only, 
	// i.e. previous string types can be added here for improved performance
	struct LIBZEF_DLL_EXPORTED EntityTypeStruct{
        GraphData * gd = nullptr;
#include "blobs.h.ET.gen"
		EntityType operator() (const std::string& string_input) const;  // we want to be able to use RT("CNCMachine") for unknown types
		EntityType operator() (const char* char_array_input) const { return operator()(std::string(char_array_input)); }
        // These may return an EntityType with a different gd, for convenience.
		EntityType operator() (EZefRef uzr) const;
		EntityType operator() (ZefRef zr) const;

        operator str() const;

	};
	LIBZEF_DLL_EXPORTED EntityType operator| (EZefRef uzr, const EntityTypeStruct& ET_);
	LIBZEF_DLL_EXPORTED EntityType operator| (ZefRef zr, const EntityTypeStruct& ET_);
	constexpr EntityTypeStruct ET;   //singleton instance to address enum value as ET.Machine using zef	
	// EntityType get_entity_type_from_string(const std::string& name);  //fwd declaration
	// std::string get_string_name_from_entity_type(EntityType et);  //fwd declaration



//                        ____      _       _   _           _____                                           
//                       |  _ \ ___| | __ _| |_(_) ___  _ _|_   _|   _ _ __   ___  ___                      
//    _____ _____ _____  | |_) / _ \ |/ _` | __| |/ _ \| '_ \| || | | | '_ \ / _ \/ __|   _____ _____ _____ 
//   |_____|_____|_____| |  _ <  __/ | (_| | |_| | (_) | | | | || |_| | |_) |  __/\__ \  |_____|_____|_____|
//                       |_| \_\___|_|\__,_|\__|_|\___/|_| |_|_| \__, | .__/ \___||___/                     
//                                                               |___/|_|                           


	struct LIBZEF_DLL_EXPORTED RelationType {
		constexpr RelationType(token_value_t n = 0) : relation_type_indx(n) {};
		token_value_t relation_type_indx;
		bool operator== (const RelationType& rhs) const { return relation_type_indx == rhs.relation_type_indx; }
		bool operator!= (const RelationType& rhs) const { return relation_type_indx != rhs.relation_type_indx; }
        operator str() const;
	};
    inline void to_json(json& j, const RelationType & rt) {
        j = json{
            {"zef_type", "RelationType"},
            {"relation_type_indx", rt.relation_type_indx}
        };
    }

    inline void from_json(const json& j, RelationType & rt) {
        assert(j["zef_type"].get<std::string>() == "RelationType");
        j.at("relation_type_indx").get_to(rt.relation_type_indx);
    }

	// same, but for Realtions (edges)
	struct LIBZEF_DLL_EXPORTED RelationTypeStruct{
#include "blobs.h.RT.gen"
		RelationType operator() (const std::string& string_input) const;  // we want to be able to use RT("UsedBy") for unknown types
		RelationType operator() (const char* string_input) const;  // we want to be able to use RT("UsedBy") for unknown types
		RelationType operator() (EZefRef uzr) const;
		RelationType operator() (ZefRef zr) const;
	};
	LIBZEF_DLL_EXPORTED RelationType operator| (EZefRef uzr, const RelationTypeStruct& RT_);
	LIBZEF_DLL_EXPORTED RelationType operator| (ZefRef zr, const RelationTypeStruct& RT_);


	constexpr RelationTypeStruct RT;   //singleton instance to address enum value as RT.UsedBy using zef	
	// RelationType get_relation_type_from_string(const std::string& name);  //fwd declaration
	// std::string get_string_name_from_relation_type(RelationType rt);  //fwd declaration

	LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& o, RelationType rt);

    //////////////////////////////
    // * Zef Keywords


	struct LIBZEF_DLL_EXPORTED Keyword {
		constexpr Keyword(token_value_t n = 0) : indx(n) {};
		token_value_t indx;
		bool operator== (const Keyword& rhs) const { return indx == rhs.indx; }
		bool operator!= (const Keyword& rhs) const { return indx != rhs.indx; }
        operator str() const;
	};

	// same, but for Realtions (edges)
	struct LIBZEF_DLL_EXPORTED KeywordStruct{
#include "blobs.h.KW.gen"
		Keyword operator() (const std::string& string_input) const;  // we want to be able to use KW("UsedBy") for unknown types
		Keyword operator() (const char* string_input) const;  // we want to be able to use KW("UsedBy") for unknown types
	};

	constexpr KeywordStruct KW;   //singleton instance to address enum value as KW.UsedBy using zef	
	// Keyword get_keyword_from_string(const std::string& name);  //fwd declaration
	// std::string get_string_name_from_keyword(Keyword kw);  //fwd declaration

	LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& o, Keyword kw);

    //////////////////////////////
    // * Zef Enums

    struct LIBZEF_DLL_EXPORTED ZefEnumStruct {
        // both the structs and their instantiations are declared inside this struct
#include "zef_enums.h.zefenumstruct.gen"
        ZefEnumValue operator() (const std::string& enum_type, const std::string& enum_val) const;

        // Just for python
        struct Partial {
            std::string enum_type;
        };
        Partial partial(const std::string& enum_type) const {
            return Partial{enum_type};
        }
    };
    constexpr ZefEnumStruct EN;  //singleton instance to address enums as EN.SalesOrderStatus.Completed



    //////////////////////////////
    // * ValueRepType

    inline enum_indx round_down_mod16(enum_indx x) {
        return x - (x % 16);
    }

	struct LIBZEF_DLL_EXPORTED ValueRepType {
		constexpr ValueRepType(enum_indx n = 0) : value(n) {};
		enum_indx value;
        operator str() const;
        constexpr bool operator==(const ValueRepType & other) const { return this->value == other.value; }
        constexpr bool operator!=(const ValueRepType & other) const { return !(*this == other); }
	};
	LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& o, ValueRepType aet);
    inline void to_json(json& j, const ValueRepType & aet) {
        j = json{
            {"zef_type", "ValueRepType"},
            {"value", aet.value}
        };
    }

    inline void from_json(const json& j, ValueRepType & aet) {
        assert(j["zef_type"].get<std::string>() == "ValueRepType");
        j.at("value").get_to(aet.value);
    }

    struct ValueRepTypeStruct {
		struct Enum_ {
#include "blobs.h.AET_enum.gen"
			ValueRepType operator() (std::string name) const { return internals::get_vrt_from_enum_type_name_string(name); }
		};
		static constexpr Enum_ Enum{};

		struct QuantityFloat_ {
#include "blobs.h.AET_qfloat.gen"
            ValueRepType operator() (std::string name) const { return internals::get_vrt_from_quantity_float_name_string(name); }
		};
		static constexpr QuantityFloat_ QuantityFloat{};		

		struct QuantityInt_ {
#include "blobs.h.AET_qint.gen"
			ValueRepType operator() (std::string name) const { return internals::get_vrt_from_quantity_int_name_string(name); }
		};
		static constexpr QuantityInt_ QuantityInt{};		

        static constexpr ValueRepType _unspecified{ 0 };
		static constexpr ValueRepType String{ 1 };
		static constexpr ValueRepType Bool{ 2 };
		static constexpr ValueRepType Float{ 3 };
		static constexpr ValueRepType Int{ 4 };
		static constexpr ValueRepType Time{ 5 };
		static constexpr ValueRepType Serialized{ 6 };
		static constexpr ValueRepType Any{ 7 };
		static constexpr ValueRepType Type{ 8 };

		ValueRepType operator() (EZefRef uzr) const;
		ValueRepType operator() (ZefRef zr) const;
    };
    constexpr ValueRepTypeStruct VRT;

	inline bool is_zef_subtype(ValueRepType vrt1, ValueRepType vrt_super) { return vrt1 == vrt_super; }
	inline bool is_zef_subtype(ValueRepType vrt1, ValueRepTypeStruct::Enum_ enum_super_struct) { return (vrt1.value >= 65536) && (vrt1.value % 16 == 1); }
	inline bool is_zef_subtype(ValueRepType vrt1, ValueRepTypeStruct::QuantityFloat_ quantity_float_super_struct) { return (vrt1.value >= 65536) && (vrt1.value % 16 == 2); }
	inline bool is_zef_subtype(ValueRepType vrt1, ValueRepTypeStruct::QuantityInt_ quantity_int_super_struct) { return (vrt1.value >= 65536) && (vrt1.value % 16 == 3); }








    //////////////////////////////
    // * Token store

	struct LIBZEF_DLL_EXPORTED TokenStore {
        // TODO: Need to replace with mapped-in append-only dictionaries.
        // TODO: Should also be file mapped when possible - not only when possible but mandatory.
        using generic_t = thread_safe_bidirectional_map<token_value_t,std::string>;
        // using ETs_t = MMap::WholeFileMapping<AppendOnlySetVariable<IndexStringPair>>;
        using ENs_t = thread_safe_zef_enum_bidirectional_map;

        generic_t ETs;
		generic_t RTs;
		generic_t KWs;
        ENs_t ENs;

        void init_defaults();
        void init_ETs();
        void init_RTs();
        void init_KWs();
        void init_ENs();

        void load_cached_tokens();
        void save_cached_tokens();

        std::optional<EntityType> ET_from_string(const std::string & s);
        EntityType ET_from_string_failhard(const std::string & s);
        std::optional<std::string> string_from_ET(const EntityType & et);
        std::string string_from_ET_failhard(const EntityType & et);

        std::optional<RelationType> RT_from_string(const std::string & s);
        RelationType RT_from_string_failhard(const std::string & s);
        std::optional<std::string> string_from_RT(const RelationType & rt);
        std::string string_from_RT_failhard(const RelationType & rt);

        std::optional<Keyword> KW_from_string(const std::string & s);
        Keyword KW_from_string_failhard(const std::string & s);
        std::optional<std::string> string_from_KW(const Keyword & kw);
        std::string string_from_KW_failhard(const Keyword & kw);

        std::optional<ZefEnumValue> EN_from_string(const string_pair & s);
        ZefEnumValue EN_from_string_failhard(const string_pair & s);
        std::optional<string_pair> string_from_EN(const ZefEnumValue & en);
        string_pair string_from_EN_failhard(const ZefEnumValue & en);

        // The add_ variants will create if it is missing. These should only be
        // called for local graphs and not the global token store.
        EntityType add_ET_from_string(const std::string & s);
        RelationType add_RT_from_string(const std::string & s);
        Keyword add_KW_from_string(const std::string & s);
        ZefEnumValue add_EN_from_string(const string_pair & s);

        TokenStore(MMap::FileGraph & fg);
        TokenStore();

        // This is internal-use only
        void force_add_entity_type(const token_value_t & indx, const std::string & name);
        void force_add_relation_type(const token_value_t & indx, const std::string & name);
        void force_add_enum_type(const enum_indx & indx, const std::string& name);
        void force_add_keyword(const token_value_t & indx, const std::string & name);
    };

	// on the first run this is created and lives forever. All graphs created and destroyed register and unregister here
	// The definition of this fct has to be in graph.cpp, that the singleton is instantiated once only (the fct contains a static)
    LIBZEF_DLL_EXPORTED TokenStore& global_token_store();



    ////////////////////////////////////
    // * Delegate struct

    // Note: a delegate of "order 0" is a representation of an instance.

    struct LIBZEF_DLL_EXPORTED DelegateTX {
        bool operator==(const DelegateTX & other) const { return true; }
    };
    LIBZEF_DLL_EXPORTED std::ostream & operator<<(std::ostream & o, const DelegateTX & d);
    struct LIBZEF_DLL_EXPORTED DelegateRoot {
        bool operator==(const DelegateRoot & other) const { return true; }
    };
    LIBZEF_DLL_EXPORTED std::ostream & operator<<(std::ostream & o, const DelegateRoot & d);

    struct Delegate;

    struct LIBZEF_DLL_EXPORTED DelegateRelationTriple {
        RelationType rt;

        std::shared_ptr<Delegate> source;
        std::shared_ptr<Delegate> target;
        DelegateRelationTriple(RelationType rt, Delegate source, Delegate target);
        DelegateRelationTriple(RelationType rt, std::shared_ptr<Delegate> source, std::shared_ptr<Delegate> target);

        bool operator==(const DelegateRelationTriple & other) const;
    };
    LIBZEF_DLL_EXPORTED std::ostream & operator<<(std::ostream & o, const DelegateRelationTriple & d);

    struct LIBZEF_DLL_EXPORTED Delegate {
        int order;
        using var_t = std::variant<
            EntityType,
            ValueRepType,
            RelationType,

            DelegateRelationTriple,
            DelegateTX,
            DelegateRoot
            >;
        var_t item;
        Delegate(int order, var_t item) : order(order), item(item) {}
        Delegate(EntityType et) : order(0), item(et) {}
        Delegate(ValueRepType vrt) : order(0), item(vrt) {}
        Delegate(RelationType rt) : order(0), item(rt) {}
        Delegate(Delegate source, RelationType rt, Delegate target) : order(0), item(DelegateRelationTriple{rt,source,target}) {}
        Delegate(const Delegate& other) = default;
        Delegate(Delegate&& other) = default;

        bool operator==(const Delegate & other) const {
            bool same_item = std::visit([&](auto & left, auto & right) -> bool {
                using Tl = typename std::decay_t<decltype(left)>;
                using Tr = typename std::decay_t<decltype(right)>;
                if constexpr(std::is_same_v<Tl, Tr>)
                                return left == right;
                else
                    return false;
            },
                item, other.item);
            return same_item && other.order == order;
        }
    };
    LIBZEF_DLL_EXPORTED std::ostream & operator<<(std::ostream & o, const Delegate & d);

    inline bool DelegateRelationTriple::operator==(const DelegateRelationTriple & other) const {
        return rt == other.rt && *source == *other.source && *target == *other.target;
    }

    // The first of these is actually implemented in high_level_api.cpp
    LIBZEF_DLL_EXPORTED Delegate delegate_of(EZefRef ezr);
    LIBZEF_DLL_EXPORTED Delegate delegate_of(ZefRef zr);

    LIBZEF_DLL_EXPORTED Delegate delegate_of(EntityType et);
    LIBZEF_DLL_EXPORTED Delegate delegate_of(ValueRepType vrt);
    LIBZEF_DLL_EXPORTED Delegate delegate_of(RelationType rt);

    template<class SRC, class TRG>
    DelegateRelationTriple delegate_of(const SRC & source, RelationType rt, const TRG & target) {
        return DelegateRelationTriple{rt, delegate_of(source), delegate_of(target)};
    }

    LIBZEF_DLL_EXPORTED Delegate delegate_of(const Delegate & d);
}

namespace std {
    template<>
    struct hash<zefDB::EntityType> {
        size_t operator()(const zefDB::EntityType & et) {
            return zefDB::get_hash(et.entity_type_indx);
        }
    };

    template<>
    struct hash<zefDB::RelationType> {
        size_t operator()(const zefDB::RelationType & rt) {
            return zefDB::get_hash(rt.relation_type_indx);
        }
    };

    template<>
    struct hash<zefDB::Keyword> {
        size_t operator()(const zefDB::Keyword & kw) {
            return zefDB::get_hash(kw.indx);
        }
    };

    // template<>
    // struct hash<zefDB::EntityTypeContext> {
    //     size_t operator()(const zefDB::EntityTypeContext & et) {
    //         return zefDB::get_hash(et);
    //     }
    // };

    template<>
    struct hash<zefDB::ZefEnumValue> {
        size_t operator()(const zefDB::ZefEnumValue & en) {
            return zefDB::get_hash(en.value);
        }
    };

    template<>
    struct hash<zefDB::ValueRepType> {
        std::size_t operator() (const zefDB::ValueRepType& vrt) const { 
            size_t s = zefDB::hash_char_array("ValueRepType");
            zefDB::hash_combine(s, zefDB::get_hash(vrt.value));
            return s;
        }
    };
}
