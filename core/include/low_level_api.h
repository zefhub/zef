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
	
    struct SerializedValue {
        std::string type;
        std::string data;
        bool operator==(const SerializedValue & other) const {
            // Testing hash first for faster bailout
            return type == other.type && data == other.data;
        }
    };
    inline std::ostream& operator<<(std::ostream& os, SerializedValue & serialized_value) {
        os << "SerializedValue{'" << serialized_value.type << "'}";
        return os;
    }

	struct LIBZEF_DLL_EXPORTED AttributeEntityType {
		constexpr AttributeEntityType() : rep_type(0), complex_value(std::nullopt) {};
		constexpr AttributeEntityType(ValueRepType rep_type) : rep_type(rep_type), complex_value(std::nullopt) {};
		AttributeEntityType(SerializedValue complex_value) : rep_type(VRT._unspecified), complex_value(complex_value) {};

        // In the future, this will become a ZefValue. For now, we are making a
        // "union" except it doesn't share space and isn't contiguous in memory.
        // If complex_value is assigned then it means that is the value of the
        // value type.
		ValueRepType rep_type;
        std::optional<SerializedValue> complex_value;
        operator str() const;
        bool _is_complex() const {
            return (bool)complex_value;
        }

        bool operator== (const AttributeEntityType & other) const {
            if(this->_is_complex() && other._is_complex())
                return *(this->complex_value) == *(other.complex_value);
            else if(this->_is_complex() || other._is_complex())
                return false;
            else
                return this->rep_type == other.rep_type;
        }
        bool operator!= (const AttributeEntityType & other) const { return !(*this == other); }
	};
	LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& o, const AttributeEntityType & aet);

	struct LIBZEF_DLL_EXPORTED AttributeEntityTypeStruct {
		struct Enum_ {
// contextually: only contains enum types
// internal encoding for Enum:  AET.Enum.SomeEnumType.value % 16 = 1
// #include "blobs.h.AET_enum.gen"
			// the following allows us to call AET.Enum("SomeNewEnumNameIOnlyKnowAtRuntime")
			AttributeEntityType operator() (std::string name) const { return VRT.Enum(name); }
		};
		static constexpr Enum_ Enum{};

		struct QuantityFloat_ {
			// contextually: only contains enum values of enum type Unit
			// internal encoding for QuantityFloat:  AET.QuantityFloat.SomeUnit.value % 16 = 2
// #include "blobs.h.AET_qfloat.gen"

            AttributeEntityType operator() (std::string name) const { return VRT.QuantityFloat(name); }
		};
		static constexpr QuantityFloat_ QuantityFloat{};		

		struct QuantityInt_ {
			// contextually: only contains enum values of enum type Unit
			// internal encoding for QuantityInt:  AET.QuantityInt.SomeUnit.value % 16 = 3
// #include "blobs.h.AET_qint.gen"

			AttributeEntityType operator() (std::string name) const { return VRT.QuantityInt(name); }
		};
		static constexpr QuantityInt_ QuantityInt{};		

		AttributeEntityType _unspecified{ VRT._unspecified };
		AttributeEntityType String{ VRT.String };
		AttributeEntityType Bool{ VRT.Bool };
		AttributeEntityType Float{ VRT.Float };
		AttributeEntityType Int{ VRT.Int };
		AttributeEntityType Time{ VRT.Time };
		AttributeEntityType Serialized{ VRT.Serialized };
		AttributeEntityType Any{ VRT.Any };
		AttributeEntityType Type{ VRT.Type };

		AttributeEntityType operator() (EZefRef uzr) const;
		AttributeEntityType operator() (ZefRef zr) const;
		AttributeEntityType operator() (const blobs_ns::ATTRIBUTE_ENTITY_NODE & zr) const;
	};

	LIBZEF_DLL_EXPORTED extern AttributeEntityTypeStruct AET;

	inline bool is_zef_subtype(const AttributeEntityType & aet1, const AttributeEntityType & aet_super) {
        if(aet_super._is_complex())
            throw std::runtime_error("TODO: Need to call into python for this");
        else if(aet1._is_complex())
            return false;
        else
            return is_zef_subtype(aet1.rep_type, aet_super.rep_type);
    }
	inline bool is_zef_subtype(AttributeEntityType aet1, AttributeEntityTypeStruct::Enum_ enum_super_struct) { return !aet1._is_complex() && is_zef_subtype(aet1.rep_type, VRT.Enum); }
	inline bool is_zef_subtype(AttributeEntityType aet1, AttributeEntityTypeStruct::QuantityFloat_ quantity_float_super_struct) { return !aet1._is_complex() && is_zef_subtype(aet1.rep_type, VRT.QuantityFloat); }
    inline bool is_zef_subtype(AttributeEntityType aet1, AttributeEntityTypeStruct::QuantityInt_ quantity_int_super_struct) { return !aet1._is_complex() && is_zef_subtype(aet1.rep_type, VRT.QuantityInt); }

	inline bool is_zef_subtype(EZefRef uzr, AttributeEntityTypeStruct AET) { return is_zef_subtype(uzr, BT.ATTRIBUTE_ENTITY_NODE); }
	inline bool is_zef_subtype(EZefRef uzr, AttributeEntityType aet_super) { return is_zef_subtype(uzr, AET) && is_zef_subtype(AET(uzr), aet_super); }
    inline bool is_zef_subtype(EZefRef uzr, AttributeEntityTypeStruct::Enum_ aet_super) { return is_zef_subtype(uzr, AET) && is_zef_subtype(AET(uzr), aet_super); }
    inline bool is_zef_subtype(EZefRef uzr, AttributeEntityTypeStruct::QuantityFloat_ aet_super) { return is_zef_subtype(uzr, AET) && is_zef_subtype(AET(uzr), aet_super); }
    inline bool is_zef_subtype(EZefRef uzr, AttributeEntityTypeStruct::QuantityInt_ aet_super) { return is_zef_subtype(uzr, AET) && is_zef_subtype(AET(uzr), aet_super); }
	inline bool is_zef_subtype(ZefRef zr, AttributeEntityTypeStruct AET) { return is_zef_subtype(zr.blob_uzr, AET); }
	inline bool is_zef_subtype(ZefRef zr, AttributeEntityType aet_super) { return is_zef_subtype(zr.blob_uzr, aet_super); }
    inline bool is_zef_subtype(ZefRef zr, AttributeEntityTypeStruct::Enum_ aet_super) { return is_zef_subtype(zr.blob_uzr, aet_super); }
    inline bool is_zef_subtype(ZefRef zr, AttributeEntityTypeStruct::QuantityFloat_ aet_super) { return is_zef_subtype(zr.blob_uzr, aet_super); }
    inline bool is_zef_subtype(ZefRef zr, AttributeEntityTypeStruct::QuantityInt_ aet_super) { return is_zef_subtype(zr.blob_uzr, aet_super); }




    using value_variant_t = std::variant<bool,int,double,str,Time,ZefEnumValue,QuantityFloat,QuantityInt,SerializedValue,AttributeEntityType>;

    inline ValueRepType get_vrt_from_ctype(const bool & value) { return VRT.Bool; }
    inline ValueRepType get_vrt_from_ctype(const int & value) { return VRT.Int; }
    inline ValueRepType get_vrt_from_ctype(const double & value) { return VRT.Float; }
    inline ValueRepType get_vrt_from_ctype(const std::string & value) { return VRT.String; }
    inline ValueRepType get_vrt_from_ctype(const Time & value) { return VRT.Time; }
    inline ValueRepType get_vrt_from_ctype(const SerializedValue & value) { return VRT.Serialized; }
    inline ValueRepType get_vrt_from_ctype(const ZefEnumValue & value) { return VRT.Enum(value.enum_type()); }
    inline ValueRepType get_vrt_from_ctype(const QuantityFloat & value) { return ValueRepType{value.unit.value + 2}; }
    inline ValueRepType get_vrt_from_ctype(const QuantityInt & value) { return ValueRepType{value.unit.value + 3}; }
    inline ValueRepType get_vrt_from_ctype(const ValueRepType & value) { return VRT.Type; }
    inline ValueRepType get_vrt_from_ctype(const AttributeEntityType & value) { return VRT.Type; }
    
	namespace internals{

        // Compare function for looking up values in the value hashmap, using hash and blob index.
        template<class T>
        std::function<int(value_hash_t,blob_index)> create_compare_func_for_value_node(GraphData & gd, const T * value);

        template<>
        std::function<int(value_hash_t,blob_index)> create_compare_func_for_value_node(GraphData & gd, const value_variant_t * value);

		// have similar API to instantiate and link low level blobs / EZefRefs
		LIBZEF_DLL_EXPORTED EZefRef instantiate(BlobType bt, GraphData& gd);
		LIBZEF_DLL_EXPORTED EZefRef instantiate(EZefRef src, BlobType bt, EZefRef trg, GraphData& gd);


        template<typename T>
        value_hash_t value_hash(const T & value) {
            return get_hash(value);
        }

        // template value_hash_t value_hash(const bool & value);
        // template value_hash_t value_hash(const int & value);
        // template value_hash_t value_hash(const double & value);
        // template value_hash_t value_hash(const str & value);
        // template value_hash_t value_hash(const Time & value);
        // template value_hash_t value_hash(const ZefEnumValue & value);
        // template value_hash_t value_hash(const QuantityFloat & value);
        // template value_hash_t value_hash(const QuantityInt & value);

        template<>
        inline value_hash_t value_hash(const SerializedValue & value) { return std::hash<str>()(value.data); }
        template<>
        inline value_hash_t value_hash(const value_variant_t & value) {
            return std::visit([](auto & x) { return value_hash(x); }, value);
        }

        // Value nodes
        template<typename T>
        EZefRef instantiate_value_node(const T & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template EZefRef instantiate_value_node(const bool & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template EZefRef instantiate_value_node(const int & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template EZefRef instantiate_value_node(const double & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template EZefRef instantiate_value_node(const str & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template EZefRef instantiate_value_node(const Time & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template EZefRef instantiate_value_node(const ZefEnumValue & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template EZefRef instantiate_value_node(const QuantityFloat & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template EZefRef instantiate_value_node(const QuantityInt & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template EZefRef instantiate_value_node(const SerializedValue & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template EZefRef instantiate_value_node(const AttributeEntityType & value, GraphData& g);

        template<typename T>
        std::optional<EZefRef> search_value_node(const T & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template std::optional<EZefRef> search_value_node(const bool & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template std::optional<EZefRef> search_value_node(const int & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template std::optional<EZefRef> search_value_node(const double & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template std::optional<EZefRef> search_value_node(const str & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template std::optional<EZefRef> search_value_node(const Time & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template std::optional<EZefRef> search_value_node(const ZefEnumValue & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template std::optional<EZefRef> search_value_node(const QuantityFloat & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template std::optional<EZefRef> search_value_node(const QuantityInt & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template std::optional<EZefRef> search_value_node(const SerializedValue & value, GraphData& g);
        LIBZEF_DLL_EXPORTED extern template std::optional<EZefRef> search_value_node(const AttributeEntityType & value, GraphData& g);






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
			case BT.ATTRIBUTE_ENTITY_NODE: return true;
			case BT.VALUE_NODE: return true;
			case BT.RELATION_EDGE: return true;
			case BT.TX_EVENT_NODE: return true;
			case BT.ROOT_NODE: return true;
			default: return false;
			};
		}

        inline bool is_foreign_rae_blob(BlobType bt) {
			switch (bt) {
			case BT.FOREIGN_ENTITY_NODE: return true;
			case BT.FOREIGN_ATTRIBUTE_ENTITY_NODE: return true;
			case BT.FOREIGN_RELATION_EDGE: return true;
			// case BT.FOREIGN_TX_EVENT_NODE: return true;
			// case BT.FOREIGN_ROOT_NODE: return true;
            case BT.FOREIGN_GRAPH_NODE: return true;
			default: return false;
			};
        }

        LIBZEF_DLL_EXPORTED bool is_root(EZefRef z);
        LIBZEF_DLL_EXPORTED bool is_delegate(EZefRef z);
        LIBZEF_DLL_EXPORTED bool is_delegate_relation_group(EZefRef z);
        LIBZEF_DLL_EXPORTED bool has_delegate(EZefRef z);

        template <typename T>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const T & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const int & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const double & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const bool & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, char const * const & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const Time & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const QuantityFloat & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const QuantityInt & value_to_be_assigned);
        extern template void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const ZefEnumValue & value_to_be_assigned);

        template<>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const std::string & value_to_be_assigned);
        template<>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const SerializedValue & value_to_be_assigned);
        template<>
        void copy_to_buffer(char* data_buffer_ptr, unsigned int& buffer_size_in_bytes, const AttributeEntityType & value_to_be_assigned);

        template<class T>
        bool is_compatible_rep_type(const ValueRepType & vrt);

        template<> bool is_compatible_rep_type<value_variant_t>(const ValueRepType & vrt);
        template<> bool is_compatible_rep_type<bool>(const ValueRepType & vrt);
        template<> bool is_compatible_rep_type<int>(const ValueRepType & vrt);
        template<> bool is_compatible_rep_type<double>(const ValueRepType & vrt);
        template<> bool is_compatible_rep_type<str>(const ValueRepType & vrt);
        template<> bool is_compatible_rep_type<const char*>(const ValueRepType & vrt);
        template<> bool is_compatible_rep_type<Time>(const ValueRepType & vrt);
        template<> bool is_compatible_rep_type<SerializedValue>(const ValueRepType & vrt);
        template<> bool is_compatible_rep_type<AttributeEntityType>(const ValueRepType & vrt);

        // These can only check that they are in the class of e.g.
        // QuantityFloats, but not the specific units or enum_type
        template<> bool is_compatible_rep_type<ZefEnumValue>(const ValueRepType & vrt);
        template<> bool is_compatible_rep_type<QuantityFloat>(const ValueRepType & vrt);
        template<> bool is_compatible_rep_type<QuantityInt>(const ValueRepType & vrt);

        // TODO: Call into python (registered function) if AET is complex
        template<class T>
        bool is_compatible(const T & val, const AttributeEntityType & aet);

        extern template bool is_compatible(const bool & val, const AttributeEntityType & aet);
        extern template bool is_compatible(const int & val, const AttributeEntityType & aet);
        extern template bool is_compatible(const double & val, const AttributeEntityType & aet);
        extern template bool is_compatible(const str & val, const AttributeEntityType & aet);
        // extern template bool is_compatible(const char const * val, const AttributeEntityType & aet);
        extern template bool is_compatible(const Time & val, const AttributeEntityType & aet);
        extern template bool is_compatible(const SerializedValue & val, const AttributeEntityType & aet);
        extern template bool is_compatible(const ZefEnumValue & en, const AttributeEntityType & aet);
        extern template bool is_compatible(const QuantityFloat & q, const AttributeEntityType & aet);
        extern template bool is_compatible(const QuantityInt & q, const AttributeEntityType & aet);
        extern template bool is_compatible(const AttributeEntityType & val, const AttributeEntityType & aet);
        template<>
        LIBZEF_DLL_EXPORTED bool is_compatible(const EZefRef & z, const AttributeEntityType & aet);
        template<>
        LIBZEF_DLL_EXPORTED bool is_compatible(const value_variant_t & z, const AttributeEntityType & aet);

        // Note: QuantityInt/Float and Enum will just have to be unscoped here.

        // Because the pair-product of all types is a large space, we are going
        // to leave this as a header-only function.

        // Stupid class required to make the special case of T == V work out.
        template<typename T>
        struct value_cast_Impl {
            template<typename V>
            static T impl(V val) {
                throw std::runtime_error("Unable to convert");
            }
            static T impl(T val) {
                return val;
            }
        };

        template<> template<>
		inline double value_cast_Impl<double>::impl(int val) { return double(val); }
        template<> template<>
		inline int value_cast_Impl<int>::impl(double val) { 
			if (fabs(val - round(val)) > 1E-8)
				throw std::runtime_error("converting a double to an int, but the double was numerically not sufficiently close to an in to make rounding safe");
			return int(val);
		}
        template<> template<>
		inline bool value_cast_Impl<bool>::impl(int val) { 
			if(val == 1) return true; 
			if(val == 0) return false; 
			throw std::runtime_error("converting an int to a bool, but the value was neither 0 or 1");
		}
        template<> template<>
		inline int value_cast_Impl<int>::impl(bool val) { 
			if(val) return 1; 
			else return 0;
		}

		template<typename T, typename V>
		T value_cast(V val) {
            return value_cast_Impl<T>::impl(val);
        }

        template<class T>
        T value_from_node(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        template<class T>
        T value_from_node(const blobs_ns::VALUE_NODE& av);

        extern template str value_from_node<str>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae); 
        extern template SerializedValue value_from_node<SerializedValue>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae); 
        extern template double value_from_node<double>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template int value_from_node<int>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template QuantityInt value_from_node<QuantityInt>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template QuantityFloat value_from_node<QuantityFloat>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template ZefEnumValue value_from_node<ZefEnumValue>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template bool value_from_node<bool>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template AttributeEntityType value_from_node<AttributeEntityType>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);
        extern template value_variant_t value_from_node<value_variant_t>(const blobs_ns::ATOMIC_VALUE_ASSIGNMENT_EDGE& aae);

        extern template str value_from_node<str>(const blobs_ns::VALUE_NODE& av); 
        extern template SerializedValue value_from_node<SerializedValue>(const blobs_ns::VALUE_NODE& av); 
        extern template double value_from_node<double>(const blobs_ns::VALUE_NODE& av);
        extern template int value_from_node<int>(const blobs_ns::VALUE_NODE& av);
        extern template QuantityInt value_from_node<QuantityInt>(const blobs_ns::VALUE_NODE& av);
        extern template QuantityFloat value_from_node<QuantityFloat>(const blobs_ns::VALUE_NODE& av);
        extern template ZefEnumValue value_from_node<ZefEnumValue>(const blobs_ns::VALUE_NODE& av);
        extern template bool value_from_node<bool>(const blobs_ns::VALUE_NODE& av);
        extern template AttributeEntityType value_from_node<AttributeEntityType>(const blobs_ns::VALUE_NODE& av);
        extern template value_variant_t value_from_node<value_variant_t>(const blobs_ns::VALUE_NODE& av);

		template <typename T>
		T abs_val(T val) {       
			return val > 0 ? val : -val;
		}                        
                                 
                                 
		inline EZefRef get_RAE_INSTANCE_EDGE(EZefRef my_entity_or_rel) {
			for (auto ind : AllEdgeIndexes(my_entity_or_rel)) {
				if (ind < 0) {   
					auto candidate = EZefRef(abs_val(ind), *graph_data(my_entity_or_rel));
					if (get<BlobType>(candidate) == BlobType::RAE_INSTANCE_EDGE) return candidate;
				}                
			}                    
			throw std::runtime_error("We should not have landed here in get_RAE_INSTANCE_EDGE: there should have been one el to return");
			return my_entity_or_rel; // hack to suppress compiler warnings
		}                        

		inline EZefRef get_TO_DELEGATE_EDGE(EZefRef my_entity_or_rel) {
			for (auto ind : AllEdgeIndexes(my_entity_or_rel)) {
				if (ind < 0) {   
					auto candidate = EZefRef(abs_val(ind), *graph_data(my_entity_or_rel));
					if (get<BlobType>(candidate) == BlobType::TO_DELEGATE_EDGE) return candidate;
				}                
			}                    
			throw std::runtime_error("We should not have landed here in get_TO_DELEGATE_EDGE: there should have been one el to return");
			return my_entity_or_rel; // hack to suppress compiler warnings
		}                        


    }
}

namespace std {
    template<>
    struct hash<zefDB::SerializedValue> {
        std::size_t operator() (const zefDB::SerializedValue& val) const { 
            size_t s = zefDB::hash_char_array("SerializedValue");
            zefDB::hash_combine(s, zefDB::get_hash(val.type));
            zefDB::hash_combine(s, zefDB::get_hash(val.data));
            return s;
        }
    };
    template<>
    struct hash<zefDB::AttributeEntityType> {
        std::size_t operator() (const zefDB::AttributeEntityType& aet) const { 
            size_t s = zefDB::hash_char_array("AttributeEntityType");
            if(aet._is_complex()) {
                zefDB::hash_combine(s, zefDB::get_hash(*aet.complex_value));
            } else {
                zefDB::hash_combine(s, aet.rep_type);
            }
            return s;
        }
    };
}