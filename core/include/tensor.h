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

#include "blobs.h"
#include <array>
#include <variant>
#include <vector>
#include <string>
#include <initializer_list>

namespace zefDB {

	template <typename ScalarType, int TensorOrder>
	struct Tensor {};





	//                                      _                 ___        _                                                     
	//                         ___  _ __ __| | ___ _ __      / _ \      | |_ ___ _ __  ___  ___  _ __ ___                      
	//    _____ _____ _____   / _ \| '__/ _` |/ _ \ '__|    | | | |     | __/ _ \ '_ \/ __|/ _ \| '__/ __|   _____ _____ _____ 
	//   |_____|_____|_____| | (_) | | | (_| |  __/ |       | |_| |     | ||  __/ | | \__ \ (_) | |  \__ \  |_____|_____|_____|
	//                        \___/|_|  \__,_|\___|_|        \___/       \__\___|_| |_|___/\___/|_|  |___/                     
	//                                                                                                           

	template <typename ScalarType>
	struct Tensor<ScalarType, 0> {
		ScalarType val;
	};









	//                                      _                _       _                                                     
	//                         ___  _ __ __| | ___ _ __     / |     | |_ ___ _ __  ___  ___  _ __ ___                      
	//    _____ _____ _____   / _ \| '__/ _` |/ _ \ '__|    | |     | __/ _ \ '_ \/ __|/ _ \| '__/ __|   _____ _____ _____ 
	//   |_____|_____|_____| | (_) | | | (_| |  __/ |       | |     | ||  __/ | | \__ \ (_) | |  \__ \  |_____|_____|_____|
	//                        \___/|_|  \__,_|\___|_|       |_|      \__\___|_| |_|___/\___/|_|  |___/                     
	//                                                                                                                   


	// partial template specialization
	template <typename ScalarType>
	struct Tensor<ScalarType, 1> {
		struct Iterator;
		struct const_Iterator;

		using var = std::variant<
			std::array<ScalarType, 0>,   // SSO
			std::array<ScalarType, 1>,
			std::array<ScalarType, 2>,
			std::array<ScalarType, 3>,
			std::vector<ScalarType>
		>;
		var val;

		Tensor<ScalarType, 1>(var init_val) : val(init_val) {}
		Tensor<ScalarType, 1>(std::initializer_list<ScalarType> init_list) {}

		Tensor<ScalarType, 1>(int init_size) {
			if (init_size == 0) val = std::array<ScalarType, 0>();
			else if (init_size == 1) val = std::array<ScalarType, 1>();
			else if (init_size == 2) val = std::array<ScalarType, 2>();
			else if (init_size == 3) val = std::array<ScalarType, 3>();
			else                     val = std::vector<ScalarType>(init_size);
		}  // ctor only specifying the size

		ScalarType& operator[] (size_t m) { return *std::visit([m](auto& v)->ScalarType* { return &v[m]; }, val); }
		const ScalarType& operator[] (size_t m) const { return *std::visit([m](const auto& v)->ScalarType* { return &v[m]; }, val); }

		size_t size() { return std::visit([](auto& x) {return x.size(); }, val); }
		Iterator begin();
		Iterator end();		
		const_Iterator begin() const;   // East-const for clarity :)
		const_Iterator end() const;
	};




	template <typename ScalarType>
	struct Tensor<ScalarType, 1>::Iterator {
		// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
		using value_type = ScalarType;
		using reference = ScalarType&;
		using pointer = ScalarType*;
		using iterator_category = std::random_access_iterator_tag;
		using difference_type = ptrdiff_t;

		ScalarType* ptr_to_current_el = nullptr;
		Iterator& operator++() { ++ptr_to_current_el; return *this; }		// pre-increment op: this one is used mostly
		Iterator operator++(int) { return Iterator{ ptr_to_current_el++ }; }		// post incremenet
		reference operator*() { return *ptr_to_current_el; }
		std::add_const_t<reference> operator*() const { return *ptr_to_current_el; }
		bool operator!=(const Iterator& other) const { return ptr_to_current_el != other.ptr_to_current_el; }
		bool operator==(const Iterator& other) const { return ptr_to_current_el == other.ptr_to_current_el; }

		Iterator(ScalarType* ptr) : ptr_to_current_el(ptr) {}
	};

	template <typename ScalarType>
	typename Tensor<ScalarType, 1>::Iterator Tensor<ScalarType, 1>::begin() {
		return Tensor<ScalarType, 1>::Iterator(
		std::visit([](auto& v) { return v.data(); }, val) );
	}

	template <typename ScalarType>
	typename Tensor<ScalarType, 1>::Iterator Tensor<ScalarType, 1>::end() {
		return Tensor<ScalarType, 1>::Iterator(
            std::visit([](auto& v) { return v.data() + v.size(); }, val) );
	}




	template <typename ScalarType>
	struct Tensor<ScalarType, 1>::const_Iterator {
		// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
		using value_type = ScalarType;
		using reference = const ScalarType&;
		using pointer = const ScalarType*;
		using iterator_category = std::random_access_iterator_tag;
		using difference_type = ptrdiff_t;

		const ScalarType* ptr_to_current_el = nullptr;
		const_Iterator& operator++() { ++ptr_to_current_el; return *this; }		// pre-increment op: this one is used mostly
		const_Iterator operator++(int) { return const_Iterator{ ptr_to_current_el++ }; }		// post incremenet
		reference operator*() { return *ptr_to_current_el; }
		std::add_const_t<reference> operator*() const { return *ptr_to_current_el; }		
		bool operator!=(const const_Iterator& other) const { return ptr_to_current_el != other.ptr_to_current_el; }
		bool operator==(const const_Iterator& other) const { return ptr_to_current_el == other.ptr_to_current_el; }
	};

	template <typename ScalarType>
	typename Tensor<ScalarType, 1>::const_Iterator Tensor<ScalarType, 1>::begin() const {
		return Tensor<ScalarType, 1>::const_Iterator(
		std::visit([](const auto& v) { return &(*v.begin()); }, val) );
	}

	template <typename ScalarType>
	typename Tensor<ScalarType, 1>::const_Iterator Tensor<ScalarType, 1>::end() const {
		return Tensor<ScalarType, 1>::const_Iterator(
		std::visit([](const auto& v) { return &(*v.end()); }, val) );
	}





	// Use this version very sparingly in production to catch the case " L[RT.IfPreviousIs, RT.IfNextIs]  "

	//template <>  // no longer using template specialization: only overload sparingly for RTs at the moment
	LIBZEF_DLL_EXPORTED Tensor<RelationType, 1> operator, (RelationType rt1, RelationType rt2);
	LIBZEF_DLL_EXPORTED Tensor<RelationType, 1> operator, (Tensor<RelationType, 1> exisiting_rel_list, RelationType rt2);

	LIBZEF_DLL_EXPORTED Tensor<BlobType, 1> operator, (BlobType bt1, BlobType bt2);
	LIBZEF_DLL_EXPORTED Tensor<BlobType, 1> operator, (Tensor<BlobType, 1> exisiting_bt_list, BlobType bt2);


}
