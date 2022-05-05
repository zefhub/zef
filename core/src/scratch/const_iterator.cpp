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

#include <iostream>
#include <array>
#include <variant>
#include <vector>
#include <string>


constexpr auto print = [](const auto& x)->void { std::cout << x << std::endl; };
constexpr auto printt = [](const auto& v)->void { for (auto& el : v) std::cout << el << ", "; std::cout << std::endl; };


struct A {
    struct Iterator;
    struct const_Iterator;
    int data[10];   // just a bare array

    Iterator begin();
    Iterator end();
    const_Iterator begin() const;
    const_Iterator end() const;
};





struct A::Iterator {
    // we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
    using value_type = int;
    using reference = int&;
    using pointer = int*;
    using iterator_category = std::random_access_iterator_tag;
    using difference_type = ptrdiff_t;

    int* ptr_to_current_el = nullptr;
    Iterator& operator++() { ++ptr_to_current_el; return *this; }		// pre-increment op: this one is used mostly
    Iterator operator++(int) { return Iterator{ ptr_to_current_el++ }; }		// post incremenet
    reference operator*() { return *ptr_to_current_el; }
    std::add_const_t<reference> operator*() const { return *ptr_to_current_el; }
    value_type operator[](const difference_type& n) const { return *(ptr_to_current_el + n); }
    bool operator!=(const Iterator& other) const { return ptr_to_current_el != other.ptr_to_current_el; }
    bool operator==(const Iterator& other) const { return ptr_to_current_el == other.ptr_to_current_el; }
};


using ValType = int;

struct A::const_Iterator {
	// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
	using value_type = ValType;
	using reference = const ValType&;
	using pointer = const ValType*;
	using iterator_category = std::random_access_iterator_tag;
	using difference_type = ptrdiff_t;

	const ValType* ptr_to_current_el = nullptr;
	const_Iterator& operator++() { ++ptr_to_current_el; return *this; }		// pre-increment op: this one is used mostly
	const_Iterator operator++(int) { return const_Iterator{ ptr_to_current_el++ }; }		// post incremenet
	reference operator*() { return *ptr_to_current_el; }
	std::add_const_t<reference> operator*() const { return *ptr_to_current_el; }
	value_type operator[](const difference_type& n) const { return *(ptr_to_current_el + n); }
	bool operator!=(const const_Iterator& other) const { return ptr_to_current_el != other.ptr_to_current_el; }
	bool operator==(const const_Iterator& other) const { return ptr_to_current_el == other.ptr_to_current_el; }
};

A::Iterator A::begin(){
    print("Iterator A::begin() called");
    return Iterator{ &data[0] };
}

A::Iterator A::end(){
    print("Iterator A::end() called");
    return Iterator{ &data[10] };
}

A::const_Iterator A::begin() const {
    print("const_Iterator A::cbegin() called");
    return const_Iterator{ &data[0] };
}

A::const_Iterator A::end() const {
    print("const_Iterator A::cend() called");
    return const_Iterator{ &data[10] };
}


int sum(const A& a){
    int res = 0;
    for(const auto& x: a){
        res += x;
    }
    return res;
}


int main() {
    auto a = A();

    for(auto& x: a){
        x = 2;
    }

    for(const auto& x: a)   // whether this uses the const iterator depends on whether a is const or not. Not on the 'const' before auto!!!
        print(x);

    print("-----");
    print(sum(a));

    return 0;
}
