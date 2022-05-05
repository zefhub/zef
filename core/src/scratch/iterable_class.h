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

#include <iostream>
#include <algorithm>
#include <array>
#include <vector>
#include <variant>
#include "range/v3/all.hpp"

//struct EZefRefs {
//    struct EZefRefIt {
//        int pos;
//        EZefRefIt& operator++() { pos++; return *this; };
//        bool operator != (EZefRefIt rhs) { return pos != rhs.pos; }
//        //bool operator != (EZefRefIt rhs) { return pos != rhs.pos; }
//    };
//
//    EZefRefIt begin() { return EZefRefIt{ 0 }; }
//    EZefRefIt end() { return EZefRefIt{ 10 }; }
//};
//
//TEST_CASE("make iterable class") {
//    
//    auto u = EZefRefs();
//    for (auto el : u) {
//
//    }
//    std::cout << '\n';
//}


// ------------------------------------------------ can be used in range based for loops, but not ranges --------------------------------
// taken from https://www.artificialworlds.net/blog/2017/05/11/c-iterator-example-and-an-iterable-range/
class myit
{
private:
    int value_;
    class intholder
    {
        int value_;
    public:
        intholder(int value) : value_(value) {}
        int operator*() { return value_; }
    };
public:
    // Previously provided by std::iterator - see update below
    typedef int value_type;
    typedef std::ptrdiff_t difference_type;
    typedef int* pointer;
    typedef int& reference;
    typedef std::input_iterator_tag iterator_category;

    explicit myit(int value) : value_(value) {}
    int operator*() const { return value_; }
    bool operator==(const myit& other) const { return value_ == other.value_; }
    bool operator!=(const myit& other) const { return !(*this == other); }
    intholder operator++(int)
    {
        intholder ret(value_);
        ++* this;
        return ret;
    }
    myit& operator++()
    {
        ++value_;
        return *this;
    }
};

class Numbers
{
private:
    const int start_;
    const int end_;
public:
    Numbers(int start, int end) : start_(start), end_(end) {}
    myit begin() { return myit(start_); }
    myit end() { return myit(end_); }
};


TEST_CASE("make iterable class 1") {
    
    
    for (auto el : Numbers(4, 10)) {
        std::cout << el <<'\n';
    }
    std::cout << '\n';
}













// ------------------------------------------------ can be used in ranges --------------------------------

// based on (but adapted) // taken from https://gist.github.com/ozars/9b77cfcae53b90c883b456d1170e77fb

// type that the iterator should actually return
struct Element {
    double x;
    float y;
};


static std::array<Element, 100> el_vec;
//static std::vector<Element> el_vec;

struct Test {
    struct iterator;    // only declare here
    struct sentinel;

    
    Test() = default;
    iterator begin();
    sentinel end();

    // These are not required.
    // sentinel end() const;
    // iterator begin() const;
};

struct Test::iterator {
    // we need to specify these: pre-C++17 this was done by inheriting from std::iterator
    using value_type = Element;
    using reference = Element&;
    using pointer = Element*;
    using iterator_category = std::input_iterator_tag;
    using difference_type = ptrdiff_t;


    //explicit iterator(){}

    Test* test;
    iterator& operator++() {
        //test->counter++;
        ppos++;
        return *this;
    }
    void operator++(int) {
        (void)++* this;
    }

    reference operator*() { return el_vec[ppos]; }
    std::add_const_t<reference> operator*() const { return el_vec[ppos]; }

    bool operator!=(const iterator& rhs) const { return rhs.test != test; }
    bool operator!=(const sentinel&) const { return true; }

    template <typename T>
    bool operator==(T&& t) const { return !(*this != std::forward<T>(t)); }
    
    
    // store the position inside the iterator struct: if a new iterator is created, 
    // its position is independent from previous iterators
    int ppos=0;  
};

struct Test::sentinel {
    bool operator!=(const iterator& iterator) const {
        return iterator != *this;
    }

    // This is not required:
    // bool operator!=(const sentinel&) {
    //     return true;
    // }

    template <typename T>
    bool operator==(T&& t) const { return !(*this != std::forward<T>(t)); }
};

Test::iterator Test::begin() { return { this }; }
Test::sentinel Test::end() { return {}; }






TEST_CASE("make iterable class 1") {
    std::cout << "\n----------- 1 ---------------\n";
    int cc = 0;
    for (auto& el : el_vec) {
        el.x = cc * 1.1;
        el.y = -cc * 1.2;
        cc++;
    }

    
    auto container = Test();
    static_assert(std::is_same_v<decltype(container.begin() == container.end()), bool>);
    static_assert(std::is_same_v<decltype(container.end() == container.begin()), bool>);
    static_assert(ranges::range<Test&>, "It is not a range");
    auto rng = container | ranges::views::take(10) | ranges::views::transform([](auto x) {return x.x * x.x; });
    //for (auto n : rng) 
    //    std::cerr << n << std::endl;    
    //std::cout << "\n---------------\n";
    //for (auto n : rng) 
    //    std::cerr << n << std::endl;    
}







auto print = [](const auto& x) {
    std::cout << x << "\n";
};





// ------------------------------------------------ modified to our needs --------------------------------

// based on (but adapted) // taken from https://gist.github.com/ozars/9b77cfcae53b90c883b456d1170e77fb

// type that the iterator should actually return
struct EZefRef {
    double x;
    float y;
};



struct EZefRefs {
    struct iterator;    // only declare here

    EZefRefs() = default;
    iterator begin();
    iterator end();
    
    std::variant<
        std::array<EZefRef, 6>,
        std::vector<EZefRef>
    > my_var;

};

struct EZefRefs::iterator {
    // we need to specify these: pre-C++17 this was done by inheriting from std::iterator
    using value_type = EZefRef;
    using reference = EZefRef&;
    using pointer = EZefRef*;
    using iterator_category = std::input_iterator_tag;
    using difference_type = ptrdiff_t;


    //EZefRefs* parent_uzrs_obj;  // as an iterator, which object do I belong to? Pointer to a constant EZefRefs        
    //int current_pos;  // keep position within EZefRefs vec stored in here locally (not the EZefRefs object)

    EZefRef* ptr_to_current = nullptr;

    //explicit iterator() { parent_uzrs_obj = nullptr; }
    //iterator(EZefRefs* parent_ptr): parent_uzrs_obj(parent_ptr) {}  // we can't make this explicit if we use copy-list-initilization?

    
    // pre-increment op: this one is used mostly
    iterator& operator++() {
        ptr_to_current++;
        return *this;
    }

    // post incremenet
    iterator operator++(int) {
        iterator result(*this);
        (void)++* this;
        return result;
    }

    reference operator*() { return *ptr_to_current; }
    std::add_const_t<reference> operator*() const { return *ptr_to_current; }
    bool operator != (const iterator& rhs) const { return rhs.ptr_to_current != ptr_to_current; }

    template <typename T>
    bool operator==(T&& t) const { return !(*this != std::forward<T>(t)); }
};

EZefRefs::iterator EZefRefs::begin() { iterator it{ std::visit([](const auto& v)->EZefRef* {return (EZefRef*)&(*v.begin()); }, this->my_var) }; return it; }
EZefRefs::iterator EZefRefs::end() { iterator it{ std::visit([](const auto& v)->EZefRef* {return (EZefRef*)&(*v.end()); }, this->my_var) }; return it; }






TEST_CASE("make iterable class 1") {
    using ranges::views::take;

    std::cout << "\n----------- 2 ---------------\n";
    int cc = 0;
    for (auto& el : el_vec) {
        el.x = cc * 1.1;
        el.y = -cc * 1.2;
        cc++;
    }

    
    auto my_uzrs = EZefRefs();
    my_uzrs.my_var = std::vector<EZefRef>({ {1.2, -1.2}, {1.3, -1.2}, {1.4, -1.2} });

    for (auto el : my_uzrs)
        print(el.x);


    static_assert(std::is_same_v<decltype(my_uzrs.begin() == my_uzrs.end()), bool>);
    static_assert(std::is_same_v<decltype(my_uzrs.end() == my_uzrs.begin()), bool>);
    static_assert(ranges::range<EZefRefs&>, "It is not a range");
    
    auto rng = my_uzrs | ranges::views::transform([](auto x) {return x.x * x.x; });
    for (auto n : rng) {
        std::cerr << n << std::endl;
    } 
    std::cout << "\n---------------\n";
    for (auto n : rng) {
        std::cerr << n << std::endl;
    }
    std::cout << "\n---------------\n";
    for (auto n : my_uzrs | take(2)) {
        print(n.x);
    }
    std::cout << "\n-------- manual for loop with ++it -------\n";
    for (auto it = std::begin(my_uzrs); it != std::end(my_uzrs); it++)
        print((*it).x);


}
