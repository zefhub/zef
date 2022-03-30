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
//demonstrates custom class with iterator and range
// taken from https://gist.github.com/ozars/9b77cfcae53b90c883b456d1170e77fb
#include <iostream>
#include <iterator>
#include<algorithm>
#include <range/v3/all.hpp>

struct Test {
    struct iterator;
    struct sentinel;
    int counter;
    Test() = default;
    iterator begin();
    sentinel end();

    // These are not required.
    // sentinel end() const;
    // iterator begin() const;
};

struct Test::iterator {
    using value_type = int;
    using reference = int&;
    using pointer = int*;
    using iterator_category = std::input_iterator_tag;
    using difference_type = ptrdiff_t;

    Test* test;
    iterator& operator++() {
        std::cout << "operator++() called\n";
        test->counter++;
        return *this;
    }
    //void operator++(int) {
    //    (void)++* this;
    //}
    iterator operator++(int) {
        auto res = iterator(*this);
        (void)++* this;
        return res;
    }
    reference operator*() { std::cout << "using reference operator*() ...\n"; return test->counter; }
    std::add_const_t<reference> operator*() const { std::cout << "using std::add_const_t<reference> operator*() ...\n"; return test->counter; }

    bool operator!=(const iterator& rhs) const { return rhs.test != test; }
    bool operator!=(const sentinel&) const { return true; }

    template <typename T>
    bool operator==(T&& t) const { return !(*this != std::forward<T>(t)); }
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

TEST_CASE("") {


    auto container = Test();
    auto container2 = Test();
    static_assert(std::is_same_v<decltype(container.begin() == container.end()), bool>);
    static_assert(std::is_same_v<decltype(container.end() == container.begin()), bool>);
    static_assert(ranges::range<Test&>, "It is not a range");
    auto rng = container | ranges::views::stride(2) | ranges::views::take(10);
    for (auto& n : rng) {
        std::cerr << n << std::endl;
        n += 1;
    }
    
    std::for_each(
        container2.begin(),
        container2.end(),
        [](int x) {std::cout << "   x = " << x << "\n"; return 1; }
    );
        

    // this works and calls the reference opertor*: gives access to modify the underlying data
    //ranges::for_each(
    //    container2,
    //    [](int& x) {x += 2; std::cout << "   x = " << x << "\n"; x = 42; }
    //);


}
