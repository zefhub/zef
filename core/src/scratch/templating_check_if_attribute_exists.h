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
#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>
#include <type_traits>
#include <cstddef>


struct Hello
{
	int helloworld() { return 0; }
	double y;
};

struct Generic {
	double z;
};

// SFINAE test
template <typename T>
class has_y
{
	typedef char one;
	struct two { char x[2]; };
	template <typename C> static one test(decltype(&C::y));
	template <typename C> static two test(...);
public:
	enum { value = sizeof(test<T>(0)) == sizeof(char) };
};


template <typename T>
int function_that_returns_y_if_present(T s) {

	if constexpr (has_y<T>::value) return s.y;
	else return -1;
}



TEST_CASE("check for member") {
	std::cout << has_y<Hello>::value << std::endl;
	std::cout << has_y<Generic>::value << std::endl;
	std::cout << function_that_returns_y_if_present(Hello({ 42 }));
	std::cout << function_that_returns_y_if_present(Generic({ 41 }));
}
