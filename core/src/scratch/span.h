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
#include <doctest.h>
#include "../span_cpp11.h"
#define TCB_SPAN_NAMESPACE_NAME std


TEST_CASE("try span") {
	using namespace std;
	constexpr int m = 7;
	int a[m];

	span<int> my_span(a, m);
	print(my_span[0]);
	print("---");

	for (auto& el : my_span)
		el = 1;
	my_span[6] = 0;
	for (auto el : my_span)
		print(el);
	auto it = std::find_if(my_span.begin(), my_span.end(),
		[](int x)->bool { return x == 0; }
		);
	print("~~~~");
	print(*it);
	print("~~~~");
	*it = 42;
	for (auto el : my_span)
		print(el);
}
