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
