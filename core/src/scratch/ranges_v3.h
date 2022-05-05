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
#include <iostream>
#include <unordered_map>
#include <cassert>
#include <cstring>
#include <sstream> 
#include "range/v3/all.hpp"
#define TCB_SPAN_NAMESPACE_NAME spancpp11
#include "../zefDB_utils.h"


using namespace zefDB;


TEST_CASE("stop on either first zero or max length (can be made safe for non null terminated char[] or custom data conventions)") {
	using namespace ranges;

	int max_length = 4;
	int x1[] = { 3, 7, 123, 7 };
	int x2[] = { 3, 7, 0, 8 };
	int x3[] = { 3, 7, 0, 0 };


	print(views::all(x1 | views::take(max_length) | views::take_while([](int x) {return x != 0; })));
	print(views::all(x2 | views::take(max_length) | views::take_while([](int x) {return x != 0; })));
	print(views::all(x3 | views::take(max_length) | views::take_while([](int x) {return x != 0; })));
	timer tt;

	for (int ct = 0; ct < 2000000; ct++) {
		auto x = views::all(x1 | views::take(max_length) | views::take_while([](int x) {return x != 0; }));
	}
	print(tt);
	print("^^^^^^^^^^^^^^^^^^");
}



// A range that iterates over all the characters in a
// null-terminated string.
class c_string_range : public ranges::view_facade<c_string_range>
{
	friend ranges::range_access;
	char const* sz_ = "";
	char const& read() const { return *sz_; }
	bool equal(ranges::default_sentinel_t) const { return *sz_ == '\0'; }
	//void next() { sz_++;  ++sz_; }  // we can also do other steps
	void next() { sz_++; }
public:
	c_string_range() = default;
	explicit c_string_range(char const* sz) : sz_(sz)
	{
		assert(sz != nullptr);
	}
};

TEST_CASE("make own class iterable") {
	using namespace ranges;
	auto m = c_string_range("hello there");
	
	std::cout << (views::all(m)) << std::endl;
}




// which type is a view?
auto lambda_fct_that_returns_view = [](int repetitions) {
	using namespace ranges;
	auto m = c_string_range("dis maar baie zef!");
	auto my_view = views::repeat_n(m, repetitions);
	return my_view;
};

TEST_CASE("make own class iterable") {
	using namespace ranges;
	std::cout << (lambda_fct_that_returns_view(3)| views::join) << std::endl;
}






using Ints = std::vector<int>;
using Intss = std::vector<Ints>;
using Intsss = std::vector<Intss>;

TEST_CASE("stitch different vectors into one range") {
	using namespace ranges;
	Intss vs = {
		{4,5,7, 2},
		{40,50,70, 0},
		{400,500,700, 1},
	};
	Ints v = { 8,6,3,0 };   // the last int defines wich vec of th vs continues

	//auto ed = w | views::take(6) | views::transform([](int n) {return n; });
	//auto ed = w | views::take(6) | views::for_each([](int i) {return yield_if(i % 2 == 1, i * i); });

	//auto odds = w | views::for_each([](int i) { return yield_if(i % 2 == 1, i * i); });
	//auto odds = w | views::for_each([](int i) { return yield(i * i); });

	//auto w = v | views::transform([=](int n) { return n; });
	//std::cout <<"\n\n **************\n" << (views::all(w) | views::take(4)) << "\n\n";
	
	
	std::cout <<"\n\n **************\n" << (views::all(vs) | views::for_each([](auto vec) { return views::all(vec); })) << "\n\n";
	
	// this is flattened into one sequence! Is for_each just flat_map?
	std::cout <<"\n\n **************\n" << (views::all(vs) | views::for_each([](auto vec) { 
				return views::iota(11) | views::take(4); // for each vector passed in, return a seq. [11, 12, 13, 14]
			})) << "\n\n";
	

	// this returns a seq of seqs
	std::cout << "\n\n **************\n" << (views::all(vs) | views::transform([](auto vec) {
		return views::iota(11) | views::take(4); // for each vector passed in, return a seq. [11, 12, 13, 14]
		})) << "\n\n";


	// wtf?????????? The lambda below only returns a non-empty range if the arg is passed by reference
	std::cout <<"\n\n **************\n" << (views::all(vs) | views::transform([](const Ints& vec) { 
				//static Ints temp = vec;	// if passed by value and declared static: it has access to this data later on and returns it
				return views::all(vec); // for each vector passed in, return a seq. [11, 12, 13, 14]
			}) | views::join  ) << std::endl;

	std::cout << "\n\n **************\n" << views::all(vs[1])  << "\n\n";
}


//TEST_CASE("") {
//	using namespace ranges;
//	// Define an infinite range containing all the Pythagorean triples:
//	auto triples = views::for_each(views::iota(1), [](int z) {
//		return views::for_each(views::iota(1, z + 1), [=](int x) {
//			return views::for_each(views::iota(x, z + 1), [=](int y) {
//				return yield_if(x * x + y * y == z * z,
//					std::make_tuple(x, y, z));
//				});
//			});
//		});

//	//// This alternate syntax also works:
//	// auto triples = iota(1)      >>= [] (int z) { return
//	//                iota(1, z+1) >>= [=](int x) { return
//	//                iota(x, z+1) >>= [=](int y) { return
//	//    yield_if(x*x + y*y == z*z,
//	//        std::make_tuple(x, y, z)); };}; };

//	// Display the first 100 triples
//	RANGES_FOR(auto triple, triples | views::take(100))
//	{
//		std::cout << '(' << std::get<0>(triple) << ',' << std::get<1>(triple)
//			<< ',' << std::get<2>(triple) << ')' << '\n';
//	}
//}




TEST_CASE("rv3") {
	std::vector<int> v{ 62,2234, 234,63,12,8,23,453 };
	using namespace ranges;

	auto vi = v
		| views::take(6)
		| views::take_while([](int x) {return x > 61; })
		| views::transform([](int x) {return std::to_string(x); })
		| views::intersperse("|")
		| views::intersperse("*")
		//| views::drop_last(4)
		;
	std::cout << views::all(vi) << '\n';
}



TEST_CASE("rv3") {
	std::string s{ "hello" };
	// output: h e l l o
	ranges::for_each(s, [](char c) { std::cout << c << ' '; });
	std::cout << '\n';

	std::vector<int> vi{ 9, 4, 5, 2, 9, 1, 0, 2, 6, 7, 4, 5, 6, 5, 9, 2, 7,
				1, 4, 5, 3, 8, 5, 0, 2, 9, 3, 7, 5, 7, 5, 5, 6, 1,
				4, 3, 1, 8, 4, 0, 7, 8, 8, 2, 6, 5, 3, 4, 5 };
	using namespace ranges;
	vi |= actions::sort | actions::unique;
	// prints: [0,1,2,3,4,5,6,7,8,9]
	std::cout << views::all(vi) << '\n';

}








struct Blob {
	Ints v;
	int subsequent_blob_indx;
};

using Blobs = std::vector<Blob>;

// the following fct is tricy to get right: different ranges (even if they're all a range of ints) are
// of different C++ type: they know what they are composed of. Since one fct needs to always return the same
// type, this is a problem if different branches in the recursive call structure return different types.
// Solution: use any_view<T> to make them the same type of range
auto give_me_all_following_indxs(const Blob& b, Blobs& my_blobs) {
	using namespace ranges;
	Ints tt = {};
	if (b.subsequent_blob_indx == 0) return any_view<int>( views::all(b.v) | views::take_while([](int x) {return x != 0; }));
	else return any_view<int>(views::concat(views::all(b.v), give_me_all_following_indxs(my_blobs[b.subsequent_blob_indx], my_blobs)) | views::take_while([](int x) {return x != 0; }));
};


// functional structure to mock the essence of the edge list architecture using deferred edge lists
TEST_CASE("stitch different vectors into one range") {
	using namespace ranges;
	Blobs my_blobs = {
		{ {4,5,7, 2}, 2},
		{ {14, 9,12}, 0},
		{ {28}, 3},
		{ {39, 1, 34, 37}, 1}
	};

	//auto give_me_all_following_indxs = [&my_blobs](const Blob& b) {
	//	if (b.subsequent_blob_indx == 0) return views::all(b.v);
	//	else return views::concat(views::all(b.v), (*this)(my_blobs[b.subsequent_blob_indx]));
	//};


	std::cout << give_me_all_following_indxs(my_blobs[0], my_blobs);

}


struct struct_with_array {
	int my_array[1];

};



TEST_CASE("range over old-school array and termination") {
	using namespace ranges;
	void* mem_pool = ::operator new(1024);

	struct_with_array& s = *((struct_with_array*)mem_pool);
	s.my_array[0] = 42;
	s.my_array[1] = 43;
	s.my_array[2] = 44;
	ranges::span<int> my_span(s.my_array, 3);
	for (auto el : my_span)
		std::cout << el << "  -  ";
	std::cout << views::all(my_span) << std::endl;
	std::cout << views::all(span<int>(s.my_array, 3)) << std::endl;

	::operator delete(mem_pool);
}




TEST_CASE("rv3") {
	std::vector<int> v{ 62,2234, 234,63,12,8,23,453 };
	using namespace ranges;

	auto vi = v
		| views::take(6)
		| views::take_while([](int x) {return x > 61; })		
		;

	std::cout <<"\n---------\n\n";

	auto it = begin(vi);
	std::cout << *it << '\n';
	std::cout << "end="<< int(bool(it==end(vi))) << '\n';
	it++;
	std::cout << "end="<< int(bool(it==end(vi))) << '\n';
	std::cout << *it << '\n';
	it++;
	std::cout << "end="<< int(bool(it==end(vi))) << '\n';
	std::cout << *it << '\n';
	it++;
	std::cout << "end="<< int(bool(it==end(vi))) << '\n';
	std::cout << *it << '\n';
	it++;
	std::cout << "end="<< int(bool(it==end(vi))) << '\n';
	std::cout << *it << '\n';
	std::cout << *begin(vi) << '\n';
	
}



