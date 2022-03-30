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

#include<algorithm>

using std::cout;

using blob_index = int;
struct EZefRef {
	void* blob_ptr = nullptr;
};

struct ZefRef {
	EZefRef blob_uzr;
	EZefRef tx{ nullptr };
};



namespace internals {
	blob_index& subsequent_deferred_edge_list_index(EZefRef uzr) {
		return *new blob_index{ 42 };
	}
}


struct AllEdgeIndexes {
	struct Iterator;
	struct Sentinel;
	EZefRef uzr_with_edges;

	AllEdgeIndexes() = delete;
	AllEdgeIndexes(EZefRef uzr) : uzr_with_edges(uzr) {};
	AllEdgeIndexes(ZefRef zr) : uzr_with_edges(zr.blob_uzr) {};

	AllEdgeIndexes::Iterator begin() const;
	AllEdgeIndexes::Sentinel end() const;
};

struct AllEdgeIndexes::Iterator {
	// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
	using value_type = blob_index;
	using reference = blob_index&;
	using pointer = blob_index*;
	using iterator_category = std::input_iterator_tag;  // TODO: make this iterator random access at some point?
	using difference_type = ptrdiff_t;

	blob_index* ptr_to_current_edge_element = nullptr;
	EZefRef current_blob_pointed_to{ nullptr };
	blob_index* ptr_to_last_edge_element_in_current_blob = nullptr;

	// pre-increment op: this one is used mostly
	AllEdgeIndexes::Iterator& operator++() {
		++ptr_to_current_edge_element;
		// if we are at the end of the local list and there is another edge list attached:
		if (ptr_to_current_edge_element == ptr_to_last_edge_element_in_current_blob) {
			blob_index my_subsequent_deferred_edge_list_index = internals::subsequent_deferred_edge_list_index(current_blob_pointed_to);
			//if (my_subsequent_deferred_edge_list_index != 0) {
			//	current_blob_pointed_to = EZefRef{ my_subsequent_deferred_edge_list_index, graph_data(current_blob_pointed_to) };
			//	ptr_to_current_edge_element = internals::edge_indexes(current_blob_pointed_to);
			//	ptr_to_last_edge_element_in_current_blob = ptr_to_current_edge_element + internals::local_edge_indexes_capacity(current_blob_pointed_to);
			//}
		}
		return *this;
	}

	// post incremenet
	AllEdgeIndexes::Iterator operator++(int) {
		AllEdgeIndexes::Iterator holder(*this);  // create copy to return before incrementing
		(void)++* this;
		return holder;
	}

	reference operator*() { cout << "using reference operator*() ...\n"; return *ptr_to_current_edge_element; }
	std::add_const_t<reference> operator*() const { cout << "using std::add_const_t<reference> operator*() ...\n"; return *ptr_to_current_edge_element; }


	bool operator==(AllEdgeIndexes::Sentinel sent) const;
	bool operator!=(AllEdgeIndexes::Sentinel sent) const;

};


struct AllEdgeIndexes::Sentinel {
	bool operator==(const AllEdgeIndexes::Iterator& it) const
	{
		return false;
		//return it == *this;
	}

	bool operator!=(const AllEdgeIndexes::Iterator& it)
	{
		return false;
		//return !(it == *this);
	}
};



bool AllEdgeIndexes::Iterator::operator==(AllEdgeIndexes::Sentinel sent) const
{
	return (*ptr_to_current_edge_element == 0)  // the edge list is filled with 0's if no more edges to be linked
		||
		(ptr_to_current_edge_element == ptr_to_last_edge_element_in_current_blob
			&& internals::subsequent_deferred_edge_list_index(current_blob_pointed_to) == 0);
}

bool AllEdgeIndexes::Iterator::operator!=(AllEdgeIndexes::Sentinel sent) const
{
	return !(*this == sent);
}

//
//AllEdgeIndexes::Iterator AllEdgeIndexes::begin() const {
//	blob_index* ptr_to_first_el_in_array = internals::edge_indexes(uzr_with_edges);
//	return AllEdgeIndexes::Iterator{
//		ptr_to_first_el_in_array,  // returns an optional ptr to the first element of the array for the respective uzr		
//		uzr_with_edges.blob_ptr,
//		ptr_to_first_el_in_array + internals::local_edge_indexes_capacity(uzr_with_edges)
//	};
//}


AllEdgeIndexes::Sentinel AllEdgeIndexes::end() const {
	return AllEdgeIndexes::Sentinel{};
}


bool pred(int x) {
	cout << "pred called with " << x << "\n";
	return x > 0;
}

int f1() {
	cout << "f1\n";
	return 1;
}
int f2() {
	cout << "f2\n";
	return 2;
}

TEST_CASE("") {

	cout << "here!\n";
	int len = 5;
	auto v = new double[len];
	
	//std::for_each(v, v+len, [m=float(0)](auto& el) mutable {el = 42+(m+=1.6); });
	
	auto F = [len](auto& w) {
		std::for_each(w, w + len, [m = float(0)](auto& el) mutable {el = 42 + (m += 1.6); });
	};
	
	F(v);

	std::for_each(v, v + len, [](auto& el) {cout << el << "\n"; });
	
	delete[] v;


	// are both sides of the termnary op evaluated? No!
	//int x = pred(2) ? f1() : f2();
	//x = pred(-2) ? f1() : f2();
	//x = pred(-3) ? f1() : f2();
	//x = pred(3) ? f1() : f2();

}
