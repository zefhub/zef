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
#include <variant>
#include <vector>

using std::cout;


struct GraphData {
	double m;
};




struct EZefRef {
	void* ptr;
	bool operator != (const EZefRef& other) { return ptr != other.ptr; }
};


GraphData& graph_data(EZefRef my_uzr) {
	GraphData* ggg = new GraphData;
	return *ggg;
}


std::ostream& operator<<(std::ostream& o, const EZefRef& uzr) {
	o << "<EZefRef with ptr to " << uzr.ptr << ">";
	return o;
}


struct ZefRef {
	void* blob_ptr;
	EZefRef tx;
};

std::ostream& operator<<(std::ostream& o, const ZefRef& zr) {
	o << "<ZefRef with ptr to " << zr.blob_ptr << ">";
	return o;
}

namespace constants {
	constexpr int ZefRefs_local_array_size = 5;
	constexpr int EZefRefs_local_array_size = 7;
}

















struct ZefRefs {
	struct Iterator;   // only declare here

	ZefRefs* delegate_ptr = nullptr; // if this is null, then the data is actually stored here. Otherwise in the ZefRefs struct this points to
	EZefRef reference_frame_tx = EZefRef{ nullptr };  // for the graph reference frame: which graph and which time slice?
	int len = 0;  // the actual length (could be <ZefRefs_local_array_size or larger if dynamically allocated)
	EZefRef local_array[constants::ZefRefs_local_array_size];  // may overflow

	EZefRef* _get_array_begin() {
		EZefRef* res;
		if (delegate_ptr == nullptr) res = &local_array[0];
		else res = delegate_ptr->_get_array_begin();
		return res;
	}

	void _common_copy_from(const ZefRefs& to_copy) {
		constexpr int base_size_bytes = sizeof(ZefRefs) - constants::ZefRefs_local_array_size * sizeof(EZefRef);
		std::memcpy(this, &to_copy, sizeof(ZefRefs));
		if (delegate_ptr != nullptr) {
			delegate_ptr = new(len, graph_data(reference_frame_tx)) ZefRefs(len, graph_data(reference_frame_tx), true);
			std::memcpy(delegate_ptr, to_copy.delegate_ptr, base_size_bytes + len * sizeof(EZefRef));
		}
	}

	void _common_move_from(ZefRefs&& to_move) {
		constexpr int base_size_bytes = sizeof(ZefRefs) - constants::ZefRefs_local_array_size * sizeof(EZefRef);
		ZefRefs* this_delegate_ptr = delegate_ptr;  // if something is attached here, we give it to the to_move object to be destructed afterwards
		// a dynamic object is attached on to_move: steal this. It is also definitely sufficient to copy the non-overflowing part of to_move 
		if (to_move.delegate_ptr != nullptr) {
			std::memcpy(this, &to_move, sizeof(ZefRefs));  // we definitely copy the non-overflowing part only
			delegate_ptr = to_move.delegate_ptr;
		}
		else if (to_move.len <= std::max(len, constants::ZefRefs_local_array_size)) {  // both objects are local and to_move fits into this: we can just do a bare copy and don't need to allocate
			std::memcpy(this, &to_move, base_size_bytes + to_move.len * sizeof(EZefRef));
		}
		else {  // both objects are local, but to_move is to large to copy here: allocate a new dynamic ZefRefs
			GraphData& gd = graph_data(reference_frame_tx);
			delegate_ptr = new(len, gd) ZefRefs(to_move.len, gd, true);
			std::memcpy(delegate_ptr, &to_move, base_size_bytes + to_move.len * sizeof(EZefRef));
		}
		to_move.delegate_ptr = this_delegate_ptr;  // if something was attached to this object before, the dtor on the passed object should clean it up
		
	}

	ZefRefs() = delete;  // one has the specify the required capacity upfront
	
	// we need to tell the ctor how much space we need beforehand!
	ZefRefs(int required_list_length, GraphData& gd, bool I_am_allowed_to_overflow=false){
		len = required_list_length;
		if (!I_am_allowed_to_overflow && (required_list_length > constants::ZefRefs_local_array_size)) {
			delegate_ptr = new(required_list_length, gd) ZefRefs(required_list_length, gd, true);
		}
	}

	// logical domain requirement: the tx referenced (definiting time slice and reference graph) have to be the same for all EZefRef in the vector passed in
	ZefRefs(std::vector<ZefRef> v_init) : 
		ZefRefs(v_init.size(), [&v_init]()->GraphData& {
				if (v_init.empty()) return *(GraphData*)nullptr;  // we need to give something as a graph reference frame, even if the vector passed is empty
				else return graph_data(v_init.front().tx);
			}())   // call delegating constructor
	{		
		EZefRef* ptr_to_write_to = _get_array_begin();
		for (auto& el : v_init) {
			//if (el.tx != reference_frame_tx) throw std::runtime_error("Attempt to initialize a ZefRefs struct: a vectr<ZefRef> was passed, but their reference graphs don't all agree.");   //TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			*(ptr_to_write_to++) = EZefRef{el.blob_ptr};			
		}
	};

	// copy ctor
	ZefRefs(const ZefRefs& to_copy) {
		_common_copy_from(to_copy);
	}

	// copy assignment operator
	ZefRefs& operator=(const ZefRefs& to_copy) {
		_common_copy_from(to_copy);
		return *this;
	}

	// requirement: leave the object that was moved in a valid state. This should no longer own the 
	// externally referenced ZefRefs, which is now owned by this new object
	ZefRefs(ZefRefs&& to_move){	
		_common_move_from(std::move(to_move));  // don't forget that the r-val ref passed in becomes an l-val ref within this fct
		to_move.delegate_ptr = nullptr;
	}

	// move assignment operator
	ZefRefs& operator=(ZefRefs&& to_move) {
		_common_move_from(std::move(to_move));  // don't forget that the r-val ref passed in becomes an l-val ref within this fct
		return *this;
	}

	~ZefRefs() {
		if (delegate_ptr != nullptr)
			delete delegate_ptr;
	}

	// the GraphData object is passed along to the mem allocator that it can be allocated behind the gd object.
	void* operator new(std::size_t size_to_allocate, int actual_length_to_allocate, GraphData& gd) {
		constexpr int base_size_bytes = sizeof(ZefRefs) - constants::ZefRefs_local_array_size * sizeof(EZefRef);		
		return ::operator new(base_size_bytes + actual_length_to_allocate*sizeof(EZefRef));
	}

	void operator delete(void* ptr) noexcept{
		::operator delete(ptr);
	}

	Iterator begin();
	Iterator end();
};



struct ZefRefs::Iterator {
	// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
	using value_type = ZefRef;
	using reference = ZefRef&;
	using pointer = ZefRef*;
	using iterator_category = std::input_iterator_tag;  // TODO: make this iterator random access at some point?
	using difference_type = ptrdiff_t;

	EZefRef* ptr_to_current_uzr = nullptr;
	EZefRef reference_frame_tx = EZefRef{ nullptr };  // we need to have access to this on the fly in the iterator as well, to create a ZefRef to return



	// pre-increment op: this one is used mostly
	Iterator& operator++() {
		++ptr_to_current_uzr;
		return *this;
	}

	// post incremenet
	Iterator operator++(int) {
		return Iterator{ ptr_to_current_uzr++, reference_frame_tx };
	}

	value_type operator*() { //ZefRef z; z.blob_ptr = ptr_to_current_uzr->ptr; return z; } // 
		return ZefRef{ ptr_to_current_uzr->ptr, reference_frame_tx };
	}	

	value_type operator[](const difference_type& n) const {
		return ZefRef{ (ptr_to_current_uzr+n)->ptr, reference_frame_tx };
	}

	bool operator!=(const Iterator& other) const {
		return ptr_to_current_uzr != other.ptr_to_current_uzr;
	}

	bool operator==(const Iterator& other) const {
		return ptr_to_current_uzr == other.ptr_to_current_uzr;
	}

};


ZefRefs::Iterator ZefRefs::begin() {
	return (delegate_ptr == nullptr) ? Iterator{ &local_array[0] } : Iterator{ &delegate_ptr->local_array[0] };
}


ZefRefs::Iterator ZefRefs::end() {
	return (delegate_ptr == nullptr) ? Iterator{ &local_array[len] } : Iterator{ &delegate_ptr->local_array[len] };
}



TEST_CASE("ca") {
	GraphData gd;


	auto zs1 = ZefRefs(8, gd);

	cout << "*******************  zs1   ***********************\n\n";

	auto zs2 = ZefRefs(3, gd);

	cout << "*******************  zs2   ***********************\n\n";
	auto zs3 = zs1;
	cout << "*******************  zs3   ***********************\n\n";
	auto zs4 = ZefRefs(std::move(zs1));
	

	zs4.delegate_ptr->local_array[0] = EZefRef{ (void*)(1) };
	zs4.delegate_ptr->local_array[1] = EZefRef{nullptr};
	zs4.delegate_ptr->local_array[2] = EZefRef{(void*)(8)};
	zs4.delegate_ptr->local_array[3] = EZefRef{(void*)(17)};
	zs4.delegate_ptr->local_array[4] = EZefRef{(void*)(18)};

	for (auto el : zs4)
		std::cout << "hello: " << el << "\n";
	
	

	auto tx = EZefRef{ (void*)123333 };

	auto v = std::vector<ZefRef>{
		ZefRef{(void*)1, tx},
		ZefRef{(void*)3, tx},
		ZefRef{(void*)5, tx},
		ZefRef{(void*)6, tx},
		ZefRef{(void*)6, tx},
		ZefRef{(void*)6, tx},
		ZefRef{(void*)6, tx},
		ZefRef{(void*)6, tx},
		ZefRef{(void*)6, tx},
		ZefRef{(void*)6, tx},
		ZefRef{(void*)6, tx},
		ZefRef{(void*)60000, tx},
		ZefRef{(void*)6, tx},
		ZefRef{(void*)7, tx}
	};
	
	auto z_from_v = ZefRefs(v);

	for (auto el : z_from_v)
		std::cout << "zs: " << el << "\n";

	cout << "*******************  zs5   ***********************\n\n";

	//ZefRefs zzz(std::move(z_from_v));
	zs2 = std::move(z_from_v);


	for (auto el : zs2)
		std::cout << "zs: " << el << "\n";

	cout << "*******************  zs5   ***********************\n\n";


	//
	////auto zs7(std::move(zs4));
	//auto zs7 = ZefRefs(3, gd);
	//zs7 = std::move(zs4);



	//
	//auto my_it = zs7.begin();
	//std::cout << "[]: " << my_it[0] << "\n";
	//std::cout << "[]: " << my_it[1] << "\n";
	//std::cout << "[]: " << my_it[2] << "\n";
	//std::cout << "[]: " << my_it[3] << "\n";

	//std::cout << "explicit: " << zs7.delegate_ptr->local_array[0] << "\n";
	//std::cout << "explicit: " << zs7.delegate_ptr->local_array[1] << "\n";
	//std::cout << "explicit: " << zs7.delegate_ptr->local_array[2] << "\n";
	//std::cout << "explicit: " << zs7.delegate_ptr->local_array[3] << "\n";

}




























struct EZefRefs {
	struct Iterator;	

	enum class EZefRefs_type : unsigned char { optA_local_data, optB_remote_data, optC_blob_edge_list };
	// absorb EZefRefs_type into the union: affects the memory placement, making it more compact. 
	// This is always at the same position for all union memebers
	struct optA_local_data {
		EZefRefs_type my_type;  
		int len;
		EZefRef local_arr[constants::EZefRefs_local_array_size];
	};
	struct optB_remote_data {
		EZefRefs_type my_type;
		int len;  // we can also store the length here in addition
		EZefRefs* ptr_to_dynamically_allocated_optA;
	};
	struct optC_blob_edge_list {
		EZefRefs_type my_type;
		void* ptr_to_blob_with_edge_list;
	};

	// we can't use a variant if the array is of flexible length at the end: std::variant for MSVC and g++ allocates the instance flag byte behind the data
	using uzr_union = union {
		optA_local_data my_optA_local_data; 
		optB_remote_data my_optB_remote_data; 
		optC_blob_edge_list my_optC_blob_edge_list; 
	};	
	uzr_union data;

	// the GraphData object is passed along to the mem allocator that it can be allocated behind the gd object.
	void* operator new(std::size_t size_to_allocate, int actual_length_to_allocate, GraphData& gd) {
		constexpr int base_size_bytes = sizeof(EZefRefs) - constants::EZefRefs_local_array_size * sizeof(EZefRef);
		cout
			<< "EZefRefs::new op called with size_to_allocate = " << size_to_allocate
			<< " actual size allocated=" << base_size_bytes + actual_length_to_allocate * sizeof(EZefRef) << "\n";
		return ::operator new(base_size_bytes + actual_length_to_allocate * sizeof(EZefRef));  //TODO: allocate this next to gd
	}
	void operator delete(void* ptr) noexcept {
		cout << "EZefRefs::delete called for " << ptr << "\n";
		::operator delete(ptr);
	}


	EZefRef* _get_array_begin() { 
		switch (data.my_optA_local_data.my_type) {
		case(EZefRefs_type::optA_local_data): {cout << "  _get_array_begin ___>"<< &data.my_optA_local_data.local_arr[0] <<"   <____\n";	return &data.my_optA_local_data.local_arr[0]; }
		case(EZefRefs_type::optB_remote_data): {cout << "delegating arr. begin\n";	return data.my_optB_remote_data.ptr_to_dynamically_allocated_optA->_get_array_begin(); }
		default: {throw std::runtime_error("Invalid option in EZefRefs::_get_array_begin: we should never have landed here."); }
		}
	}

	EZefRefs() = delete;
	// Give me a preallocated EZefRefs object that a function somewhere else can immediately write the data in to
	// sepcify gd to be passed on to allocator: for which graph will these EZefRefs be used? Keep it next to the blobs
	EZefRefs(int required_list_length, GraphData& gd, bool I_am_allowed_to_overflow = false) {
		if (!I_am_allowed_to_overflow && (required_list_length > constants::ZefRefs_local_array_size)) {
			cout << "########## EZefRefs ctor called: will allocate dynamocaly\n";
			data.my_optB_remote_data.my_type = EZefRefs_type::optB_remote_data;
			cout << " ~~~~ addrses my_type " << &(data.my_optB_remote_data.my_type) << "\n";
			data.my_optB_remote_data.ptr_to_dynamically_allocated_optA = new(required_list_length, gd) EZefRefs(required_list_length, gd, true);
			cout << "dynamically allocated a new EZefRef at " << data.my_optB_remote_data.ptr_to_dynamically_allocated_optA << "\n\n";
		}
		else {
			cout << "########## EZefRefs ctor called "<< this <<": will allocatin locally at "<< this <<"\n";
			data.my_optA_local_data.my_type = EZefRefs_type::optA_local_data;
			data.my_optA_local_data.len = required_list_length;
			cout << " ~~~~ addrses my_type " << &(data.my_optA_local_data.my_type) << "\n";
			std::memset(data.my_optA_local_data.local_arr, 0, constants::ZefRefs_local_array_size * sizeof(EZefRef));  // clear the array			
		}
	}

	EZefRefs(std::vector<EZefRef> v_init, GraphData& gd) : EZefRefs(v_init.size(), gd) {  // call delegating constructor
		
		cout << "########## EZefRefs ctor called "<< this <<" from vec of uzr: "<< v_init.size() <<"\n";
		
		cout << " ~~~~ addrses this " << this << " A " << &data.my_optA_local_data << " B " << &(data.my_optB_remote_data)
			<< " A " << &(data.my_optA_local_data.my_type) << " B " << & (data.my_optB_remote_data.my_type) << "\n";
		cout << " ~~~~ local type: " << int((unsigned char)(data.my_optA_local_data.my_type)) << "  len = " << data.my_optA_local_data.len << "\n";
		std::memcpy(_get_array_begin(), &v_init.front(), v_init.size() * sizeof(EZefRef));
	};
	

	void _common_copy_from(const EZefRefs& to_copy) {
		cout << "_common_copy_from called for EZefRefs\n";		
		//constexpr int base_size_bytes = sizeof(EZefRefs) - constants::EZefRefs_local_array_size * sizeof(EZefRef);
		std::memcpy(this, &to_copy, sizeof(EZefRefs));
		if (data.my_optA_local_data.my_type == EZefRefs_type::optB_remote_data) {
				//TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			//data.my_optB_remote_data.ptr_to_dynamically_allocated_optA 
			//	= new(data.my_optB_remote_data.len, graph_data(reference_frame_tx)) ZefRefs(len, graph_data(reference_frame_tx), true);   ////aaaaaargh!!! we need gd here to. Save this in the EZefRef class 
			//std::memcpy(delegate_ptr, to_copy.delegate_ptr, sizeof(base_size_bytes + len * sizeof(EZefRef)));
		}
	}

	// copy ctor
	EZefRefs(const ZefRefs& to_copy) {
		cout << "######### copy ctor called\n";
		_common_copy_from(to_copy);
	}

	// copy assignment operator
	EZefRefs& operator=(const EZefRefs& to_copy) {
		cout << "######### copy assignment called\n";
		_common_copy_from(to_copy);
		return *this;
	}

	EZefRefs(EZefRefs&& to_move) {
		std::memcpy(this, &to_move, sizeof(EZefRefs));
		// requirement: leave the object that was moved in a valid state. This should no longer own the 
		// externally referenced EZefRefs, which is now owned by this new object
		if(to_move.data.my_optA_local_data.my_type == EZefRefs_type::optB_remote_data)
			to_move.data.my_optA_local_data.my_type = EZefRefs_type::optA_local_data;
	}

	// move assignment operator
	//EZefRefs& operator=(EZefRefs&& to_move) {
	//	ZefRefs* this_delegate_ptr = delegate_ptr;  // if something is attached here, we give it to the to_move object to be destructed afterwards
	//	std::memcpy(this, &to_move, sizeof(ZefRefs));  // we definitely copy the non-overflowing part only
	//	if (to_move.data.my_optA_local_data.my_type == EZefRefs_type::optB_remote_data)
	//		to_move.data.my_optA_local_data.my_type = EZefRefs_type::optA_local_data;
	//	return *this;
	//}

	~EZefRefs() {
		cout << "!!!!!!!!  EZefRefs dtor called at " << this << "\n";
		if (data.my_optA_local_data.my_type == EZefRefs_type::optB_remote_data) {
			delete data.my_optB_remote_data.ptr_to_dynamically_allocated_optA;
		}
	};

	Iterator begin();
	Iterator end();
};




struct EZefRefs::Iterator {
	// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
	using value_type = EZefRef;
	using reference = EZefRef&;
	using pointer = EZefRef*;
	using iterator_category = std::input_iterator_tag;
	using difference_type = ptrdiff_t;

	void* ptr_to_UZR_or_blob_with_edges;
	// A) if this is -1: ptr_to_UZR_or_blob_with_edges points to contiguous array of EZefRef 
	// B) if this is >=0: ptr_to_UZR_or_blob_with_edges points to blob with edges and this index is the index within the local array
	int switch_and_index_within_local_blob;
	

	// pre-increment op: this one is used mostly
	Iterator& operator++() {
		if (switch_and_index_within_local_blob==-1) {  // we have etiher optA or optB
			ptr_to_UZR_or_blob_with_edges = ((EZefRef*)ptr_to_UZR_or_blob_with_edges) + 1;
		}
		else {   //TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

		}
		return *this;
	}


	//// post incremenet
	//Iterator operator++(int) {
	//	return Iterator{ ptr_to_current_uzr++, reference_frame_tx };
	//}

	value_type operator*() {
		EZefRef res;
		if (switch_and_index_within_local_blob == -1) {  // we have etiher optA or optB
			res = *((EZefRef*)ptr_to_UZR_or_blob_with_edges);
		} else {   
			//TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		}
		return res;
	}

	bool operator!=(const Iterator& other) const {
		return ptr_to_UZR_or_blob_with_edges != other.ptr_to_UZR_or_blob_with_edges || 
			switch_and_index_within_local_blob != other.switch_and_index_within_local_blob;
	}

	bool operator==(const Iterator& other) const {
		return ptr_to_UZR_or_blob_with_edges == other.ptr_to_UZR_or_blob_with_edges &&
			switch_and_index_within_local_blob == other.switch_and_index_within_local_blob;
	}
};



EZefRefs::Iterator EZefRefs::begin()  {
	return data.my_optA_local_data.my_type != EZefRefs_type::optC_blob_edge_list ?
		Iterator{ (void*)_get_array_begin(), -1 } :
		Iterator{ data.my_optC_blob_edge_list.ptr_to_blob_with_edge_list, 0 };
}
EZefRefs::Iterator EZefRefs::end() {
	cout << " %%%%%%%%%%%%  In end:  len = " << data.my_optA_local_data.len << "\n";
	return data.my_optA_local_data.my_type != EZefRefs_type::optC_blob_edge_list ?
		Iterator{ (void*)((std::uintptr_t)_get_array_begin() + data.my_optA_local_data.len * sizeof(EZefRef)), -1 } :
		Iterator{ data.my_optC_blob_edge_list.ptr_to_blob_with_edge_list, 0};   //TODO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
}


//
//
//TEST_CASE("EZefRefs") {
//	//auto u = EZefRef{ nullptr };
//	//auto gd = GraphData();
//
//	//auto us1 = EZefRefs(std::vector<EZefRef>{ u, EZefRef{ (void*)(123) },u,u,u }, gd);
//
//	//cout << "  ****begin: " << us1.begin().ptr_to_UZR_or_blob_with_edges << "\n";
//	//cout << "  ****end iterator ptr: " << us1.end().ptr_to_UZR_or_blob_with_edges << "\n";
//
//	//for (auto el : us1)
//	//	cout <<"  -> "<< el <<"\n";
//
//	//cout << "------done ---------\n";
//
//}
