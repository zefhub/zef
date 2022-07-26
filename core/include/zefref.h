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

#include <iostream>
#include <array>
#include <vector>
#include <variant>

#include <cassert>

#include "fwd_declarations.h"
#include "zefDB_utils.h"
#include "scalars.h"

namespace zefDB {
	using std::cout;

	struct LIBZEF_DLL_EXPORTED EZefRef {  //unversioned ZefRef: do not carry along the tx version, i.e. temporal slice of the graph we are looking at
		// we could also use the index, but then it is not clear which graph a ZefRef is 
		// associated with and we need to pass the graph around everywhere
		void* blob_ptr = nullptr;
				
		//EZefRef(const EZefRef& b) : blob_ptr(b.blob_ptr) {}  // can we just keep the default ctor? Otherwise we also have to generte cpy assignment opr etc.
		EZefRef() = default;
		EZefRef(const EZefRef& b) = default;
		explicit EZefRef(ZefRef uzr);
		explicit EZefRef(void* ptr);
		// first cast on uintptr_t to be able to shift by the my_blob_index 
		EZefRef(blob_index my_blob_index, const GraphData& graph_data_to_get_graph_from);
		EZefRef(blob_index my_blob_index, const Graph& graph);
		explicit operator bool() const { return blob_ptr != nullptr &&  (*(char*)blob_ptr != char(0)); }		
		//EZefRef& operator=(const EZefRef& b) {blob_ptr = b.blob_ptr; return *this;}
	};


	// fundamental structure to pass around to compactly represent edges and nodes of all types
	struct LIBZEF_DLL_EXPORTED ZefRef  {
		EZefRef blob_uzr;
		EZefRef tx {nullptr};
		ZefRef(EZefRef blob_uzr_, EZefRef tx_) : blob_uzr(blob_uzr_), tx(tx_) {};
		ZefRef(blob_index my_blob_index, GraphData& graph_data_to_get_graph_from, EZefRef tx_):
			blob_uzr(EZefRef(my_blob_index, graph_data_to_get_graph_from)), tx(tx_) {};
		explicit ZefRef(void* ptr, EZefRef tx_): blob_uzr(EZefRef(ptr)), tx(tx_) {};
		explicit ZefRef(EZefRef uzr) : blob_uzr(EZefRef(uzr)), tx(EZefRef{ nullptr }) {};
	};

	inline EZefRef::EZefRef(ZefRef b) : blob_ptr(b.blob_uzr.blob_ptr) {};

    inline void * ptr_from_blob_index(blob_index ind, const GraphData& gd) {
        return (void*)(((std::uintptr_t)&gd) + constants::blob_indx_step_in_bytes*ind);
    }

    LIBZEF_DLL_EXPORTED bool operator== (EZefRef b1, EZefRef b2);
	LIBZEF_DLL_EXPORTED bool operator!= (EZefRef b1, EZefRef b2);
	LIBZEF_DLL_EXPORTED bool operator== (ZefRef b1, ZefRef b2);
	LIBZEF_DLL_EXPORTED bool operator!= (ZefRef b1, ZefRef b2);

	LIBZEF_DLL_EXPORTED blob_index index(EZefRef b);
    LIBZEF_DLL_EXPORTED blob_index index(ZefRef zr);

	LIBZEF_DLL_EXPORTED GraphData* graph_data(EZefRef b);
	LIBZEF_DLL_EXPORTED GraphData* graph_data(ZefRef zr);

	// for any given ZefRef: get access to the associated graph_data struct sitting at the very beginning of the mempool
	LIBZEF_DLL_EXPORTED GraphData* graph_data(const void* blob_ptr);

	template <typename T>
	T& get(EZefRef uzr) {
		return *(T*)uzr.blob_ptr;
	}
	
	template <typename T>
	T& get(ZefRef zr) {
		return *(T*)zr.blob_uzr.blob_ptr;
	}

	template <typename T>
	T& get(void * ptr) {
		return *(T*)ptr;
	}



// iterable in range based for loops, with ranges-V3 and in Python
// based on https://gist.github.com/ozars/9b77cfcae53b90c883b456d1170e77fb to generate a ranges-V3 iterator without a view-facade
// see https://www.justsoftwaresolutions.co.uk/cplusplus/generating_sequences.html for some info




//                           _   _ _____     __ ____       __                          
//                          | | | |__  /___ / _|  _ \ ___ / _|___                      
//      _____ _____ _____   | | | | / // _ \ |_| |_) / _ \ |_/ __|   _____ _____ _____ 
//     |_____|_____|_____|  | |_| |/ /|  __/  _|  _ <  __/  _\__ \  |_____|_____|_____|
//                           \___//____\___|_| |_| \_\___|_| |___/                     
//                                                                                   
	// with forward Iterator for C++ range-based for loop, ranges-V3 & Python iterable
	struct LIBZEF_DLL_EXPORTED EZefRefs {
		struct Iterator;    // only declare here
		struct const_Iterator;    // only declare here
		struct PyIterator;    // used as a Python Iterator: here the Iterator also needs to store the end Iterator

		EZefRefs* delegate_ptr = nullptr; // if this is null, then the data is actually stored here. Otherwise in the EZefRefs struct this points to
		int len = 0;  // the actual length (could be <EZefRefs_local_array_size or larger if dynamically allocated)
		EZefRef local_array[constants::EZefRefs_local_array_size];  // may overflow

		

		EZefRef* _get_array_begin();
		const EZefRef* _get_array_begin_const() const;

		void _common_copy_from(const EZefRefs& to_copy) ;
		void _common_move_from(EZefRefs&& to_move) ;

		EZefRefs() = delete;  // one has the specify the required capacity upfront

		// we need to tell the ctor how much space we need beforehand!
        // Danny: I changed this so that the gd isn't used anymore, because it was ill-constructed from nullptr most of the time.
		explicit EZefRefs(int required_list_length, GraphData* gd, bool I_am_allowed_to_overflow = false);
        explicit EZefRefs(int required_list_length, bool I_am_allowed_to_overflow = false) ;

		// logical domain requirement: the tx referenced (definiting time slice and reference graph) have to be the same for all EZefRef in the vector passed in
		EZefRefs(const std::vector<EZefRef>& v_init, bool I_am_allowed_to_overflow = false);

		// copy ctor
		EZefRefs(const EZefRefs& to_copy);

		// copy assignment operator
		EZefRefs& operator=(const EZefRefs& to_copy) ;

		// requirement: leave the object that was moved in a valid state. This should no longer own the 
		// externally referenced EZefRefs, which is now owned by this new object
		EZefRefs(EZefRefs&& to_move) ;

		// move assignment operator
		EZefRefs& operator=(EZefRefs&& to_move) ;

		~EZefRefs() ;


		// the GraphData object is passed along to the mem allocator that it can be allocated behind the gd object.
		void* operator new(std::size_t size_to_allocate, int actual_array_length_to_allocate, GraphData* gd);
		void operator delete(void * ptr, int actual_array_length_to_allocate, GraphData* gd) noexcept;
		void* operator new(std::size_t size_to_allocate, int actual_array_length_to_allocate);
		void operator delete(void * ptr, int actual_array_length_to_allocate) noexcept;

		// This default allocator (not taking any additional arguments) is required when using functions returning a EZefRef within Python via pybind11
		void* operator new(std::size_t size_to_allocate);

		void operator delete(void* ptr) noexcept;

		EZefRef operator[] (int n) const;

		Iterator begin();
		Iterator end();

		const_Iterator begin() const;
		const_Iterator end() const;

        // Adding this in to more easily map to std::vector (doing this in particular for pybind now)
        int size() const;
	};

	inline int length(const EZefRefs& uzrs) { return uzrs.delegate_ptr == nullptr ? uzrs.len : uzrs.delegate_ptr->len; }
        
    inline GraphData* graph_data(const EZefRefs& uzrs) {
        return (uzrs.len == 0) ? (GraphData*)nullptr : graph_data(uzrs[0]);
    }

	



	struct EZefRefs::Iterator {
		// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
		using value_type = EZefRef;
		using reference = EZefRef&;
		using pointer = EZefRef*;
		using iterator_category = std::input_iterator_tag;  // TODO: make this iterator random access at some point?
		using difference_type = ptrdiff_t;

		pointer ptr_to_current_uzr = nullptr;

		// pre-increment op: this one is used mostly
		Iterator& operator++() ;
		// post incremenet
		Iterator operator++(int) ;
		Iterator operator+(int shift) const ;
		
		reference operator*() { return *ptr_to_current_uzr; }
		std::add_const_t<reference> operator*() const { return *ptr_to_current_uzr; }		

		value_type operator[](const difference_type& n) const ;

		bool operator!=(const Iterator& other) const ;

		bool operator==(const Iterator& other) const ;
	};

	struct EZefRefs::const_Iterator {
		// we need to specify these: pre-C++17 this was done by inheriting from std::const_Iterator
		using value_type = EZefRef;
		using reference = const EZefRef&;
		using pointer = const EZefRef*;
		using iterator_category = std::input_iterator_tag;  // TODO: make this iterator random access at some point?
		using difference_type = ptrdiff_t;

		pointer ptr_to_current_uzr = nullptr;

		// pre-increment op: this one is used mostly
		const_Iterator& operator++() ;
		// post incremenet
		const_Iterator operator++(int) ;
		const_Iterator operator+(int shift) const ;
		
		reference operator*() { return *ptr_to_current_uzr; }
		std::add_const_t<reference> operator*() const { return *ptr_to_current_uzr; }		

		value_type operator[](const difference_type& n) const ;

		bool operator!=(const const_Iterator& other) const ;

		bool operator==(const const_Iterator& other) const ;
	};



	// This class is only needed to create an Iterator for Python: the end needs to be baked in. Don't carry this around in the C++ Iterator
	struct EZefRefs::PyIterator {
		EZefRefs::Iterator main_it;
		EZefRefs::Iterator end_it; // store the end Iterator: Python needs to raise a StopIteration exception and only has this as context
	};











//                           _____     __ ____       __                          
//                          |__  /___ / _|  _ \ ___ / _|___                      
//      _____ _____ _____     / // _ \ |_| |_) / _ \ |_/ __|   _____ _____ _____ 
//     |_____|_____|_____|   / /|  __/  _|  _ <  __/  _\__ \  |_____|_____|_____|
//                          /____\___|_| |_| \_\___|_| |___/                     
//                                                                         

		
	struct LIBZEF_DLL_EXPORTED ZefRefs {
		struct Iterator;    // only declare here
		struct const_Iterator;    // only declare here
		struct PyIterator;    // used as a Python Iterator: here the Iterator also needs to store the end Iterator

		ZefRefs* delegate_ptr = nullptr; // if this is null, then the data is actually stored here. Otherwise in the ZefRefs struct this points to
		EZefRef reference_frame_tx = EZefRef{ nullptr };  // for the graph reference frame: which graph and which time slice?
		int len = 0;  // the actual length (could be <ZefRefs_local_array_size or larger if dynamically allocated)
		EZefRef local_array[constants::ZefRefs_local_array_size];  // may overflow


		EZefRef* _get_array_begin() ;

		const EZefRef* _get_array_begin_const() const ;

		void _common_copy_from(const ZefRefs& to_copy) ;

		void _common_move_from(ZefRefs&& to_move) noexcept ;

		ZefRefs() = delete;  // one has to specify the required capacity upfront

		void _common_constructor(int length, EZefRef reference_frame_tx, bool allow_overflow);

		// we need to tell the ctor how much space we need beforehand!
		// ZefRefs(int required_list_length, GraphData& gd, bool I_am_allowed_to_overflow = false) ;
		ZefRefs(int required_list_length, EZefRef reference_frame_tx, bool I_am_allowed_to_overflow = false) ;

		// logical domain requirement: the tx referenced (definiting time slice and reference graph) have to be the same for all EZefRef in the vector passed in
		ZefRefs(const std::vector<ZefRef>& v_init, bool I_am_allowed_to_overflow = false, const EZefRef& reference_frame_tx = EZefRef(nullptr));

		// copy ctor
		ZefRefs(const ZefRefs& to_copy) ;

		// copy assignment operator
		ZefRefs& operator=(const ZefRefs& to_copy) ;

		// requirement: leave the object that was moved in a valid state. This should no longer own the 
		// externally referenced ZefRefs, which is now owned by this new object
		ZefRefs(ZefRefs&& to_move) noexcept ;

		// move assignment operator
		ZefRefs& operator=(ZefRefs&& to_move) noexcept ;

		~ZefRefs() ;

		// the GraphData object is passed along to the mem allocator that it can be allocated behind the gd object.
		void* operator new(std::size_t size_to_allocate, int actual_array_length_to_allocate, GraphData* gd);
		void operator delete(void * ptr, int actual_array_length_to_allocate, GraphData* gd) noexcept;
		void* operator new(std::size_t size_to_allocate, int actual_array_length_to_allocate);
		void operator delete(void * ptr, int actual_array_length_to_allocate) noexcept;

		// This default allocator (not taking any additional arguments) is required when using functions returning a ZefRef within Python via pybind11
		void* operator new(std::size_t size_to_allocate) ;

		void operator delete(void* ptr) noexcept ;

		ZefRef operator[] (int n) const ;

		Iterator begin();
		Iterator end();
		const_Iterator begin() const;
		const_Iterator end() const;

        // Adding this in to more easily map to std::vector (doing this in particular for pybind now)
        int size() const;
	};
	inline int length(const ZefRefs& zrs) { return zrs.delegate_ptr == nullptr ? zrs.len : zrs.delegate_ptr->len; }
    inline GraphData* graph_data(const ZefRefs& zrs) { return zrs.reference_frame_tx.blob_ptr == nullptr ? (GraphData*)nullptr : graph_data(zrs.reference_frame_tx); }

	struct ZefRefs::Iterator {
		// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
		using value_type = ZefRef;
		using reference = ZefRef&;
		using pointer = ZefRef*;
		using iterator_category = std::input_iterator_tag;  // TODO: make this iterator random access at some point?
		using difference_type = ptrdiff_t;

		EZefRef* ptr_to_current_uzr;// = nullptr;
		EZefRef reference_frame_tx;// = EZefRef{ nullptr };  // we need to have access to this on the fly in the iterator as well, to create a ZefRef to return



		// pre-increment op: this one is used mostly
		Iterator& operator++();

		// post incremenet
		Iterator operator++(int);
		Iterator operator+(int shift) const;

		value_type operator*();

		value_type operator[](const difference_type& n) const ;

		bool operator!=(const Iterator& other) const ;

		bool operator==(const Iterator& other) const ;
	};
	struct ZefRefs::const_Iterator {
		// we need to specify these: pre-C++17 this was done by inheriting from std::Iterator
		using value_type = ZefRef;
		using reference = const ZefRef&;
		using pointer = const ZefRef*;
		using iterator_category = std::input_iterator_tag;  // TODO: make this iterator random access at some point?
		using difference_type = ptrdiff_t;

		const EZefRef* ptr_to_current_uzr;// = nullptr;
		// Note: tx can't be const to allow for copy assignment which is needed in some STL functions.
		EZefRef reference_frame_tx;// = EZefRef{ nullptr };  // we need to have access to this on the fly in the iterator as well, to create a ZefRef to return



		// pre-increment op: this one is used mostly
		const_Iterator& operator++();

		// post incremenet
		const_Iterator operator++(int);
		const_Iterator operator+(int shift) const;

        // Note: although the would normally return references, inside these
        // functions a ZefRef is constructed and needs to be returned by value,
        // otherwise it would be freed and an invalid reference returned.
		value_type operator*();
		value_type operator[](const difference_type& n) const ;

		bool operator!=(const const_Iterator& other) const ;

		bool operator==(const const_Iterator& other) const ;
	};


	// This class is only needed to create an Iterator for Python: the end needs to be baked in. Don't carry this around in the C++ Iterator
	struct ZefRefs::PyIterator {
		ZefRefs::Iterator main_it;
		ZefRefs::Iterator end_it; // store the end Iterator: Python needs to raise a StopIteration exception and only has this as context
	};








//                         _   _ _____     __ ____       __                              
//                        | | | |__  /___ / _|  _ \ ___ / _|___ ___                      
//    _____ _____ _____   | | | | / // _ \ |_| |_) / _ \ |_/ __/ __|   _____ _____ _____ 
//   |_____|_____|_____|  | |_| |/ /|  __/  _|  _ <  __/  _\__ \__ \  |_____|_____|_____|
//                         \___//____\___|_| |_| \_\___|_| |___/___/                     
//                                                                                       

	struct LIBZEF_DLL_EXPORTED EZefRefss {
		struct PyIterator; 

		std::vector<EZefRefs> v;

		EZefRefss() = delete;
		EZefRefss(std::vector<EZefRefs>&& v_) : v(std::move(v_)) {};
		EZefRefss(const std::vector<EZefRefs>& v_) : v(v_) {};
		EZefRefss(size_t n) { v.reserve(n); };

		size_t len() const { return v.size(); }
		EZefRefs operator[] (int indx) const { return v[indx]; }

		decltype(v.begin()) begin() { return v.begin(); };
		decltype(v.end()) end() { return v.end(); };
		decltype(v.cbegin()) begin() const { return v.cbegin(); };
		decltype(v.cend()) end() const { return v.cend(); };
	};
	inline int length(const EZefRefss& uzrss) { return uzrss.len(); }


	// This class is only needed to create an Iterator for Python: the end needs to be baked in. Don't carry this around in the C++ Iterator
	struct EZefRefss::PyIterator {
		decltype(std::vector<EZefRefs>().begin()) main_it;
		decltype(std::vector<EZefRefs>().begin()) end_it; // store the end Iterator: Python needs to raise a StopIteration exception and only has this as context
	};






//                           _____     __ ____       __                              
//                          |__  /___ / _|  _ \ ___ / _|___ ___                      
//      _____ _____ _____     / // _ \ |_| |_) / _ \ |_/ __/ __|   _____ _____ _____ 
//     |_____|_____|_____|   / /|  __/  _|  _ <  __/  _\__ \__ \  |_____|_____|_____|
//                          /____\___|_| |_| \_\___|_| |___/___/                     
//                                                                      


	struct LIBZEF_DLL_EXPORTED ZefRefss {
		struct PyIterator;    // used as a Python Iterator: here the Iterator also needs to store the end Iterator
		
		std::vector<ZefRefs> v;
		EZefRef reference_frame_tx = EZefRef{nullptr};

		ZefRefss() = delete;
		ZefRefss(std::vector<ZefRefs>&& v_) : v(std::move(v_)) {};
		ZefRefss(const std::vector<ZefRefs>& v_) : v(v_) {};
		ZefRefss(size_t n) { v.reserve(n); };

		size_t len() const { return v.size(); }
		ZefRefs operator[] (int indx) const { return v[indx]; }


		decltype(v.begin()) begin() { return v.begin(); };
		decltype(v.end()) end() { return v.end(); };	
		decltype(v.cbegin()) begin() const { return v.cbegin(); };
		decltype(v.cend()) end() const { return v.cend(); };
	};
	inline int length(const ZefRefss& zrss) { return zrss.len(); }


	// This class is only needed to create an Iterator for Python: the end needs to be baked in. Don't carry this around in the C++ Iterator
	struct ZefRefss::PyIterator {
		decltype(std::vector<ZefRefs>().begin()) main_it;
		decltype(std::vector<ZefRefs>().begin()) end_it; // store the end Iterator: Python needs to raise a StopIteration exception and only has this as context
	};




    LIBZEF_DLL_EXPORTED std::ostream& operator << (std::ostream& os, EZefRef uzr);
    LIBZEF_DLL_EXPORTED std::string low_level_blob_info(const EZefRef & uzr);


    LIBZEF_DLL_EXPORTED std::ostream& operator<<(std::ostream& o, const EZefRefs& uzrs);
    LIBZEF_DLL_EXPORTED std::ostream& operator<<(std::ostream& o, const EZefRefss& uzrss);






}
