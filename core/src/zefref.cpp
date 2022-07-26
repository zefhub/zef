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

#include "zefref.h"
#include "graph.h"
#include "butler/butler.h"
#include "tools.h"

namespace zefDB {
	using std::cout;

    void EZefRef_blob_check(const EZefRef & ezr) {
#ifdef DEBUG
        // We are testing here to see if EZefRef is called with a non-blob,
        // which is not allowed because of the ensure confusion that can
        // ensure!

        // The logic of ensuring memory relies on alloc/get both behaving
        // similarly on the blobs. If we go outside of the blobs then this
        // makes life very complicated!

        // This check is not perfect but will pick up some cases which fail,
        // which will hopefully be enough to identify bugs.
        // auto bt = get<BlobType>(ezr.blob_ptr);
        // Note: the above creates a new EZefRef so we end up in an infinite chain
        auto bt = *(BlobType*)(ezr.blob_ptr);
        if(bt == BlobType::_unspecified || bt >= BlobType::_last_blobtype) {
            print_backtrace();

            std::cerr << "Shouldn't be making a EZefRef that points at anything other than a blob." << std::endl;
            std::cerr << "You might want to use blob_index_from_ptr or ptr_from_blob_index instead." << std::endl;

            abort();
        }
#endif
    }
    EZefRef::EZefRef(blob_index my_blob_index, const GraphData& graph_data_to_get_graph_from) : 
        blob_ptr(ptr_from_blob_index(my_blob_index, graph_data_to_get_graph_from))
		{
			if (my_blob_index < 0 || my_blob_index > graph_data_to_get_graph_from.write_head) {
				std::cout << "EZefRef ctor called with index " << my_blob_index << std::endl;
                print_backtrace();
                abort();
				throw std::runtime_error("EZefRef initialized with index outside of valid range for this graph");
			}
            Butler::ensure_or_get_range(blob_ptr, blobs_ns::max_basic_blob_size);
            EZefRef_blob_check(*this);
		};

    EZefRef::EZefRef(blob_index my_blob_index, const Graph& g) : EZefRef(my_blob_index, g.my_graph_data()) {};

    EZefRef::EZefRef(void* ptr) : blob_ptr(ptr) {
        // TODO: This is likely to slow things down. For now, we will put up with it for clarity in the memory model.
        // Options to improve:
        // - only do this in traversal or explicit creation (vs creation from known quantities, e.g. from a zefref)
        // - include a shortcut "everything is loaded" on graph.
        if(blob_ptr != nullptr) {
            Butler::ensure_or_get_range(blob_ptr, blobs_ns::max_basic_blob_size);
            EZefRef_blob_check(*this);
        }
    };

    bool operator== (EZefRef b1, EZefRef b2) { return b1.blob_ptr == b2.blob_ptr;	}
	bool operator!= (EZefRef b1, EZefRef b2) { return b1.blob_ptr != b2.blob_ptr;	}
	bool operator== (ZefRef b1, ZefRef b2) {
        if (b1.tx.blob_ptr != b2.tx.blob_ptr)
            throw std::runtime_error("Should not compare ZefRefs between different time slices. See <URL here...> for a discussion.");
        return b1.blob_uzr.blob_ptr == b2.blob_uzr.blob_ptr;
    }
	bool operator!= (ZefRef b1, ZefRef b2) { return !(b1 == b2); }

	blob_index index(EZefRef b) {
		#ifdef ZEF_DEBUG
		assert(b.blob_ptr != nullptr);
		#endif // ZEF_DEBUG
        return blob_index_from_ptr(b.blob_ptr);
	}

	blob_index index(ZefRef zr) {		
		return index(zr.blob_uzr);
	}

	// for any given ZefRef: get access to the associated graph_data struct sitting at the very beginning of the mempool
	GraphData* graph_data(EZefRef b) {
		#ifdef ZEF_DEBUG
		assert(b.blob_ptr != nullptr);
		#endif // ZEF_DEBUG
		return graph_data(b.blob_ptr);
	}

	GraphData* graph_data(ZefRef zr) {
		return graph_data(zr.blob_uzr);
	}

	// for any given ZefRef: get access to the associated graph_data struct sitting at the very beginning of the mempool
	GraphData* graph_data(const void* blob_ptr) {
		assert(blob_ptr != nullptr);
		return (GraphData*)(blobs_ptr_from_blob(blob_ptr));
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
    EZefRef* EZefRefs::_get_array_begin() {
			return (delegate_ptr == nullptr) ? &local_array[0] : delegate_ptr->_get_array_begin();
		}
    const EZefRef* EZefRefs::_get_array_begin_const() const{
			return (delegate_ptr == nullptr) ? &local_array[0] : delegate_ptr->_get_array_begin();
		}

    void EZefRefs::_common_copy_from(const EZefRefs& to_copy) {
		constexpr int base_size_bytes = sizeof(EZefRefs) - constants::EZefRefs_local_array_size * sizeof(EZefRef);
		len = to_copy.len;
		GraphData* gd = to_copy.len == 0 ? (GraphData*)nullptr : graph_data(to_copy[0]);
		if (to_copy.delegate_ptr != nullptr) {
			delegate_ptr = new(to_copy.len, gd) EZefRefs(to_copy.len, gd, true);
			std::memcpy(delegate_ptr, to_copy.delegate_ptr, base_size_bytes + to_copy.len * sizeof(EZefRef));
		}
		else if (to_copy.len > constants::EZefRefs_local_array_size) {
			delegate_ptr = new(to_copy.len, gd) EZefRefs(to_copy.len, gd, true);
			std::memcpy(delegate_ptr, &to_copy, base_size_bytes + to_copy.len * sizeof(EZefRef));
		}
		else {
			delegate_ptr = nullptr;
			std::memcpy(local_array, to_copy.local_array, to_copy.len * sizeof(EZefRef));
		}
	}


    void EZefRefs::_common_move_from(EZefRefs&& to_move) {
			constexpr int base_size_bytes = sizeof(EZefRefs) - constants::EZefRefs_local_array_size * sizeof(EZefRef);
			EZefRefs* this_delegate_ptr = delegate_ptr;  // if something is attached here, we give it to the to_move object to be destructed afterwards
			// a dynamic object is attached on to_move: steal this. It is also definitely sufficient to copy the non-overflowing part of to_move 
			if (to_move.delegate_ptr != nullptr) {
				std::memcpy(this, &to_move, sizeof(EZefRefs));  // we definitely copy the non-overflowing part only
				delegate_ptr = to_move.delegate_ptr;
			}
			else if (to_move.len <= std::max(len, constants::EZefRefs_local_array_size)) {  // both objects are local and to_move fits into this: we can just do a bare copy and don't need to allocate
				std::memcpy(this, &to_move, base_size_bytes + to_move.len * sizeof(EZefRef));
			}
			else {  // both objects are local, but to_move is to large to copy here: allocate a new dynamic EZefRefs
				GraphData* gd = graph_data(to_move);
				delegate_ptr = new(to_move.len, gd) EZefRefs(to_move.len, gd, true);
				std::memcpy(delegate_ptr, &to_move, base_size_bytes + to_move.len * sizeof(EZefRef));
			}
			to_move.delegate_ptr = this_delegate_ptr;  // if something was attached to this object before, the dtor on the passed object should clean it up
		}



		// we need to tell the ctor how much space we need beforehand!
    EZefRefs::EZefRefs(int required_list_length, GraphData* gd, bool I_am_allowed_to_overflow) {
            len = required_list_length;
			if (!I_am_allowed_to_overflow && (required_list_length > constants::EZefRefs_local_array_size)) {
				delegate_ptr = new(required_list_length) EZefRefs(required_list_length, gd, true);
			}
		}
    EZefRefs::EZefRefs(int required_list_length, bool I_am_allowed_to_overflow) {
            len = required_list_length;
			if (!I_am_allowed_to_overflow && (required_list_length > constants::EZefRefs_local_array_size)) {
				delegate_ptr = new(required_list_length) EZefRefs(required_list_length, true);
			}
		}


		// logical domain requirement: the tx referenced (definiting time slice and reference graph) have to be the same for all EZefRef in the vector passed in
    EZefRefs::EZefRefs(const std::vector<EZefRef>& v_init, bool I_am_allowed_to_overflow) :
            EZefRefs(v_init.size(), I_am_allowed_to_overflow)   // call delegating constructor. Allowed to overflow
		{
			EZefRef* ptr_to_write_to = _get_array_begin();
			for (auto& el : v_init) {				
				*(ptr_to_write_to++) = el;
			}
		};

		// copy ctor
		EZefRefs::EZefRefs(const EZefRefs& to_copy) {
			_common_copy_from(to_copy);
		}

		// copy assignment operator
		EZefRefs& EZefRefs::operator=(const EZefRefs& to_copy) {
			_common_copy_from(to_copy);
			return *this;
		}

		// requirement: leave the object that was moved in a valid state. This should no longer own the 
		// externally referenced EZefRefs, which is now owned by this new object
    EZefRefs::EZefRefs(EZefRefs&& to_move) {
			_common_move_from(std::move(to_move));  // don't forget that the r-val ref passed in becomes an l-val ref within this fct
			to_move.delegate_ptr = nullptr;
		}

		// move assignment operator
    EZefRefs& EZefRefs::operator=(EZefRefs&& to_move) {
			_common_move_from(std::move(to_move));  // don't forget that the r-val ref passed in becomes an l-val ref within this fct
			return *this;
		}

    EZefRefs::~EZefRefs() {
			if (delegate_ptr != nullptr)
				delete delegate_ptr;
		}


    void* EZefRefs::operator new(std::size_t size_to_allocate, int actual_array_length_to_allocate, GraphData * gd) {
        return operator new(size_to_allocate, actual_array_length_to_allocate);
    }
    void EZefRefs::operator delete(void * ptr, int actual_array_length_to_allocate, GraphData * gd) noexcept {
			::operator delete(ptr);
		}

    void* EZefRefs::operator new(std::size_t size_to_allocate, int actual_array_length_to_allocate) {
			constexpr int base_size_bytes = sizeof(EZefRefs) - constants::EZefRefs_local_array_size * sizeof(EZefRef);

			size_t actual_size_to_allocate = std::max(size_to_allocate, base_size_bytes + actual_array_length_to_allocate * sizeof(EZefRef));
			auto res = ::operator new(actual_size_to_allocate);
			return res;
		}
		
    void EZefRefs::operator delete(void * ptr, int actual_array_length_to_allocate) noexcept {
			::operator delete(ptr);
		}

		// This default allocator (not taking any additional arguments) is required when using functions returning a EZefRef within Python via pybind11
    void* EZefRefs::operator new(std::size_t size_to_allocate) {
			return EZefRefs::operator new(size_to_allocate, 0);
		}

    void EZefRefs::operator delete(void* ptr) noexcept {
			::operator delete(ptr);
		}

    EZefRef EZefRefs::operator[] (int n) const { 
			#ifdef ZEF_DEBUG
					if (n < 0 || n >= (delegate_ptr == nullptr ? len : delegate_ptr->len))
						throw std::runtime_error("IndexError when calling EZefRefs[...]");
			#endif
			return (delegate_ptr == nullptr) ? local_array[n] : delegate_ptr->local_array[n];				
		}

    int EZefRefs::size() const { return length(*this); }
	



		// pre-increment op: this one is used mostly
    EZefRefs::Iterator& EZefRefs::Iterator::operator++() {
			++ptr_to_current_uzr;
			return *this;
		}

		// post incremenet
    EZefRefs::Iterator EZefRefs::Iterator::operator++(int) {
			return Iterator{ ptr_to_current_uzr++ };
		}
	
	EZefRefs::Iterator EZefRefs::Iterator::operator+(int shift) const {
			return Iterator{ ptr_to_current_uzr + shift };
		}

    EZefRefs::Iterator::value_type EZefRefs::Iterator::operator[](const difference_type& n) const {
			return EZefRef{ (ptr_to_current_uzr + n)->blob_ptr };
		}

    bool EZefRefs::Iterator::operator!=(const EZefRefs::Iterator& other) const {
			return ptr_to_current_uzr != other.ptr_to_current_uzr;
		}

    bool EZefRefs::Iterator::operator==(const EZefRefs::Iterator& other) const {
			return ptr_to_current_uzr == other.ptr_to_current_uzr;
		}

	// The begin() / end() fcts need to be in the same namespace as EZefRef
    EZefRefs::Iterator EZefRefs::begin() {
        return EZefRefs::Iterator{_get_array_begin()};
	}
	EZefRefs::Iterator EZefRefs::end() {
        return EZefRefs::Iterator{_get_array_begin() + len};
	}


		// pre-increment op: this one is used mostly
    EZefRefs::const_Iterator& EZefRefs::const_Iterator::operator++() {
			++ptr_to_current_uzr;
			return *this;
		}

		// post incremenet
    EZefRefs::const_Iterator EZefRefs::const_Iterator::operator++(int) {
			return const_Iterator{ ptr_to_current_uzr++ };
		}
	
	EZefRefs::const_Iterator EZefRefs::const_Iterator::operator+(int shift) const {
			return const_Iterator{ ptr_to_current_uzr + shift };
		}

    EZefRefs::const_Iterator::value_type EZefRefs::const_Iterator::operator[](const difference_type& n) const {
			return EZefRef{ (ptr_to_current_uzr + n)->blob_ptr };
		}

    bool EZefRefs::const_Iterator::operator!=(const EZefRefs::const_Iterator& other) const {
			return ptr_to_current_uzr != other.ptr_to_current_uzr;
		}

    bool EZefRefs::const_Iterator::operator==(const EZefRefs::const_Iterator& other) const {
			return ptr_to_current_uzr == other.ptr_to_current_uzr;
		}

	// The begin() / end() fcts need to be in the same namespace as EZefRef
    EZefRefs::const_Iterator EZefRefs::begin() const {
        return EZefRefs::const_Iterator{_get_array_begin_const()};
	}
	EZefRefs::const_Iterator EZefRefs::end() const {
        return EZefRefs::const_Iterator{_get_array_begin_const() + len};
	}







//                           _____     __ ____       __                          
//                          |__  /___ / _|  _ \ ___ / _|___                      
//      _____ _____ _____     / // _ \ |_| |_) / _ \ |_/ __|   _____ _____ _____ 
//     |_____|_____|_____|   / /|  __/  _|  _ <  __/  _\__ \  |_____|_____|_____|
//                          /____\___|_| |_| \_\___|_| |___/                     
//                                                                         

		
    EZefRef* ZefRefs::_get_array_begin() {
			return (delegate_ptr == nullptr) ? &local_array[0] : delegate_ptr->_get_array_begin();
		}

    const EZefRef* ZefRefs::_get_array_begin_const() const {
			return (delegate_ptr == nullptr) ? &local_array[0] : delegate_ptr->_get_array_begin();
		}

    void ZefRefs::_common_copy_from(const ZefRefs& to_copy) {
		constexpr int base_size_bytes = sizeof(ZefRefs) - constants::ZefRefs_local_array_size * sizeof(EZefRef);
		len = to_copy.len;
		reference_frame_tx = to_copy.reference_frame_tx;
		if (to_copy.delegate_ptr != nullptr) {
			delegate_ptr = new(to_copy.len, graph_data(reference_frame_tx)) ZefRefs(to_copy.len, to_copy.reference_frame_tx, true);
			std::memcpy(delegate_ptr, to_copy.delegate_ptr, base_size_bytes + to_copy.len * sizeof(EZefRef));
		}
		else if (to_copy.len > constants::ZefRefs_local_array_size) {
			delegate_ptr = new(to_copy.len, graph_data(reference_frame_tx)) ZefRefs(to_copy.len, to_copy.reference_frame_tx, true);
			std::memcpy(delegate_ptr, &to_copy, base_size_bytes + to_copy.len * sizeof(EZefRef));
		}
		else {
			delegate_ptr = nullptr;
			std::memcpy(local_array, to_copy.local_array, to_copy.len * sizeof(EZefRef));
		}
	}

    void ZefRefs::_common_move_from(ZefRefs&& to_move) noexcept {
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
				GraphData* gd = graph_data(reference_frame_tx);
				reference_frame_tx = to_move.reference_frame_tx;
				delegate_ptr = new(len, gd) ZefRefs(to_move.len, reference_frame_tx, true);
				std::memcpy(delegate_ptr, &to_move, base_size_bytes + to_move.len * sizeof(EZefRef));
			}
			to_move.delegate_ptr = this_delegate_ptr;  // if something was attached to this object before, the dtor on the passed object should clean it up
		}

    void ZefRefs::_common_constructor(int length, EZefRef reference_frame_tx, bool allow_overflow) {
        this->reference_frame_tx = reference_frame_tx;
        this->len = length;
        if (!allow_overflow && (length > constants::ZefRefs_local_array_size)) {
            // delegate_ptr = new(required_list_length, gd) ZefRefs(required_list_length, gd, true);
            GraphData* gd = (reference_frame_tx.blob_ptr == nullptr ? (GraphData*)nullptr : &Graph(reference_frame_tx).my_graph_data());
            delegate_ptr = new(length, gd) ZefRefs(length, reference_frame_tx, true);
        }
    }

		// we need to tell the ctor how much space we need beforehand!
    // ZefRefs::ZefRefs(int required_list_length, GraphData& gd, bool I_am_allowed_to_overflow) {
    ZefRefs::ZefRefs(int required_list_length, EZefRef reference_frame_tx, bool I_am_allowed_to_overflow)
    {
        _common_constructor(required_list_length, reference_frame_tx, I_am_allowed_to_overflow);
    }

		// logical domain requirement: the tx referenced (definiting time slice and reference graph) have to be the same for all EZefRef in the vector passed in
        ZefRefs::ZefRefs(const std::vector<ZefRef>& v_init, bool I_am_allowed_to_overflow, const EZefRef& reference_frame_tx)
        {
            if(reference_frame_tx.blob_ptr == nullptr) {
                if(v_init.empty()) {
                    std::cerr << "Warning! Creating ZefRefs without valid tx" << std::endl;
                    if(zwitch.throw_on_zefrefs_no_tx()) {
                        print_backtrace();
                        throw std::runtime_error("No tx for a ZefRefs!");
                    }
                }
                this->reference_frame_tx = v_init.empty() ? EZefRef(nullptr) : v_init.front().tx;
            } else
                this->reference_frame_tx = reference_frame_tx;
            _common_constructor(v_init.size(), this->reference_frame_tx, I_am_allowed_to_overflow);
            EZefRef* ptr_to_write_to = _get_array_begin();
			for (auto& el : v_init) {
				if (el.tx != this->reference_frame_tx) throw std::runtime_error("Attempt to initialize a ZefRefs struct: a vectr<ZefRef> was passed, but their reference graphs don't all agree. " + to_str(this->reference_frame_tx) + " " + to_str(el.tx));
				*(ptr_to_write_to++) = el.blob_uzr;
			}
		};

		// copy ctor
    ZefRefs::ZefRefs(const ZefRefs& to_copy) {
			_common_copy_from(to_copy);
		}

		// copy assignment operator
    ZefRefs&    ZefRefs::operator=(const ZefRefs& to_copy) {
			_common_copy_from(to_copy);
			return *this;
		}

		// requirement: leave the object that was moved in a valid state. This should no longer own the 
		// externally referenced ZefRefs, which is now owned by this new object
    ZefRefs::ZefRefs(ZefRefs&& to_move) noexcept {
			_common_move_from(std::move(to_move));  // don't forget that the r-val ref passed in becomes an l-val ref within this fct
			to_move.delegate_ptr = nullptr;
		}

		// move assignment operator
    ZefRefs&    ZefRefs::operator=(ZefRefs&& to_move) noexcept {
			_common_move_from(std::move(to_move));  // don't forget that the r-val ref passed in becomes an l-val ref within this fct
			return *this;
		}

    ZefRefs::~ZefRefs() {
			if (delegate_ptr != nullptr)
				delete delegate_ptr;
		}

		// the GraphData object is passed along to the mem allocator that it can be allocated behind the gd object.
    void*    ZefRefs::operator new(std::size_t size_to_allocate, int actual_array_length_to_allocate, GraphData* gd) {
        return operator new(size_to_allocate, actual_array_length_to_allocate);
    }
    void ZefRefs::operator delete(void * ptr, int actual_array_length_to_allocate, GraphData* gd) noexcept {
		::operator delete(ptr);
    }

    void*    ZefRefs::operator new(std::size_t size_to_allocate, int actual_array_length_to_allocate) {
        constexpr int base_size_bytes = sizeof(ZefRefs) - constants::ZefRefs_local_array_size * sizeof(EZefRef);
        size_t actual_size_to_allocate = std::max(size_to_allocate, base_size_bytes + actual_array_length_to_allocate * sizeof(EZefRef));

        auto res = ::operator new(actual_size_to_allocate);
        return res;
        //return ::operator new(base_size_bytes + actual_length_to_allocate * sizeof(EZefRef));
    }
    void ZefRefs::operator delete(void * ptr, int actual_array_length_to_allocate) noexcept {
		::operator delete(ptr);
    }

    // This default allocator (not taking any additional arguments) is required when using functions returning a ZefRef within Python via pybind11
    void* ZefRefs::operator new(std::size_t size_to_allocate) {
        return ZefRefs::operator new(size_to_allocate, 0);
    }

    void ZefRefs::operator delete(void* ptr) noexcept {
        ::operator delete(ptr);
    }

    ZefRef ZefRefs::operator[] (int n) const {
		#ifdef ZEF_DEBUG
		if (n < 0 || n >= (delegate_ptr==nullptr ? len : delegate_ptr->len))
			throw std::runtime_error("IndexError when calling ZefRefs[...]");
		#endif
			return ZefRef{ (delegate_ptr == nullptr) ? local_array[n] : delegate_ptr->local_array[n], reference_frame_tx };
		}

    int ZefRefs::size() const { return length(*this); }
		

		// pre-increment op: this one is used mostly
    ZefRefs::Iterator& ZefRefs::Iterator::operator++() {
			++ptr_to_current_uzr;
			return *this;
		}

	ZefRefs::Iterator ZefRefs::Iterator::operator+(int shift) const {
		return Iterator{ ptr_to_current_uzr + shift, reference_frame_tx };
	}

		// post incremenet
    ZefRefs::Iterator ZefRefs::Iterator::operator++(int) {
        return ZefRefs::Iterator{ ptr_to_current_uzr++, reference_frame_tx };
		}

    ZefRefs::Iterator::value_type ZefRefs::Iterator::operator*() { //ZefRef z; z.blob_ptr = ptr_to_current_uzr->ptr; return z; } // 
			return ZefRef{ ptr_to_current_uzr->blob_ptr, reference_frame_tx };
		}

    ZefRefs::Iterator::value_type ZefRefs::Iterator::operator[](const difference_type& n) const {
			return ZefRef{ (ptr_to_current_uzr + n)->blob_ptr, reference_frame_tx };
		}

    bool ZefRefs::Iterator::operator!=(const ZefRefs::Iterator& other) const {
			return ptr_to_current_uzr != other.ptr_to_current_uzr ||
				reference_frame_tx != other.reference_frame_tx;
		}

    bool ZefRefs::Iterator::operator==(const ZefRefs::Iterator& other) const {
			return ptr_to_current_uzr == other.ptr_to_current_uzr &&
				reference_frame_tx == other.reference_frame_tx;
		}


	ZefRefs::Iterator ZefRefs::begin() {
		return ZefRefs::Iterator{ _get_array_begin(), reference_frame_tx };
	}
	ZefRefs::Iterator ZefRefs::end() {
		return ZefRefs::Iterator{ _get_array_begin()+len, reference_frame_tx };
	}
    



    
		// pre-increment op: this one is used mostly
    ZefRefs::const_Iterator& ZefRefs::const_Iterator::operator++() {
			++ptr_to_current_uzr;
			return *this;
		}

	ZefRefs::const_Iterator ZefRefs::const_Iterator::operator+(int shift) const {
		return const_Iterator{ ptr_to_current_uzr + shift, reference_frame_tx };
	}

		// post incremenet
    ZefRefs::const_Iterator ZefRefs::const_Iterator::operator++(int) {
        return ZefRefs::const_Iterator{ ptr_to_current_uzr++, reference_frame_tx };
		}

    ZefRefs::const_Iterator::value_type ZefRefs::const_Iterator::operator*() { //ZefRef z; z.blob_ptr = ptr_to_current_uzr->ptr; return z; } // 
			return ZefRef{ ptr_to_current_uzr->blob_ptr, reference_frame_tx };
		}

    ZefRefs::const_Iterator::value_type ZefRefs::const_Iterator::operator[](const difference_type& n) const {
			return ZefRef{ (ptr_to_current_uzr + n)->blob_ptr, reference_frame_tx };
		}

    bool ZefRefs::const_Iterator::operator!=(const ZefRefs::const_Iterator& other) const {
			return ptr_to_current_uzr != other.ptr_to_current_uzr ||
				reference_frame_tx != other.reference_frame_tx;
		}

    bool ZefRefs::const_Iterator::operator==(const ZefRefs::const_Iterator& other) const {
			return ptr_to_current_uzr == other.ptr_to_current_uzr &&
				reference_frame_tx == other.reference_frame_tx;
		}


	ZefRefs::const_Iterator ZefRefs::begin() const {
		return ZefRefs::const_Iterator{ _get_array_begin_const(), reference_frame_tx };
	}
	ZefRefs::const_Iterator ZefRefs::end() const {
		return ZefRefs::const_Iterator{ _get_array_begin_const() + len, reference_frame_tx };
	}
}
