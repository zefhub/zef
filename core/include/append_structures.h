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

// NOTES:
//
// In this file there are so many potential issues with aliasing and the whole
// C++ machinery around classes makes it tough to reason out exactly what's
// going to happen. I think this should be rewritten into C-style object
// maangement, which would be clearer as to what exactly is going on.

#pragma once

#include <string>
#include <cstring>
#include <optional>
#include <functional>
#include <memory>
#include <random>

#include "constants.h"

namespace zefDB { 

    enum class AppendOnlyKind : unsigned char {
        _unspecified,
        SET,
        DICT_FIXED,
        BINARY_TREE,
        SET_VARIABLE,
        DICT_VARIABLE,
        COLLISION_HASH_MAP,
    };

    //////////////////////////////////////////////
    // ** utilities required for variable sized structs

    template<class T, class T2>
    void explicit_copy(T & target, const T2 & source);

    //////////////////////////////////////////////////
    // * Element helper structs


    // TODO: THIS IS JUST REDOING THE CONCEPT OF ZEFVALUES... REPLACE THIS LATER!!!!
    // TODO: THIS IS JUST REDOING THE CONCEPT OF ZEFVALUES... REPLACE THIS LATER!!!!
    // TODO: THIS IS JUST REDOING THE CONCEPT OF ZEFVALUES... REPLACE THIS LATER!!!!
    // TODO: THIS IS JUST REDOING THE CONCEPT OF ZEFVALUES... REPLACE THIS LATER!!!!
    // TODO: THIS IS JUST REDOING THE CONCEPT OF ZEFVALUES... REPLACE THIS LATER!!!!
    // TODO: THIS IS JUST REDOING THE CONCEPT OF ZEFVALUES... REPLACE THIS LATER!!!!
    // TODO: THIS IS JUST REDOING THE CONCEPT OF ZEFVALUES... REPLACE THIS LATER!!!!
    // TODO: THIS IS JUST REDOING THE CONCEPT OF ZEFVALUES... REPLACE THIS LATER!!!!
    // TODO: THIS IS JUST REDOING THE CONCEPT OF ZEFVALUES... REPLACE THIS LATER!!!!
    // TODO: THIS IS JUST REDOING THE CONCEPT OF ZEFVALUES... REPLACE THIS LATER!!!!

    //////////////////////////////
    // ** Fixed size pair

    template<class T1, class T2>
    struct FixedPair {
        bool deleted = false;
        T1 first;
        T2 second;

        bool operator==(const FixedPair & other) const {
            return !deleted && first == other.first && second == other.second;
        }
        bool operator!=(const FixedPair & other) const {
            return !(*this == other);
        }
    };
    template<class T1, class T2>
    inline std::ostream& operator<<(std::ostream& o, const FixedPair<T1,T2> & pair) {
        o << "FixedPair(" << pair.first << ", " << pair.second << ")";
        return o;
    }


    //////////////////////////////
    // ** Variable-sized pair

    template<class T1, class T2>
    struct VariablePair {
        bool deleted;
        T1 first;
        // T2 second;

        T2& get_second() const {
            return *(T2*)((char*)this + offsetof(VariablePair, first) + first.true_sizeof());
        }
        size_t size() const {
            return offsetof(VariablePair, first) + first.true_sizeof() + get_second().true_sizeof();
        }

        size_t true_sizeof() const {
            return size();
        }

        VariablePair& operator=(const VariablePair & other) = delete;
        VariablePair(const VariablePair & other) = delete;
        VariablePair() = delete;

        // struct Safe {
        //     typename T1::Safe first;
        //     typename T2::Safe second;
        // };
        using Safe = std::pair<typename T1::Safe, typename T2::Safe>;
        Safe safe_copy() const {
            if(deleted)
                throw std::runtime_error("Should never be returning a safe copy of a deleted item");
            return Safe{first.safe_copy(), get_second().safe_copy()};
        }
        static size_t true_sizeof_from_safe(const typename VariablePair<T1,T2>::Safe & me) {
            return offsetof(VariablePair, first) + T1::true_sizeof_from_safe(me.first) + T2::true_sizeof_from_safe(me.second);
        }
    };

    template<class T1, class T2>
    std::ostream& operator<<(std::ostream& o, const VariablePair<T1,T2> & pair) {
        o << "VariablePair(" << pair.deleted << ", " << pair.first << ", " << pair.get_second() << ")";
        return o;
    }

    template<class T1, class T2>
    inline void explicit_copy(VariablePair<T1,T2> & me, const typename VariablePair<T1,T2>::Safe & other) {
        me.deleted = false;
        explicit_copy(me.first, other.first);
        explicit_copy(me.get_second(), other.second);
    }


    //////////////////////////////
    // ** specific subtypes for string and blob_index

    struct VariableString {
        size_t len;

        char* data() const {
            return (char*)this + sizeof(VariableString);
        }
        std::string get_string() const {
            return std::string(data(), len);
        }

        size_t true_sizeof() const {
            return sizeof(VariableString) + len;
        }

        using Safe = std::string;
        static size_t true_sizeof_from_safe(const Safe & s) {
            return sizeof(VariableString) + s.size();
        }

        Safe safe_copy() const {
            return get_string();
        }
    };

    inline std::ostream& operator<<(std::ostream& o, const VariableString & s) {
        o << "VariableString(" << s.get_string() << ")";
        return o;
    }


    template<>
    inline void explicit_copy(VariableString & me, const std::string & other) {
        me.len = other.size();
        other.copy(me.data(), me.len);
    }

    // Even though this isn't variable, expose it the same way
    struct VariableBlobIndex {
        blob_index indx;

        size_t true_sizeof() const {
            return sizeof(VariableBlobIndex);
        }

        using Safe = blob_index;
        static size_t true_sizeof_from_safe(const Safe & s) {
            return sizeof(VariableBlobIndex);
        }

        Safe safe_copy() const {
            return indx;
        }
    };

    inline std::ostream& operator<<(std::ostream& o, const VariableBlobIndex & s) {
        o << "VariableBlobIndex(" << s.indx << ")";
        return o;
    }


    template<>
    inline void explicit_copy(VariableBlobIndex & me, const blob_index & other) {
        me.indx = other;
    }


    // //////////////////////////////
    // // ** specifically index and string pair

    // struct IndexStringPair {
    //     size_t size;
    //     enum_indx indx;
    //     char* data() {
    //         return (char*)this + sizeof(IndexStringPair);
    //     }
    //     const char* data() const {
    //         return (const char*)this + sizeof(IndexStringPair);
    //     }
    //     std::string get_string() const {
    //         return std::string(data(), size);
    //     }

    //     // Only construct manually and only move via explicit_copy
    //     IndexStringPair& operator=(const IndexStringPair & other) = delete;
    //     IndexStringPair(const IndexStringPair & other) = delete;
    //     IndexStringPair() = delete;
    //     IndexStringPair(std::initializer_list<IndexStringPair>) = delete;

    //     struct Safe {
    //         enum_indx indx;
    //         std::string s;
    //     };
    // };

    // template<>
    // inline void explicit_copy(IndexStringPair & me, const IndexStringPair::Safe & other) {
    //     me.size = other.s.size();
    //     me.indx = other.indx;
    //     other.s.copy(me.data(), me.size);
    //     // std::cerr << "Did explicit_copy" << " " << other.s << " " << other.indx << " " << me.size << " " << me.indx << " " << std::string(me.data(), me.size) << " " << (void*)me.data() << std::endl;
    // }

    // template<>
    // inline auto safe_copy(const IndexStringPair & me) {
    //     return IndexStringPair::Safe{me.indx, me.get_string()};
    // }

    // template<>
    // inline size_t true_sizeof(const IndexStringPair & me) {
    //     // std::cerr << "Normal sizeof: " << sizeof(IndexStringPair) << " " << me.size << " " << sizeof(IndexStringPair) + me.size << std::endl;
    //     return sizeof(IndexStringPair) + me.size;
    // }

    // template<>
    // inline size_t true_sizeof(const IndexStringPair::Safe & me) {
    //     // std::cerr << "Safe sizeof: " << sizeof(IndexStringPair) << " " << me.s.size() << " " << sizeof(IndexStringPair) + me.s.size() << std::endl;
    //     return sizeof(IndexStringPair) + me.s.size();
    // }

    //////////////////////////////
    // * Fixed sized collections

    //////////////////////////////
    // ** set

    template <class T>
    struct AppendOnlySet {
        AppendOnlyKind kind;
        char kind_version;

        size_t _size;
        size_t _upstream_size;
        size_t _revision;

        size_t& size() { return _size; }
        size_t& upstream_size() { return _upstream_size; }
        size_t& revision() { return _revision; }

        using ensure_func_t = const std::function<AppendOnlySet&(size_t new_head)>;
    
        // Accessing the actual data from here
        // char marker;
        T* data() const {
            return (T*)((uintptr_t)this + sizeof(AppendOnlySet));
        }
        T& operator[](const size_t pos) const {
            return data()[pos];
        }

        struct Iterator {
            const AppendOnlySet & parent;
            size_t pos;

            Iterator(const AppendOnlySet & parent, size_t pos) : parent(parent), pos(pos) {}
            bool operator ==(const Iterator & other) {return pos == other.pos;}
            bool operator !=(const Iterator & other) {return !(*this == other);}
            const T& operator *() {return parent[pos];}
            Iterator & operator ++() {
                // return Iterator(parent, pos+1);
                pos++;
                return *this;
            }

            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = T;
            using pointer           = T*;
            using reference         = T&;
        };
        Iterator begin() const {return Iterator(*this, 0);}
        Iterator end() const {return Iterator(*this, _size);}

        bool contains(const T & needle) {
            for(const auto & item : *this) {
                if(item == needle)
                    return true;
            }
            return false;
        }

        std::vector<T> as_vector() {
            return std::vector<T>(begin(), end());
        }

        void append(const T & item, const ensure_func_t & ensure_func) {
            if(contains(item)){
                throw std::runtime_error("AppendOnlySet already contains item: " + to_str(item));
            }
            auto& new_this = ensure_func(sizeof(AppendOnlySet) + sizeof(T)*(_size+1));
            new_this[new_this._size] = item;
            new_this._size++;
        }

        void append(T && item, const ensure_func_t & ensure_func) {
            if(contains(item)) {
                throw std::runtime_error("AppendOnlySet already contains item: " + to_str(item));
            }
            auto& new_this = ensure_func(sizeof(AppendOnlySet) + sizeof(T)*(_size+1));
            new_this[new_this._size] = std::move(item);
            new_this._size++;
        }

        std::string create_diff(size_t from, size_t to) {
            std::string out;
            for(auto it = Iterator(*this, from); it != Iterator(*this, to); ++it) {
                out += std::string((char*)&*it, sizeof(T));
            }
            return out;
        }

        void apply_diff(std::string diff, const ensure_func_t & ensure_func) {
            // Do one ensure func for the range we know is required now, that
            // way we can be sure the data won't move afterwards
            size_t new_size = _size + diff.size()/sizeof(T);
            auto & new_this = ensure_func(sizeof(AppendOnlySet) + sizeof(T)*new_size);

            const T * data = (T*)diff.c_str();
            if(diff.size() % sizeof(T) != 0)
                throw std::runtime_error("Diff isn't a multiple of data type");
            // We have to do the append manually here, as ensure_func can't be called with a smaller size
            for(int i = 0 ; i < diff.size()/sizeof(T) ; i++) {
                new_this[new_this._size] = std::move(data[i]);
                new_this._size++;
            }
        }

        void _construct(bool uninitialized, const ensure_func_t & ensure_func) {
            if(uninitialized) {
                auto& new_this = ensure_func(sizeof(AppendOnlySet));
                new_this.kind = AppendOnlyKind::SET;
                new_this._size = 0;
                new_this._upstream_size = 0;
                new_this._revision = 0;
            } else {
                if(kind != AppendOnlyKind::SET)
                    throw std::runtime_error("SET is not a SET");
                _upstream_size = 0;
            }
        }
    };

    //////////////////////////////
    // ** dict


    template<class KEY, class VAL>
    struct AppendOnlyDictFixed {
        // This is a stub - a complete hack around AppendOnlySet
        using element_t = FixedPair<KEY,VAL>;
        using set_t = AppendOnlySet<element_t>;
        AppendOnlyKind kind;
        char kind_version;
        set_t set;

        size_t& size() { return set.size(); }
        size_t& upstream_size() { return set.upstream_size(); }
        size_t& revision() { return set.revision(); }

        using ensure_func_t = const std::function<AppendOnlyDictFixed&(size_t new_head)>;
        auto map_ensure_func(const ensure_func_t & ensure_func) {
            return [&ensure_func](size_t new_head)->set_t& {
                AppendOnlyDictFixed& new_this = ensure_func(new_head + offsetof(AppendOnlyDictFixed, set));
                return new_this.set;
            };
        }
    
        typename set_t::Iterator begin() const {return set.begin();}
        typename set_t::Iterator end() const {return set.end();}

        std::optional<VAL> maybe_at(const KEY & needle) {
            for(const auto & item : *this) {
                if(item.first == needle)
                    return item.second;
            }
            return {};
        }
        VAL at(const KEY & needle) {
            auto maybe = maybe_at(needle);
            if(maybe)
                return *maybe;
            throw std::out_of_range("Couldn't find " + to_str(needle) + " in AppendOnlyDictFixed.");
        }

        bool contains(const KEY & needle) {
            return bool(maybe_at(needle));
        }

        std::vector<element_t> as_vector() {
            return set.as_vector();
        }

        void append(const KEY & key, const VAL & val, const ensure_func_t & ensure_func) {
            if(contains(key))
                throw std::runtime_error("AppendOnlySet already contains key: " + to_str(key));
            set.append(element_t{key,val}, map_ensure_func(ensure_func));
        }

        void append(KEY && key, VAL && val, const ensure_func_t & ensure_func) {
            if(contains(key))
                throw std::runtime_error("AppendOnlySet already contains key: " + to_str(key));
            set.append(element_t{std::move(key),std::move(val)}, map_ensure_func(ensure_func));
        }

        std::string create_diff(size_t from, size_t to) {
            return set.create_diff(from, to);
        }

        void apply_diff(std::string diff, const ensure_func_t & ensure_func) {
            set.apply_diff(diff, map_ensure_func(ensure_func));
        }

        void _construct(bool uninitialized, const ensure_func_t & ensure_func) {
            if(uninitialized) {
                auto& new_this = ensure_func(sizeof(AppendOnlyDictFixed));
                new_this.kind = AppendOnlyKind::DICT_FIXED;
                new_this.set._construct(uninitialized, map_ensure_func(ensure_func));
                // Note: can't be assured that new_this is still new_this after the set _construct call.
            } else {
                if(kind != AppendOnlyKind::DICT_FIXED)
                    throw std::runtime_error("DICT_FIXED is not a DICT_FIXED");
                set._construct(uninitialized, map_ensure_func(ensure_func));
                // Note: can't be assured that this is still this after the set _construct call.
            }
        }
    };

    template<class KEY, class VAL>
    struct AppendOnlyBinaryTree {
        struct Element {
            KEY key;
            VAL val;
            size_t left;
            size_t right;
        };

        AppendOnlyKind kind;
        char kind_version;
        size_t _size;
        size_t _upstream_size;
        size_t _revision;

        size_t& size() { return _size; }
        size_t& upstream_size() { return _upstream_size; }
        size_t& revision() { return _revision; }

        using ensure_func_t = const std::function<AppendOnlyBinaryTree&(size_t new_head)>;
        // auto map_ensure_func(const ensure_func_t & ensure_func) {
        //     return [&ensure_func](size_t new_head)->set_t& {
        //         AppendOnlyDictFixed& new_this = ensure_func(new_head);
        //         return new_this.set;
        //     };
        // }

        Element* index_to_element(size_t index) const {
            return (Element*)((uintptr_t)this + sizeof(AppendOnlyBinaryTree) + index*sizeof(Element));
        }
        size_t element_to_index(Element* el) const {
            return (el - index_to_element(0));
        }

        std::vector<Element> as_vector() {
            return std::vector<Element>(index_to_element(0), index_to_element(_size));
        }


        Element * find_element(const KEY & needle, bool return_last=false) {
            if(_size == 0)
                return nullptr;

            Element * cur = index_to_element(0);
            Element * last = cur;

            while(true) {
                if(cur->key == needle)
                    break;
                else if(needle < cur->key) {
                    if(cur->left == 0)
                        break;
                    last = cur;
                    cur = index_to_element(cur->left);
                } else { //if(needle > cur->key) {
                    if(cur->right == 0)
                        break;
                    last = cur;
                    cur = index_to_element(cur->right);
                }
            }
            if(return_last)
                return last;
            return cur;
        }

        std::optional<VAL> maybe_at(const KEY & needle) {
            if(_size == 0)
                return {};
            auto el = find_element(needle);
            if(el->key == needle)
                return el->val;
            return {};
        }
        VAL at(const KEY & needle) {
            auto maybe = maybe_at(needle);
            if(maybe)
                return *maybe;
            throw std::out_of_range("Couldn't find " + to_str(needle) + " in AppendOnlyBinaryTree.");
        }

        bool contains(const KEY & needle) {
            return bool(maybe_at(needle));
        }

        AppendOnlyBinaryTree* _append(KEY && key, VAL && val, const ensure_func_t & ensure_func, bool already_ensured) {
            auto el = find_element(key);
            if(el != nullptr && el->key == key)
                throw std::runtime_error("AppendOnlyBinaryTree already contains key: " + to_str(key));

            size_t el_indx = element_to_index(el);
            AppendOnlyBinaryTree * new_this;
            if(already_ensured)
                new_this = this;
            else
                new_this = &ensure_func(sizeof(AppendOnlyBinaryTree) + sizeof(Element)*(_size+1));

            el = new_this->index_to_element(el_indx);

            auto at_end = new_this->index_to_element(new_this->_size);
            at_end->key = key;
            at_end->val = val;
            if(new_this->_size > 0) {
                if(key < el->key)
                    el->left = new_this->_size;
                else
                    el->right = new_this->_size;
            }
            new_this->_size++;

            return new_this;
        }
        AppendOnlyBinaryTree* append(const KEY & key, const VAL & val, const ensure_func_t & ensure_func, bool already_ensured=false) {
            KEY _key = key;
            VAL _val = val;
            return _append(std::move(_key), std::move(_val), ensure_func, already_ensured);
        }

        AppendOnlyBinaryTree* append(KEY && key, VAL && val, const ensure_func_t & ensure_func, bool already_ensured=false) {
            return _append(std::move(key), std::move(val), ensure_func, already_ensured);
        }

        void _pop(const KEY &key, const VAL & val, const ensure_func_t &ensure_func) {
            // This is a low level function that should only be called when
            // absolutely sure it makes sense to pop the final element from the
            // tree. The key and val must be passed in to validate that the
            // caller is popping the right thing.
            int to_pop_ind = this->_size - 1;
            auto to_pop = this->index_to_element(to_pop_ind);
            if(to_pop->key != key || to_pop->val != val) {
                std::cerr << to_pop->key << ":" << to_pop->val << std::endl;
                std::cerr << key << ":" << val << std::endl;
                throw std::runtime_error("Pop called with something that doesn't match the final element in the tree");
            }
            auto before_el = find_element(key, true);
            if(before_el->left == to_pop_ind) {
                before_el->left = 0;
            } else if(before_el->right == to_pop_ind) {
                before_el->right = 0;
            } else {
                throw std::runtime_error("Couldn't find parent of element that is being popped.");
            }

            _size--;
            AppendOnlyBinaryTree * new_this = &ensure_func(sizeof(AppendOnlyBinaryTree) + sizeof(Element)*(_size));
            assert(new_this == this);
        }

        std::string create_diff(size_t from, size_t to) {
            std::string out((char*)index_to_element(from), (char*)index_to_element(to));
            return out;
        }

        void apply_diff(std::string diff, const ensure_func_t & ensure_func) {
            // Do one ensure func for the range we know is required now, that
            // way we can be sure the data won't move afterwards
            // size_t new_size = _size + diff.size()/sizeof(Element);
            // auto & new_this = ensure_func(sizeof(AppendOnlyBinaryTree) + sizeof(Element)*new_size);

            // const Element * data = (Element*)diff.c_str();
            // if(diff.size() % sizeof(Element) != 0)
            //     throw std::runtime_error("Diff isn't a multiple of data type");
            // // We have to do the append manually here, as ensure_func can't be called with a smaller size
            // Element * cur = new_this.index_to_element(0);
            // for(int i = 0 ; i < diff.size()/sizeof(Element) ; i++) {
            //     *cur = std::move(data[i]);
            //     cur++;
            // }
            // new_this._size = new_size;

            if(diff.size() % sizeof(Element) != 0)
                throw std::runtime_error("Diff isn't a multiple of data type");

            size_t new_size = _size + diff.size()/sizeof(Element);
            auto & new_this = ensure_func(sizeof(AppendOnlyBinaryTree) + sizeof(Element)*new_size);

            const Element * data = (Element*)diff.c_str();
            // Element * cur = new_this.index_to_element(0);
            // for(int i = 0 ; i < diff.size()/sizeof(Element) ; i++) {
            //     *cur = std::move(data[i]);
            //     cur++;
            // }
            for(int i = 0 ; i < diff.size()/sizeof(Element) ; i++)
                new_this.append(data[i].key, data[i].val, ensure_func, true);

            if(new_this._size != new_size)
                throw std::runtime_error("Size after appending diff is not what was expected.");
        }

        void _construct(bool uninitialized, const ensure_func_t & ensure_func) {
            if(uninitialized) {
                auto& new_this = ensure_func(sizeof(AppendOnlyBinaryTree));
                new_this.kind = AppendOnlyKind::BINARY_TREE;
                new_this._size = 0;
                new_this._upstream_size = 0;
                new_this._revision = 0;
            } else {
                if(kind != AppendOnlyKind::BINARY_TREE)
                    throw std::runtime_error("BINARY_TREE is not a BINARY_TREE");
                _upstream_size = 0;
            }
        }
    };

    // This is basically the same as a binary tree, but there is a concept of
    // hash and value for the comparison, where the value is looked up from the
    // graph.
    template<class KEY, class VAL>
    struct AppendOnlyCollisionHashMap {
        // using KEY = value_hash_t;
        // using VAL = blob_index;
        struct Element {
            KEY key;
            VAL val;
            size_t left;
            size_t right;
        };

        AppendOnlyKind kind;
        char kind_version;
        size_t _size;
        size_t _upstream_size;
        size_t _revision;

        size_t& size() { return _size; }
        size_t& upstream_size() { return _upstream_size; }
        size_t& revision() { return _revision; }

        using ensure_func_t = const std::function<AppendOnlyCollisionHashMap&(size_t new_head)>;
        using compare_func_t = const std::function<char(const KEY & key, const VAL & val)> &;

        Element* index_to_element(size_t index) const {
            if(index == -1)
                return nullptr;
            else
                return (Element*)((uintptr_t)this + sizeof(AppendOnlyCollisionHashMap) + index*sizeof(Element));
        }
        size_t element_to_index(Element* el) const {
            if(el == nullptr)
                return -1;
            else
                return (el - index_to_element(0));
        }

        std::vector<Element> as_vector() {
            return std::vector<Element>(index_to_element(0), index_to_element(_size));
        }

        std::pair<Element*,Element*> find_element(compare_func_t compare_func) {
            if(_size == 0)
                return std::pair(nullptr, nullptr);

            Element * cur = index_to_element(0);
            Element * last = nullptr;

            while(true) {
                int compare_ret = compare_func(cur->key, cur->val);
                if(compare_ret == 0)
                    break;

                last = cur;
                if(compare_ret < 0) {
                    if(cur->left == 0) {
                        cur = nullptr;
                        break;
                    }
                    cur = index_to_element(cur->left);
                } else {
                    if(cur->right == 0) {
                        cur = nullptr;
                        break;
                    }
                    cur = index_to_element(cur->right);
                }
            }
            return std::pair(last, cur);
        }

        std::optional<VAL> maybe_at(compare_func_t compare_func) {
            if(_size == 0)
                return {};
            auto el = find_element(compare_func).second;
            if(el == nullptr)
                return {};
            return el->val;
        }
        VAL at(compare_func_t compare_func) {
            auto maybe = maybe_at(compare_func);
            if(maybe)
                return *maybe;
            throw std::out_of_range("Couldn't find item in AppendOnlyCollisionHashMap.");
        }

        bool contains(const KEY & needle) {
            return bool(maybe_at(needle));
        }

        AppendOnlyCollisionHashMap* _append(KEY && key, VAL && val, const compare_func_t & compare_func, const ensure_func_t & ensure_func, bool already_ensured) {
            Element * last_el;
            Element * cur_el;
            std::tie(last_el, cur_el) = find_element(compare_func);
            if(cur_el != nullptr)
                throw std::runtime_error("AppendOnlyCollisionHashMap already contains key/val: " + to_str(key) + "/" + to_str(val));

            size_t el_indx = element_to_index(last_el);
            AppendOnlyCollisionHashMap * new_this;
            if(already_ensured)
                new_this = this;
            else
                new_this = &ensure_func(sizeof(AppendOnlyCollisionHashMap) + sizeof(Element)*(_size+1));

            last_el = new_this->index_to_element(el_indx);

            auto at_end = new_this->index_to_element(new_this->_size);
            at_end->key = key;
            at_end->val = val;
            if(last_el != nullptr) {
                if(compare_func(last_el->key, last_el->val) < 0)
                    last_el->left = new_this->_size;
                else
                    last_el->right = new_this->_size;
            }
            new_this->_size++;

            return new_this;
        }
        AppendOnlyCollisionHashMap* append(const KEY & key, const VAL & val, const compare_func_t & compare_func, const ensure_func_t & ensure_func, bool already_ensured=false) {
            KEY _key = key;
            VAL _val = val;
            return _append(std::move(_key), std::move(_val), compare_func, ensure_func, already_ensured);
        }

        AppendOnlyCollisionHashMap* append(KEY && key, VAL && val, const compare_func_t & compare_func, const ensure_func_t & ensure_func, bool already_ensured=false) {
            return _append(std::move(key), std::move(val), compare_func, ensure_func, already_ensured);
        }

        void _pop(const compare_func_t & compare_func, const ensure_func_t &ensure_func) {
            // This is a low level function that should only be called when
            // absolutely sure it makes sense to pop the final element from the
            // tree. The key and val must be passed in to validate that the
            // caller is popping the right thing.
            int to_pop_ind = this->_size - 1;
            auto to_pop = this->index_to_element(to_pop_ind);
            if(compare_func(to_pop->key, to_pop->val) != 0) {
                throw std::runtime_error("Pop called with something that doesn't match the final element in the tree");
            }
            auto p = find_element(compare_func);
            Element * before_el = p.first;
            if(p.second == nullptr)
                throw std::runtime_error("Couldn't find element to pop it.");
            if(before_el == nullptr) {
                // Nothing to do
            } else if(before_el->left == to_pop_ind) {
                before_el->left = 0;
            } else if(before_el->right == to_pop_ind) {
                before_el->right = 0;
            } else {
                throw std::runtime_error("Couldn't find parent of element that is being popped.");
            }

            _size--;
            AppendOnlyCollisionHashMap * new_this = &ensure_func(sizeof(AppendOnlyCollisionHashMap) + sizeof(Element)*(_size));
            assert(new_this == this);
        }

        std::string create_diff(size_t from, size_t to) {
            std::string out((char*)index_to_element(from), (char*)index_to_element(to));
            return out;
        }

        void apply_diff(std::string diff, const ensure_func_t & ensure_func) {
            if(diff.size() % sizeof(Element) != 0)
                throw std::runtime_error("Diff isn't a multiple of data type");

            size_t new_size = _size + diff.size()/sizeof(Element);
            auto & new_this = ensure_func(sizeof(AppendOnlyCollisionHashMap) + sizeof(Element)*new_size);

            const Element * data = (Element*)diff.c_str();
            // We can't do true appends, as that requires knowing the true
            // value. Here we have to take it on face value. However, we do need
            // to update the left/right indexes. So we need to fake the
            // comparison function. This is hence why we have the requirement
            // that any hash collisions always go to the left and not use the
            // value itself to make unpredictable choices.

            Element * cur = new_this.index_to_element(0);
            for(int i = 0 ; i < diff.size()/sizeof(Element) ; i++) {
                const KEY & key = data[i].key;
                const VAL & val = data[i].val;
                auto compare_func = [&key,&val](KEY other_key, VAL other_val) {
                    if(other_key != key)
                        return key < other_key ? -1 : +1;

                    if(other_val == val)
                        throw std::runtime_error("Duplicate value encountered while inserting into CollisionHashMap.");
                    else
                        return -1;
                };
                new_this.append(key, val, compare_func, ensure_func, true);
            }

            if(new_this._size != new_size)
                throw std::runtime_error("Size after appending diff is not what was expected.");
        }

        void _construct(bool uninitialized, const ensure_func_t & ensure_func) {
            if(uninitialized) {
                auto& new_this = ensure_func(sizeof(AppendOnlyCollisionHashMap));
                new_this.kind = AppendOnlyKind::COLLISION_HASH_MAP;
                new_this._size = 0;
                new_this._upstream_size = 0;
                new_this._revision = 0;
            } else {
                if(kind != AppendOnlyKind::COLLISION_HASH_MAP)
                    throw std::runtime_error("COLLISION_HASH_MAP is not a COLLISION_HASH_MAP");
                _upstream_size = 0;
            }
        }
    };

    //////////////////////////////////////////////
    // * Variable sized collections

    
    //////////////////////////////
    // ** set

    template <class ELEMENT>
    struct AppendOnlySetVariable {
        AppendOnlyKind kind;
        char kind_version;
        // Size in bytes
        size_t _size;
        size_t _upstream_size;
        size_t _revision;
        size_t& size() { return _size; }
        size_t& upstream_size() { return _upstream_size; }
        size_t& revision() { return _revision; }

        using SAFE = typename ELEMENT::Safe;
        // using SAFE = std::invoke_result<safe_copy<ELEMENT>,const ELEMENT&>::type;
        // using SAFE = std::result_of<safe_copy<ELEMENT>>(const ELEMENT&)::type;
        using ensure_func_t = const std::function<AppendOnlySetVariable&(size_t new_head)>;

        // The raw data
        uintptr_t data() const {
            return (uintptr_t)this + sizeof(AppendOnlySetVariable);
        }

        struct Iterator {
            uintptr_t ptr;

            Iterator() : ptr(0) {}
            Iterator(uintptr_t ptr) : ptr(ptr) {}
            bool operator ==(const Iterator & other) {return ptr == other.ptr;}
            bool operator !=(const Iterator & other) {return !(*this == other);}
            const ELEMENT& operator *() {return *(ELEMENT*)ptr;}
            ELEMENT& _direct() {return *(ELEMENT*)ptr;}
            const ELEMENT* operator ->() {return (ELEMENT*)ptr;}
            Iterator & operator ++() {
                ptr += (**this).true_sizeof();
                return *this;
            }

            using iterator_category = std::forward_iterator_tag;
            using difference_type   = std::ptrdiff_t;
            using value_type        = ELEMENT;
            using pointer           = ELEMENT*;
            using reference         = ELEMENT&;
        };
        Iterator begin() const {return Iterator(data());}
        Iterator end() const {return Iterator(data() + _size);}

        std::optional<SAFE> contains(std::function<bool(const ELEMENT &)> pred) {
            for(const auto & item : *this) {
                if(pred(item))
                    return item.safe_copy();
            }
            return {};
        }

        std::vector<SAFE> as_vector() {
            std::vector<SAFE> out;
            // std::transform(begin(), end(), std::back_inserter(out),
            //                [](const ELEMENT & el) { return el.safe_copy(); });
            for(const auto & item : *this) {
                if(item.deleted)
                    continue;
                out.push_back(item.safe_copy());
            }
            return out;
        }

        void append(const SAFE & el, const ensure_func_t & ensure_func) {
            size_t new_size = _size + ELEMENT::true_sizeof_from_safe(el);
            
            auto& new_this = ensure_func(sizeof(AppendOnlySetVariable) + new_size);
            ELEMENT& old_end = new_this.end()._direct();
            explicit_copy(old_end, el);
            new_this._size = new_size;
        }

        void _pop(const SAFE & el, const ensure_func_t & ensure_func) {
            // This item should be the last one. But we have to traverse through the entire list to find it.
            Iterator last;
            for(auto itr = this->begin() ; itr != this->end() ; ++itr) {
                last = itr;
            }
            // TODO: Handle when set is empty
            if(last->safe_copy() != el)
                throw std::runtime_error("Can't pop item from set as it is not the same as what was there.");
            // Checking, can calculate new_size in two different ways
            auto new_size = _size - last->true_sizeof();
            _size = new_size;
            auto new_this = &ensure_func(sizeof(AppendOnlySetVariable) + new_size);
            assert(new_this == this);
        }

        void _construct(bool uninitialized, const ensure_func_t & ensure_func) {
            if(uninitialized) {
                auto& new_this = ensure_func(sizeof(AppendOnlySetVariable));
                new_this.kind = AppendOnlyKind::SET_VARIABLE;
                new_this._size = 0;
                new_this._upstream_size = 0;
                new_this._revision = 0;
            } else {
                if(kind != AppendOnlyKind::SET_VARIABLE)
                    throw std::runtime_error("SET_VARIABLE is not a SET_VARIABLE");
                _upstream_size = 0;
            }
        }

        // Maybe later...
        // std::string create_diff(size_t from);
        // void apply_diff(std::string diff);

        std::string create_diff(size_t from, size_t to) {
            std::string out;
            for(auto it = Iterator(data() + from); it != Iterator(data() + to); ++it) {
                out += std::string((char*)&*it, it->true_sizeof());
            }
            return out;
        }

        AppendOnlySetVariable* apply_diff(std::string diff, const ensure_func_t & ensure_func) {
            // This manually appends raw ELEMENTs, in contrast to append which appends an ELEMENT::Safe
            size_t new_size = _size + diff.size();
            auto new_this = &ensure_func(sizeof(AppendOnlySetVariable) + new_size);

            const char * data = diff.c_str();
            const char * end = diff.c_str() + diff.size();
            char * target = (char*)&new_this->end()._direct();
            while(data < end) {
                size_t this_size = ((const ELEMENT*)data)->true_sizeof();
                memcpy(target, data, this_size);
                data += this_size;
                target += this_size;
            }
            new_this->_size = new_size;
            return new_this;
        }

    };

    //////////////////////////////
    // ** dict

    template<class KEY, class VAL>
    struct AppendOnlyDictVariable {
        // This is a stub - a complete hack around AppendOnlySetVariable
        using element_t = VariablePair<KEY,VAL>;
        using element_Safe = typename element_t::Safe;
        using KEY_Safe = typename KEY::Safe;
        using VAL_Safe = typename VAL::Safe;
        using set_t = AppendOnlySetVariable<element_t>;
        AppendOnlyKind kind;
        char kind_version;
        set_t set;

        size_t& size() { return set.size(); }
        size_t& upstream_size() { return set.upstream_size(); }
        size_t& revision() { return set.revision(); }

        using ensure_func_t = const std::function<AppendOnlyDictVariable&(size_t new_head)>;
        auto map_ensure_func(const ensure_func_t & ensure_func) {
            return [&ensure_func](size_t new_head)->set_t& {
                AppendOnlyDictVariable& new_this = ensure_func(new_head + offsetof(AppendOnlyDictVariable, set));
                return new_this.set;
            };
        }
        AppendOnlyDictVariable* map_ensured_ptr(const set_t * ptr) {
            return (AppendOnlyDictVariable*)((char*)ptr - offsetof(AppendOnlyDictVariable, set));
        }
    
        typename set_t::Iterator begin() const {return set.begin();}
        typename set_t::Iterator end() const {return set.end();}

        element_t* maybe_at_internal(const KEY_Safe & needle, bool last_seen=false) {
            // for(const auto & item : *this) {

            std::optional<typename set_t::Iterator> last_deleted;
            for(auto itr = this->begin() ; itr != this->end() ; ++itr) {
                if(itr->deleted) {
                    last_deleted = itr;
                    continue;
                }
                if(itr->first.safe_copy() == needle) 
                    return &itr._direct();
            }
            if(last_seen && last_deleted)
                return &last_deleted->_direct();
            return nullptr;
        }
        std::optional<VAL_Safe> maybe_at(const KEY_Safe & needle) {
            element_t * ptr = maybe_at_internal(needle);
            if(ptr == nullptr)
                return {};
            
            return ptr->get_second().safe_copy();
        }
        VAL_Safe at(const KEY_Safe & needle) {
            auto maybe = maybe_at(needle);
            if(maybe)
                return *maybe;
            print_backtrace();
            throw std::out_of_range("Couldn't find " + to_str(needle) + " in AppendOnlyDictVariable.");
        }

        bool contains(const KEY_Safe & needle) {
            return bool(maybe_at(needle));
        }

        std::vector<element_Safe> as_vector() {
            return set.as_vector();
        }

        void append(const KEY_Safe & key, const VAL_Safe & val, const ensure_func_t & ensure_func) {
            KEY_Safe _key = key;
            VAL_Safe _val = val;
            _append(std::move(_key), std::move(_val), ensure_func);
        }
        void append(KEY_Safe && key, VAL_Safe && val, const ensure_func_t & ensure_func) {
            _append(std::move(key), std::move(val), ensure_func);
        }

        void _append(KEY_Safe && key, VAL_Safe && val, const ensure_func_t & ensure_func) {
            element_t * maybe = maybe_at_internal(key);
            if(maybe != nullptr) {
                maybe->deleted = true;
            }
            set.append(element_Safe{key,val}, map_ensure_func(ensure_func));
        }

        void _pop(const KEY_Safe & key, const VAL_Safe & val, const ensure_func_t & ensure_func) {
            set._pop(element_Safe{key,val}, map_ensure_func(ensure_func));
            // In the event that we deleted a previous key, look it up
            auto maybe_last_seen = maybe_at_internal(key, true);
            if(maybe_last_seen != nullptr) {
                maybe_last_seen->deleted = false;
            }
        }

        std::string create_diff(size_t from, size_t to) {
            return set.create_diff(from, to);
        }

        AppendOnlyDictVariable * apply_diff(std::string diff, const ensure_func_t & ensure_func) {
            size_t old_size = size();
            AppendOnlyDictVariable * new_this = map_ensured_ptr(set.apply_diff(diff, map_ensure_func(ensure_func)));
            // We are now retroactively applying the delete logic, but only for things that were before the original end.
            element_t * old_end = (element_t*)((char*)new_this->set.data() + old_size);
            element_t * target = old_end;
            element_t * end = (element_t*)&new_this->end()._direct();
            while(target < end) {
                size_t this_size = target->true_sizeof();
                element_t * maybe = new_this->maybe_at_internal(target->first.safe_copy());
                if(maybe != nullptr) {
                    if(maybe < old_end) {
                        maybe->deleted = true;
                    }
                }
                target = (element_t*)((char*)target + this_size);
            }
            return new_this;
        }

        void _construct(bool uninitialized, const ensure_func_t & ensure_func) {
            if(uninitialized) {
                auto& new_this = ensure_func(sizeof(AppendOnlyDictVariable));
                new_this.kind = AppendOnlyKind::DICT_VARIABLE;
                new_this.set._construct(uninitialized, map_ensure_func(ensure_func));
                // Note: can't be assured that new_this is still new_this after the set _construct call.
            } else {
                if(kind != AppendOnlyKind::DICT_VARIABLE)
                    throw std::runtime_error("DICT_VARIABLE is not a DICT_VARIABLE");
                set._construct(uninitialized, map_ensure_func(ensure_func));
                // Note: can't be assured that this is still this after the set _construct call.
            }
        }
    };



    //////////////////////////////
    // * Misc

    // Sort of relevant here:
    template<class FUNC>
    enum_indx unused_index(const FUNC & contains) {
        while(true) {
            static std::random_device rd;  //Will be used to obtain a seed for the random number engine
            static std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
            static std::uniform_int_distribution<enum_indx> dis(0, (std::numeric_limits<enum_indx>::max)());
            enum_indx ran = dis(gen);

            if(!contains(ran))
                return ran;
        }
    }


}
