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

#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <vector>

#include "fwd_declarations.h"

#include <parallel_hashmap/phmap.h>

namespace zefDB {
//     // TODO: Want a wrapped unordered_map accessor class. This should be
//     // generic and users of it shouldn't care about the implementation
//     // (locks/fixed-size/etc...).
//     //
//     // Implementation possibilities:
//     // - unordered_map with locks (fine if not accessed very often)
//     // - unordered_map per thread with messages passed back and forth.
//     // - manual implementation of fixed buckets + linked list
//     //
//     // Ideally want an imlementation which has simple lock-free update, plus a lockable "restructure".
//     // Could be done with 2 variables:
//     // - int: increment when reader enters, decrement when reader exits. When zero, writer is allowed to restructre.
//     // - bool: restructuring, when true, reader blocks, and once other int hits zero, writer can restructure.
//     // Is this just a simple multiple-readers+1-writer loop?
//     // This is likely a problem - more than 1 writer is possible... is it?
//     // In any case, that could be solved with an additional bool for
//     // writing, that only writers can about.
//     //
//     // Could start down this route, where a writer using an unordered_map is
//     // always "restructuring" by the nature of the STL container, but later
//     // on open it up for different implementations.
//     //
//     // Note: could change the implementation to be a read-copy-update algorithm.
//     template <typename KEY, typename VAL, typename HASH=std::hash<KEY>> 
//     struct thread_safe_unordered_map {
// #if __cplusplus == 202002L
//         // This wouldn't work exactly... need to figure this out. Maybe this
//         // isn't possible with simple atomics? We might need a CV to make it
//         // work. Unless a CV is only necessary for the writers, but not the
//         // readers?
// #error "Maps in C++20 don't work currently!"
//         std::atomic_int reading;
//         std::atomic_bool writing;
// #else
//         std::shared_mutex m;
// #endif

//         using map_t = typename std::unordered_map<KEY,VAL,HASH>;
//         map_t map;

//         template <class T>
//         auto do_read(T func) {
// #if __cplusplus == 202002L
//             // writing.wait(false);
//             // reading++;
// #else
//             std::shared_lock lock(m);
//             return func();
// #endif
//         }

//         template <class T>
//         auto do_write(T func) {
// #if __cplusplus == 202002L
// #else
//             std::unique_lock lock(m);
//             return func();
// #endif
//         }


//         // ** Reading functions
// #define READ_FUNC(name)                         \
//         template <class... ARGS>                \
//         auto name(ARGS... args) {               \
//             return do_read([&]() {              \
//                 return map.name(args...);       \
//             });                                 \
//         }

//         READ_FUNC(contains);
//         READ_FUNC(empty);
//         READ_FUNC(size);
//         READ_FUNC(max_size);
//         READ_FUNC(at);
//         READ_FUNC(count);
//         // READ_FUNC(operator[]);
//         READ_FUNC(find);

//         // ** Writing functions

// #define WRITE_FUNC(name)                        \
//         template <class... ARGS>                \
//         auto name(ARGS... args) {               \
//             return do_write([&]() {             \
//                 return map.name(args...);       \
//             });                                 \
//         }

//         WRITE_FUNC(clear);
//         WRITE_FUNC(insert);
//         WRITE_FUNC(insert_or_assign);
//         WRITE_FUNC(emplace);
//         WRITE_FUNC(emplace_hint);
//         WRITE_FUNC(try_emplace);
//         WRITE_FUNC(erase);
//         WRITE_FUNC(swap);
//         WRITE_FUNC(extract);
//         WRITE_FUNC(merge);

//         // ** Iterator
//         // The iterator must obtain a read-lock for the lifetime of the iterator.
//         // struct Iterator {
//         // };
//         using it_t = typename map_t::iterator;
//         using cit_t = typename map_t::const_iterator;

//         struct ConstIterator : cit_t {
//             using lock_t = typename std::shared_lock<std::shared_mutex>;

//             lock_t lock;
//             ConstIterator(cit_t it, lock_t && lock) : cit_t(it), lock(std::move(lock)) {}
//         };

//         struct Iterator : it_t {
//             using lock_t = typename std::shared_lock<std::shared_mutex>;

//             it_t it;
//             lock_t lock;
//             Iterator(it_t it, lock_t && lock) : it_t(it), lock(std::move(lock)) {}
//         };

// #define CONSTITERATOR_FUNC(name)                                \
//         template <class... ARGS>                                \
//         ConstIterator name(ARGS... args) {                      \
//             std::shared_lock lock(m);                           \
//             cit_t iterator = map.name(args...);                 \
//             return ConstIterator(iterator, std::move(lock));    \
//         }

//         CONSTITERATOR_FUNC(cbegin);
//         CONSTITERATOR_FUNC(cend);

//         // Don't need a unique_lock for the iterator! We're not modifying the
//         // dict structure, only the contents of one element.
// #define ITERATOR_FUNC(name)                             \
//         template <class... ARGS>                        \
//         Iterator name(ARGS... args) {                   \
//             std::shared_lock lock(m);                   \
//             it_t iterator = map.name(args...);          \
//             return Iterator(iterator, std::move(lock)); \
//         }


//         ITERATOR_FUNC(begin);
//         ITERATOR_FUNC(end);

//         // ** Special cases

//         template <class T>
//         VAL& operator[](T key) {
//             // Because operator[] may create a new key, this could require
//             // write access. We also don't want to simply upgrade the lock
//             // to do this, as that can lead to blocking conditions.
//             {
//                 std::shared_lock lock(m);
//                 auto it = map.find(key);
//                 if(it != map.end())
//                     return it->second;
//             }
//             // If we get here, we need write access
//             {
//                 std::unique_lock lock(m);
//                 return map[key];
//             }
//         }
            

//     };

    // Note: big problem with this class is its size even when only using 1
    // "parallel" bin. Would be smaller to use flat_hash_map with our own manual
    // locking around it. Although a mutex is a rather large object on its own.
    template<class KEY,class VAL,class HASH_FCT = std::hash<KEY>>
    using thread_safe_unordered_map = phmap::parallel_flat_hash_map<
        KEY, VAL,
        HASH_FCT,
        std::equal_to<KEY>,
        std::allocator<std::pair<const KEY, VAL>>,
        1,
        std::mutex>;


    namespace hidden {
        template <typename T1, typename T2>
        struct bidirectional_map {
            // T1 counter = 0;  // translate a string into an extended enum numerical type. For each new string added, assign a
            using map1_t = typename std::unordered_map<T1, T2>;
            using map2_t = typename std::unordered_map<T2, T1>;
            map1_t map1;
            map2_t map2;

            std::vector<std::tuple<T1, T2>> all_entries_as_list() {
                return ranges::views::all(map1)
                    | ranges::views::transform([](const auto& x)->std::tuple<T1, T2> { return { x.first, x.second }; })
                    | ranges::to<std::vector>
                    | ranges::actions::sort([](auto& x1, auto& x2) { return std::get<0>(x1) < std::get<0>(x2); });
            }

            std::vector<T2> all_keys() {
                return ranges::views::all(map1)
                    | ranges::views::transform([](const auto& x)->T2 { return x.second; })
                    | ranges::to<std::vector>;
            }

            std::vector<T1> all_indices() {
                return ranges::views::all(map1)
                    | ranges::views::transform([](const auto& x)->T1 { return x.first; })
                    | ranges::to<std::vector>;
            }

            // T1 insert(T2 val2) {
            //     T1 indx = generate_unused_random_number();
            //     insert(indx, val2);
            //     return indx;
            // }

            void insert(T1 indx, T2 val2) {
                map1[indx] = val2;
                map2[val2] = indx;
            }

            T2 at(const T1& val1) { 
                try { return map1.at(val1); } 
                catch (...) { 
                    std::cout << "key not found: " << val1 << std::endl; 
                    throw std::runtime_error("key not found in bidirectional_map"); 
                } 
            }
            T1 at(const T2& val2) { 
                try { return map2.at(val2); }
                catch (...) {
                    std::cout << "string key not found: " << val2 << std::endl;
                    throw std::runtime_error("string key not found in bidirectional_map");
                }
            }

            // T1 get_or_insert_and_get_enum_number(const T2& val) {
            //     auto found_val_it = map2.find(val);  // if the key has been added previously, it will have been found
            //     if (found_val_it != map2.end()) return found_val_it->second;
            //     else return insert(val);			
            // }

            size_t size() {
                return map1.size();
            }
        
            bool contains(const T1& val) {
                return map1.find(val) != map1.end();
            }

            bool contains(const T2& val) {
                return map2.find(val) != map2.end();
            }

            T1 generate_unused_random_number() {
                if (map1.size() >= map1.max_size())
                    throw std::runtime_error("We have run out of indexes in bidirectional_map: too many relation or entity type added for which the type is translated to an index!\n");

                static std::random_device rd;  //Will be used to obtain a seed for the random number engine
                static std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
                static std::uniform_int_distribution<T1> dis(0, (std::numeric_limits<T1>::max)());
                T1 ran = dis(gen);

                return contains(ran) ? generate_unused_random_number() : ran;
            }
        };

    }




    template <typename T1, typename T2>
    struct thread_safe_bidirectional_map {
#if __cplusplus == 202002L
#error "Maps in C++20 don't work currently!"
#else
        std::shared_mutex m;
#endif

        using map_t = typename hidden::bidirectional_map<T1,T2>;
        map_t map;

        template <class T>
        auto do_read(T func) {
#if __cplusplus == 202002L
#else
            std::shared_lock lock(m);
            return func();
#endif
        }

        template <class T>
        auto do_write(T func) {
#if __cplusplus == 202002L
#else
            std::unique_lock lock(m);
            return func();
#endif
        }


        // ** Reading functions
#define READ_FUNC(name)                         \
        template <class... ARGS>                \
        auto name(ARGS... args) {               \
            return do_read([&]() {              \
                return map.name(args...);       \
            });                                 \
        }

        READ_FUNC(at);
        READ_FUNC(contains);
        READ_FUNC(size);
        READ_FUNC(all_entries_as_list);
        READ_FUNC(all_keys);
        READ_FUNC(all_indices);
        READ_FUNC(generate_unused_random_number);

        typename map_t::map1_t copy_of_map1(void) {
            return do_read([&]()->typename map_t::map1_t { return map.map1; });
        }
        typename map_t::map2_t copy_of_map2(void) {
            return do_read([&]()->typename map_t::map2_t { return map.map2; });
        }

        // ** Writing functions
#define WRITE_FUNC(name)                        \
        template <class... ARGS>                \
        auto name(ARGS... args) {               \
            return do_write([&]() {             \
                return map.name(args...);       \
            });                                 \
        }

        WRITE_FUNC(insert);

        // ** Special cases

        // VAL& get_or_insert_and_get_enum_number(const std::string& val) {
        //     // Because this may create a new key, this could require
        //     // write access. We also don't want to simply upgrade the lock
        //     // to do this, as that can lead to blocking conditions.
        //     {
        //         std::shared_lock lock(m);
        //         if (map.contains(val))
        //             return map.get_or_insert_and_get_enum_number(val);
        //     }
        //     // If we get here, we need write access
        //     {
        //         std::unique_lock lock(m);
        //         return map.get_or_insert_and_get_enum_number(val);
        //     }
        // }

        // We sometimes want to find an item, but without worrying about the
        // lock between calls, i.e. we should get back the item atomically, or
        // get back "not found".
        std::optional<T1> maybe_at(T2 key) {
            std::shared_lock lock(m);
            auto out = map.map2.find(key);
            if(out == map.map2.cend())
                return {};
            return std::make_optional<T1>(out->second);
        }
        std::optional<T2> maybe_at(T1 key) {
            std::shared_lock lock(m);
            auto out = map.map1.find(key);
            if(out == map.map1.cend())
                return {};
            return std::make_optional<T2>(out->second);
        }
    };

    namespace hidden {
        struct LIBZEF_DLL_EXPORTED zef_enum_bidirectional_map {
            using map1_t = typename std::unordered_map<std::string, enum_indx>;
            using map2_t = typename std::unordered_map<enum_indx, string_pair>;
            map1_t enum_name_to_indx;     // map from enum name -> index
            map2_t indx_to_string_pair;     // map from enum value (int) -> name
            std::vector<std::tuple<enum_indx, std::string, std::string>> all_entries_as_list();
            std::vector<std::string> all_enum_types();
            std::vector<std::string> all_enum_values(const std::string & needle);
            std::vector<enum_indx> all_indices();
            void disp() ;
            enum_indx generate_unused_random_number() ;
            // enum_indx insert(string_pair sp) ;
            void insert(enum_indx new_indx, string_pair sp) ;
            string_pair at(enum_indx indx_key) ;
            enum_indx at(const string_pair& sp) ;
            // enum_indx get_or_insert_and_get_enum_number(const string_pair& name_pair) ;
            bool contains(enum_indx indx_key) ;
            bool contains(const string_pair& name_pair) ;
            size_t size();
        };
    }

    // TODO: This is a lot of duplication, need to improve this.
    struct thread_safe_zef_enum_bidirectional_map {
#if __cplusplus == 202002L
#error "Maps in C++20 don't work currently!"
#else
        std::shared_mutex m;
#endif

        using map_t = typename hidden::zef_enum_bidirectional_map;
        map_t map;

        template <class T>
        auto do_read(T func) {
#if __cplusplus == 202002L
#else
            std::shared_lock lock(m);
            return func();
#endif
        }

        template <class T>
        auto do_write(T func) {
#if __cplusplus == 202002L
#else
            std::unique_lock lock(m);
            return func();
#endif
        }


        // ** Reading functions
#define READ_FUNC(name)                         \
        template <class... ARGS>                \
        auto name(ARGS... args) {               \
            return do_read([&]() {              \
                return map.name(args...);       \
            });                                 \
        }

        READ_FUNC(at);
        READ_FUNC(contains);
        READ_FUNC(size);
        READ_FUNC(all_entries_as_list);
        READ_FUNC(all_enum_types);
        READ_FUNC(all_enum_values);
        READ_FUNC(all_indices);
        READ_FUNC(generate_unused_random_number);

        typename map_t::map1_t copy_of_enum_name_to_indx(void) {
            return do_read([&]()->typename map_t::map1_t { return map.enum_name_to_indx; });
        }
        typename map_t::map2_t copy_of_indx_to_string_pair(void) {
            return do_read([&]()->typename map_t::map2_t { return map.indx_to_string_pair; });
        }

        // ** Writing functions
#define WRITE_FUNC(name)                        \
        template <class... ARGS>                \
        auto name(ARGS... args) {               \
            return do_write([&]() {             \
                return map.name(args...);       \
            });                                 \
        }

        WRITE_FUNC(insert);

        // Because functions are often used with brace initalisation lists, these
        // need to know what they are going to be constructing, so we define the
        // actual function here too.
        auto insert(enum_indx indx, string_pair val) {
            return do_write([&]() {
                return map.insert(indx, val);
            });
        }
        auto contains(string_pair val) {
            return do_read([&]() {
                return map.contains(val);
            });
        }
        auto at(string_pair val) {
            return do_read([&]() {
                return map.at(val);
            });
        }

        // ** Special cases
        // enum_indx get_or_insert_and_get_enum_number(const string_pair& val) {
        //     // Because this may create a new key, this could require
        //     // write access. We also don't want to simply upgrade the lock
        //     // to do this, as that can lead to blocking conditions.
        //     {
        //         std::shared_lock lock(m);
        //         if (map.contains(val))
        //             return map.get_or_insert_and_get_enum_number(val);
        //     }
        //     // If we get here, we need write access
        //     {
        //         std::unique_lock lock(m);
        //         return map.get_or_insert_and_get_enum_number(val);
        //     }
        // }

        template<class... ARGS>
        thread_safe_zef_enum_bidirectional_map(ARGS... args)
            : map(args...) {}

        // We sometimes want to find an item, but without worrying about the
        // lock between calls, i.e. we should get back the item atomically, or
        // get back "not found".
        std::optional<enum_indx> maybe_at(string_pair key) {
            std::shared_lock lock(m);
            if(map.contains(key))
                return map.at(key);
            else
                return {};
        }
        std::optional<string_pair> maybe_at(enum_indx key) {
            std::shared_lock lock(m);
            if(map.contains(key))
                return map.at(key);
            else
                return {};
        }
    };



}
