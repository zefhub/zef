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

#include <vector>
#include <array>
#include <unordered_map>
#include <algorithm>
#include <cassert>
#include <iostream>
#include <chrono>
#include <sstream>      // std::stringstream
#include <random>
#include <optional>
#include <functional>
#include <variant>
#include <deque>
#include <cstring>
#include "include_fs.h"

// #include "fwd_declarations.h"
#include "constants.h"

#include "range/v3/all.hpp"

#include "base64.hpp"

namespace zefDB {
	constexpr auto print = [](const auto& x)->void { std::cout << x << std::endl; };
	constexpr auto printt = [](const auto& v)->void { for (auto& el : v) std::cout << el << ", "; std::cout << std::endl; };



	template<typename T>
	bool operator== (std::optional<T> x1, std::optional<T> x2) {
		bool b1 = bool(x1);
		bool b2 = bool(x2);
		if (!b1 && !b2) return true;
		if (b1 && b2) return *x1 == *x2;
		return false;  // one is set, the other is not
	}


	template<typename T>
	bool operator!= (std::optional<T> x1, std::optional<T> x2) {
		return !(x1 == x2);
	}

	// my_mod has to be a power of two, e.g. 1024 for this to work!
	inline size_t modulus_pow_two_binary(size_t x, size_t my_mod) {
		assert(my_mod % 2 == 0);
		return x & (my_mod - 1);  // bitwise and 
	}


	constexpr auto contains = [](const auto& container, auto element)->bool {
		return container.find(element) != container.end();
	};



	constexpr auto contains2 = [](const auto& container, auto element)->bool {
		return std::find(container.begin(), container.end(), element) != container.end();
	};


	template <typename T>
	bool find_element(const std::vector<T>& v, T value) {
		//is this element contained in the vector?
		for (auto& el : v) {
			if (el == value) return true;  // the element was found. Exit after the first instance.			
		}
		return false;  //element was not found
	}


	template <typename T>
	size_t count_element_num_occurences(std::vector<T>& v, T value) {
		//how often is this element contained in the vector?
		size_t count = 0;
		for (auto& el : v) {
			if (el == value) count++;  // the element was found.
		}
		return count;
	}


	template <typename T>
	bool find_element_and_erase(std::vector<T>& v, T value) {
		//searches for the first occurrence of value and erases the element in the vector
		for (unsigned int pos = 0; pos < v.size(); pos++) {
			if (v[pos] == value) {
				v.erase(v.begin() + pos);
				return true;  // the element was found. Exit after the first instance.
			}
		}
		return false;  //element was not found
	}


	template <typename T>
	bool element_is_in_list(T element, std::vector<T>& L) {
		for (auto& x : L)
			if (x == element) return true;
		return false;
	}


	template <typename T>
	bool element_is_in_list_mark_pos(T element, std::vector<T>& L, size_t& pos) {
		for (auto& x : L)
			if (x == element) {
				pos = &x - &L[0];
				return true;
			}
		pos = -1;
		return false;
	}


	template<typename T>
	bool check_if_two_vectors_contain_same_elements(std::vector<T> v1, std::vector<T> v2)
	{
		//are they simply a different permutation?
		if (v1.size() != v2.size()) return false;
		std::sort(v1.begin(), v1.end());
		std::sort(v2.begin(), v2.end());
		return v1 == v2;
	}

    template <class T>
	std::string to_str(T val) {
		std::stringstream ss;
		ss << val;
		return ss.str();
	}

    template<class T>
	std::string to_str(const std::atomic<T> & val) {
		std::stringstream ss;
		ss << val.load();
		return ss.str();
	}



	// we need to calculate hashes from char arrays at compile time to use switch on strings for looking up 
	// entity_type and relation_type enums, given a string. Taken from 
	// https://hbfs.wordpress.com/2017/01/10/strings-in-c-switchcase-statements/
	uint64_t constexpr mix_for_hash_char_array(char m, uint64_t s)
	{
		return ((s << 7) + ~(s >> 3)) + ~m;
	}

	uint64_t constexpr hash_char_array(const char* m)
	{
		return (*m) ? mix_for_hash_char_array(*m, hash_char_array(m + 1)) : 0;
	}
	


	class timer
	{
	private:
		std::chrono::high_resolution_clock::time_point start_;

	public:
		timer()	{ reset(); }
		void reset(){ start_ = std::chrono::high_resolution_clock::now(); }
		std::chrono::microseconds elapsed() const { 
			return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start_);
		}
		friend std::ostream& operator<<(std::ostream& sout, timer const& t)	{
			return sout << t.elapsed().count() << "us";
		}
	};







	class NullStream : public std::ostream {
	public:
		NullStream() : std::ostream(nullptr) {}
		NullStream(const NullStream&) : std::ostream(nullptr) {}
	};

	template <class T>
	const NullStream& operator<<(NullStream&& os, const T& value) {
		return os;
	}

	const auto null_stream = NullStream();







	// some utility functions for concatenating strings

	inline std::string operator+ (int m, std::string s) {
		std::stringstream ss;
		ss << m << s;
		return ss.str();
	}

	inline std::string operator+ (std::string s, int m) {
		std::stringstream ss;
		ss << s << m;
		return ss.str();
	}

	inline std::string operator+ (std::string s, double m) {
		std::stringstream ss;
		ss << s << m;
		return ss.str();
	}

	inline std::string operator* (int m, std::string s) {
		std::stringstream ss;
		for (int c = 0; c < m; c++)
			ss << s;
		return ss.str();
	}
	inline std::string operator* (std::string s, int m) { return m * s; }




    inline const unsigned long long& generate_random_number_random_device() {
        static std::random_device rd;  //Will be used to obtain a seed for the random number engine
        static std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
        static std::uniform_int_distribution<unsigned long long> dis(std::numeric_limits<unsigned long long>::min(), std::numeric_limits<unsigned long long>::max());
        static std::vector<unsigned long long> buffer(constants::random_number_random_device_buffer_size, 0);
        static int pos = 0;
        if (pos == 0) {
            while (pos < constants::random_number_random_device_buffer_size) buffer[pos++] = dis(gen);				
        }			
        return buffer[--pos];
    }

    struct RAII_CallAtEnd {
        std::function<void()> func;
        RAII_CallAtEnd(std::function<void()> func) : func(func) {}
        ~RAII_CallAtEnd() { func(); }
    };

    inline bool parse_string_bool(std::string s) {
        if(s == "0" ||
           s == "NO" ||
           s == "no" ||
           s == "FALSE" ||
           s == "false")
            return false;
        if(s == "1" ||
           s == "YES" ||
           s == "yes" ||
           s == "TRUE" ||
           s == "true")
            return true;
        throw std::runtime_error("Unknown string to convert to bool: '" + s + "'");
    }
    inline bool check_env_bool(const char * var, bool default_val=false) {
        char * env = std::getenv(var);
        if(env == nullptr)
            return default_val;

        std::string s_env(env);
        if(s_env == "")
            return default_val;

        try {
            return parse_string_bool(s_env);
        } catch(...) {
            std::cerr << "Warning, found value for environment variable " << var << "='" << s_env << "' but was not recognised. Value should be one of 0, 1, NO, YES, FALSE, TRUE." << std::endl;
        }

        return default_val;
    }

    inline bool starts_with(std::string s, std::string start) {
        if(s.size() < start.size())
            return false;
        return (s.substr(0, start.size()) == start);
    }

    inline bool any_files_with_prefix(std::filesystem::path prefix) {
        for(auto const& dir_entry : std::filesystem::directory_iterator{prefix.parent_path()}) {
            std::string dir_str = dir_entry.path().filename().string();
            std::string prefix_str = prefix.filename().string();
            if(starts_with(dir_str, prefix_str))
                return true;
        }
        return false;
    }

    // Stealing from StackOverflow: https://stackoverflow.com/questions/20112221/invoking-a-function-automatically-on-stdthread-exit-in-c11
    inline void on_thread_exit(std::function<void()> func) {
        thread_local struct ThreadExiter {
            std::deque<std::function<void()>> callbacks;

            ~ThreadExiter() {
                for (auto &callback: callbacks) {
                    callback();
                }
            }
        } exiter;

        exiter.callbacks.emplace_front(std::move(func));
    }

}
