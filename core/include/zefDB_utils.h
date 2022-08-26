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
        std::filesystem::path parent;
        if(prefix.has_parent_path())
            parent = prefix.parent_path();
        else
            parent = ".";

        if(!std::filesystem::exists(parent))
            return false;

        for(auto const& dir_entry : std::filesystem::directory_iterator{parent}) {
            std::string dir_str = dir_entry.path().filename().string();
            std::string prefix_str = prefix.filename().string();
            if(starts_with(dir_str, prefix_str))
                return true;
        }
        return false;
    }

    // Stealing from StackOverflow: https://stackoverflow.com/questions/20112221/invoking-a-function-automatically-on-stdthread-exit-in-c11
    inline void on_thread_exit(std::string name, std::function<void()> func) {
        thread_local struct ThreadExiter {
            std::deque<std::pair<std::string, std::function<void()>>> callbacks;

            ~ThreadExiter() {
                for (auto &p: callbacks) {
                    auto & name = std::get<0>(p);
                    auto & callback = std::get<1>(p);
                    // std::cerr << "Going to call one callback: " << name << std::endl;
                    callback();
                    // std::cerr << "Finished calling callback: " << name << std::endl;
                }
            }
        } exiter;

        exiter.callbacks.emplace_front(name, std::move(func));
    }

#ifdef ZEF_WIN32
}
#include <windows.h>
#include <sysinfoapi.h>
#include <io.h>
#include <fcntl.h>

namespace zefDB {
    inline std::string WindowsErrorMsg() {
        LPVOID lpMsgBuf;
        FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                      NULL,
                      GetLastError(),
                      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                      (LPTSTR)&lpMsgBuf,
                      0, NULL);

        std::string ret((LPCTSTR)lpMsgBuf);

        LocalFree(lpMsgBuf);
        return ret;
    }

    constexpr size_t WIN_FILE_LOCK_BYTES = 1024;
    inline bool OSLockFile(int fd, bool exclusive=true, bool block=false) {
        HANDLE h = (HANDLE)_get_osfhandle(fd);
        OVERLAPPED overlapped;
        overlapped.Offset = 0;
        overlapped.OffsetHigh = 0;
        overlapped.hEvent = 0;
        int flags = 0;
        if(exclusive)
            flags |= LOCKFILE_EXCLUSIVE_LOCK;
        if(!block)
            flags |= LOCKFILE_FAIL_IMMEDIATELY;
        if (!LockFileEx(h, flags, NULL, WIN_FILE_LOCK_BYTES, 0, &overlapped))
            return false;
        return true;
    }
    inline void OSUnlockFile(int fd, bool flush=false) {
        HANDLE h = (HANDLE)_get_osfhandle(fd);
        if(flush) {
            if (!FlushFileBuffers(h))
                throw std::runtime_error("Problem flushing file to disk: " + WindowsErrorMsg());
        }
        if (!UnlockFile(h, 0, 0, WIN_FILE_LOCK_BYTES, 0))
            throw std::runtime_error("Problem unlocking file: " + WindowsErrorMsg());
    }
#else
}

#include <sys/file.h>
#include <unistd.h>

namespace zefDB {

    inline bool OSLockFile(int fd, bool exclusive=true, bool block=false) {
        int flags = 0;
        if(exclusive)
            flags |= LOCK_EX;
        if(!block)
            flags |= LOCK_NB;
        if(-1 == flock(fd, flags))
            return false;
        return true;
    }
    inline void OSUnlockFile(int fd, bool flush=false) {
        if(flush)
            fsync(fd);
        if (-1 == flock(fd, LOCK_UN))
            throw std::runtime_error("Problem unlocking file: " + errno);
    }
#endif

    // Acquires a lock on a given named file in the given directory. This file
    // should not itself contain any data. Uses flock or similar.
    struct FileLock {
        std::filesystem::path path;
        int locked = false;
        int fd = -1;
        FileLock(std::filesystem::path path, bool exclusive=true)
            : path(path) {
            if(std::filesystem::is_directory(path))
                path /= ".lock";
            fd = open(path.string().c_str(), O_RDWR | O_CREAT, 0600);
            if(fd == -1)
                throw std::runtime_error("Opening lockfile '" + path.string() + "' failed.");
                
            locked = OSLockFile(fd, exclusive, true);
            if(!locked)
                throw std::runtime_error("Locking file failed.");
        }
        ~FileLock() {
            if(fd != -1) {
                if(locked)
                    OSUnlockFile(fd);
                close(fd);
            }
        }
    };

    template<typename T, class... Types>
    inline bool variant_eq(const T& t, const std::variant<Types...>& v) {
        const T* c = std::get_if<T>(&v);
        if(c)
            return *c == t;
        else
            return false;
    }

    template<typename T, class... Types>
    inline bool variant_eq(const std::variant<Types...>& v, const T& t) {
        return variant_eq(t, v);
    }

    template<class... Types>
    inline bool variant_eq(const std::variant<Types...>& v, const std::variant<Types...>& t) {
        return t == v;
    }
    
    // This is apparently the hash_combine that's in boost. Taken from
    // https://stackoverflow.com/questions/19195183/how-to-properly-hash-the-custom-struct.
    template <class T>
    inline void hash_combine(std::size_t & s, const T & v)
    {
        std::hash<T> h;
        s ^= h(v) + 0x9e3779b9 + (s<< 6) + (s>> 2);
    }

    template <class T>
    inline size_t get_hash(const T & thing) {
        return std::hash<T>{}(thing);
    }

    // STOLEN FROM CPPREFERENCE
    // helper type for the visitor #4
    template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
    // explicit deduction guide (not needed as of C++20)
    template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;
}