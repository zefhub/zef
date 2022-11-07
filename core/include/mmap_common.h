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

// This file should only be included from mmap.h

#include <iostream>
#include <vector>
#include <stdexcept>
#include <bitset>
#include <limits>
#include <array>

#include "include_fs.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <math.h>
#include <string.h>
#include <mutex>
#include <shared_mutex>
#include <atomic>

#include "constants.h"
#include "zefDB_utils.h"
#include "uids.h"

namespace zefDB {
    namespace MMap {

        constexpr unsigned int PAGE_BITMAP_BITS = ZEF_UID_SHIFT / ZEF_PAGE_SIZE;

        void * OSMMap(int fd, size_t offset, size_t size, void * location);

        // * Utils

        inline void * floor_ptr(const void * ptr, size_t mod) { return (char*)ptr - (size_t)ptr % mod; }
        // Check if the below works and is any faster
        // void * floor_ptr(void * ptr, size_t mod) { return ptr & (my_mod - 1); }
        inline void * ceil_ptr(const void * ptr, size_t mod) {
            void * temp = floor_ptr(ptr, mod);
            if(temp != ptr)
                temp = (char*)temp + mod;
            return temp;
        }

        void * align_to_system_pagesize(const void * ptr);

        inline void error(const char * s) {
            std::cerr << "Error: " << s << std::endl;
            // exit(1);
            throw std::runtime_error(s);
        }
        inline void error_p(const char * s) {
            perror(s);
            // exit(1);
            throw std::runtime_error(s);
        }

        inline size_t fd_size(int fd) {
            struct stat buf;
            if(0 != fstat(fd, &buf))
                error_p("Could not fstat fd.");
            return buf.st_size;
        }

        // OS specific
        bool is_fd_writable(int fd);


        //////////////////////////////
        // * FileGraph

        struct FileAlreadyLocked : public std::exception {
            std::filesystem::path path;
            std::string s;
            int what_errno;
            FileAlreadyLocked(std::filesystem::path path) : path(path) {
                what_errno = errno;
                s = std::string("Can't acquire exclusive lock on filegraph (") + path.string() + "), aborting. Errno = " + to_str(what_errno);
            }
            const char * what() const noexcept {
                return s.c_str();
            }
        };

        struct FileGraphWrongVersion : public std::exception {
            std::filesystem::path path;
            int version;
            std::string extra;

            // This is so we can guarantee the life of the what() string.
            std::string s;

            FileGraphWrongVersion(std::filesystem::path path, int version, std::string extra="") : path(path), version(version), extra(extra) {
                s = std::string("Filegraph (") + path.string() + "), was the wrong version (" + to_str(version) + ").";
                if(extra != "")
                    s += " " + extra;
            }
            const char * what() const noexcept {
                return s.c_str();
            }
        };

        struct FileGraph {
            static constexpr size_t invalid_page = std::numeric_limits<size_t>::max();
            std::filesystem::path path_prefix;
            std::vector<int> fds = {};
            // std::vector<size_t> file_size_in_pages = {};
            struct Element_v1 {
                // Pages are stored in separate files, with an offset of pages in that file.
                // A special file_index=0 means "this file".
                size_t file_index;
                size_t offset;
                Element_v1()
                    : offset(invalid_page),
                        file_index(0) {}
            };

            struct WholeFile_v1 {
                size_t file_index = invalid_page;
                size_t head = 0;
            };
            // struct Prefix_v1 {
            //     int version = 1;
            //     blob_index last_update = 0;
            //     std::array<Element_v1, 2*MMap::PAGE_BITMAP_BITS> page_info;
            // };
            // struct Prefix_v2 {
            //     int version = 2;
            //     zefDB::BaseUID uid;
            //     blob_index last_update = 0;
            //     std::array<Element_v1, 2*MMap::PAGE_BITMAP_BITS> page_info;

            //     Prefix_v2(BaseUID uid) : uid(uid) {};
            // };
            struct Prefix_v3 {
                // TODO: This would be great as a flat zef value later, which
                // would handle the arbitrary length items better.

                // We can cheat here a little with bumping version when the data
                // layout changes. This has the effect of throwing away old
                // versions, but it is very manual
                constexpr static int VERSION = 4;
                // int version = 3;
                int version = VERSION;
                zefDB::BaseUID uid;
                blob_index last_update = 0;
                std::array<Element_v1, MMap::PAGE_BITMAP_BITS> page_info;

                // Next free file is 1, as we are currently 0.
                int next_free_file_index = 1;

                // Where can we map the token file structure
                WholeFile_v1 tokens_ET;
                WholeFile_v1 tokens_RT;
                WholeFile_v1 tokens_EN;

                WholeFile_v1 uid_lookup;
                WholeFile_v1 euid_lookup;
                WholeFile_v1 tag_lookup;

                // WholeFile_v1 tokens_ET_dict;
                
                Prefix_v3(BaseUID uid) : uid(uid) {};
            };
            struct Prefix_v5 {
                // TODO: This would be great as a flat zef value later, which
                // would handle the arbitrary length items better.

                // We can cheat here a little with bumping version when the data
                // layout changes. This has the effect of throwing away old
                // versions, but it is very manual
                constexpr static int VERSION = 5;
                int version = VERSION;
                zefDB::BaseUID uid;
                blob_index last_update = 0;
                std::array<Element_v1, MMap::PAGE_BITMAP_BITS> page_info;

                // Next free file is 1, as we are currently 0.
                int next_free_file_index = 1;

                // Where can we map the token file structure
                WholeFile_v1 tokens_ET;
                WholeFile_v1 tokens_RT;
                WholeFile_v1 tokens_EN;

                WholeFile_v1 uid_lookup;
                WholeFile_v1 euid_lookup;
                WholeFile_v1 tag_lookup;
                WholeFile_v1 av_hash_lookup;

                // WholeFile_v1 tokens_ET_dict;
                
                Prefix_v5(BaseUID uid) : uid(uid) {};
            };
            using latest_Prefix_t = Prefix_v5;
            constexpr static int filegraph_default_version = latest_Prefix_t::VERSION;

            void * main_file_mapping = nullptr;

            // Functions to access files in pages
            bool is_page_in_file(size_t index) const;
            std::tuple<size_t,size_t> get_page_offset(size_t index);
            void add_page_to_file(size_t index, const void * data, size_t data_len);
            size_t file_size_in_pages(size_t file_index);

            // Functions to access files as single-structure
            size_t file_size_in_bytes(size_t file_index) const;
            int get_fd(size_t file_index); 
            std::filesystem::path get_filename(size_t file_index) const; 

            int get_version() const {
                return *(int*)main_file_mapping;
            }

            // This return value will be constantly updated, and is safe to use
            // so long as the bit representation of the prefix structures only
            // increases over time.
            latest_Prefix_t * get_prefix() const;
            size_t prefix_size(int version) const;
            int get_free_file_index() const;

            void convert_prefix_struct(BaseUID uid);

            // This pattern will repeat a lot. Perhaps need a macro to handle it?
            blob_index get_latest_blob_index() const;
            void set_latest_blob_index(blob_index new_value);

            void flush_mmap();

            std::optional<std::string> validate_preload(BaseUID uid);

            FileGraph(std::filesystem::path path_prefix, BaseUID uid, bool fallback_to_fresh=false, bool force_fresh=false);
            ~FileGraph();
        };

        std::filesystem::path filename_with_index(std::filesystem::path prefix, size_t file_index);
        inline bool filegraph_exists(std::filesystem::path prefix) {
            return std::filesystem::exists(filename_with_index(prefix, 0));
        }

        LIBZEF_DLL_EXPORTED void delete_filegraph_files(std::filesystem::path path_prefix);

        // * Basic struct manipulation

            // TODO:
            // TODO:
            // TODO:
            // TODO:
            // TODO: I think using MALLOC would break things now, as many places
            // assume they can find a MMapAllocInfo at the start of a GraphData.
            // This should be disabled.
            // TODO:
            // TODO:
            // TODO:
            // TODO:
        constexpr int MMAP_STYLE_MALLOC = 1;
        constexpr int MMAP_STYLE_ANONYMOUS = 2;
        constexpr int MMAP_STYLE_FILE_BACKED = 3;
        // constexpr int MMAP_STYLE_SHARED = 4;
        // This should never be hit inside of the mmap functions themselves. It
        // is only useful for outside intent.
        constexpr int MMAP_STYLE_AUTO = 999;
        struct MMapAllocInfo {
            void * location;
            // int blob_fd;
            FileGraph * file_graph;
            int style;
            // TODO: Maybe include a bit for whether the pages are fully loaded (up to
            // the current max). This is to optimise checks to avoid slower lazy loading
            // checks when we have already ensured all pages are loaded.
            std::bitset<PAGE_BITMAP_BITS> occupied_pages;
            std::bitset<PAGE_BITMAP_BITS> loaded_pages;
            MMapAllocInfo(MMapAllocInfo & other) = delete;
            MMapAllocInfo() = default;
        };

        inline void * blobs_ptr_from_mmap(const void * ptr) {
            // Need to leave room for the info struct
            void * new_ptr = ceil_ptr((char*)ptr + sizeof(MMapAllocInfo), ZEF_UID_SHIFT);

            assert((size_t)new_ptr % ZEF_UID_SHIFT == 0);
            assert(new_ptr > ptr);
            return new_ptr;
        }
        inline void * blobs_ptr_from_blob(const void * ptr) { return floor_ptr(ptr, ZEF_UID_SHIFT); }

        inline MMapAllocInfo& info_from_blobs(const void * ptr) {
            ptr = (char*)ptr - sizeof(MMapAllocInfo);
            return *(MMapAllocInfo*)ptr;
        }
        inline void * blobs_ptr_from_info(MMapAllocInfo * ptr) { return (char*)ptr + sizeof(MMapAllocInfo); }
        inline MMapAllocInfo& info_from_blob(const void * ptr) { return info_from_blobs(blobs_ptr_from_blob(ptr)); }



        inline bool is_page_alloced(MMapAllocInfo & info, size_t page_ind) {
            if (page_ind < 0 || page_ind >= PAGE_BITMAP_BITS)
                error("Accessing page out of range");
            return info.loaded_pages[page_ind];
        }
        inline size_t ptr_to_page_ind(const void * ptr) {
            return ((size_t)ptr % ZEF_UID_SHIFT) / ZEF_PAGE_SIZE;
        }
        inline bool is_page_alloced(const void * ptr) {
            MMapAllocInfo& info = info_from_blob(ptr);
            size_t page_ind = ptr_to_page_ind(ptr);
            return is_page_alloced(info, page_ind);
        }

        inline bool is_range_alloced(const void * target_ptr, size_t size) {
            MMapAllocInfo& info = info_from_blob(target_ptr);
            // Note: need (size-1) here.
            size_t page_ind_low = ptr_to_page_ind(target_ptr);
            size_t page_ind_high = ptr_to_page_ind((char*)target_ptr+(size-1));
            for (auto page_ind = page_ind_low ; page_ind <= page_ind_high ; page_ind++) {
                if (!is_page_alloced(info, page_ind))
                    return false;
            }
            return true;
        }

        LIBZEF_DLL_EXPORTED void ensure_page(MMapAllocInfo & info, size_t page_ind);
#ifdef ZEFDB_TEST_NO_MMAP_CHECKS
        LIBZEF_DLL_EXPORTED void ensure_page_direct(MMapAllocInfo & info, size_t page_ind);
#endif

#ifdef ZEFDB_TEST_NO_MMAP_CHECKS
        inline void ensure_or_alloc_range(const void * ptr, size_t size) {
        }
        inline void ensure_or_alloc_range_direct(const void * ptr, size_t size) {
#else
        inline void ensure_or_alloc_range(const void * ptr, size_t size) {
#endif
            
            MMapAllocInfo& info = info_from_blob(ptr);
            // TODO: See if this can be optimised by converting to a bitshift. I suspect
            // it would be compile-time optimised anyway though
            size_t page_ind_low = ptr_to_page_ind(ptr);
            size_t page_ind_high = ptr_to_page_ind((char*)ptr+(size-1));
            for (auto page_ind = page_ind_low ; page_ind <= page_ind_high ; page_ind++) {
#ifdef ZEFDB_TEST_NO_MMAP_CHECKS
                ensure_page_direct(info, page_ind);
#else
                ensure_page(info, page_ind);
#endif
            }
        }


        // * Alloc list functions

        LIBZEF_DLL_EXPORTED void* create_mmap(int style=MMAP_STYLE_ANONYMOUS, FileGraph * file_graph=nullptr);
        // inline void* create_mmap(FileGraph * file_graph) {
        //     return create_mmap(MMAP_STYLE_FILE_BACKED, file_graph);
        // }

        LIBZEF_DLL_EXPORTED void flush_mmap(MMapAllocInfo& info);
        LIBZEF_DLL_EXPORTED void flush_mmap(MMapAllocInfo& info, blob_index latest_blob);
        LIBZEF_DLL_EXPORTED void page_out_mmap(MMapAllocInfo& info);
        LIBZEF_DLL_EXPORTED std::tuple<size_t,size_t,size_t,size_t> report_sizes(MMapAllocInfo& info);
        LIBZEF_DLL_EXPORTED void print_malloc_arenas(void);

        LIBZEF_DLL_EXPORTED void destroy_mmap(MMapAllocInfo& info);
        inline void destroy_mmap(const void * ptr) { destroy_mmap(info_from_blobs(ptr)); }

        // * Whole file mappings

        // Note: this first struct (_WholeFileMapping) is the meat using only void*, but it is best
        // wrapped with a template structure that follows it (WholeFileMapping).
        struct Anonymous {};
        struct LIBZEF_DLL_EXPORTED _WholeFileMapping {
            // Anything that could potentially increase the memory usage should
            // grab exclusive access. Everything else grabs shared access.
            std::shared_mutex m;
            std::mutex m_writer;

            std::atomic<void*> ptr = nullptr;
            // If fd == 0 then this is an anonymous mapping, which will possibly require mem copying.
            int fd = 0;

            bool map_initialized = false;

            // Size is the allocated amount and is technically only needed for
            // anonymous maps, but for consistency I'll use it for file maps too.
            size_t size = 0;
            constexpr static size_t uninitialized_size = 10;

            // When enlarging, the head must always be updated. This is usually
            // stored in a persistent area (e.g. the info section of the file-graph index).
            size_t * head = nullptr;

            struct Pointer {
                _WholeFileMapping * parent;
                std::shared_lock<std::shared_mutex> lock;
                std::unique_lock<std::mutex> writer_lock;

                void* ptr();
                Pointer(_WholeFileMapping * parent, bool writer);
                Pointer(Pointer &&) = default;
                Pointer & operator =(Pointer && other) {
                    parent = std::move(other.parent);
                    lock = std::move(other.lock);
                    writer_lock = std::move(other.writer_lock);
                    return *this;
                }

                void* ensure_head(size_t new_head, bool allow_shrink=false);
            };

            Pointer get() {
                // read access only
                return Pointer(this, false);
            }
            Pointer get_writer() {
                // resize allowed
                return Pointer(this, true);
            }

            void initialize();
            _WholeFileMapping(int fd, size_t * head);
            _WholeFileMapping(Anonymous);
            _WholeFileMapping(FileGraph & fg, FileGraph::WholeFile_v1 & info);
            _WholeFileMapping(const _WholeFileMapping & other) = delete;
            ~_WholeFileMapping();
        };

        template <class T>
        struct LIBZEF_DLL_EXPORTED WholeFileMapping {
            using map_t = T;
            _WholeFileMapping map;

            struct Pointer {
                _WholeFileMapping::Pointer p;
                T& operator *() {
                    return *(T*)p.ptr();
                }
                T* operator->() {
                    return (T*)p.ptr();
                }

                T& ensure_head(size_t new_head, bool allow_shrink=false) {
                    return *(T*)p.ensure_head(new_head, allow_shrink);
                }
                constexpr auto ensure_func(bool allow_shrink=false) {
                    return std::bind(&WholeFileMapping::Pointer::ensure_head, this, std::placeholders::_1, allow_shrink);
                };

                Pointer(_WholeFileMapping::Pointer && p)
                    : p(std::move(p)) {}

                Pointer(Pointer && p) = default;
                Pointer & operator =(Pointer && p) = default;
            };

            Pointer get() {
                return Pointer(std::move(map.get()));
            }
            Pointer get_writer() {
                return Pointer(std::move(map.get_writer()));
            }
                    
            template<class... ARGS>
            WholeFileMapping(ARGS&&... args) :
            map(std::forward<ARGS>(args)...) {
                auto p = get_writer();
                // new(&*p) T(uninitialized, p.ensure_func());
                p->_construct(!map.map_initialized, p.ensure_func());
                map.map_initialized = true;
            }
        };

    }
}