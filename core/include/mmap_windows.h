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
#include <vector>
#include <stdexcept>
#include <bitset>
#include <limits>
#include <array>

#include "include_fs.h"

#include <sys/stat.h>
//#include <sys/mman.h>
//#include <sys/file.h>
#include <fcntl.h>
//#include <unistd.h>
#include <assert.h>
#include <math.h>
#include <string.h>
#include <mutex>
#include <shared_mutex>
#include <atomic>

#include "constants.h"
#include "zefDB_utils.h"
#include "uids.h"

#include <windows.h>
#include <sysinfoapi.h>
#include <io.h>

// For print_backtrace equivalent
//#include <stdio.h>
//#include <execinfo.h>
//#include <stdlib.h>
//#include <unistd.h>

// The layout of memory:
//
// The below description has changed a bit, in that there is no more BLOBS
// section. However, the rest remains the same.
//
//  Note: the entire range is initially reserved (an mmap with PROT_NONE)
//  When "mmap-ed" is written below, it implies PROT_READ and maybe PROT_WRITE
// ┌──────────────────────────────────────┐
// │                                      │  Alignments
// │               UNUSED                 │  ──────────
// │                                      │
// ├──────────────────────────────────────┤◄─System page
// │          mmap-ed but unused          │
// │                                      │
// ├──────────────────────────────────────┤
// │         Info structure (mmaped)      │
// ├──────────────────────────────────────┤◄─ZEF_UID_SHIFT
// │                   Blob1              │
// │   BLOBS (mmaped)  Blob2              │
// │                   Blob3              │
// │xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx│
// │                                      │
// │           Unused blob range          │
// │                                      │
// ├──────────────────────────────────────┤◄─ZEF_UID_SHIFT
// │                                      │
// │                UNUSED                │
// │                                      │
// │xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx│
//
// Note: the info structure appears directly before the BLOBS range.
// Note: the UNUSED ranges at the beginning and end are variable depending on what has been returned by the initial mmap reservation.
// Note: the size of the blobs/uids ranges are each ZEF_UID_SHIFT.




namespace zefDB {
    namespace MMap {

        constexpr size_t WIN_FILE_LOCK_BYTES = 1024;
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

        inline void *WindowsMMap(int fd, size_t offset, size_t size, void * location) {
            if(fd == 0) {
                // Anonymous map
                if(location != NULL)
                    throw std::runtime_error("Can't map anonymous with location given.");
                void * ret = VirtualAlloc(NULL, size, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
                if(ret == NULL)
                    throw std::runtime_error("Unable to map anonymous: " + WindowsErrorMsg());
                return ret;
            } else {
                // Going to hope that closing a file mapping handle leaves the mapped views alive.
                // TODO: lookup the possibility of inter-process competition for creating file mappings - as they shared across processes?
                // TODO: Handle the high/low parts
                HANDLE h = (HANDLE)_get_osfhandle(fd);
                HANDLE map_h = CreateFileMappingA(h, NULL, PAGE_READWRITE, 0, offset+size, NULL);
                if (map_h == NULL || GetLastError() == ERROR_ALREADY_EXISTS)
                    throw std::runtime_error("Unable to create file mapping: " + WindowsErrorMsg());
                void* ret = MapViewOfFileEx(map_h, FILE_MAP_ALL_ACCESS, 0, offset, size, location);
                if(ret == NULL)
                    throw std::runtime_error("Unable to map view of file: " + WindowsErrorMsg());
                if (!CloseHandle(map_h))
                    throw std::runtime_error("Unable to close file mapping: " + WindowsErrorMsg());
                return ret;
            }
        }

        inline bool WindowsLockFile(int fd) {
            HANDLE h = (HANDLE)_get_osfhandle(fd);
            if (!LockFile(h, 0, 0, WIN_FILE_LOCK_BYTES, 0))
                return false;
            return true;
        }
        inline void WindowsUnlockFile(int fd, bool flush=false) {
            HANDLE h = (HANDLE)_get_osfhandle(fd);
            if(flush) {
                if (!FlushFileBuffers(h))
                    throw std::runtime_error("Problem flushing file to disk: " + WindowsErrorMsg());
            }
            if (!UnlockFile(h, 0, 0, WIN_FILE_LOCK_BYTES, 0))
                    throw std::runtime_error("Problem unlocking file: " + WindowsErrorMsg());
        }

        constexpr unsigned int PAGE_BITMAP_BITS = ZEF_UID_SHIFT / ZEF_PAGE_SIZE;

        // * Utils

        inline void * floor_ptr(void * ptr, size_t mod) { return (char*)ptr - (size_t)ptr % mod; }
        // Check if the below works and is any faster
        // void * floor_ptr(void * ptr, size_t mod) { return ptr & (my_mod - 1); }
        inline void * ceil_ptr(void * ptr, size_t mod) {
            void * temp = floor_ptr(ptr, mod);
            if(temp != ptr)
                temp = (char*)temp + mod;
            return temp;
        }

        inline size_t getpagesize() {
            SYSTEM_INFO sys_info;
            GetSystemInfo(&sys_info);
            return sys_info.dwPageSize;
        }
        inline void * align_to_system_pagesize(void * ptr) { return floor_ptr(ptr, getpagesize()); }

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

        inline bool is_fd_writable(int fd) {
            BY_HANDLE_FILE_INFORMATION info;
            HANDLE handle = (HANDLE)_get_osfhandle(fd);
            GetFileInformationByHandle(handle, &info);
            // The options O_WRONLY or O_RDWR do not share bits, so this is technically
            // not correct, however in our case we never have a O_WRONLY scenario.
            return (info.dwFileAttributes & O_RDWR);
        }

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
            static constexpr size_t invalid_page = (std::numeric_limits<size_t>::max)();
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
                // int version = 3;
                int version = 4;
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
            using latest_Prefix_t = Prefix_v3;

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

            void flush_mmap() {
                // Note: FlushViewOfFile is asynchronous.
                FlushViewOfFile(main_file_mapping, prefix_size(get_version()));
            }

            std::optional<std::string> validate_preload(BaseUID uid);

            FileGraph(std::filesystem::path path_prefix, BaseUID uid, bool fallback_to_fresh=false, bool force_fresh=false);
            ~FileGraph() {
                if(main_file_mapping != nullptr) {
                    // Note: this is not doing a sync flush, but we will be waiting on FlushFileBuffers below which will make this effectively synchronous.
                    FlushViewOfFile(main_file_mapping, prefix_size(get_version()));
                    if(!UnmapViewOfFile(main_file_mapping)) {
                        std::cerr << "Problem unmapping main_file_mapping. Ignoring." << std::endl;
                        std::cerr << WindowsErrorMsg() << std::endl;
                    };
                }
                for(auto & fd : fds) {
                    if(fd != -1) {
                        try {
                            WindowsUnlockFile(fd, true);
                        } catch(const std::exception & exc) {
                            std::cerr << "Problem unlocking file: " + std::string(exc.what()) << std::endl;
                            std::cerr << "Ignoring because in ~FileGraph" << std::endl;
                        }
                        close(fd);
                    }
                }
            }
        };

        std::filesystem::path filename_with_index(std::filesystem::path prefix, size_t file_index);
        inline bool filegraph_exists(std::filesystem::path prefix) {
            return std::filesystem::exists(filename_with_index(prefix, 0));
        }

        LIBZEF_DLL_EXPORTED void delete_filegraph_files(std::filesystem::path path_prefix);

        constexpr int filegraph_v3_version_num = 4;
        // constexpr int filegraph_default_version = 3;
        constexpr int filegraph_default_version = 4;


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

        inline void * blobs_ptr_from_mmap(void * ptr) {
            // Need to leave room for the info struct
            void * new_ptr = ceil_ptr((char*)ptr + sizeof(MMapAllocInfo), ZEF_UID_SHIFT);

            assert((size_t)new_ptr % ZEF_UID_SHIFT == 0);
            assert(new_ptr > ptr);
            return new_ptr;
        }
        inline void * blobs_ptr_from_blob(void * ptr) { return floor_ptr(ptr, ZEF_UID_SHIFT); }

        inline MMapAllocInfo& info_from_blobs(void * ptr) {
            ptr = (char*)ptr - sizeof(MMapAllocInfo);
            return *(MMapAllocInfo*)ptr;
        }
        inline void * blobs_ptr_from_info(MMapAllocInfo * ptr) { return (char*)ptr + sizeof(MMapAllocInfo); }
        inline MMapAllocInfo& info_from_blob(void * ptr) { return info_from_blobs(blobs_ptr_from_blob(ptr)); }



        inline bool is_page_alloced(MMapAllocInfo & info, size_t page_ind) {
            if (page_ind < 0 || page_ind >= PAGE_BITMAP_BITS)
                error("Accessing page out of range");
            return info.loaded_pages[page_ind];
        }
        inline size_t ptr_to_page_ind(void * ptr) {
            return ((size_t)ptr % ZEF_UID_SHIFT) / ZEF_PAGE_SIZE;
        }
        inline bool is_page_alloced(void * ptr) {
            MMapAllocInfo& info = info_from_blob(ptr);
            size_t page_ind = ptr_to_page_ind(ptr);
            return is_page_alloced(info, page_ind);
        }

        inline bool is_range_alloced(void * target_ptr, size_t size) {
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
        inline void ensure_or_alloc_range(void * ptr, size_t size) {
        }
        inline void ensure_or_alloc_range_direct(void * ptr, size_t size) {
#else
        inline void ensure_or_alloc_range(void * ptr, size_t size) {
#endif
            // // This is a very dirty hack to ensure we never alloc something too small
            // if(size < blobs_ns::max_basic_blob_size) {
            //     std::cerr << "Warning! Tried to ensure a mem range that is smaller than max_basic_blob_size!" << std::endl;
            //     // Can't use print_backtrace here due to cyclic deps.
            //     void *array[50];
            //     size_t size;
            //     size = backtrace(array, 50);
            //     backtrace_symbols_fd(array, size, 1);
            // }
            
            MMapAllocInfo& info = info_from_blob(ptr);
            // TODO: See if this can be optimised by converting to a bitshift. I suspect
            // it would be compile-time optimised anyway though
            size_t page_ind_low = ptr_to_page_ind(ptr);
            size_t page_ind_high = ptr_to_page_ind((char*)ptr+(size-1));
            for (auto page_ind = page_ind_low ; page_ind <= page_ind_high ; page_ind++)
#ifdef ZEFDB_TEST_NO_MMAP_CHECKS
                ensure_page_direct(info, page_ind);
#else
                ensure_page(info, page_ind);
#endif
        }



        // * Alloc list functions

        LIBZEF_DLL_EXPORTED void* create_mmap(int style=MMAP_STYLE_ANONYMOUS, FileGraph * file_graph=nullptr);
        // inline void* create_mmap(FileGraph * file_graph) {
        //     return create_mmap(MMAP_STYLE_FILE_BACKED, file_graph);
        // }

        LIBZEF_DLL_EXPORTED void flush_mmap(MMapAllocInfo& info);
        LIBZEF_DLL_EXPORTED void flush_mmap(MMapAllocInfo& info, blob_index latest_blob);
        LIBZEF_DLL_EXPORTED void page_out_mmap(MMapAllocInfo& info);
        //LIBZEF_DLL_EXPORTED std::tuple<size_t,size_t,size_t,size_t> report_sizes(MMapAllocInfo& info);
        LIBZEF_DLL_EXPORTED void print_malloc_arenas(void);

        LIBZEF_DLL_EXPORTED void destroy_mmap(MMapAllocInfo& info);
        inline void destroy_mmap(void * ptr) { destroy_mmap(info_from_blobs(ptr)); }

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
            ~_WholeFileMapping() {
                if(fd != 0 && size > 0) {
                    FlushViewOfFile(ptr, size);
                    if(!UnmapViewOfFile(ptr)) {
                        std::cerr << WindowsErrorMsg() << std::endl;
                        std::cerr << "Problem unmapping WholeFileMapping." << std::endl;
                    };
                }
                if(fd == 0 && head != nullptr)
                    delete head;
            }
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