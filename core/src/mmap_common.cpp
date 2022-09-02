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

#include <stdio.h>

#include "zwitch.h"

namespace zefDB {
    namespace MMap {

        // * Alloc list functions

        // Keeps track of all mmap info structures
        std::vector<MMapAllocInfo*> alloc_list;

        //////////////////////////////
        // * FileGraph

        void delete_filegraph_files(std::filesystem::path path_prefix) {
            for(auto const& dir_entry : std::filesystem::directory_iterator{path_prefix.parent_path()}) {
                std::string dir_str = dir_entry.path().filename().string();
                std::string prefix_str = path_prefix.filename().string() + "_";
                if(starts_with(dir_str, prefix_str)) {
                    std::filesystem::remove(dir_entry.path());
                }
            }
        }

        FileGraph::FileGraph(std::filesystem::path path_prefix, BaseUID uid, bool fallback_to_fresh, bool force_fresh)
            : path_prefix(path_prefix) {
            auto path = get_filename(0);
            if(std::filesystem::exists(path)) {
                if(force_fresh) {
                    if(zwitch.developer_output())
                        std::cerr << "Found existing filegraph but deleting it to force fresh graph." << std::endl;
                } else {
                    bool locked = false;
                    try {
                        if(zwitch.developer_output())
                            std::cerr << "Found file to load graph from." << std::endl;
                        // First load bare minimum to determine version
                        int fd = get_fd(0);
                        // At this time, we have no option to open this file read-only, so we must obtain a lock on the file.
                        locked = OSLockFile(fd, true, false);
                        if(!locked)
                            throw FileAlreadyLocked(path);

                        // As early as possible, set the file size in pages.
                        size_t file_size = fd_size(fd);
                        if(file_size % ZEF_PAGE_SIZE)
                            throw std::runtime_error("File graph is not a multiple of the zef page size");
                        // file_size_in_pages = file_size / ZEF_PAGE_SIZE;
                        if(file_size_in_pages(0) < 1)
                            throw std::runtime_error("Filegraph is badly empty/nearly-empty");

                        // Setup the initial mapping to get the version
                        main_file_mapping = OSMMap(fd, 0, ZEF_PAGE_SIZE, NULL);

                        // With the version, create the mapping for the full prefix.
                        size_t map_size = prefix_size(get_version());
                        // TODO: Check file is at least this long.
                        main_file_mapping = OSMMap(fd, 0, map_size, NULL);
                        if(zwitch.developer_output())
                            std::cerr << "Existing file had latest blob index of: " << get_latest_blob_index() << std::endl;

                        auto reason = validate_preload(uid);
                        if(reason)
                            throw std::runtime_error("Filegraph did not pass preload validation checks: " + *reason);

                        return;
                    } catch(const FileAlreadyLocked & e) {
                        if(locked)
                            OSUnlockFile(fds[0]);
                        if(fds.size() > 0) {
                            close(fds[0]);
                            fds.clear();
                        }
                        throw;
                    } catch(const FileGraphWrongVersion & e) {
                        std::cerr << e.what() << std::endl;
                        std::cerr << "Going to recover from upstream." << std::endl;
                        if(locked)
                            OSUnlockFile(fds[0]);
                        if(fds.size() > 0) {
                            close(fds[0]);
                            fds.clear();
                        }
                    } catch(const std::exception & e) {
                        std::cerr << "Problem loading file graph '" << path << "': " << e.what() << std::endl;
                        if(!fallback_to_fresh)
                            throw;

                        std::cerr << "Going to delete graph and fallback to fresh filegraph" << std::endl;
                        if(locked)
                            OSUnlockFile(fds[0]);
                        if(fds.size() > 0) {
                            close(fds[0]);
                            fds.clear();
                        }
                    }
                }
            } else {
                developer_output("Did not find existing file.");
            }

            // We get here if the file doesn't exist, or we fallback from a corrupted file.
            // Remove all files which match the path prefix
            delete_filegraph_files(path_prefix);

            // Create the file
            int version = filegraph_default_version;
            size_t base_size = prefix_size(version);
            // We are using O_TRUNC here in the event that this is a fallback.
            int locked = false;
            int fd = get_fd(0);
            if(fd == -1)
                error_p("Error opening filegraph.");
            try {
                // At this time, we have no option to open this file read-only, so we must obtain a lock on the file.
                locked = OSLockFile(fd, true, false);
                if(!locked)
                    throw FileAlreadyLocked(path);

                // Should not need to ftruncate on windows as CreateFileMapping will take care of it.
#ifndef ZEF_WIN32
                ftruncate(fd, base_size);
#endif

                // Map in the space for the prefix
                main_file_mapping = OSMMap(fd, 0, base_size, NULL);

                // Finally setup the prefix structure in the file.
                // if (version == 1)
                //     new(main_file_mapping) Prefix_v1{};
                // else if (version == 2)
                //     new(main_file_mapping) Prefix_v2{BaseUID::from_hex(uid)};
                if (version == Prefix_v3::VERSION)
                    new(main_file_mapping) Prefix_v3{uid};
                else if (version == Prefix_v5::VERSION)
                    new(main_file_mapping) Prefix_v5{uid};
                else
                    throw std::runtime_error("Can't handle this version");

            } catch(const std::exception & e) {
                std::cerr << "Problem setting up new file graph '" << path << "': " << e.what() << std::endl;
                if(locked)
                    OSUnlockFile(fds[0]);
                if(fds.size() > 0) {
                    close(fds[0]);
                    fds.clear();
                }
                throw;
            }
        }

        size_t FileGraph::prefix_size(int version) const {
            size_t temp;
            // if (version == 1) {
            //     size_t temp = sizeof(Prefix_v1);
            if (version == Prefix_v3::VERSION)
                temp = sizeof(Prefix_v3);
            else if (version == Prefix_v5::VERSION)
                temp = sizeof(Prefix_v5);
            else
                throw FileGraphWrongVersion(path_prefix, version, "Don't know prefix_size.");
            size_t num_pages = temp / ZEF_PAGE_SIZE + 1;
            // std::cerr << "Prefix num pages: " << num_pages << std::endl;
            // std::cerr << "Prefix byte size: " << num_pages*ZEF_PAGE_SIZE << std::endl;
            return num_pages * ZEF_PAGE_SIZE;
        }

        FileGraph::latest_Prefix_t * FileGraph::get_prefix() const {
            if(get_version() > filegraph_default_version)
                throw FileGraphWrongVersion(path_prefix, get_version(), "Too new");
            if(get_version() == latest_Prefix_t::VERSION)
                return (latest_Prefix_t*)main_file_mapping;
            // Modify the filegraph to update to the latest version
            if(get_version() == Prefix_v3::VERSION) {
                // This should not cause a resize of the prefix space.
                // Just initialise the extra field (av_hash_lookup)
                std::cerr << "Updating FileGraph prefix version " << Prefix_v3::VERSION << " to " << latest_Prefix_t::VERSION;
                auto recast = (latest_Prefix_t*)main_file_mapping;
                recast->version = latest_Prefix_t::VERSION;
                new(&recast->av_hash_lookup) WholeFile_v1;
                return recast;
            }
            // This will be used for incompatibility with bit representation.
            throw FileGraphWrongVersion(path_prefix, get_version());
        }

        // This pattern will repeat a lot. Perhaps need a macro to handle it?
        blob_index FileGraph::get_latest_blob_index() const {
            if(get_version() >= Prefix_v3::VERSION)
                return get_prefix()->last_update;
            else
                throw FileGraphWrongVersion(path_prefix, get_version(), "No latest_blob_index.");
        }
        void FileGraph::set_latest_blob_index(blob_index new_value) {
            if(get_version() >= Prefix_v3::VERSION) {
                auto prefix = get_prefix();
                if(prefix->last_update > new_value)
                    throw std::runtime_error("The last update blob index should never decrease!");
                prefix->last_update = new_value;
            } else
                throw FileGraphWrongVersion(path_prefix, get_version(), "No set_latest_blob_index.");
        }

        bool FileGraph::is_page_in_file(size_t index) const {
            if (get_version() >= Prefix_v3::VERSION) {
                auto * prefix = get_prefix();
                return prefix->page_info[index].offset != invalid_page;
            } else {
                throw FileGraphWrongVersion(path_prefix, get_version(), "No is_page_in_file.");
            }
        }

        std::tuple<size_t,size_t> FileGraph::get_page_offset(size_t index) {
            // Note: the page index starts from 1 so there is an empty page at
            // the beginning for the prefix information.
            if (get_version() >= Prefix_v3::VERSION) {
                Prefix_v3 & prefix = *(Prefix_v3*)main_file_mapping;
                size_t offset = prefix.page_info[index].offset;
                if (offset == invalid_page) {
                    // Create new space for this
                    // Need to choose a file_index - for now will hardcode index 0
                    size_t file_index = 0;
                    prefix.page_info[index].file_index = file_index;
                    size_t new_offset = file_size_in_pages(file_index);
                    // Shouldn't need to ftruncate on windows
#ifndef ZEF_WIN32
                    ftruncate(get_fd(file_index), ZEF_PAGE_SIZE*(new_offset+1));
#endif

                    prefix.page_info[index].offset = new_offset;

                    return std::make_tuple(new_offset*ZEF_PAGE_SIZE, file_index);
                } else {
                    size_t file_index = prefix.page_info[index].file_index;
                    if (file_index != 0)
                        throw std::runtime_error("Can't handle non-0 file indices yet.");
                    return std::make_tuple(offset*ZEF_PAGE_SIZE, file_index);
                }
            } else {
                throw FileGraphWrongVersion(path_prefix, get_version());
            }
        }

        size_t FileGraph::file_size_in_pages(size_t file_index) {
            int fd = get_fd(file_index);
            return fd_size(fd) / ZEF_PAGE_SIZE;
        }

        int FileGraph::get_fd(size_t file_index) {
            if(fds.size() <= file_index) {
                fds.resize(file_index+1, -1);
            }
            if(fds[file_index] == -1) {
                int fd;
                // fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
                auto path = get_filename(file_index);
                fd = open(path.string().c_str(), O_RDWR | O_CREAT, 0600);
                if(fd == -1) {
                    perror("Opening fd");
                    throw std::runtime_error("Unable to open file: " + path.string());
                }
                fds[file_index] = fd;
            }
            return fds[file_index];
        }

        int FileGraph::get_free_file_index() const {
            auto * p = get_prefix();
            return p->next_free_file_index++;
        }

        std::filesystem::path FileGraph::get_filename(size_t file_index) const {
            return filename_with_index(path_prefix, file_index);
        }

        std::filesystem::path filename_with_index(std::filesystem::path prefix, size_t file_index) {
            std::string num = std::to_string(file_index);
            num = std::string(5 - num.length(), '0') + num;
              
            auto copy = prefix;
            copy += "_" + num + ".zefgraph";
            return copy;
        }

        std::optional<std::string> FileGraph::validate_preload(BaseUID uid) {
            if (get_latest_blob_index() <= constants::ROOT_NODE_blob_index) {
                return std::string("Latest blob is not late enough in file graph (") + get_latest_blob_index() + ").";
            }
            if (get_version() >= 2) {
                if(get_prefix()->uid != uid)
                    return std::string("UIDs don't match");
            }
            return {};
        }


        //////////////////////////////////////////
        // * Whole file mapping
        _WholeFileMapping::_WholeFileMapping(int fd, size_t * head) : fd(fd), head(head) {
            map_initialized = (*head > 0);
            initialize();
        }

        _WholeFileMapping::_WholeFileMapping(Anonymous) : fd(0), head(nullptr) {
            map_initialized = false;
            initialize();
        }

        _WholeFileMapping::_WholeFileMapping(FileGraph & fg, FileGraph::WholeFile_v1 & info) {
            map_initialized = (info.file_index != FileGraph::invalid_page);
            if(!map_initialized)
                info.file_index = fg.get_free_file_index();
            fd = fg.get_fd(info.file_index);
            head = &info.head;

            initialize();
        }

        void _WholeFileMapping::initialize() {
            if(fd == 0) {
                head = new size_t;
                *head = 0;
                size = uninitialized_size;
            } else {
                size = fd_size(fd);
                if(size == 0) {
                    size = uninitialized_size;
                    // Shouldn't need to ftruncate on windows
#ifndef ZEF_WIN32
                    ftruncate(fd, size);
#endif
                }
            }

            ptr = OSMMap(fd, 0, size, NULL);
        }

        void* _WholeFileMapping::Pointer::ptr() {
            return parent->ptr.load();
        }
        _WholeFileMapping::Pointer::Pointer(_WholeFileMapping * parent, bool writer)
            : parent(parent),
              lock(parent->m, std::defer_lock),
              writer_lock(parent->m_writer, std::defer_lock) {
            // Note the order here - writer lock first before reader lock to avoid deadlocks.
            if(writer)
                writer_lock.lock();
            lock.lock();
        }

        bool port_mremap_inplace(void * old_ptr, size_t old_size, size_t new_size, int fd) {
            // See if we can mimic mremap using mmap without MAP_FIXED.
#ifdef __linux__
            void * new_ptr = mremap(old_ptr, old_size, new_size, 0);
            // If successful, just return
            if(new_ptr != MAP_FAILED) {
                return true;
            }
            if(errno != ENOMEM)
                throw std::runtime_error("Some other kind of error other than can't enlarge mapping.");
            return false;
#elif defined(ZEF_WIN32)
            // TODO: this might be possible with AWE but will have to investigate that more thoroughly before launching into an implementation.
            return false;
#else
            int flags;
            if(fd == 0)
                flags = MAP_PRIVATE | MAP_ANONYMOUS;
            else
                flags = MAP_SHARED;
            void * new_ptr = mmap(old_ptr, new_size, PROT_READ | PROT_WRITE, flags, fd, 0);
            if(old_ptr == new_ptr) {
                std::cerr << "Successfully mremaped inplace with our func" << std::endl;
                return true;
            }

            if(new_ptr == MAP_FAILED) {
                perror("a");
                throw std::runtime_error("Unexpected error in port_mremap_inplace");
            }

            // We didn't get the same location, so undo and let the caller hanlde it.
            munmap(new_ptr, new_size);
            return false;
#endif
        }

        void* port_mremap_move(void * old_ptr, size_t old_size, size_t new_size, int fd) {
            // See if we can mimic mremap using mmap without MAP_FIXED.
#ifdef __linux__
            void * new_ptr = mremap(old_ptr, old_size, new_size, MREMAP_MAYMOVE);
            if(new_ptr == MAP_FAILED)
                throw std::runtime_error("Unable to remap and enlarge memory.");
            return new_ptr;
#elif defined(ZEF_WIN32)
            void * new_ptr = OSMMap(fd, 0, new_size, nullptr);
            if (fd == 0) {
                memcpy(new_ptr, old_ptr, old_size);
                VirtualFree(old_ptr, 0, MEM_RELEASE);
            } else {
                // TODO:
                throw std::runtime_error("TODO: handle freeing mremaped files");
            }
            return new_ptr;
#else
            int flags;
            if(fd == 0)
                flags = MAP_PRIVATE | MAP_ANONYMOUS;
            else
                flags = MAP_SHARED;
            void * new_ptr = mmap(nullptr, new_size, PROT_READ | PROT_WRITE, flags, fd, 0);
            if(new_ptr == MAP_FAILED) {
                perror("a");
                throw std::runtime_error("Unexpected error in port_mremap_move");
            }
            if(fd == 0)
                memcpy(new_ptr, old_ptr, old_size);
            munmap(old_ptr, old_size);
            return new_ptr;
#endif
        }

        void* _WholeFileMapping::Pointer::ensure_head(size_t new_head, bool allow_shrink) {
            // std::cerr << "Ensure head with " << new_head << std::endl;
            if(!writer_lock)
                throw std::runtime_error("Can't ensure_space without writer_lock");

            if(parent->head != nullptr) {
                // std::cerr  << "New head: " << new_head << " vs " << *parent->head << std::endl;
                if(*parent->head > new_head && !allow_shrink)
                    throw std::runtime_error("A WholeFileMapping is meant to be append-only.");
                *parent->head = new_head;
            }


            if(new_head <= parent->size)
                return parent->ptr;

            size_t old_size = parent->size;
            parent->size = std::max(parent->size * 2, new_head);

            void * old_ptr = parent->ptr;
            // Try and remap without moving
            if(parent->fd == 0) {
                // Nothing to do except remap.
            } else {
#ifndef ZEF_WIN32
                ftruncate(parent->fd, parent->size);
#endif
            }
            if(port_mremap_inplace(old_ptr, old_size, parent->size, parent->fd)) {
                // If successful, just return
                return parent->ptr;
            }

            // Otherwise, we need to kick everyone out and do this more thoroughly
            lock.unlock();
            {
                std::unique_lock ulock(parent->m);
                parent->ptr = port_mremap_move(old_ptr, old_size, parent->size, parent->fd);
            }
            lock.lock();
            return parent->ptr;
        }
    }
}