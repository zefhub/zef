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

//#include "mmap.h"
//#include <sys/file.h>
#include <stdio.h>

// #include <malloc.h>

#include "zwitch.h"

namespace zefDB {
    namespace MMap {

        // ** Page based manipulation

#ifdef ZEFDB_TEST_NO_MMAP_CHECKS
        void ensure_page(MMapAllocInfo & info, size_t page_ind) {
        }
        void ensure_page_direct(MMapAllocInfo & info, size_t page_ind) {
#else
        void ensure_page(MMapAllocInfo & info, size_t page_ind) {
#endif
            if (is_page_alloced(info, page_ind))
                return;
            // std::cerr << "Allocing new page" << std::endl;

            if(info.style == MMAP_STYLE_MALLOC)
                error("Can't extend malloc pages");

            void * blobs_ptr = blobs_ptr_from_info(&info);

            void * blob_start = (char*)blobs_ptr + page_ind*ZEF_PAGE_SIZE;

            if (info.style == MMAP_STYLE_ANONYMOUS) {
                if(NULL == VirtualAlloc(blob_start, ZEF_PAGE_SIZE, MEM_COMMIT, PAGE_READWRITE))
                    throw std::runtime_error("Could not alloc new anonymous blobs page: " + WindowsErrorMsg());
                // TODO: Disable write on read-only graph
                memset(blob_start, 0, ZEF_PAGE_SIZE);
            }
            else if (info.style == MMAP_STYLE_FILE_BACKED) {
                FileGraph & fg = *info.file_graph;
                auto [offset, file_index] = fg.get_page_offset(page_ind);
                int fd = fg.get_fd(file_index);
                // std::cerr << "*** Mapping file offset: " << offset << " to blob offset: " << (intptr_t)blob_start - (intptr_t)blobs_ptr << std::endl;
                WindowsMMap(fd, offset, ZEF_PAGE_SIZE, blob_start);

                // Windows doesn't guarantee new file contents are zeroed out.
                if(!info.occupied_pages[page_ind])
                    memset(blob_start, 0, ZEF_PAGE_SIZE);
            }

            info.occupied_pages[page_ind] = 1;
            info.loaded_pages[page_ind] = 1;
        }

        void unload_page(MMapAllocInfo & info, size_t page_ind) {
            if (!is_page_alloced(info, page_ind))
                return;

            if(info.style == MMAP_STYLE_MALLOC)
                error("Can't unload malloc pages");
            else if (info.style == MMAP_STYLE_ANONYMOUS)
                error("Should never unload anonymous pages");

            void * blobs_ptr = blobs_ptr_from_info(&info);

            void * blob_start = (char*)blobs_ptr + page_ind*ZEF_PAGE_SIZE;

            FileGraph & fg = *info.file_graph;
            // size_t [offset,file_index] = fg.get_page_offset(page_ind);
            if(!UnmapViewOfFile(blob_start))
                throw std::runtime_error("Unable to unmap view of file: " + WindowsErrorMsg());

            info.loaded_pages[page_ind] = 0;
        }

        // ** Whole-file based manipulation

        // * Alloc list functions

        // Keeps track of all mmap info structures
        std::vector<MMapAllocInfo*> alloc_list;

        void* create_mmap(int style, FileGraph * file_graph) {
            void * location;
            if (style == MMAP_STYLE_MALLOC) {
                location = malloc(MAX_MMAP_SIZE);
                memset(location, 0, MAX_MMAP_SIZE);
            } else {
                // This should create a mmap which doesn't take any physical memory.
                // This reserves the space, so that we can fill it with other maps later on.
                location = VirtualAlloc(NULL, MAX_MMAP_SIZE, MEM_RESERVE, PAGE_READWRITE);
                if(location == NULL)
                    throw std::runtime_error("Could not map memory: " + WindowsErrorMsg());
            }

            void * blob_ptr = blobs_ptr_from_mmap(location);

            // Need to allow the info struct to have read/write access
            // TODO: We could also allow other information to go here to keep it close together.
            void * info_ptr = &info_from_blobs(blob_ptr);
            assert(info_ptr >= location);
            if (style != MMAP_STYLE_MALLOC) {
                // if(MAP_FAILED == mmap(info_ptr, sizeof(MMapAllocInfo), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0)) {
                void * page_aligned = align_to_system_pagesize(info_ptr);
                if(NULL == VirtualAlloc(page_aligned, ((char*)blob_ptr - (char*)page_aligned), MEM_COMMIT, PAGE_READWRITE))
                    throw std::runtime_error("Could not allocate info struct: " + WindowsErrorMsg());
            }

            MMapAllocInfo * info = new (info_ptr) MMapAllocInfo();
            info->location = location;
            // info->blob_fd = blob_fd;
            info->style = style;
            info->occupied_pages = 0;
            // std::cerr << "Mmap info structure sizeof is " << sizeof(MMapAllocInfo) << " which should be " << (8 + 8 + 4 + PAGE_BITMAP_BITS / 8 * 2) << std::endl;

            if (style == MMAP_STYLE_MALLOC) {
                // All pages in the range start out as alloced.
                info->occupied_pages.set();
            } else if (style == MMAP_STYLE_FILE_BACKED) {
                // This gives ownership of the file graph to the mmap info
                info->file_graph = file_graph;
                for(int ind = 0; ind < PAGE_BITMAP_BITS ; ind++) {
                    if(info->file_graph->is_page_in_file(ind)) {
                        // std::cerr << "Found existing page (" << ind << ") in file so mapping it in." << std::endl;
                        info->occupied_pages[ind] = 1;
                    }
                }
            } else if (style == MMAP_STYLE_ANONYMOUS) {
            } else {
                throw std::runtime_error("Unknown style for create_mmap to handle.");
            }

            alloc_list.push_back(info);
            return blob_ptr;
        }

        void flush_mmap(MMapAllocInfo& info) {
            // TODO: Figure this out, and put some comments when I have!
            //msync(info.location, MAX_MMAP_SIZE, MS_SYNC);
            if(info.style == MMap::MMAP_STYLE_FILE_BACKED)
                info.file_graph->flush_mmap();
        }
        void flush_mmap(MMapAllocInfo& info, blob_index latest_blob) {
            // TODO: Figure this out, and put some comments when I have!
            //msync(info.location, MAX_MMAP_SIZE, MS_SYNC);
            if(info.style == MMap::MMAP_STYLE_FILE_BACKED) {
                info.file_graph->set_latest_blob_index(latest_blob);
                info.file_graph->flush_mmap();
            }
        }
        void page_out_mmap(MMapAllocInfo& info) {
            flush_mmap(info);
            if (info.style == MMAP_STYLE_FILE_BACKED) {
                throw std::runtime_error("Not on windows");
                //FileGraph & fg = *info.file_graph;
                //char * blobs_ptr = (char*)blobs_ptr_from_info(&info);
                //for(int page_ind = 0 ; page_ind < PAGE_BITMAP_BITS ; page_ind++) {
                //    if(-1 == madvise(blobs_ptr + page_ind*ZEF_PAGE_SIZE, ZEF_PAGE_SIZE, MADV_DONTNEED))
                //        error_p("Couldn't madvise.");

                //    // madvise(blobs_ptr + page_ind*ZEF_PAGE_SIZE, ZEF_PAGE_SIZE, MADV_PAGEOUT);

                //    // unload_page(info, page_ind);
                //    // if(MAP_FAILED == mmap(blobs_ptr + page_ind*ZEF_PAGE_SIZE, ZEF_PAGE_SIZE, PROT_NONE, MAP_ANONYMOUS|MAP_PRIVATE, -1, 0))
                //    //     error_p("Couldn't mmap fake back.");
                //}
                // std::cerr << "Tried to advise" << std::endl;
            }
        }

        void destroy_mmap(MMapAllocInfo& info) {
            if(info.style == MMAP_STYLE_MALLOC) {
                free(info.location);
            } else if (info.style == MMAP_STYLE_ANONYMOUS) {
                // munmap can remove multiple mappings, so no need to loop through all alloced pages
                VirtualFree(info.location, 0, MEM_RELEASE);
            } else if (info.style == MMAP_STYLE_FILE_BACKED) {
                // if(close(info.blob_fd) != 0 || close(info.uid_fd) != 0)
                //     error_p("Problem closing fds for mmap");
                flush_mmap(info);
                delete info.file_graph;
                VirtualFree(info.location, 0, MEM_RELEASE);
            }
        }




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
                    bool flock_ret = false;
                    try {
                        if(zwitch.developer_output())
                            std::cerr << "Found file to load graph from." << std::endl;
                        // First load bare minimum to determine version
                        int fd = get_fd(0);
                        HANDLE h = (HANDLE)_get_osfhandle(fd);
                        // At this time, we have no option to open this file read-only, so we must obtain a lock on the file.
                        flock_ret = LockFile(h, 0, 0, WIN_FILE_LOCK_BYTES, 0);
                        if(!flock_ret)
                            throw FileAlreadyLocked(path);

                        // As early as possible, set the file size in pages.
                        size_t file_size = fd_size(fd);
                        if(file_size % ZEF_PAGE_SIZE)
                            throw std::runtime_error("File graph is not a multiple of the zef page size");
                        // file_size_in_pages = file_size / ZEF_PAGE_SIZE;
                        if(file_size_in_pages(0) < 1)
                            throw std::runtime_error("Filegraph is badly empty/nearly-empty");

                        // Setup the initial mapping to get the version
                        main_file_mapping = WindowsMMap(fd, 0, ZEF_PAGE_SIZE, NULL);

                        // With the version, create the mapping for the full prefix.
                        size_t map_size = prefix_size(get_version());
                        // TODO: Check file is at least this long.
                        if(!UnmapViewOfFile(main_file_mapping))
                            throw std::runtime_error("Unable to unmap view of file: " + WindowsErrorMsg());
                        main_file_mapping = WindowsMMap(fd, 0, map_size, NULL);
                        if(zwitch.developer_output())
                            std::cerr << "Existing file had latest blob index of: " << get_latest_blob_index() << std::endl;

                        auto reason = validate_preload(uid);
                        if(reason)
                            throw std::runtime_error("Filegraph did not pass preload validation checks: " + *reason);

                        return;
                    } catch(const FileAlreadyLocked & e) {
                        if(flock_ret)
                            WindowsUnlockFile(fds[0], false);
                        if(fds.size() > 0) {
                            close(fds[0]);
                            fds.clear();
                        }
                        throw;
                    } catch(const FileGraphWrongVersion & e) {
                        std::cerr << e.what() << std::endl;
                        std::cerr << "Going to recover from upstream." << std::endl;
                        if(flock_ret)
                            WindowsUnlockFile(fds[0], false);
                        if(fds.size() > 0) {
                            close(fds[0]);
                            fds.clear();
                        }
                    } catch(const std::exception & e) {
                        std::cerr << "Problem loading file graph '" << path << "': " << e.what() << std::endl;
                        if(!fallback_to_fresh)
                            throw;

                        std::cerr << "Going to delete graph and fallback to fresh filegraph" << std::endl;
                        if(flock_ret)
                            WindowsUnlockFile(fds[0], false);
                        if(fds.size() > 0) {
                            close(fds[0]);
                            fds.clear();
                        }
                    }
                }
            } else {
                if(zwitch.developer_output())
                    std::cerr << "Did not find existing file." << std::endl;
            }

            // We get here if the file doesn't exist, or we fallback from a corrupted file.
            // Remove all files which match the path prefix
            delete_filegraph_files(path_prefix);

            // Create the file
            int version = filegraph_default_version;
            size_t base_size = prefix_size(version);
            // We are using O_TRUNC here in the event that this is a fallback.
            int flock_ret = false;
            int fd = get_fd(0);
            if(fd == -1)
                error_p("Error opening filegraph.");
            try {
                // At this time, we have no option to open this file read-only, so we must obtain a lock on the file.
                flock_ret = WindowsLockFile(fd);
                if(!flock_ret)
                    throw FileAlreadyLocked(path);

                // Should not need to ftruncate on windows as CreateFileMapping will take care of it.
                //ftruncate(fd, base_size);

                // Map in the space for the prefix
                main_file_mapping = WindowsMMap(fd, 0, base_size, NULL);

                // Finally setup the prefix structure in the file.
                // if (version == 1)
                //     new(main_file_mapping) Prefix_v1{};
                // else if (version == 2)
                //     new(main_file_mapping) Prefix_v2{BaseUID::from_hex(uid)};
                if (version == filegraph_v3_version_num)
                    new(main_file_mapping) Prefix_v3{uid};
                else
                    throw std::runtime_error("Can't handle this version");

            } catch(const std::exception & e) {
                std::cerr << "Problem setting up new file graph '" << path << "': " << e.what() << std::endl;
                if(flock_ret)
                    WindowsUnlockFile(fds[0], false);
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
            if (version == filegraph_v3_version_num)
                temp = sizeof(Prefix_v3);
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
            if(get_version() >= filegraph_v3_version_num)
                return (latest_Prefix_t*)main_file_mapping;
            else
                // This will be used for incompatibility with bit representation.
                throw FileGraphWrongVersion(path_prefix, get_version());
        }

        // This pattern will repeat a lot. Perhaps need a macro to handle it?
        blob_index FileGraph::get_latest_blob_index() const {
            if(get_version() >= filegraph_v3_version_num)
                return get_prefix()->last_update;
            else
                throw FileGraphWrongVersion(path_prefix, get_version(), "No latest_blob_index.");
        }
        void FileGraph::set_latest_blob_index(blob_index new_value) {
            if(get_version() >= filegraph_v3_version_num) {
                auto prefix = get_prefix();
                if(prefix->last_update > new_value)
                    throw std::runtime_error("The last update blob index should never decrease!");
                prefix->last_update = new_value;
            } else
                throw FileGraphWrongVersion(path_prefix, get_version(), "No set_latest_blob_index.");
        }

        bool FileGraph::is_page_in_file(size_t index) const {
            if (get_version() >= filegraph_v3_version_num) {
                auto * prefix = get_prefix();
                return prefix->page_info[index].offset != invalid_page;
            } else {
                throw FileGraphWrongVersion(path_prefix, get_version(), "No is_page_in_file.");
            }
        }

        std::tuple<size_t,size_t> FileGraph::get_page_offset(size_t index) {
            // Note: the page index starts from 1 so there is an empty page at
            // the beginning for the prefix information.
            if (get_version() == filegraph_v3_version_num) {
                Prefix_v3 & prefix = *(Prefix_v3*)main_file_mapping;
                size_t offset = prefix.page_info[index].offset;
                if (offset == invalid_page) {
                    // Create new space for this
                    // Need to choose a file_index - for now will hardcode index 0
                    size_t file_index = 0;
                    prefix.page_info[index].file_index = file_index;
                    size_t new_offset = file_size_in_pages(file_index) + 1;
                    // Shouldn't need to ftruncate on windows
                    //ftruncate(get_fd(file_index), ZEF_PAGE_SIZE*(new_offset+1));

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
                auto path = get_filename(file_index).string();
                fd = open(path.c_str(), O_RDWR | O_CREAT, 0600);
                if(fd == -1) {
                    perror("Opening fd");
                    throw std::runtime_error("Unable to open file: " + path);
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
            int flags;
            if(fd == 0) {
                head = new size_t;
                *head = 0;
                size = uninitialized_size;
                //flags = MAP_PRIVATE | MAP_ANONYMOUS;
            } else {
                size = fd_size(fd);
                if(size == 0) {
                    size = uninitialized_size;
                    // Shouldn't need ftruncate for windows
                    //ftruncate(fd, size);
                }
                //flags = MAP_SHARED;
            }

            ptr = WindowsMMap(fd, 0, size, NULL);
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
#if defined(__linux__)
            void * new_ptr = mremap(old_ptr, old_size, new_size, 0);
            // If successful, just return
            if(new_ptr != MAP_FAILED) {
                return true;
            }
            if(errno != ENOMEM)
                throw std::runtime_error("Some other kind of error other than can't enlarge mapping.");
            return false;
#elif defined(_MSC_VER)
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
#if defined(__linux__)
            void * new_ptr = mremap(old_ptr, old_size, new_size, MREMAP_MAYMOVE);
            if(new_ptr == MAP_FAILED)
                throw std::runtime_error("Unable to remap and enlarge memory.");
            return new_ptr;
#elif defined(_MSC_VER)
            void * new_ptr = WindowsMMap(fd, 0, new_size, nullptr);
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
                // Shouldn't need this on windows
                //ftruncate(parent->fd, parent->size);
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
