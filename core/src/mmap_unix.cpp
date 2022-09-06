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

namespace zefDB {
    namespace MMap {
        void maybe_print_rss(void) {
            if(!check_env_bool("ZEFDB_MMAP_ALLOC_DEBUG"))
                return;
            FILE * file = fopen("/proc/self/status","r");
            char * line = nullptr;
            size_t n = 0;
            ssize_t read;
            while((read = getline(&line, &n, file)) != -1) {
                std::string s(line);
                if(s.find("RssFile") != std::string::npos
                   || s.find("RssAnon") != std::string::npos
                   || s.find("VmRSS") != std::string::npos
                   || s.find("VmSwap") != std::string::npos
                   )
                    std::cerr << s;
            }
            free(line);
            fclose(file);
        }

        size_t my_mincore(const void * start, size_t len, bool return_alloc=false) {
            size_t total = 0;
            
            uintptr_t ptr = (uintptr_t)start;
            FILE * file = fopen("/proc/self/smaps","r");
            char * line = nullptr;
            size_t n = 0;
            ssize_t read;
            // while((read = getline(&line, &n, file)) != -1) {
            read = getline(&line, &n, file);
            while(read != -1) {
                // This should be the start of a map
                std::string s(line, read);
                int dash = s.find('-');
                int space = s.find(' ');
                if(dash < 4 || dash == space || space < 10 || dash > 34 || space > 34) {
                    std::cerr << line << std::endl;
                    std::cerr << dash << " " << space << std::endl;
                    error("Some kind of parse error");
                }
                auto from_s = s.substr(0, dash);
                auto to_s = s.substr(dash+1, space-dash);
                // std::cerr << "PARSING: " << from_s << " " << to_s << std::endl;
                size_t from = stoull(from_s, nullptr, 16);
                size_t to = stoull(to_s, nullptr, 16);

                // Also bail early if this is not a rw map
                bool maybe_rw = s.find("rw-s") != std::string::npos;
                bool count_this = maybe_rw && from >= ptr && from <= ptr+len;

                while((read = getline(&line, &n, file)) != -1) {
                    // Abort if we find a line that is a new mmap entry
                    std::string s(line, read);
                    if (s.find(' ') < s.find(':'))
                        break;

                    if (count_this) {
                        char prefix[80];
                        char kb[80];
                        sscanf(line, "%s %s", prefix, kb);
                        if(!return_alloc && std::string(prefix) == "Rss:") {
                            total += atoi(kb) * 1024;
                        } else if(return_alloc && std::string(prefix) == "Size:") {
                            total += atoi(kb) * 1024;
                        }
                    }
                }
            }
            fclose(file);
            return total;
        }

        std::tuple<size_t,size_t,size_t,size_t> report_sizes(MMapAllocInfo& info) {
            size_t total = info.occupied_pages.count() * ZEF_PAGE_SIZE * 2;
            size_t loaded = info.loaded_pages.count() * ZEF_PAGE_SIZE * 2;
            size_t page_size = sysconf(_SC_PAGESIZE);
            int pages = (MAX_MMAP_SIZE+page_size-1) / page_size;
            unsigned char * vec = new unsigned char[pages];
            // if(-1 == mincore(info.location, MAX_MMAP_SIZE, vec))
            //     error_p("Unexpected error in mincore");
            // size_t rss = 0;
            // for(unsigned char * c = vec ; c != vec+pages ; c++) {
            //     if(*c & 0x01)
            //         rss += page_size;
            // }
            size_t rss = my_mincore(info.location, MAX_MMAP_SIZE);
            size_t mincore_total = my_mincore(info.location, MAX_MMAP_SIZE, true);
            
            return std::make_tuple(total, loaded, rss, mincore_total);
        }

        void print_malloc_arenas(void) {
            // malloc_stats();
        }

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
                // TODO: Disable write on read-only graph
                if(0 != mprotect(blob_start, ZEF_PAGE_SIZE, PROT_READ | PROT_WRITE))
                    error_p("Could not mprotect new blobs page");
                memset(blob_start, 0, ZEF_PAGE_SIZE);
            }
            else if (info.style == MMAP_STYLE_FILE_BACKED) {
                FileGraph & fg = *info.file_graph;
                auto [offset, file_index] = fg.get_page_offset(page_ind);
                int fd = fg.get_fd(file_index);
                // std::cerr << "*** Mapping file offset: " << offset << " to blob offset: " << (intptr_t)blob_start - (intptr_t)blobs_ptr << std::endl;
                if(MAP_FAILED == mmap(blob_start, ZEF_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_FIXED|MAP_SHARED, fd, offset))
                    error_p("Could not mmap new blobs page from file");
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
            if(-1 == munmap(blob_start, ZEF_PAGE_SIZE))
                error_p("Could not munmap blobs page from file");

            info.loaded_pages[page_ind] = 0;
        }

        // * Alloc list functions

        void* create_mmap(int style, FileGraph * file_graph) {
            void * location;
            if (style == MMAP_STYLE_MALLOC) {
                location = malloc(MAX_MMAP_SIZE);
                memset(location, 0, MAX_MMAP_SIZE);
            } else {
                // This should create a mmap which doesn't take any physical memory.
                // This reserves the space, so that we can fill it with other maps later on.
                int opts = MAP_ANONYMOUS | MAP_PRIVATE;
                int fd = -1;
                int prot = PROT_NONE;
                location = mmap(NULL, MAX_MMAP_SIZE, prot, opts, fd, 0);
                if(location == MAP_FAILED)
                    error_p("Could not mmap memory");
            }

            void * blob_ptr = blobs_ptr_from_mmap(location);

            // Need to allow the info struct to have read/write access
            // TODO: We could also allow other information to go here to keep it close together.
            void * info_ptr = &info_from_blobs(blob_ptr);
            assert(info_ptr >= location);
            if (style != MMAP_STYLE_MALLOC) {
                // if(MAP_FAILED == mmap(info_ptr, sizeof(MMapAllocInfo), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0)) {
                void * page_aligned = align_to_system_pagesize(info_ptr);
                if(0 != mprotect(page_aligned, ((char*)blob_ptr - (char*)page_aligned), PROT_READ | PROT_WRITE))
                    error_p("Could not mprotect info struct location.");
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
            msync(info.location, MAX_MMAP_SIZE, MS_SYNC);
            if(info.style == MMap::MMAP_STYLE_FILE_BACKED)
                info.file_graph->flush_mmap();
        }
        void flush_mmap(MMapAllocInfo& info, blob_index latest_blob) {
            msync(info.location, MAX_MMAP_SIZE, MS_SYNC);
            if(info.style == MMap::MMAP_STYLE_FILE_BACKED) {
                info.file_graph->set_latest_blob_index(latest_blob);
                info.file_graph->flush_mmap();
            }
        }

        void page_out_mmap(MMapAllocInfo& info) {
            flush_mmap(info);
            if (info.style == MMAP_STYLE_FILE_BACKED) {
                FileGraph & fg = *info.file_graph;
                char * blobs_ptr = (char*)blobs_ptr_from_info(&info);
                for(int page_ind = 0 ; page_ind < PAGE_BITMAP_BITS ; page_ind++) {
                    if(-1 == madvise(blobs_ptr + page_ind*ZEF_PAGE_SIZE, ZEF_PAGE_SIZE, MADV_DONTNEED))
                        error_p("Couldn't madvise.");

                    // madvise(blobs_ptr + page_ind*ZEF_PAGE_SIZE, ZEF_PAGE_SIZE, MADV_PAGEOUT);

                    // unload_page(info, page_ind);
                    // if(MAP_FAILED == mmap(blobs_ptr + page_ind*ZEF_PAGE_SIZE, ZEF_PAGE_SIZE, PROT_NONE, MAP_ANONYMOUS|MAP_PRIVATE, -1, 0))
                    //     error_p("Couldn't mmap fake back.");
                }
                // std::cerr << "Tried to advise" << std::endl;
            }
        }

        void destroy_mmap(MMapAllocInfo& info) {
            if(info.style == MMAP_STYLE_MALLOC) {
                free(info.location);
            } else if (info.style == MMAP_STYLE_ANONYMOUS) {
                // munmap can remove multiple mappings, so no need to loop through all alloced pages
                munmap(info.location, MAX_MMAP_SIZE);
            } else if (info.style == MMAP_STYLE_FILE_BACKED) {
                // if(close(info.blob_fd) != 0 || close(info.uid_fd) != 0)
                //     error_p("Problem closing fds for mmap");
                flush_mmap(info);
                delete info.file_graph;
                // Doing a sync flush here, instead of the async in flush_mmap.
                msync(info.location, MAX_MMAP_SIZE, MS_SYNC);
                munmap(info.location, MAX_MMAP_SIZE);
            }
        }



    }
}