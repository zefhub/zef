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
                OSMMap(fd, offset, ZEF_PAGE_SIZE, blob_start);

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

        // * Alloc list functions

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

    }
}
