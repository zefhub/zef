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

#include <sys/mman.h>
#include <sys/file.h>
#include <unistd.h>

namespace zefDB {
    namespace MMap {

        inline void *OSMMap(int fd, size_t offset, size_t size, void * location) {
            int flags;
            if(fd == 0)
                flags = MAP_PRIVATE | MAP_ANONYMOUS;
            else
                flags = MAP_SHARED;
            void * result = mmap(location, size, PROT_READ | PROT_WRITE, flags, fd, offset);
            if(result == MAP_FAILED)
                error_p("Mapping failed.");
            return result;
        }

        // * Utils

        inline void * align_to_system_pagesize(const void * ptr) { return floor_ptr(ptr, getpagesize()); }

        inline bool is_fd_writable(int fd) {
            int flags;
            flags = fcntl(fd, F_GETFL);
            // The options O_WRONLY or O_RDWR do not share bits, so this is technically
            // not correct, however in our case we never have a O_WRONLY scenario.
            return (flags & O_RDWR);
        }

        //////////////////////////////
        // * FileGraph

        inline void FileGraph::flush_mmap() {
            msync(main_file_mapping, prefix_size(get_version()), MS_ASYNC);
        }

        inline FileGraph::~FileGraph() {
            for(auto & fd : fds) {
                if(fd != -1) {
                    fsync(fd);
                    flock(fd, LOCK_UN);
                    close(fd);
                }
            }
            if(main_file_mapping != nullptr && main_file_mapping != MAP_FAILED) {
                // Doing a sync flush here, instead of the async in flush_mmap.
                msync(main_file_mapping, prefix_size(get_version()), MS_SYNC);
                munmap(main_file_mapping, prefix_size(get_version()));
            }
        }


        inline _WholeFileMapping::~_WholeFileMapping() {
            if(size > 0) {
                msync(ptr, size, MS_SYNC);
                munmap(ptr, size);
            }
            if(fd == 0 && head != nullptr)
                delete head;
        }

    }
}