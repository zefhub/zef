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

#include <windows.h>
#include <sysinfoapi.h>
#include <io.h>

namespace zefDB {
    namespace MMap {

        inline void *OSMMap(int fd, size_t offset, size_t size, void * location) {
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

        // * Utils

        inline void * align_to_system_pagesize(const void * ptr) { 
            SYSTEM_INFO sys_info;
            GetSystemInfo(&sys_info);

            return floor_ptr(ptr, sys_info.dwPageSize);
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

        inline void FileGraph::flush_mmap() {
            // Note: FlushViewOfFile is asynchronous.
            FlushViewOfFile(main_file_mapping, prefix_size(get_version()));
        }

        inline FileGraph::~FileGraph() {
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
                        OSUnlockFile(fd, true);
                    } catch(const std::exception & exc) {
                        std::cerr << "Problem unlocking file: " + std::string(exc.what()) << std::endl;
                        std::cerr << "Ignoring because in ~FileGraph" << std::endl;
                    }
                    close(fd);
                }
            }
        }

        inline _WholeFileMapping::~_WholeFileMapping() {
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
    }
}