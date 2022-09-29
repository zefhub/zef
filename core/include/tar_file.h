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


#include "include_fs.h"
#include <string>
#include <vector>
#include <optional>

namespace zefDB {
    struct FileInMemory {
        std::string filename;
        std::string contents;
        FileInMemory(std::string && filename, std::string && contents) :
            filename(filename),
            contents(contents) {}
        // // Want to try and stop passing files around and causing copies.
        // FileInMemory & operator=(const FileInMemory &) = delete;
        // FileInMemory(const FileInMemory &) = delete;
    };
    struct FileGroup {
        std::vector<FileInMemory> files;

        FileGroup(std::vector<FileInMemory> && files) : files(files) {}
        FileGroup(const std::vector<FileInMemory> & files) : files(files) {}

        FileInMemory & find_file(const std::string & filename);
    };

    FileGroup load_tar_into_memory(const std::filesystem::path & path);

    std::optional<FileInMemory> load_specific_file_from_tar(const std::filesystem::path & path, const std::string & filename);
    void save_filegroup_to_tar(const FileGroup & file_group, const std::filesystem::path & path);
}