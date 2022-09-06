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

#include "tar_file.h"

#include "zefDB_utils.h"

#include <stdlib.h>
#include <fcntl.h>
#include <archive.h>
#include <archive_entry.h>
#include <string.h>
#include <time.h>

using namespace std::string_literals;

namespace zefDB {
    FileGroup load_tar_into_memory(const std::filesystem::path & path) {
        archive * a;
        a = archive_read_new();
        RAII_CallAtEnd archive_cleanup([a]() {
            archive_read_free(a);
        });

        archive_read_support_filter_all(a);
        archive_read_support_format_tar(a);
        if(ARCHIVE_OK != archive_read_open_filename(a, path.c_str(), 0))
            throw std::runtime_error("Failed to open tar file: "s + archive_error_string(a));


        std::vector<FileInMemory> files;
        archive_entry * entry;
        while(archive_read_next_header(a, &entry) == ARCHIVE_OK) {
            std::string filename = archive_entry_pathname(entry);
            size_t size = archive_entry_size(entry);
            std::string buf;
            buf.resize(size);
            size_t read_size = archive_read_data(a, buf.data(), size);
            if(read_size != size)
                throw std::runtime_error("Unable to read tar file: "s + archive_error_string(a));

            files.emplace_back(std::move(filename), std::move(buf));
        }

        return FileGroup(std::move(files));
    }

    std::optional<FileInMemory> load_specific_file_from_tar(const std::filesystem::path & path, const std::string & filename) {
        archive * a;
        a = archive_read_new();
        RAII_CallAtEnd archive_cleanup([a]() {
            archive_read_free(a);
        });

        archive_read_support_filter_all(a);
        archive_read_support_format_tar(a);
        if(ARCHIVE_OK != archive_read_open_filename(a, path.c_str(), 0))
            throw std::runtime_error("Failed to open tar file: "s + archive_error_string(a));

        archive_entry * entry;
        while(archive_read_next_header(a, &entry) == ARCHIVE_OK) {
            std::string this_filename = archive_entry_pathname(entry);
            if(filename != this_filename) {
                archive_read_data_skip(a);
                continue;
            }

            size_t size = archive_entry_size(entry);
            std::string buf;
            buf.resize(size);
            size_t read_size = archive_read_data(a, buf.data(), size);
            if(read_size != size)
                throw std::runtime_error("Unable to read tar file: "s + archive_error_string(a));

            return FileInMemory(std::move(this_filename), std::move(buf));
        }

        return std::nullopt;
    }

    FileInMemory & FileGroup::find_file(const std::string & filename) {
        for(auto & file : files) {
            if(file.filename == filename) {
                return file;
            }
        }
        throw std::runtime_error("No file found with filename: "s + filename);
    }

    void save_filegroup_to_tar(const FileGroup & file_group, const std::filesystem::path & path) {
        archive * a;
        a = archive_write_new();
        RAII_CallAtEnd archive_cleanup([a]() {
            archive_write_free(a);
        });
        archive_write_add_filter_gzip(a);
        archive_write_set_format_pax_restricted(a);
        if(ARCHIVE_OK != archive_write_open_filename(a, path.c_str()))
            throw std::runtime_error("Failed to open tar file");

        for(auto & file : file_group.files) {
            archive_entry * entry = archive_entry_new();
            RAII_CallAtEnd entry_cleanup([entry]() {
                archive_entry_free(entry);
            });
            archive_entry_set_pathname(entry, file.filename.c_str());
            archive_entry_set_size(entry, file.contents.size());
            archive_entry_set_filetype(entry, AE_IFREG);
            archive_entry_set_mtime(entry, time(NULL), 0);
            archive_entry_set_uname(entry, "zef");
            archive_entry_set_gname(entry, "zef");
            archive_entry_set_perm(entry, 0600);
            if(ARCHIVE_OK != archive_write_header(a, entry))
                throw std::runtime_error("Unable to write header: "s + archive_error_string(a));
            size_t r = archive_write_data(a, file.contents.c_str(), file.contents.size());
            if(r != file.contents.size())
                throw std::runtime_error("Unable to write data: "s + archive_error_string(a));
        }

        archive_write_close(a);
    }
}