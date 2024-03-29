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
#include <zstd.h>
#include <string>

namespace zefDB {
    LIBZEF_DLL_EXPORTED std::string decompress_zstd(std::string input);

    // Arbitrarily chosen compression level (apparently range is 1-22)
    LIBZEF_DLL_EXPORTED std::string compress_zstd(std::string input, int compression_level = 10);
}