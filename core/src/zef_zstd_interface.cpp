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

#include "zef_zstd_interface.h"

#include <stdexcept>

namespace zefDB {
    std::string decompress_zstd(std::string input) {
        size_t r_size = ZSTD_getFrameContentSize(input.c_str(), input.length());
        if(r_size == ZSTD_CONTENTSIZE_ERROR)
            throw std::runtime_error("Not a zstd compressed string.");
        if(r_size == ZSTD_CONTENTSIZE_UNKNOWN)
            throw std::runtime_error("Unable to determine length of zstd content.");

        std::string output;
        output.resize(r_size);
        // TODO: Setup and reuse the context from ZSTD_decompressDCtx.

        size_t d_size = ZSTD_decompress(output.data(), r_size, input.c_str(), input.length());
        if (d_size != r_size) {
            std::string zstd_err = ZSTD_getErrorName(d_size);
            throw std::runtime_error("Problem decompressing zstd string. zstd error: " + zstd_err);
        }

        return output;
    }

    std::string compress_zstd(std::string input, int compression_level) {
        size_t const max_size = ZSTD_compressBound(input.length());

        std::string output;
        output.resize(max_size);

        // TODO: Setup and reuse the context from ZSTD_compressCCtx.
        size_t const c_size = ZSTD_compress(output.data(), max_size, input.c_str(), input.length(), compression_level);
        if (ZSTD_isError(c_size)) {
            std::string zstd_err = ZSTD_getErrorName(c_size);
            throw std::runtime_error("Problem compressing zstd string. zstd error: " + zstd_err);
        }

        output.resize(c_size);

        return output;
    }
}