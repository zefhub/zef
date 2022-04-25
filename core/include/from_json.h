#pragma once

#include "export_statement.h"
#include "graph.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace zefDB {
    namespace internals {
        LIBZEF_DLL_EXPORTED Graph create_from_json(std::unordered_map<blob_index,json> blobs);
    }
}