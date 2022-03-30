#pragma once

#include "export_statement.h"
#include "graph.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

#include <pybind11/pybind11.h>
#include <pybind11/embed.h>
#include <pybind11/stl.h>
namespace py = pybind11;

namespace zefDB {
    namespace internals {
        LIBZEF_DLL_EXPORTED Graph create_from_json(std::unordered_map<blob_index,json> blobs);
    }
}