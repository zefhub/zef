# Copyright 2022 Synchronous Technologies Pte Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(FetchContent)
# set(FETCHCONTENT_QUIET OFF)
# get_filename_component(FETCHCONTENT_BASE_DIR ${CMAKE_BINARY_DIR}/../_cmake_deps REALPATH)

function(ManualFetchContent_MakeAvailable name)
  FetchContent_GetProperties(${name})
  if(NOT ${${name}_POPULATED})
    FetchContent_Populate(${name})
  endif()

  add_subdirectory(${${name}_SOURCE_DIR} ${${name}_BINARY_DIR} EXCLUDE_FROM_ALL)
endfunction()

# # * nlohmann json
# message(STATUS "External: nlohmann_json")

# set(JSON_BuildTests OFF CACHE INTERNAL "")

# FetchContent_Declare(nlohmann_json
#   # GIT_REPOSITORY https://github.com/nlohmann/json.git
#   GIT_REPOSITORY https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent
#   # GIT_TAG v3.7.3
#   GIT_TAG v3.9.1
#   # GIT_TAG v3.10.4
#   GIT_SHALLOW ON
#   UPDATE_COMMAND "")

# # FetchContent_MakeAvailable(nlohmann_json)
# ManualFetchContent_MakeAvailable(nlohmann_json)
# # set(nlohmann_json_DIR ${nlohmann_json_SOURCE_DIR} CACHE PATH "" FORCE)

# * pybind11 json
message(STATUS "External: pybind11_json")

FetchContent_Declare(pybind11_json
  # GIT_REPOSITORY https://github.com/pybind/pybind11_json
  # GIT_TAG 0.2.11
  GIT_REPOSITORY https://github.com/pengwyn/pybind11_json
  GIT_TAG unsigned-conversion-order
  GIT_SHALLOW ON
  PATCH_COMMAND git apply ${CMAKE_CURRENT_LIST_DIR}/pybindjson_disable_bytes_tuple.patch
  UPDATE_COMMAND "")

ManualFetchContent_MakeAvailable(pybind11_json)
