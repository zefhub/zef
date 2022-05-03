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
#   PATCH_COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/pybindjson_disable_bytes_tuple.patch
#   UPDATE_COMMAND "")

# # FetchContent_MakeAvailable(nlohmann_json)
# ManualFetchContent_MakeAvailable(nlohmann_json)
# # set(nlohmann_json_DIR ${nlohmann_json_SOURCE_DIR} CACHE PATH "" FORCE)

# * pybind11 json
message(STATUS "External: pybind11")

FetchContent_Declare(pybind11_json
  GIT_REPOSITORY https://github.com/pybind/pybind11_json
  GIT_TAG 0.2.11
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

ManualFetchContent_MakeAvailable(pybind11_json)
