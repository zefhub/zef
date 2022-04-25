include(FetchContent)
# set(FETCHCONTENT_QUIET OFF)
get_filename_component(FETCHCONTENT_BASE_DIR ${CMAKE_BINARY_DIR}/../_cmake_deps REALPATH)

function(ManualFetchContent_MakeAvailable name)
  FetchContent_GetProperties(${name})
  if(NOT ${${name}_POPULATED})
    FetchContent_Populate(${name})
  endif()

  add_subdirectory(${${name}_SOURCE_DIR} ${${name}_BINARY_DIR} EXCLUDE_FROM_ALL)
endfunction()

# * nlohmann json
message(STATUS "External: nlohmann_json")

set(JSON_BuildTests OFF CACHE INTERNAL "")

FetchContent_Declare(nlohmann_json
  # GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_REPOSITORY https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent
  # GIT_TAG v3.7.3
  GIT_TAG v3.9.1
  # GIT_TAG v3.10.4
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

# FetchContent_MakeAvailable(nlohmann_json)
ManualFetchContent_MakeAvailable(nlohmann_json)
# set(nlohmann_json_DIR ${nlohmann_json_SOURCE_DIR} CACHE PATH "" FORCE)


# * asio

message(STATUS "External: asio")

# Note: asio doesn't properly declare a target, so we just want the
# files, not to include the cmake structure.
FetchContent_Declare(asio
  GIT_REPOSITORY https://github.com/chriskohlhoff/asio/
  GIT_TAG asio-1-20-0
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

FetchContent_GetProperties(asio)
if(NOT asio_POPULATED)
  FetchContent_Populate(asio)
endif()

add_library(asio INTERFACE)
target_include_directories(asio INTERFACE ${asio_SOURCE_DIR}/asio/include)

# * websocketpp
message(STATUS "External: websocketpp")

# Note: websocketpp doesn't properly declare a target, so we just want the
# files, not to include the cmake structure.
FetchContent_Declare(websocketpp
  GIT_REPOSITORY https://github.com/zaphoyd/websocketpp
  GIT_TAG 0.8.2
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

FetchContent_GetProperties(websocketpp)
if(NOT websocketpp_POPULATED)
  FetchContent_Populate(websocketpp)
endif()

add_library(websocketpp INTERFACE)
target_include_directories(websocketpp INTERFACE ${websocketpp_SOURCE_DIR})
target_link_libraries(websocketpp INTERFACE asio)


# * parallel-hashmap
message(STATUS "External: parallel-hashmap")

FetchContent_Declare(phmap
  GIT_REPOSITORY https://github.com/greg7mdp/parallel-hashmap
  GIT_TAG 1.33
  GIT_SHALLOW ON
  PATCH_COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/phmap_no_sources.patch
  UPDATE_COMMAND "")

ManualFetchContent_MakeAvailable(phmap)

# * doctest
message(STATUS "External: doctest")

FetchContent_Declare(doctest
  GIT_REPOSITORY https://github.com/onqtam/doctest
  GIT_TAG 2.4.6
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

ManualFetchContent_MakeAvailable(doctest)

# * ranges-v3
message(STATUS "External: ranges-v3")

FetchContent_Declare(rangesv3
  GIT_REPOSITORY https://github.com/ericniebler/range-v3
  GIT_TAG 0.11.0
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

ManualFetchContent_MakeAvailable(rangesv3)

# * blake3
message(STATUS "External: blake3")
FetchContent_Declare(blake3
  GIT_REPOSITORY https://github.com/BLAKE3-team/BLAKE3
  GIT_TAG 1.1.0
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

FetchContent_GetProperties(blake3)
if(NOT blake3_POPULATED)
  FetchContent_Populate(blake3)
endif()

add_library(blake3 INTERFACE)

target_sources(blake3 INTERFACE
    ${blake3_SOURCE_DIR}/c/blake3.c
    ${blake3_SOURCE_DIR}/c/blake3_dispatch.c
    ${blake3_SOURCE_DIR}/c/blake3_portable.c
    ${blake3_SOURCE_DIR}/c/blake3_sse2_x86-64_unix.S
    ${blake3_SOURCE_DIR}/c/blake3_sse41_x86-64_unix.S
    ${blake3_SOURCE_DIR}/c/blake3_avx2_x86-64_unix.S
    ${blake3_SOURCE_DIR}/c/blake3_avx512_x86-64_unix.S
    )

target_include_directories(blake3 INTERFACE ${blake3_SOURCE_DIR}/c)

# * jwt-cpp
message(STATUS "External: jwt-cpp")

FetchContent_Declare(jwt-cpp
  GIT_REPOSITORY https://github.com/Thalhammer/jwt-cpp
  GIT_TAG v0.6.0-rc.2
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

set(JWT_SSL_LIBRARY "OpenSSL" CACHE STRING "")
set(JWT_DISABLE_PICOJSON TRUE CACHE BOOL "")
set(JWT_BUILD_EXAMPLES OFF CACHE BOOL "")

ManualFetchContent_MakeAvailable(jwt-cpp)
