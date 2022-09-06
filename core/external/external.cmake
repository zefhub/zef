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

######################
# Macos hinting

# Try and give a good enough hint for success on macos
if (APPLE)
  if(NOT DEFINED MACOS_BREW_PREFIX)
    if(DEFINED ENV{MACOS_BREW_PREFIX})
      set(MACOS_BREW_PREFIX $ENV{MACOS_BREW_PREFIX})
    else()
      execute_process(
        COMMAND brew --prefix
        RESULT_VARIABLE BREW_RESULT
        OUTPUT_VARIABLE MACOS_BREW_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE)
      if (BREW_RESULT EQUAL 0 AND EXISTS "${MACOS_BREW_PREFIX}")
      else()
        message(SEND_ERROR "Unable to run brew to determine install prefix. Likely to fail later in the build process unless environment variables have been set manually.")
      endif()
    endif()
  endif()
  if(NOT "${MACOS_BREW_PREFIX}" STREQUAL "")
    message(STATUS "Appending brew install prefix of ${MACOS_BREW_PREFIX} to cmake path. Manually set MACOS_BREW_PREFIX='' to disable this behaviour.")

    list(APPEND CMAKE_PREFIX_PATH ${MACOS_BREW_PREFIX})
  endif()
endif()


######################
# * Setup for the upstream pulling

set(LICENSE_DIR ${CMAKE_CURRENT_BINARY_DIR}/licenses)
if(LIBZEF_PYZEF_BUNDLED)
  set(TOP_LICENSE_FILE "${LICENSE_DIR}/LICENSE.libzef")
else()
  set(TOP_LICENSE_FILE "${LICENSE_DIR}/LICENSE")
endif()
set(LICENSE_FILES ${TOP_LICENSE_FILE})

# Initialise the top file - which could be in different places if we are being compiled from pyzef sdist
find_file(REPO_LICENSE
  LICENSE
  PATHS ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../
  NO_DEFAULT_PATH
  NO_CACHE
  REQUIRED)
  
file(READ ${REPO_LICENSE} OUR_LICENSE)
file(WRITE ${TOP_LICENSE_FILE} ${OUR_LICENSE})

file(APPEND ${TOP_LICENSE_FILE} "\nThe following files contain additional bundled licenses:\n\n")

###########
function(create_license_file SHORT_NAME LICENSE_FILE HAS_COPYING PREAMBLE)
  file(READ ${LICENSE_FILE} CONTENTS)
  file(WRITE ${LICENSE_DIR}/LICENSE.${SHORT_NAME} ${PREAMBLE})
  file(APPEND ${LICENSE_DIR}/LICENSE.${SHORT_NAME} ${CONTENTS})

  list(APPEND LICENSE_FILES ${LICENSE_DIR}/LICENSE.${SHORT_NAME})
  # Add to the top file
  file(APPEND ${TOP_LICENSE_FILE} "LICENSE.${SHORT_NAME}\n")

  get_filename_component(SEARCH_DIR ${LICENSE_FILE} DIRECTORY)
  if(HAS_COPYING)
    # Note: copying a file only works when not renaming, (otherwise it creates a
    # directory) and the COPY_FILE variant is only avaiable in version 3.21, so
    # instead we cheat with the configure_file command.
    # file(COPY ${SEARCH_DIR}/COPYING DESTINATION ${LICENSE_DIR}/COPYING.${SHORT_NAME})
    configure_file(${SEARCH_DIR}/COPYING ${LICENSE_DIR}/COPYING.${SHORT_NAME} COPYONLY)
    list(APPEND LICENSE_FILES ${LICENSE_DIR}/COPYING.${SHORT_NAME})
  endif()

  if(EXISTS ${SEARCH_DIR}/NOTICE)
    message(fatal_error "A NOTICE file was found that we haven't taken care of!")
  endif()

  # Variables need to be propagated back up
  set(LICENSE_FILES ${LICENSE_FILES} PARENT_SCOPE)
endfunction()
##########

include(FetchContent)
# set(FETCHCONTENT_QUIET OFF)
# get_filename_component(FETCHCONTENT_BASE_DIR ${CMAKE_BINARY_DIR}/../_cmake_deps REALPATH)

function(ManualFetchContent_MakeAvailable name)
  FetchContent_GetProperties(${name})
  if(NOT ${${name}_POPULATED})
    FetchContent_Populate(${name})
  endif()

  add_subdirectory(${${name}_SOURCE_DIR} ${${name}_BINARY_DIR} EXCLUDE_FROM_ALL)
  # I don't understand but somehow rangesv3 (and only rangesv3) prevents us for propagating the source dir up higher
  set(LAST_SOURCE_DIR ${${name}_SOURCE_DIR} PARENT_SCOPE)
endfunction()

find_package(PkgConfig QUIET)

# * openssl

# This is only here for consistency. It doesn't have an option to pull from
# upstream and only builds using the system libraries.

# This also comes first to enable other packages below to make use of our
# results in finding openssl.

if(WIN32)
  # Above I lied - we do need to build on windows.
  message(STATUS "Going to build OpenSSL from source")
  
  # set(BUILD_OPENSSL ON CACHE BOOL "" FORCE)
  # set(OPENSSL_BRANCH openssl-3.0.3 CACHE STRING "" FORCE)
  # FetchContent_Declare(opensslsrc
  #   GIT_REPOSITORY https://github.com/viaduck/openssl-cmake
  #   GIT_TAG 3.0
  #   GIT_SHALLOW ON
  #   UPDATE_COMMAND "")

  # ManualFetchContent_MakeAvailable(opensslsrc)

  set(WITH_APPS OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(opensslsrc
    GIT_REPOSITORY https://github.com/janbar/openssl-cmake
    GIT_TAG 1.1.1n-20220327
    GIT_SHALLOW ON
    UPDATE_COMMAND "")

  ManualFetchContent_MakeAvailable(opensslsrc)

  add_library(openssl INTERFACE)
  target_link_libraries(openssl INTERFACE ssl crypto)
else()
  if(APPLE)
    # Brew on macos doesn't link these things in by default to not shadow the main macos openssl 
    execute_process(
      COMMAND brew --prefix openssl@1.1
      RESULT_VARIABLE BREW_RESULT
      OUTPUT_VARIABLE BREW_OPENSSL_PATH
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (BREW_RESULT EQUAL 0 AND EXISTS "${BREW_OPENSSL_PATH}")
      message(STATUS "Adding '${BREW_OPENSSL_PATH}' to cmake prefix path")
      list(APPEND CMAKE_PREFIX_PATH ${BREW_OPENSSL_PATH})
    endif()
    message(STATUS "Brew prefix check result was: ${BREW_RESULT}")
  endif()

  find_package(OpenSSL QUIET)
  if(OPENSSL_FOUND)
    message(STATUS "Found openssl with find_package")
    message(STATUS "Found openssl libraries at: ${OPENSSL_LIBRARIES}")
    message(STATUS "Found openssl includes at: ${OPENSSL_INCLUDE_DIR}")
    add_library(openssl INTERFACE IMPORTED)
    target_link_libraries(openssl INTERFACE OpenSSL::SSL OpenSSL::Crypto)
  else()
    if(OpenSSL_FOUND)
      message(STATUS "Found different openssl tag with find_package but ignoring (shouldn't do this)")
    endif()
    if(NOT OPENSSL_FOUND)
      if(PKGCONFIG_FOUND)
        pkg_check_modules(OPENSSL openssl)
        if(OPENSSL_FOUND)
          message(STATUS "Found openssl with pkg-config")
          set(OPENSSL_LIBRARIES ${OPENSSL_LINK_LIBRARIES})
          set(OPENSSL_INCLUDE_DIR ${OPENSSL_INCLUDE_DIRS})
        endif()
      endif()
    endif()
    if(NOT OPENSSL_FOUND)
      if (APPLE AND DEFINED BREW_OPENSSL_PATH)
        set(OPENSSL_OTHER_SEARCH_PATH "${BREW_OPENSSL_PATH}")
        message(STAUS "Going to use extra openssl_other_search_path")
      endif()
      find_library(OPENSSL_LIBRARIES ssl PATHS ${OPENSSL_OTHER_SEARCH_PATH})
      find_path(OPENSSL_INCLUDE_DIR openssl/ssl.h PATHS ${OPENSSL_OTHER_SEARCH_PATH})
      if(OPENSSL_LIBRARIES AND OPENSSL_INCLUDE_DIR)
        set(OPENSSL_FOUND TRUE)
        message(STATUS "Found openssl with find_library and find_path")
      endif()
    endif()

    if(OPENSSL_FOUND)
      message(STATUS "Found openssl libraries at: ${OPENSSL_LIBRARIES}")
      message(STATUS "Found openssl includes at: ${OPENSSL_INCLUDE_DIR}")
      add_library(openssl INTERFACE IMPORTED)
      target_include_directories(openssl SYSTEM INTERFACE ${OPENSSL_INCLUDE_DIRS})
      target_link_libraries(openssl INTERFACE ${OPENSSL_LIBRARIES})
    else()
      message(FATAL_ERROR "Couldn't find openssl via cmake, pkg-config or find_library")
    endif()
  endif()
endif()

add_library (external::openssl ALIAS openssl)

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

create_license_file("nlohmann_json" ${nlohmann_json_SOURCE_DIR}/LICENSE.MIT NO "This library bundles Niels Lohmann's JSON library (https://github.com/nlohmann/json)\n\nFrom distribution point of (https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent).\n\nThe full text of the nlohmann_json license follows below.\n\n")

# * yaml-cpp
message(STATUS "External: yaml-cpp")

FetchContent_Declare(yaml-cpp
  GIT_REPOSITORY https://github.com/jbeder/yaml-cpp
  GIT_TAG yaml-cpp-0.7.0
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

ManualFetchContent_MakeAvailable(yaml-cpp)
set_target_properties(yaml-cpp PROPERTIES
  POSITION_INDEPENDENT_CODE ON)

create_license_file("yaml-cpp" ${LAST_SOURCE_DIR}/LICENSE NO "This library bundles the yaml-cpp library (https://github.com/jbeder/yaml-cpp)\n\nThe full text of the yaml-cpp license follows below.\n\n")


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

add_library(asio INTERFACE IMPORTED)
target_include_directories(asio SYSTEM INTERFACE ${asio_SOURCE_DIR}/asio/include)
target_link_libraries(asio INTERFACE openssl)

create_license_file("asio" ${asio_SOURCE_DIR}/asio/LICENSE_1_0.txt YES "This library bundles the non-Boost ASIO networking library (https://github.com/chriskohlhoff/asio)\n\nThe full text of the ASIO license follows below.\n\n")

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

add_library(websocketpp INTERFACE IMPORTED)
target_include_directories(websocketpp SYSTEM INTERFACE ${websocketpp_SOURCE_DIR})
target_link_libraries(websocketpp INTERFACE asio)

create_license_file("websocketpp" ${websocketpp_SOURCE_DIR}/COPYING NO "This library bundles the websocketpp library (https://github.com/chriskohlhoff/asio)\n\nThe full text of the websocketpp license follows below.\n\n")

add_library (external::websocketpp ALIAS websocketpp)


# * parallel-hashmap
message(STATUS "External: parallel-hashmap")

FetchContent_Declare(phmap
  GIT_REPOSITORY https://github.com/greg7mdp/parallel-hashmap
  GIT_TAG 1.33
  GIT_SHALLOW ON
  PATCH_COMMAND git apply ${CMAKE_CURRENT_LIST_DIR}/phmap_no_sources.patch
  UPDATE_COMMAND "")

ManualFetchContent_MakeAvailable(phmap)

create_license_file("phmap" ${phmap_SOURCE_DIR}/LICENSE NO "This library bundles the parallel-hashmap library (https://github.com/greg7mdp/parallel-hashmap)\n\nThe full text of the phmap license follows below.\n\n")

add_library (external::phmap ALIAS phmap)

# * doctest
message(STATUS "External: doctest")

FetchContent_Declare(doctest
  GIT_REPOSITORY https://github.com/onqtam/doctest
  GIT_TAG 2.4.6
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

ManualFetchContent_MakeAvailable(doctest)

add_library (external::doctest ALIAS doctest)

# * ranges-v3
message(STATUS "External: ranges-v3")

FetchContent_Declare(rangesv3
  GIT_REPOSITORY https://github.com/ericniebler/range-v3
  GIT_TAG 0.11.0
  GIT_SHALLOW ON
  UPDATE_COMMAND "")

ManualFetchContent_MakeAvailable(rangesv3)

create_license_file("rangesv3" ${LAST_SOURCE_DIR}/LICENSE.txt NO "This library bundles the ranges-v3 library (https://github.com/ericniebler/range-v3)\n\nThe full text of the ranges-v3 license follows below.\n\n")

add_library (external::range-v3 ALIAS range-v3)

# # * blake3
# message(STATUS "External: blake3")
# FetchContent_Declare(blake3
#   GIT_REPOSITORY https://github.com/BLAKE3-team/BLAKE3
#   GIT_TAG 1.1.0
#   GIT_SHALLOW ON
#   UPDATE_COMMAND "")

# FetchContent_GetProperties(blake3)
# if(NOT blake3_POPULATED)
#   FetchContent_Populate(blake3)
# endif()

# add_library(blake3 INTERFACE)

# target_sources(blake3 INTERFACE
#     ${blake3_SOURCE_DIR}/c/blake3.c
#     ${blake3_SOURCE_DIR}/c/blake3_dispatch.c
#     ${blake3_SOURCE_DIR}/c/blake3_portable.c
#     ${blake3_SOURCE_DIR}/c/blake3_sse2_x86-64_unix.S
#     ${blake3_SOURCE_DIR}/c/blake3_sse41_x86-64_unix.S
#     ${blake3_SOURCE_DIR}/c/blake3_avx2_x86-64_unix.S
#     ${blake3_SOURCE_DIR}/c/blake3_avx512_x86-64_unix.S
#     )

# target_include_directories(blake3 SYSTEM INTERFACE ${blake3_SOURCE_DIR}/c)
# create_license_file("blake3" ${blake3_SOURCE_DIR}/LICENSE NO "This library bundles the blake3 library (https://github.com/BLAKE3-team/BLAKE3)\n\nThe full text of the license follows below.\n\n")

# * jwt-cpp
message(STATUS "External: jwt-cpp")

FetchContent_Declare(jwt-cpp
  GIT_REPOSITORY https://github.com/Thalhammer/jwt-cpp
  GIT_TAG v0.6.0-rc.2
  GIT_SHALLOW ON
  PATCH_COMMAND git apply ${CMAKE_CURRENT_LIST_DIR}/jwt-allow-no-find-package.patch
  UPDATE_COMMAND "")

#set(JWT_SSL_LIBRARY "OpenSSL" CACHE STRING "")
set(JWT_SSL_LIBRARY "manual" CACHE STRING "")
set(JWT_DISABLE_PICOJSON TRUE CACHE BOOL "")
set(JWT_BUILD_EXAMPLES OFF CACHE BOOL "")

ManualFetchContent_MakeAvailable(jwt-cpp)

create_license_file("jwt-cpp" ${jwt-cpp_SOURCE_DIR}/LICENSE NO "This library bundles the JWT++ library (https://github.com/Thalhammer/jwt-cpp)\n\nThe full text of the jwt-cpp license follows below.\n\n")

add_library (external::jwt-cpp ALIAS jwt-cpp)

# * zstd
message(STATUS "External: zstd")
if(LIBZEF_BUNDLED_ZSTD)

  FetchContent_Declare(zstd
    GIT_REPOSITORY https://github.com/facebook/zstd
    GIT_TAG v1.5.2
    GIT_SHALLOW ON
    UPDATE_COMMAND "")

  # Have to do this manually as the cmake directory is deep
  FetchContent_GetProperties(zstd)
  if(NOT ${zstd_POPULATED})
    FetchContent_Populate(zstd)
  endif()

  set(ZSTD_BUILD_SHARED OFF)
  add_subdirectory(${zstd_SOURCE_DIR}/build/cmake ${zstd_BINARY_DIR} EXCLUDE_FROM_ALL)


  add_library(libzstd_internal INTERFACE)
  target_link_libraries(libzstd_internal INTERFACE libzstd_static)
  # Because the cmake of libzstd is a little dodgy, we need to manually add in the include directories.
  get_target_property(LIBZSTD_INCLUDE_DIRECTORIES libzstd_static INCLUDE_DIRECTORIES)
  target_include_directories(libzstd_internal SYSTEM INTERFACE ${LIBZSTD_INCLUDE_DIRECTORIES})

  create_license_file("zstd" ${zstd_SOURCE_DIR}/LICENSE NO "This library bundles the Zstandard library (https://github.com/facebook/zstd)\n\nThe full text of the zstd license follows below.\n\n")
else()
  # Finding zstd through various options.
  # 1. First try pkg-config
  # 2. Then try cmake
  # 3. Then fallback to just finding the lib file
  # It seems like the cmake version does not always behave appropriately, and
  # officially there might only be a pkg-config version included.
  if(PKGCONFIG_FOUND)
    pkg_check_modules(ZSTD libzstd)
    if(ZSTD_FOUND)
      message(STATUS "Found zstd with pkg-config")
      set(ZSTD_LIBRARIES ${ZSTD_LINK_LIBRARIES})
    endif()
  endif()
  if(NOT ZSTD_FOUND)
    find_package(Zstd QUIET)
  endif()
  if(NOT ZSTD_FOUND)
    find_library(ZSTD_LIBRARIES zstd)
    find_path(ZSTD_INCLUDE_DIRS zstd.h)
    if(ZSTD_LIBRARIES AND ZSTD_INCLUDE_DIRS)
      set(ZSTD_FOUND TRUE)
    endif()
  endif()

  if(ZSTD_FOUND)
    message(STATUS "Found zstd libraries at: ${ZSTD_LIBRARIES}")
    message(STATUS "Found zstd includes at: ${ZSTD_INCLUDE_DIRS}")
    add_library(libzstd_internal INTERFACE)
    target_include_directories(libzstd_internal SYSTEM INTERFACE ${ZSTD_INCLUDE_DIRS})
    target_link_libraries(libzstd_internal INTERFACE ${ZSTD_LIBRARIES})
  else()
    message(FATAL_ERROR "Couldn't find zstd via cmake, pkg-config or find_library")
  endif()
endif()

add_library (external::libzstd ALIAS libzstd_internal)


# * libcurl
message(STATUS "External: curl")
if(LIBZEF_BUNDLED_CURL)

  # message(FATAL_ERROR "We cannot bundle libcurl with the library due to CA issues.")

  FetchContent_Declare(curl
    GIT_REPOSITORY https://github.com/curl/curl
    GIT_TAG curl-7_83_0
    GIT_SHALLOW ON
    UPDATE_COMMAND "")

  # Have to do this manually as the cmake directory is deep
  FetchContent_GetProperties(curl)
  if(NOT ${curl_POPULATED})
    FetchContent_Populate(curl)
  endif()

  set(BUILD_CURL_EXE OFF)
  set(BUILD_SHARED_LIBS OFF)
  set(BUILD_TESTING OFF)
  set(HTTP_ONLY ON)

  if(WIN32)
    set(CURL_USE_SCHANNEL ON)
  else()
    # We can't use the CA bundles as this will be in the system somewhere. Instead
    # we have to acquire these from the python side as environment variables.
    # set(CURL_CA_FALLBACK ON CACHE BOOL "" FORCE)
    set(CURL_CA_BUNDLE "none" CACHE STRING "" FORCE)
    set(CURL_CA_PATH "none" CACHE STRING "" FORCE)
    # TODO: Should set a config variable here which would allow python and the cpp
    # code to identify that it is in this bundled state. But for now, setting env
    # vars even if they aren't needed will be fine.
  endif()
  
  add_subdirectory(${curl_SOURCE_DIR} ${curl_BINARY_DIR} EXCLUDE_FROM_ALL)
  set_target_properties(libcurl PROPERTIES
    POSITION_INDEPENDENT_CODE ON)

  create_license_file("libcurl" ${curl_SOURCE_DIR}/COPYING NO "This library bundles the libcurl library (https://github.com/curl/curl)\n\nThe full text of the libcurl license follows below.\n\n")
else()
  if(PKGCONFIG_FOUND)
    pkg_check_modules(CURL libcurl)
    if(CURL_FOUND)
      message(STATUS "Found curl with pkg-config")
      set(CURL_LIBRARIES ${CURL_LINK_LIBRARIES})
    endif()
  endif()
  if(NOT CURL_FOUND)
    find_package(CURL QUIET)
  endif()
  if(NOT CURL_FOUND)
    find_library(CURL_LIBRARIES curl)
    find_path(CURL_INCLUDE_DIRS curl/curl.h)
    if(CURL_LIBRARIES AND CURL_INCLUDE_DIRS)
      set(CURL_FOUND TRUE)
    endif()
  endif()

  if(CURL_FOUND)
    message(STATUS "Found curl libraries at: ${CURL_LIBRARIES}")
    message(STATUS "Found curl includes at: ${CURL_INCLUDE_DIRS}")
    add_library(libcurl INTERFACE)
    target_include_directories(libcurl SYSTEM INTERFACE ${CURL_INCLUDE_DIRS})
    target_link_libraries(libcurl INTERFACE ${CURL_LIBRARIES})
  else()
    message(FATAL_ERROR "Couldn't find curl via cmake, pkg-config or find_library")
  endif()
endif()

add_library (external::libcurl ALIAS libcurl)


# * libarchive

message(STATUS "External: libarchive")
if(LIBZEF_BUNDLED_LIBARCHIVE)
  FetchContent_Declare(archive
    GIT_REPOSITORY https://github.com/libarchive/libarchive
    GIT_TAG v3.6.1
    GIT_SHALLOW ON
    UPDATE_COMMAND "")

  ManualFetchContent_MakeAvailable(archive)
  set_target_properties(archive_static PROPERTIES
    POSITION_INDEPENDENT_CODE ON)
  get_target_property(LIBARCHIVE_INCLUDES archive INCLUDE_DIRECTORIES)
  target_include_directories(archive_static PUBLIC ${LIBARCHIVE_INCLUDES})

  message(STATUS "Libarchive includes at: ${LIBARCHIVE_INCLUDES}")

  create_license_file("libarchive" ${LAST_SOURCE_DIR}/COPYING NO "This library bundles the libarchive library (https://github.com/libarchive/libarchive)\n\nThe full text of the libarchive license follows below.\n\n")

  add_library (external::archive ALIAS archive_static)
else()
  if(PKGCONFIG_FOUND)
    pkg_check_modules(ARCHIVE archive)
    if(ARCHIVE_FOUND)
      message(STATUS "Found archive with pkg-config")
      set(ARCHIVE_LIBRARIES ${ARCHIVE_LIBRARIES})
    endif()
  endif()
  if(NOT ARCHIVE_FOUND)
    find_package(ARCHIVE QUIET)
  endif()
  if(NOT ARCHIVE_FOUND)
    find_library(ARCHIVE_LIBRARIES archive)
    find_path(ARCHIVE_INCLUDE_DIRS archive.h)
    if(ARCHIVE_LIBRARIES AND ARCHIVE_INCLUDE_DIRS)
      set(ARCHIVE_FOUND TRUE)
    endif()
  endif()

  if(ARCHIVE_FOUND)
    message(STATUS "Found archive libraries at: ${ARCHIVE_LIBRARIES}")
    message(STATUS "Found archive includes at: ${ARCHIVE_INCLUDE_DIRS}")
  else()
    message(FATAL_ERROR "Couldn't find archive via cmake, pkg-config or find_library")
  endif()

  add_library (external::archive ALIAS archive)
endif()
