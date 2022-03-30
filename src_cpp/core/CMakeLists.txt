cmake_minimum_required(VERSION 3.8)

project(ZefDB CXX ASM)

############################
# customizable variables

if(DEFINED ENV{ZEFDB_ALLOW_NO_TLS})
  set(ZEFDB_ALLOW_NO_TLS $ENV{ZEFDB_ALLOW_NO_TLS} CACHE BOOL "Allow insecure websockets for ZefHub connections" FORCE)
else()
  set(ZEFDB_ALLOW_NO_TLS TRUE CACHE BOOL "Allow insecure websockets for ZefHub connections")
endif()
if(ZEFDB_ALLOW_NO_TLS)
  message(STATUS "Insecure connections are permitted")
else()
  message(STATUS "Insecure connections are disallowed")
endif()

if(DEFINED ENV{ZEFDB_TEST_NO_MMAP_CHECKS})
  set(ZEFDB_TEST_NO_MMAP_CHECKS $ENV{ZEFDB_TEST_NO_MMAP_CHECKS} CACHE BOOL "Test for mmap slowness" FORCE)
else()
  set(ZEFDB_TEST_NO_MMAP_CHECKS FALSE CACHE BOOL "Test for mmap slowness")
endif()
if(ZEFDB_TEST_NO_MMAP_CHECKS)
  message(WARNING "Testing mmap slowness - graphs are given 10MB of memory and other ensure page checks are disabled.")
endif()

############################
# * External single libraries

find_package(PkgConfig QUIET)

# ** openssl

if(APPLE)
  # Brew on macos doesn't link these things in by default to not shadow the main macos openssl 
  execute_process(
    COMMAND brew --prefix openssl
    RESULT_VARIABLE BREW_RESULT
    OUTPUT_VARIABLE BREW_OPENSSL_PATH
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if (BREW_RESULT EQUAL 0 AND EXISTS "${BREW_OPENSSL_PATH}")
    list(APPEND CMAKE_PREFIX_PATH ${BREW_OPENSSL_PATH})
  endif()
endif()

find_package(OpenSSL QUIET)
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
  find_library(OPENSSL_LIBRARIES ssl)
  find_path(OPENSSL_INCLUDE_DIR openssl/ssl.h)
  if(OPENSSL_LIBRARIES AND OPENSSL_INCLUDE_DIRS)
    set(OPENSSL_FOUND TRUE)
  endif()
endif()

if(OPENSSL_FOUND)
  message(STATUS "Found openssl libraries at: ${OPENSSL_LIBRARIES}")
  message(STATUS "Found openssl includes at: ${OPENSSL_INCLUDE_DIR}")
else()
  message(FATAL_ERROR "Couldn't find openssl via cmake, pkg-config or find_library")
endif()
  

# ** zstd
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
else()
  message(FATAL_ERROR "Couldn't find zstd via cmake, pkg-config or find_library")
endif()

# ** libcurl

if(PKGCONFIG_FOUND)
  pkg_check_modules(CURL libcurl)
  if(CURL_FOUND)
    message(STATUS "Found curl with pkg-config")
    set(CURL_LIBRARIES ${CURL_LINK_LIBRARIES})
  endif()
endif()
if(NOT CURL_FOUND)
  find_package(Curl QUIET)
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
else()
  message(FATAL_ERROR "Couldn't find curl via cmake, pkg-config or find_library")
endif()

############################
# * External projects

option(USE_EXTERNAL_JSON "Use an external JSON library" OFF)

add_subdirectory(external)

###########################
# libzef itself

# Explicit file list
set(SOURCES
    src/blobs.cpp
    src/graph.cpp
    src/high_level_api.cpp
    src/low_level_api.cpp
    src/mmap.cpp
    src/observable.cpp
    src/synchronization.cpp
    src/tools.cpp
    src/zwitch.cpp
    src/verification.cpp
    src/scalars.cpp
    src/tensor.cpp
    src/tokens.cpp
    src/zefref.cpp
    src/uids.cpp
    src/from_json.cpp

    src/butler/butler.cpp
    src/butler/butler_zefhub.cpp
    src/butler/communication.cpp
    src/butler/threadsafe_map.cpp
    src/butler/auth.cpp
    src/butler/auth_server.cpp
    )

# set(HEADERS
#     include/blobs.h
#     include/export_statement.h
#     include/fwd_declarations.h
#     include/observable.h
#     include/graph.h
#     include/high_level_api.h
#     include/low_level_api.h
#     include/mmap.h
#     include/synchronization.h
#     include/tensor.h
#     include/tokens.h
#     include/tools.h
#     include/verification.h
#     include/xxhash64.h
#     include/scalars.h
#     include/zef_enums.h.names_map.gen
#     include/zef_enums.h.zefenumstruct.gen
#     include/zef_script.h
#     include/zef_script_python.h
#     include/zefDB.h
#     include/zefDB_utils.h
#     include/zefref.h
#     include/zwitch.h

#     include/butler/butler.h
#     include/butler/butler_zefhub.h
#     include/butler/msgqueue.h
#     include/butler/messages.h
#     include/butler/communication.h
#     )

add_library (zef SHARED ${SOURCES})

target_include_directories(zef PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include/zefDB>
  )

target_include_directories(zef PUBLIC ${ZSTD_INCLUDE_DIRS})
target_include_directories(zef PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(zef PUBLIC ${OPENSSL_INCLUDE_DIR})

target_link_libraries(zef PUBLIC pybind11::embed)
target_link_libraries(zef PUBLIC ${OPENSSL_LIBRARIES})
target_link_libraries(zef PUBLIC ${ZSTD_LIBRARIES})
target_link_libraries(zef PUBLIC ${CURL_LIBRARIES})

MESSAGE(STATUS "Compiler version is ${CMAKE_CXX_COMPILER_VERSION}")
if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9)
  MESSAGE(STATUS "Including stdc++fs into libraries.")
  target_link_libraries(zef PUBLIC stdc++fs)
endif()

target_link_libraries(zef PUBLIC nlohmann_json::nlohmann_json)
target_link_libraries(zef PUBLIC pybind11_json)
target_link_libraries(zef PUBLIC phmap)
target_link_libraries(zef PUBLIC range-v3)
target_link_libraries(zef PRIVATE doctest)
target_link_libraries(zef PUBLIC websocketpp)
# target_link_libraries(zef PRIVATE blake3)
target_link_libraries(zef PRIVATE curl)
target_link_libraries(zef PRIVATE jwt-cpp)

if (NOT APPLE)
  target_link_libraries(zef PUBLIC rt)
endif()

set_target_properties(zef PROPERTIES
  CXX_STANDARD 17
  COMPILE_DEFINITIONS
  "DOCTEST_CONFIG_DISABLE;BUILDING_LIBZEF"
  CXX_VISIBILITY_PRESET hidden
  # This is apparently something that needs a check for all compilers, so will disable for now.
  # INTERPROCEDURAL_OPTIMIZATION ON
  # PUBLIC_HEADER "${HEADERS}"
  )
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set_target_properties(zef PROPERTIES
    CXX_VISIBILITY_PRESET default)
endif()

target_compile_definitions(zef PUBLIC LIBZEF_NO_FORKING=1)

if(ZEFDB_ALLOW_NO_TLS)
  target_compile_definitions(zef PUBLIC ZEFDB_ALLOW_NO_TLS)
endif()
if(ZEFDB_TEST_NO_MMAP_CHECKS)
  target_compile_definitions(zef PUBLIC ZEFDB_TEST_NO_MMAP_CHECKS)
endif()
  

# Extra things needed for Visual Studio
if(MSVC)
	target_compile_options(zef PUBLIC "/permissive-")
endif()

install ( TARGETS zef
  EXPORT zefDB
  LIBRARY DESTINATION lib
  PUBLIC_HEADER DESTINATION include/zefDB
  )
install(FILES auth.html DESTINATION share/zefDB)
add_custom_command(TARGET zef POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E make_directory 
                       ${CMAKE_CURRENT_BINARY_DIR}/../share/zefDB/
                   COMMAND ${CMAKE_COMMAND} -E create_symlink
                       ${CMAKE_CURRENT_SOURCE_DIR}/auth.html
                       ${CMAKE_CURRENT_BINARY_DIR}/auth.html
                   )

#############################
# The testing executable

list(REMOVE_ITEM SOURCES src/library_entrypoint.cpp)
list(APPEND SOURCES src/doctest_stub.cpp)

add_executable (zeftest ${SOURCES})
set_target_properties(zeftest PROPERTIES
  CXX_STANDARD 17
  # COMPILE_DEFINITIONS "DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN"
  EXCLUDE_FROM_ALL TRUE
  )

# Always no forking for the test script
target_compile_definitions(zeftest PUBLIC LIBZEF_NO_FORKING=1)

# target_include_directories(zeftest SYSTEM PUBLIC ${Python3_INCLUDE_DIRS})
# target_link_libraries(zeftest PUBLIC ${Python3_LIBRARIES})
target_link_libraries(zeftest PUBLIC pybind11::embed)

if(UNIX AND NOT APPLE)
    set_target_properties(zef PROPERTIES INSTALL_RPATH "$ORIGIN")
elseif(APPLE)
    set_target_properties(zef PROPERTIES INSTALL_RPATH "@loader_path")
endif()


target_include_directories(zeftest PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(zeftest PRIVATE ${RANGE_INCLUDE_DIRS})
target_include_directories(zeftest PRIVATE ${doctest_INCLUDES})
# target_include_directories(zeftest PUBLIC ${pybind11_INCLUDE_DIRS})

target_include_directories(zeftest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/blake3/c)


#############################
# Butler testing executable

add_executable (butler_test src/butler/butler_test.cpp)
set_target_properties(butler_test PROPERTIES
  CXX_STANDARD 17
  EXCLUDE_FROM_ALL TRUE
  )

# target_link_libraries(butler_test PUBLIC ${Python3_LIBRARIES})

# Always no forking for the test script
target_compile_definitions(butler_test PUBLIC LIBZEF_NO_FORKING=1)

# target_include_directories(butler_test SYSTEM PUBLIC ${Python3_INCLUDE_DIRS})
target_link_libraries(butler_test PUBLIC pybind11::embed)

target_link_libraries(butler_test PRIVATE zef)

#############################
# Danny testing stuff

add_executable (danny_test butler/test_wholefilemapping.cpp)
set_target_properties(danny_test PROPERTIES
  CXX_STANDARD 17
  EXCLUDE_FROM_ALL TRUE
  )

# target_link_libraries(butler_test PUBLIC ${Python3_LIBRARIES})

# Always no forking for the test script
target_compile_definitions(danny_test PUBLIC LIBZEF_NO_FORKING=1)

# target_include_directories(danny_test SYSTEM PUBLIC ${Python3_INCLUDE_DIRS})
target_link_libraries(danny_test PUBLIC pybind11::embed)

target_link_libraries(danny_test PRIVATE zef)

# set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(danny_test PRIVATE Threads::Threads)
