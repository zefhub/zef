# TODO: Fix up the zeftypes files so that they are grabbed and used for cogging
# inside of the build directories only.

cmake_minimum_required(VERSION 3.8)
cmake_policy(SET CMP0077 NEW)

# Use this to distinguish the full project from the individual subdirectories.
project(ZefCore CXX)

#########################
# customizable variables

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic -ggdb -g")
  add_definitions(-DDEBUG)
endif()

option(LOCAL_ZEFTYPES "Disable getting zeftypes from zefhub" FALSE)
option(ZEFDB_ALLOW_NO_TLS "Allow insecure websockets for ZefHub connections" TRUE)
option(ZEFDB_TEST_NO_MMAP_CHECKS "Test for mmap slowness" FALSE)
option(LIBZEF_PYZEF_BUNDLED "libzef is going to be bundled together with pyzef, which  means putting files in different places." FALSE)
option(LIBZEF_BUNDLED_ZSTD "Bundle libzstd in the library" TRUE)



if(ZEFDB_ALLOW_NO_TLS)
  message(STATUS "Insecure connections are permitted")
else()
  message(STATUS "Insecure connections are disallowed")
endif()

if(ZEFDB_TEST_NO_MMAP_CHECKS)
  message(WARNING "Testing mmap slowness - graphs are given 10MB of memory and other ensure page checks are disabled.")
endif()

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
  message(STATUS "Appending brew install prefix of ${MACOS_BREW_PREFIX} to cmake path. Manually set MACOS_BREW_PREFIX='' to disable this behaviour.")

  list(APPEND CMAKE_PREFIX_PATH ${MACOS_BREW_PREFIX})
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
  if(OPENSSL_LIBRARIES AND OPENSSL_INCLUDE_DIR)
    set(OPENSSL_FOUND TRUE)
  endif()
endif()

if(OPENSSL_FOUND)
  message(STATUS "Found openssl libraries at: ${OPENSSL_LIBRARIES}")
  message(STATUS "Found openssl includes at: ${OPENSSL_INCLUDE_DIR}")
else()
  message(FATAL_ERROR "Couldn't find openssl via cmake, pkg-config or find_library")
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

############################
# * The library

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
  $<INSTALL_INTERFACE:include/zef>
  )

target_include_directories(zef PUBLIC ${CURL_INCLUDE_DIRS})
target_include_directories(zef PUBLIC ${OPENSSL_INCLUDE_DIR})

target_link_libraries(zef PUBLIC ${OPENSSL_LIBRARIES})
target_link_libraries(zef PUBLIC ${CURL_LIBRARIES})

MESSAGE(STATUS "Compiler version is ${CMAKE_CXX_COMPILER_VERSION}")
if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9)
  MESSAGE(STATUS "Including stdc++fs into libraries.")
  target_link_libraries(zef PUBLIC stdc++fs)
endif()

target_link_libraries(zef PUBLIC nlohmann_json::nlohmann_json)
target_link_libraries(zef PUBLIC phmap)
target_link_libraries(zef PUBLIC range-v3)
target_link_libraries(zef PRIVATE doctest)
target_link_libraries(zef PUBLIC websocketpp)
# target_link_libraries(zef PRIVATE blake3)
target_link_libraries(zef PRIVATE curl)
target_link_libraries(zef PRIVATE jwt-cpp)
# Until I fix up our header deps this will have to be public
target_link_libraries(zef PUBLIC libzstd_internal)

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

if(LIBZEF_PYZEF_BUNDLED)
  install (TARGETS zef LIBRARY DESTINATION .)
  install(FILES auth.html DESTINATION .)
else()
  install ( TARGETS zef
    EXPORT zef
    LIBRARY DESTINATION lib
    PUBLIC_HEADER DESTINATION include/zef
    )
  install(FILES auth.html DESTINATION share/zef)
endif()

############################
# * Subprojects

# add_subdirectory(tests)

############################
# * Cmake install files

# include(CMakePackageConfigHelpers)
# # configure_package_config_file(${CMAKE_CURRENT_SOURCE_DIR}/zefDBConfig.cmake.in
# configure_file(${CMAKE_CURRENT_SOURCE_DIR}/zefDBConfig.cmake.in
#   "${CMAKE_CURRENT_BINARY_DIR}/zefDBConfig.cmake"
#   # INSTALL_DESTINATION lib/cmake/zefDB
#   @ONLY
# )
# # install(FILES "${CMAKE_CURRENT_BINARY_DIR}/zefDBConfig.cmake" DESTINATION lib/cmake/zefDB)
# install(EXPORT zef DESTINATION lib/cmake/zef)

############################
# * Cog commands

find_package(Python3 REQUIRED COMPONENTS Interpreter)

set(COG_DUMMY ${CMAKE_CURRENT_SOURCE_DIR}/dummy_cog.tracker)

if(NOT MSVC)
  # We have to get all files that will be cogged - this is a little weird.
  file(GLOB cog_files ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cog ${CMAKE_CURRENT_SOURCE_DIR}/include/*.cog)
  list(TRANSFORM cog_files REPLACE "[.]cog$" ".gen" OUTPUT_VARIABLE cog_files_output)

  # MESSAGE(STATUS "cog files" "${cog_files}")
  # MESSAGE(STATUS "cog outputs" "${cog_files_output}")

  # target_include_directories(zef PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/include)

  add_custom_target(cogbuild
    # DEPENDS ${COG_DUMMY})
    DEPENDS ${cog_files_output})
  add_dependencies(zef cogbuild)
  
  # Do the cogging itself
  add_custom_command(
    DEPENDS zeftypes_EN.json zeftypes_ET.json zeftypes_RT.json zeftypes_KW.json ${cog_files}
    OUTPUT ${cog_files_output}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running cog"
    # We don't install cog at run now, but have to have it around as a dependency.
    # COMMAND ${Python3_EXECUTABLE} -mpip install cogapp
    COMMAND ${Python3_EXECUTABLE} scripts/run_cog_gen.py
    )

  if(LOCAL_ZEFTYPES)
    # Copy over the bootstrap files if the non-bootstrap versions don't exist
    add_custom_command(
      DEPENDS templates/zeftypes_bootstrap_ET.json
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_ET.json
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Copying ET bootstrap"
      COMMAND cp templates/zeftypes_bootstrap_ET.json zeftypes_ET.json)
    add_custom_command(
      DEPENDS templates/zeftypes_bootstrap_RT.json
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_RT.json
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Copying RT bootstrap"
      COMMAND cp templates/zeftypes_bootstrap_RT.json zeftypes_RT.json)
    add_custom_command(
      DEPENDS templates/zeftypes_bootstrap_EN.json
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_EN.json
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Copying EN bootstrap"
      COMMAND cp templates/zeftypes_bootstrap_EN.json zeftypes_EN.json)
    add_custom_command(
      DEPENDS templates/zeftypes_bootstrap_KW.json
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_KW.json
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Copying KW bootstrap"
      COMMAND cp templates/zeftypes_bootstrap_KW.json zeftypes_KW.json)
  else()
    # We can find the get_zeftypes.py file in various places depending on if we
    # have been packaged into a sdist with python.
    find_file(get_zeftypes_file get_zeftypes.py REQUIERD HINTS ${CMAKE_CURRENT_SOURCE_DIR}/../scripts ${CMAKE_CURRENT_SOURCE_DIR}/scripts)
    add_custom_command(
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_ET.json
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_RT.json
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_EN.json
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_KW.json
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Grabbing latest zeftypes from zefhub using the guest account."
      # COMMAND ${Python3_EXECUTABLE} -mpip install structlog
      COMMAND ${Python3_EXECUTABLE} ${get_zeftypes_file})
  endif()

endif()

#########################################
# * All tests
enable_testing ()

#####################
# ** C tests
# # Seems like some annoying manual dependency is required in here
# add_test (NAME zeftest_build COMMAND "${CMAKE_COMMAND}" --build ${CMAKE_BINARY_DIR} --target zeftest)

# add_test (NAME zeftest COMMAND zeftest)
# set_property(TEST zeftest PROPERTY DEPENDS zeftest_build)