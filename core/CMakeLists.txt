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

cmake_minimum_required(VERSION 3.13)

include(CMakeDependentOption)

# Use this to distinguish the full project from the individual subdirectories.
project(ZefCore CXX)

#########################
# customizable variables

option(LOCAL_ZEFTYPES "Disable getting zeftypes from zefhub" FALSE)
option(ZEFDB_ALLOW_NO_TLS "Allow insecure websockets for ZefHub connections" TRUE)
option(ZEFDB_TEST_NO_MMAP_CHECKS "Test for mmap slowness" FALSE)
option(LIBZEF_PYZEF_BUNDLED "libzef is going to be bundled together with pyzef, which means putting files in different places." FALSE)
option(LIBZEF_FORCE_ASSERTS "Make asserts happen regardless of debug/release build" FALSE)

# When bundling for pyzef we default to bundling everything else.
cmake_dependent_option(LIBZEF_STATIC "Compile as a static library rather than a dynamically linked library" FALSE "NOT LIBZEF_PYZEF_BUNDLED" TRUE)
cmake_dependent_option(LIBZEF_BUNDLED_ZSTD "Bundle libzstd statically in the library" FALSE "NOT LIBZEF_PYZEF_BUNDLED" TRUE)
cmake_dependent_option(LIBZEF_BUNDLED_CURL "Bundle libcurl statically in the library" FALSE "NOT LIBZEF_PYZEF_BUNDLED" TRUE)
cmake_dependent_option(LIBZEF_BUNDLED_LIBARCHIVE "Bundle libarchive statically in the library" FALSE "NOT LIBZEF_PYZEF_BUNDLED" TRUE)

if(ZEFDB_ALLOW_NO_TLS)
  message(STATUS "Insecure connections are permitted")
else()
  message(STATUS "Insecure connections are disallowed")
endif()

if(ZEFDB_TEST_NO_MMAP_CHECKS)
  message(WARNING "Testing mmap slowness - graphs are given 10MB of memory and other ensure page checks are disabled.")
endif()

############################
# * External projects

# option(USE_EXTERNAL_JSON "Use an external JSON library" OFF)

# add_subdirectory(external)
include(external/external.cmake)

############################
# * The library

# Explicit file list
set(SOURCES
    src/blobs.cpp
    src/graph.cpp
    src/high_level_api.cpp
    src/ops_imperative.cpp
    src/zefops.cpp
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
    src/zef_config.cpp
    src/globals.cpp
    src/external_handlers.cpp
    src/conversions.cpp
    src/tar_file.cpp
    src/revise.cpp

    src/butler/butler.cpp
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
#     include/butler/msgqueue.h
#     include/butler/messages.h
#     include/butler/communication.h
#     )

if(LIBZEF_STATIC)
add_library (zef STATIC ${SOURCES})
set_target_properties(zef PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)
else()
add_library (zef SHARED ${SOURCES})
endif()
add_library (zef::zef ALIAS zef)

target_include_directories(zef PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include/zef>
  )

MESSAGE(STATUS "Compiler version is ${CMAKE_CXX_COMPILER_VERSION}")
if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9)
  MESSAGE(STATUS "Including stdc++fs into libraries.")
  target_link_libraries(zef PUBLIC stdc++fs)
endif()

target_link_libraries(zef PUBLIC nlohmann_json::nlohmann_json)
target_link_libraries(zef PRIVATE yaml-cpp::yaml-cpp)
target_link_libraries(zef PUBLIC external::phmap)
target_link_libraries(zef PUBLIC external::range-v3)
target_link_libraries(zef PRIVATE external::doctest)
target_link_libraries(zef PUBLIC external::websocketpp)
# target_link_libraries(zef PRIVATE blake3)
target_link_libraries(zef PRIVATE external::libcurl)
target_link_libraries(zef PRIVATE external::jwt-cpp)
# Until I fix up our header deps this will have to be public
target_link_libraries(zef PUBLIC external::libzstd)
target_link_libraries(zef PUBLIC external::archive)

if (UNIX AND NOT APPLE)
  target_link_libraries(zef PUBLIC rt)
endif()

set_target_properties(zef PROPERTIES
  CXX_STANDARD 17
  CXX_VISIBILITY_PRESET hidden
  # This is apparently something that needs a check for all compilers, so will disable for now.
  # INTERPROCEDURAL_OPTIMIZATION ON
  # PUBLIC_HEADER "${HEADERS}"
  )
target_compile_definitions(zef PRIVATE DOCTEST_CONFIG_DISABLE BUILDING_LIBZEF)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  if(NOT WIN32)
    target_compile_definitions(zef PRIVATE DEBUG)
    target_compile_options(zef PRIVATE -rdynamic -ggdb -g)
  endif()
endif()

if(ZEFDB_ALLOW_NO_TLS)
  target_compile_definitions(zef PUBLIC ZEFDB_ALLOW_NO_TLS)
endif()
if(ZEFDB_TEST_NO_MMAP_CHECKS)
  target_compile_definitions(zef PUBLIC ZEFDB_TEST_NO_MMAP_CHECKS)
endif()

# Make auth.html accessible for uninstalled builds
# Note: this is really only for dynamic builds, but it will be a double-bounced
# link for static builds
if(NOT MSVC)
  add_custom_command(TARGET zef POST_BUILD
                    COMMAND ${CMAKE_COMMAND} -E create_symlink
                        ${CMAKE_CURRENT_SOURCE_DIR}/auth.html
                        ${CMAKE_CURRENT_BINARY_DIR}/auth.html
                    )
endif()
  
# Extra things needed for Visual Studio
if(MSVC)
#	target_compile_options(zef PUBLIC "/permissive-")
	target_compile_definitions(zef PUBLIC NOMINMAX)
endif()

if(LIBZEF_PYZEF_BUNDLED)
  # install(TARGETS zef LIBRARY DESTINATION .)
  install(FILES auth.html DESTINATION .)
  install(FILES ${LICENSE_FILES} DESTINATION .)
else()
  install ( TARGETS zef
    EXPORT zef
    LIBRARY DESTINATION lib
    PUBLIC_HEADER DESTINATION include/zef
    )
    install(FILES auth.html DESTINATION share/zef)
    install(FILES ${LICENSE_FILES} DESTINATION share/zef)
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

#if(NOT MSVC)
  # We have to get all files that will be cogged - this is a little weird.
  file(GLOB cog_files ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cog ${CMAKE_CURRENT_SOURCE_DIR}/include/*.cog)
  list(TRANSFORM cog_files REPLACE "[.]cog$" ".gen" OUTPUT_VARIABLE cog_files_output_temp)
  list(TRANSFORM cog_files_output_temp REPLACE "^${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}" OUTPUT_VARIABLE cog_files_output)

  # This is a little dodgy, but at least these filenames as so specific, they
  # won't be accidentally included elsewhere. Need to make them be included when
  # installed to a public header directory though.
  target_include_directories(zef PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/src)
  target_include_directories(zef PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/include)

  add_custom_target(cogbuild
    DEPENDS ${cog_files_output})
  add_dependencies(zef cogbuild)
  
  # Do the cogging itself
  add_custom_command(
    # Note: this is a bit more verbose than it normally would be, but that's
    # because we never want to confuse this with files in the source directory.
    # This actually happens in the github CI when it wants to do a hash compare
    # to see if it needs to rebuild.
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/zeftypes_EN.json
        ${CMAKE_CURRENT_BINARY_DIR}/zeftypes_ET.json
        ${CMAKE_CURRENT_BINARY_DIR}/zeftypes_RT.json
        ${CMAKE_CURRENT_BINARY_DIR}/zeftypes_KW.json
        ${cog_files}
    OUTPUT ${cog_files_output}
    COMMENT "Running cog"
    # We don't install cog at run now, but have to have it around as a dependency.
    # COMMAND ${Python3_EXECUTABLE} -mpip install cogapp
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/scripts/run_cog_gen.py ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_BINARY_DIR}
    )

  if(LOCAL_ZEFTYPES)
    # Copy over the bootstrap files if the non-bootstrap versions don't exist
    add_custom_command(
      DEPENDS templates/zeftypes_bootstrap_ET.json
      OUTPUT zeftypes_ET.json
      COMMENT "Copying ET bootstrap"
      COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/templates/zeftypes_bootstrap_ET.json zeftypes_ET.json)
    add_custom_command(
      DEPENDS templates/zeftypes_bootstrap_RT.json
      OUTPUT zeftypes_RT.json
      COMMENT "Copying RT bootstrap"
      COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/templates/zeftypes_bootstrap_RT.json zeftypes_RT.json)
    add_custom_command(
      DEPENDS templates/zeftypes_bootstrap_EN.json
      OUTPUT zeftypes_EN.json
      COMMENT "Copying EN bootstrap"
      COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/templates/zeftypes_bootstrap_EN.json zeftypes_EN.json)
    add_custom_command(
      DEPENDS templates/zeftypes_bootstrap_KW.json
      OUTPUT zeftypes_KW.json
      COMMENT "Copying KW bootstrap"
      COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/templates/zeftypes_bootstrap_KW.json zeftypes_KW.json)
  else()
    # We can find the get_zeftypes.py file in various places depending on if we
    # have been packaged into a sdist with python.
    find_file(get_zeftypes_file get_zeftypes.py REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR}/../scripts ${CMAKE_CURRENT_SOURCE_DIR}/scripts)
    add_custom_command(
      OUTPUT zeftypes_ET.json
      OUTPUT zeftypes_RT.json
      OUTPUT zeftypes_EN.json
      OUTPUT zeftypes_KW.json
      COMMENT "Grabbing latest zeftypes from zefhub using the guest account."
      COMMAND ${Python3_EXECUTABLE} ${get_zeftypes_file})
  endif()

#endif()

# Finally handle the static/dynamic linking part in export_statement.h
configure_file(include/export_statement.h.in include/export_statement.h)

#########################################
# * All tests
enable_testing ()

#####################
# ** C tests
# # Seems like some annoying manual dependency is required in here
# add_test (NAME zeftest_build COMMAND "${CMAKE_COMMAND}" --build ${CMAKE_BINARY_DIR} --target zeftest)

# add_test (NAME zeftest COMMAND zeftest)
# set_property(TEST zeftest PROPERTY DEPENDS zeftest_build)


if(WIN32)
  add_subdirectory(test_windows EXCLUDE_FROM_ALL)
endif()