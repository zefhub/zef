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

# Use this to distinguish the full project from the individual subdirectories.
project(PyZef)

############################
# * Python

find_package(Python3 ${PYTHON_VERSION_ARG} REQUIRED COMPONENTS Development.Module Interpreter)

# Find pybind
message(STATUS "Using python executable of ${Python3_EXECUTABLE}")
execute_process(COMMAND ${Python3_EXECUTABLE} -m pybind11 --cmakedir
  OUTPUT_VARIABLE PYBIND_CMAKE_LOCATION
  OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Using pybind11 cmake location of ${PYBIND_CMAKE_LOCATION}")
list(APPEND CMAKE_PREFIX_PATH ${PYBIND_CMAKE_LOCATION})
find_package(pybind11 CONFIG REQUIRED)

message(STATUS "The following message may not be reliable...")
message(STATUS "Python libraries found at ${Python3_LIBRARIES}")
message(STATUS "Python executable found at ${Python3_EXECUTABLE}")

# ############################
# # * Pip

# add_custom_command(
#   OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/pip_output.log
#   DEPENDS requirements.txt
#   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#   COMMENT "Pip-ing"
#   COMMAND ${Python3_EXECUTABLE} -mpip install -r requirements.txt | tee pip_output.log)
# add_custom_target(pip ALL
#   DEPENDS pip_output.log)
# add_dependencies(pyzef pip)
# # Sometimes zef needs this too
# add_dependencies(zef pip)



############################
# * Zef dep

# We either use an installed zef, or fallback to using a zefcore source folder.

if(NOT DEFINED LIBZEF_PYZEF_BUNDLED)
  find_package(zef QUIET)
  if(zef_FOUND)
    set(LIBZEF_PYZEF_BUNDLED FALSE)
  else()
    message(WARNING "Did not find zef as a compiled package. Going to look for source folder")
    set(LIBZEF_PYZEF_BUNDLED TRUE)
  endif()
endif()

if(LIBZEF_PYZEF_BUNDLED)
  find_path(Zef_DIR zefDBConfig.cmake.in REQUIRED)
  # Note: no EXCLUDE_FROM_ALL as we want to install the lib file as well.
  add_subdirectory(${Zef_DIR} ${CMAKE_CURRENT_BINARY_DIR}/zef-build)
endif()

############################
# * External deps

# TODO: This will need better handling if zef is found as a package
include(external/external.cmake)

############################
# * Library

pybind11_add_module (pyzef
  build_zefDB.cpp
  zefops.cpp
  internals.cpp
  )

if(LIBZEF_PYZEF_BUNDLED)
  if(APPLE)
    set_target_properties(pyzef PROPERTIES INSTALL_RPATH "@loader_path")
  else()
    set_target_properties(pyzef PROPERTIES INSTALL_RPATH "\$ORIGIN")
  endif()
endif()

set_target_properties(pyzef
    PROPERTIES
    CXX_STANDARD 17)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  if(NOT WIN32)
    target_compile_definitions(pyzef PRIVATE DEBUG)
    target_compile_options(pyzef PRIVATE -rdynamic -ggdb -g)
  endif()
endif()

target_link_libraries(pyzef PUBLIC zef::zef)

target_link_libraries(pyzef PUBLIC pybind11_json)

# We are assuming this is an install into the python extension build directory.
# Anything else should not be installing
install(TARGETS pyzef LIBRARY DESTINATION .)

# We need the licenses for pybind11 and the pybind_json libraries. Technically,
# as we could be compiling anywhere, we should grab the pybind11 license from
# the site-packages directory... however it is not shipped in there! So instead
# we will do our due diligence and manually include the license file.
install(FILES
  LICENSE.pybind11
  LICENSE.pybind_json
  DESTINATION .)

# Finally we'll make a file up that references these license files and our own.
# Note that the libzef license is going to be installed in the directory as
# LICENSE.libzef and referencing its own dependencies.
set(TOP_LICENSE_FILE "${CMAKE_CURRENT_BINARY_DIR}/LICENSE")
install(FILES
  ${TOP_LICENSE_FILE}
  DESTINATION .)

find_file(REPO_LICENSE
  LICENSE
  PATHS ${CMAKE_CURRENT_SOURCE_DIR}/../ ${CMAKE_CURRENT_SOURCE_DIR}/../../
  NO_DEFAULT_PATH
  NO_CACHE
  REQUIRED)
file(READ ${REPO_LICENSE} OUR_LICENSE)
file(WRITE ${TOP_LICENSE_FILE} ${OUR_LICENSE})

file(APPEND ${TOP_LICENSE_FILE} "\nThe following files contain additional bundled licenses:\n\nLICENSE.libzef\nLICENSE.pybind11\nLICENSE.pybind_json")

#########################################
# * All tests
enable_testing ()

######################
# ** Token tests
add_test (NAME token_test
  COMMAND bash assert_no_created_tokens.sh
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/scripts
  )
set_tests_properties(token_test PROPERTIES
  ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}:$ENV{PYTHONPATH};ZEFDB_QUIET=YES;ZEFHUB_AUTH_KEY=GUEST"
  DEPENDS pyzef
  TIMEOUT 30)

######################
# ** Python tests
add_test (NAME pyzef_test
  COMMAND python3 -munittest discover -s ${CMAKE_CURRENT_SOURCE_DIR}/tests -p *.py
  # COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_script.py # This won't work on GH but will at least let me test the start of the program.
  )
  set_property(TEST pyzef_test PROPERTY DEPENDS pyzef)
set_tests_properties(pyzef_test PROPERTIES
  ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}:$ENV{PYTHONPATH};ZEFDB_QUIET=YES;ZEFDB_DEVELOPER_LOCAL_TOKENS=1;ZEFHUB_URL=MASTER"
  DEPENDS pyzef
  TIMEOUT 60)
