cmake_minimum_required(VERSION 3.8)

# Use this to distinguish the full project from the individual subdirectories.
project(PyZef)

############################
# * Python

# if(ENV{ZEFDB_STATIC_PYTHON})
#   # This only works for newer cmakes
#   set(Python3_FIND_STRATEGY LOCATION)
#   set(Python3_USE_STATIC_LIBS TRUE)
#   if($ENV{FORCE_PYTHON_VERSION})
#     set(PYTHON_VERSION_ARG "EXACT" $ENV{FORCE_PYTHON_VERSION})
#   endif()
# endif()

# Going to rely on pybind for the python libraries, as a way to circumvent issues with earlier cmake versions.

find_package(Python3 ${PYTHON_VERSION_ARG} REQUIRED COMPONENTS Development.Module Interpreter)

# Prepare build deps
# option(ZEFDB_INSTALL_PIP_DEPS "Install python build dependencies using Python3_EXECUTEABLE -mpip" ON)
# if(ZEFDB_INSTALL_PIP_DEPS)
#   message(STATUS "Installing python build dependencies using pip.")
#   execute_process(COMMAND ${Python3_EXECUTABLE} -m pip install -r ${CMAKE_CURRENT_SOURCE_DIR}/requirements_build.txt)
# endif()

# Find pybind
# Note: pybind is needed for libzef as a convenience.
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
  set(LIBZEF_PYZEF_BUNDLED TRUE)
  # Note: no EXCLUDE_FROM_ALL as we want to install the lib file as well.
  add_subdirectory(${Zef_DIR} ${CMAKE_CURRENT_BINARY_DIR}/zef-build)
endif()

############################
# * External deps

# TODO: This will need better handling if zef is found as a package
add_subdirectory(external)

############################
# * Library

pybind11_add_module (pyzef
  build_zefDB.cpp
  zefops.cpp
  internals.cpp
  )

if(LIBZEF_PYZEF_BUNDLED)
  if(APPLE)
    set_target_properties(pyzef PROPERTIES INSTALL_RPATH "@rpath")
  else()
    set_target_properties(pyzef PROPERTIES INSTALL_RPATH "\$ORIGIN")
  endif()
endif()

set_target_properties(pyzef
    PROPERTIES
    CXX_STANDARD 17)

# target_include_directories (pyzef PUBLIC ${RANGE_INCLUDE_DIRS})

# if(UNIX AND NOT APPLE)
#   # This should take us up the lib directory where both libzef and libpython are.
#   set_target_properties(pyzef PROPERTIES INSTALL_RPATH "$ORIGIN")
# elseif(APPLE)
#   set_target_properties(pyzef PROPERTIES INSTALL_RPATH "@loader_path")
# endif()
# add_custom_command(TARGET pyzef
#   POST_BUILD
#   COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE:pyzef> ${ZEFDB_DIR}/zef/`basename $<TARGET_FILE:pyzef>`
#   COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE:zef> ${ZEFDB_DIR}/zef/`basename $<TARGET_FILE:zef>`
# )

target_link_libraries(pyzef PUBLIC zef)
# target_include_directories(pyzef PRIVATE ${Zef_INCLUDE})

target_link_libraries(pyzef PUBLIC pybind11_json)



# Things needed for Visual Studio
if(MSVC)
	target_compile_options(pyzef PUBLIC "/permissive-")
    target_compile_options(pyzef PUBLIC "/bigobj")
    set_target_properties(pyzef PROPERTIES SUFFIX ".pyd")
    set_target_properties(pyzef PROPERTIES ENABLE_EXPORTS 1)
	set_target_properties(pyzef PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS 1)
endif()
  
# We are assuming this is an install into the python extension build directory.
# Anything else should not be installing
install(TARGETS pyzef LIBRARY DESTINATION .)

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