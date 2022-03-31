cmake_minimum_required(VERSION 3.8)

# Use this to distinguish the full project from the individual subdirectories.
project(Zef)

#########################
# customizable variables

# Force using environment variable when specified
if(DEFINED ENV{LOCAL_ZEFTYPES})
  set(LOCAL_ZEFTYPES $ENV{LOCAL_ZEFTYPES} CACHE BOOL "Disable getting zeftypes from zefhub" FORCE)
else()
  set(LOCAL_ZEFTYPES FALSE CACHE BOOL "Disable getting zeftypes from zefhub")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic -ggdb -g")
  add_definitions(-DDEBUG)
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
# * Python

if(ENV{ZEFDB_STATIC_PYTHON})
  # This only works for newer cmakes
  set(Python3_FIND_STRATEGY LOCATION)
  set(Python3_USE_STATIC_LIBS TRUE)
  if($ENV{FORCE_PYTHON_VERSION})
    set(PYTHON_VERSION_ARG "EXACT" $ENV{FORCE_PYTHON_VERSION})
  endif()
endif()

# Going to rely on pybind for the python libraries, as a way to circumvent issues with earlier cmake versions.

# Things for debugging brew python version issues
# execute_process(COMMAND which python3
#   OUTPUT_VARIABLE Python3_EXECUTABLE
#   OUTPUT_STRIP_TRAILING_WHITESPACE)
# set(Python3_FIND_STRATEGY VERSION)
# set(Python3_FIND_FRAMEWORK LAST)
find_package(Python3 ${PYTHON_VERSION_ARG} REQUIRED COMPONENTS Development Interpreter)

# Prepare build deps
option(ZEFDB_INSTALL_PIP_DEPS "Install python build dependencies using Python3_EXECUTEABLE -mpip" ON)
if(ZEFDB_INSTALL_PIP_DEPS)
  message(STATUS "Installing python build dependencies using pip.")
  execute_process(COMMAND ${Python3_EXECUTABLE} -m pip install -r ${CMAKE_CURRENT_SOURCE_DIR}/requirements_build.txt)
endif()

# Find pybind
# Note: pybind is needed for libzef as a convenience.
execute_process(COMMAND ${Python3_EXECUTABLE} -m pybind11 --cmakedir
  OUTPUT_VARIABLE PYBIND_CMAKE_LOCATION
  OUTPUT_STRIP_TRAILING_WHITESPACE)
message(STATUS "Using pybind11 cmake location of ${PYBIND_CMAKE_LOCATION}")
list(APPEND CMAKE_PREFIX_PATH ${PYBIND_CMAKE_LOCATION})
find_package(pybind11 CONFIG REQUIRED)

message(STATUS "The following message may not be reliable...")
message(STATUS "Python libraries found at ${Python3_LIBRARIES}")
message(STATUS "Python executable found at ${Python3_EXECUTABLE}")

############################
# * Subprojects

add_subdirectory(src_cpp/core)
add_subdirectory(src_cpp/build_python_package)
add_subdirectory(src_cpp/c_tests)


############################
# * Cmake install files

# include(CMakePackageConfigHelpers)
# # configure_package_config_file(${CMAKE_CURRENT_SOURCE_DIR}/zefDBConfig.cmake.in
# configure_file(${CMAKE_CURRENT_SOURCE_DIR}/zefDBConfig.cmake.in
#   "${CMAKE_CURRENT_BINARY_DIR}/zefDBConfig.cmake"
#   # INSTALL_DESTINATION lib/cmake/zefDB
#   @ONLY
# )
# install(FILES "${CMAKE_CURRENT_BINARY_DIR}/zefDBConfig.cmake" DESTINATION lib/cmake/zefDB)
# install(EXPORT zefDB DESTINATION lib/cmake/zefDB)

############################
# * Pip

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/pip_output.log
  DEPENDS requirements.txt
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Pip-ing"
  COMMAND ${Python3_EXECUTABLE} -mpip install -r requirements.txt | tee pip_output.log)
add_custom_target(pip ALL
  DEPENDS pip_output.log)
add_dependencies(pyzef pip)
# Sometimes zef needs this too
add_dependencies(zef pip)

############################
# * Cog commands

set(COG_DUMMY ${CMAKE_CURRENT_SOURCE_DIR}/dummy_cog.tracker)

if(NOT MSVC)
  add_custom_target(cogbuild ALL
    DEPENDS ${COG_DUMMY})
  add_dependencies(zef cogbuild)

  # Do the cogging itself
  add_custom_command(
    DEPENDS zeftypes_EN.json zeftypes_ET.json zeftypes_RT.json zeftypes_KW.json
    OUTPUT ${COG_DUMMY}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Running cog"
    COMMAND ${Python3_EXECUTABLE} scripts/run_cog_gen.py
    # COMMAND python3 run_cog_inplace.py
    COMMAND date > ${COG_DUMMY})

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
    add_custom_command(
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_ET.json
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_RT.json
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_EN.json
      OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/zeftypes_KW.json
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Grabbing latest zeftypes from zefhub user the guest account."
      COMMAND pip3 install zstd
      COMMAND python3 scripts/get_zeftypes.py)
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

######################
# ** Token tests
add_test (NAME token_test
  COMMAND bash assert_no_created_tokens.sh
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/scripts
  )
set_tests_properties(token_test PROPERTIES
  ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}:$ENV{PYTHONPATH};ZEFDB_QUIET=YES;ZEFHUB_AUTH_KEY=GUEST"
  DEPENDS pyzef
  TIMEOUT 10)

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