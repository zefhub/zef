cmake_minimum_required(VERSION 3.8)

project(ZefDB CXX ASM)

############################
# customizable variables

###########################
# libzef itself

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
