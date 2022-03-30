
add_executable (c_test src/test.cpp)
set_target_properties(c_test PROPERTIES
  CXX_STANDARD 17
  EXCLUDE_FROM_ALL TRUE
  )

target_link_libraries(c_test PRIVATE zef)

find_package(Threads REQUIRED)
target_link_libraries(c_test PRIVATE Threads::Threads)

target_link_libraries(c_test PUBLIC pybind11::embed)