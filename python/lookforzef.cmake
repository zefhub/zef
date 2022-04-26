find_package(zef QUIET)
if(NOT zef_FOUND)
  message(FATAL_ERROR "Did not find zef")
endif()