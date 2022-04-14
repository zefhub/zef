project(PyZefDB)

get_filename_component(ZEFDB_DIR "../../" REALPATH)

pybind11_add_module (pyzef
  build_zefDB.cpp
  zefops.cpp
  internals.cpp
  )

set_target_properties(pyzef
    PROPERTIES
    CXX_STANDARD 17)

target_include_directories (pyzef PUBLIC ${RANGE_INCLUDE_DIRS})

if(UNIX AND NOT APPLE)
  # This should take us up the lib directory where both libzef and libpython are.
  set_target_properties(pyzef PROPERTIES INSTALL_RPATH "$ORIGIN")
elseif(APPLE)
  set_target_properties(pyzef PROPERTIES INSTALL_RPATH "@loader_path")
endif()

target_link_libraries(pyzef PUBLIC zef)

add_custom_command(TARGET pyzef
  POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE:pyzef> ${ZEFDB_DIR}/zef/`basename $<TARGET_FILE:pyzef>`
  COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE:zef> ${ZEFDB_DIR}/zef/`basename $<TARGET_FILE:zef>`
)
  
install ( TARGETS pyzef
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  )

# Things needed for Visual Studio
if(MSVC)
	target_compile_options(pyzef PUBLIC "/permissive-")
    target_compile_options(pyzef PUBLIC "/bigobj")
    set_target_properties(pyzef PROPERTIES SUFFIX ".pyd")
    set_target_properties(pyzef PROPERTIES ENABLE_EXPORTS 1)
	set_target_properties(pyzef PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS 1)
endif()
