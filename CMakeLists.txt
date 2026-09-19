cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME duckdb_object_storage)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include rust/include)

set(EXTENSION_SOURCES
    src/duckdb_object_storage_extension.cpp
    src/slatedb_cache_stats.cpp
    src/slatedb_config_utils.cpp
    src/slatedb_ffi_utils.cpp
    src/slatedb_file_handle.cpp
    src/slatedb_file_system.cpp
    src/slatedb_io_stats.cpp
    src/slatedb_path_utils.cpp)

# ---------------------------------------------------------------------------
# SlateDB rust crate (https://slatedb.io/)
# ---------------------------------------------------------------------------
find_program(CARGO_EXECUTABLE cargo)
if(NOT CARGO_EXECUTABLE)
  message(FATAL_ERROR "cargo is required to build the SlateDB filesystem")
endif()

if(APPLE)
  set(PLATFORM_LIBS
      m
      c
      System
      resolv
      "-framework CoreFoundation"
      "-framework IOKit"
      "-framework SystemConfiguration"
      "-framework Security")
elseif(UNIX)
  set(PLATFORM_LIBS m c dl pthread resolv)
elseif(WIN32)
  set(PLATFORM_LIBS
      ntdll
      crypt32
      ncrypt
      secur32
      ws2_32
      userenv
      bcrypt
      advapi32)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CARGO_PROFILE_FLAG "")
  set(CARGO_PROFILE_DIR "debug")
else()
  set(CARGO_PROFILE_FLAG "--release")
  set(CARGO_PROFILE_DIR "release")
endif()

set(RUST_TARGET_DIR "${CMAKE_BINARY_DIR}/rust-slatedb-fs")
if(DEFINED Rust_CARGO_TARGET AND NOT "${Rust_CARGO_TARGET}" STREQUAL "")
  set(RUST_CARGO_TARGET_FLAG "--target=${Rust_CARGO_TARGET}")
  set(SLATEDB_FS_LIB
      "${RUST_TARGET_DIR}/${Rust_CARGO_TARGET}/${CARGO_PROFILE_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}slatedb_fs${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
else()
  set(RUST_CARGO_TARGET_FLAG "")
  set(SLATEDB_FS_LIB
      "${RUST_TARGET_DIR}/${CARGO_PROFILE_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}slatedb_fs${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
endif()

file(
  GLOB_RECURSE
  SLATEDB_FS_RUST_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/rust/src/*.rs"
  "${CMAKE_CURRENT_SOURCE_DIR}/rust/include/*.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/rust/Cargo.toml"
  "${CMAKE_CURRENT_SOURCE_DIR}/rust/Cargo.lock")

add_custom_command(
  OUTPUT "${SLATEDB_FS_LIB}"
  COMMAND
    ${CMAKE_COMMAND} -E env --unset=CC --unset=CXX --unset=LD
    ${CARGO_EXECUTABLE} build --manifest-path
    "${CMAKE_CURRENT_SOURCE_DIR}/rust/Cargo.toml" --target-dir
    "${RUST_TARGET_DIR}" ${CARGO_PROFILE_FLAG} ${RUST_CARGO_TARGET_FLAG}
  DEPENDS ${SLATEDB_FS_RUST_SOURCES}
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust"
  COMMENT "Building SlateDB filesystem rust crate"
  VERBATIM)

add_custom_target(slatedb_fs_rust DEPENDS "${SLATEDB_FS_LIB}")

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} "${SLATEDB_FS_LIB}" ${PLATFORM_LIBS})
target_link_libraries(${LOADABLE_EXTENSION_NAME} "${SLATEDB_FS_LIB}"
                      ${PLATFORM_LIBS})
add_dependencies(${EXTENSION_NAME} slatedb_fs_rust)
add_dependencies(${LOADABLE_EXTENSION_NAME} slatedb_fs_rust)

option(DUCKDB_OBJECT_STORAGE_BUILD_CPP_TESTS
       "Build duckdb-object-storage C++ unit tests" OFF)
if(DUCKDB_OBJECT_STORAGE_BUILD_CPP_TESTS)
  add_executable(
    duckdb_object_storage_cpp_tests test/cpp/scoped_directory.cpp
                                    test/cpp/test_slatedb_file_system.cpp)
  target_include_directories(duckdb_object_storage_cpp_tests
                             PRIVATE "${CMAKE_SOURCE_DIR}/third_party/catch")
  target_link_libraries(duckdb_object_storage_cpp_tests duckdb_static)
  link_extension_libraries(duckdb_object_storage_cpp_tests "")
  add_dependencies(duckdb_object_storage_cpp_tests slatedb_fs_rust)
  add_custom_target(
    check_duckdb_object_storage_cpp
    COMMAND $<TARGET_FILE:duckdb_object_storage_cpp_tests>
    DEPENDS duckdb_object_storage_cpp_tests
    USES_TERMINAL)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
