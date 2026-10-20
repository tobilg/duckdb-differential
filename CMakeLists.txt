cmake_minimum_required(VERSION 3.5)

# Built by DuckDB's extension build system (extension_config.cmake); see the Makefile.
set(TARGET_NAME differential)

# Dependencies come from vcpkg (vcpkg.json, pinned through the vcpkg commit).
find_package(OpenSSL REQUIRED)
find_package(unofficial-sqlite3 CONFIG REQUIRED)
find_package(AWSSDK CONFIG COMPONENTS core s3 REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME} VERSION 1.0.0)

# DuckDB's parent build sets C++11 as a normal variable; this extension needs C++17
# (std::filesystem, std::optional, std::shared_mutex) for every target in this directory.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include src src/duckdb_adapter)

# Byte store and metadata authority: no DuckDB dependency (also used by the unit tests).
set(STORE_SOURCES
    src/storage/store.cpp
    src/storage/segment.cpp
    src/metadata/control.cpp
    src/metadata/retention.cpp
    src/metadata/resolve.cpp
    src/providers/local/local_provider.cpp)
set(EXTENSION_SOURCES
    src/differential_extension.cpp
    src/duckdb_adapter/probe_filesystem.cpp
    src/duckdb_adapter/durable_filesystem.cpp
    src/duckdb_adapter/control_functions.cpp
    src/duckdb_adapter/secrets.cpp
    src/duckdb_adapter/differential_catalog.cpp
    src/duckdb_adapter/follow.cpp
    src/providers/s3/snapshot.cpp
    src/providers/s3/remote_gc.cpp
    src/providers/s3/shipping.cpp
    src/providers/s3/s3_provider.cpp
    src/providers/s3/disk_cache.cpp
    src/duckdb_adapter/remote_cache.cpp
    ${STORE_SOURCES})

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

set(DIFFERENTIAL_LIBRARIES aws-cpp-sdk-s3 aws-cpp-sdk-core unofficial::sqlite3::sqlite3 OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${EXTENSION_NAME} ${DIFFERENTIAL_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${DIFFERENTIAL_LIBRARIES})

# Failpoints, gates and raw-path probes for the test suites (`make integration`).
# Release artifacts (`make release`, community builds) never contain them.
option(DIFFERENTIAL_TEST_HOOKS "Compile environment-driven fault injection and diagnostic probes" OFF)
if(DIFFERENTIAL_TEST_HOOKS)
  target_compile_definitions(${EXTENSION_NAME} PRIVATE DIFFERENTIAL_TEST_HOOKS)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE DIFFERENTIAL_TEST_HOOKS)

  # C++ unit tests of the byte store and metadata authority, and the SDK capability probe.
  enable_testing()
  add_library(differential_store STATIC ${STORE_SOURCES})
  target_compile_definitions(differential_store PUBLIC DIFFERENTIAL_TEST_HOOKS)
  target_link_libraries(differential_store PUBLIC unofficial::sqlite3::sqlite3 OpenSSL::Crypto)
  foreach(test store_test store_faults control_test control_races local_pools shared_test remote_gc_test)
    add_executable(${test} test/unit/${test}.cpp)
    target_link_libraries(${test} PRIVATE differential_store)
    add_test(NAME ${test} COMMAND ${test})
  endforeach()
  add_executable(s3_test test/integration/s3_test.cpp src/providers/s3/s3_provider.cpp)
  target_link_libraries(s3_test PRIVATE aws-cpp-sdk-s3 aws-cpp-sdk-core OpenSSL::Crypto)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
