cmake_minimum_required(VERSION 3.16)

project(minikv VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MINIKV_BUILD_TESTS "Build MiniKV tests" ON)
option(MINIKV_BUILD_TOOLS "Build MiniKV CLI and benchmark tools" ON)

add_library(minikv
    src/bloom_filter.cpp
    src/db.cpp
    src/lru_cache.cpp
    src/sstable.cpp
    src/status.cpp
    src/wal.cpp
)

target_include_directories(minikv
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)

target_compile_options(minikv PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(minikv PUBLIC pthread)

if(MINIKV_BUILD_TOOLS)
    add_executable(minikv_cli tools/minikv_cli.cpp)
    target_link_libraries(minikv_cli PRIVATE minikv)

    add_executable(minikv_bench tools/minikv_bench.cpp)
    target_link_libraries(minikv_bench PRIVATE minikv)
endif()

if(MINIKV_BUILD_TESTS)
    enable_testing()
    add_executable(minikv_tests tests/minikv_tests.cpp)
    target_link_libraries(minikv_tests PRIVATE minikv)
    add_test(NAME minikv_tests COMMAND minikv_tests)
endif()
