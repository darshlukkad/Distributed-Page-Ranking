cmake_minimum_required(VERSION 3.16)

project(distributed_pagerank LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -Wpedantic)

set(COMMON_SOURCES
    src/common/config.cpp
    src/net/socket_utils.cpp
    src/net/message.cpp
    src/worker/partition_loader.cpp
)

add_executable(coordinator
    src/coordinator/main.cpp
    ${COMMON_SOURCES}
)
target_include_directories(coordinator PRIVATE src)

add_executable(worker
    src/worker/main.cpp
    ${COMMON_SOURCES}
)
target_include_directories(worker PRIVATE src)
