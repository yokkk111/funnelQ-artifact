cmake_minimum_required(VERSION 3.14)
project(mbft)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

set(MBFT_CORE_SOURCES
    src/bf.cpp
    src/blockchain.cpp
    src/hash.cpp
    src/merkle.cpp
    src/query.cpp
    src/transaction.cpp
)

add_library(mbft_core STATIC ${MBFT_CORE_SOURCES})
add_executable(mbft src/main.cpp)
target_link_libraries(mbft PRIVATE mbft_core)

add_executable(mbft_adapt src/adapt.cpp src/adapt_main.cpp)
target_link_libraries(mbft_adapt PRIVATE mbft_core)
