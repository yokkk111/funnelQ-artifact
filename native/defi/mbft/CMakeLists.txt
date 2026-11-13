cmake_minimum_required(VERSION 3.14)
project(mbft_resident_query LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT DEFINED MBFT_SOURCE)
  message(FATAL_ERROR "Pass -DMBFT_SOURCE=<original package root>")
endif()
set(CORE ${MBFT_SOURCE}/mbft)
add_executable(mbft_resident main.cpp
  ${CORE}/src/adapt.cpp ${CORE}/src/bf.cpp ${CORE}/src/blockchain.cpp
  ${CORE}/src/hash.cpp ${CORE}/src/merkle.cpp ${CORE}/src/query.cpp
  ${CORE}/src/transaction.cpp)
target_include_directories(mbft_resident PRIVATE ${CORE}/include)
