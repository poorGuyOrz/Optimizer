cmake_minimum_required(VERSION 3.0.0)
project(Optmizer VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 20)

include(CTest)
enable_testing()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build)
add_subdirectory(cpp)