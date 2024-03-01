cmake_minimum_required(VERSION 3.27)
project(hzncsv LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

add_library(hzncsv STATIC src/hzncsv.cpp src/hzncsv.h)

add_executable(hzncsv_sample src/hzncsv.h app/sample.cpp)
target_include_directories(hzncsv_sample PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
add_custom_command(TARGET hzncsv_sample POST_BUILD COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/assets/sample.csv ${CMAKE_BINARY_DIR}/assets/)