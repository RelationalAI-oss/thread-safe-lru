cmake_minimum_required(VERSION 2.8)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(thread-safe-lru)

    add_executable(benchmark benchmark.cpp)

    include(FindPkgConfig)
    pkg_search_module(TBB REQUIRED tbb)
    target_link_libraries(benchmark ${TBB_LIBRARIES})
    include_directories(${TBB_INCLUDE_DIRS})

    # Boost is needed for the benchmark only
    include(FindBoost)
    find_package(Boost REQUIRED COMPONENTS locale)
    target_link_libraries(benchmark ${Boost_LIBRARIES})
    include_directories(${Boost_INCLUDE_DIRS})
    

    set(CMAKE_CXX_FLAGS "-std=c++11 -O3")
endif()

file(GLOB LRUCache_HEADERS "${CMAKE_CURRENT_SOURCE_DIR}/thread-safe-lru/*.h")
file(GLOB LRUCache_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
file(GLOB LRUCache_SRC ${LRUCache_HEADERS} ${LRUCache_SOURCES})

add_library(thread-safe-lru ${LRUCache_SRC})

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
