project(pagedb)
cmake_minimum_required(VERSION 2.8)

if(COMMAND cmake_policy)
    cmake_policy(SET CMP0015 NEW)
endif(COMMAND cmake_policy)

#set(CMAKE_C_COMPILER	"/usr/bin/clang")
#set(CMAKE_CXX_COMPILER	"/usr/bin/clang++")
#set(CMAKE_AR			"/usr/bin/llvm-ar")
#set(CMAKE_LINKER		"/usr/bin/llvm-ld")
#set(CMAKE_NM			"/usr/bin/llvm-nm")
#set(CMAKE_OBJDUMP		"/usr/bin/llvm-objdump")
#set(CMAKE_RANLIB		"/usr/bin/llvm-ranlib")

set(CXXLIB2_ROOT	/home/wilbur/workspace/lte/src/cxxlib2)
set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib/${CMAKE_BUILD_TYPE})
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin/${CMAKE_BUILD_TYPE})

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    set(PTHREAD pthread)
    set(DL      dl)
    set(RT      rt)
    add_definitions(-fPIC)
endif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

if(${CMAKE_SYSTEM_NAME} MATCHES "bsd")
    set(PTHREAD pthread)
    set(DL      dl)
    set(RT      rt)
    add_definitions(-fPIC)
endif(${CMAKE_SYSTEM_NAME} MATCHES "bsd")
if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    add_definitions("/W3 /wd4996 /wd4290")
endif(${CMAKE_SYSTEM_NAME} MATCHES "Windows")

if(${CMAKE_BUILD_TYPE} MATCHES Debug)
    add_definitions(-DDEBUG)
endif(${CMAKE_BUILD_TYPE} MATCHES Debug)

set(PAGEDB_SRC
	pagedb/db.hpp pagedb/slice.hpp
	pagedb/status.hpp
	src/db_block.hpp src/db_block.cpp
    src/db_file.hpp src/db_file.cpp
	)

include_directories(${CXXLIB2_ROOT})
include_directories(${CMAKE_SOURCE_DIR})
link_directories(${CXXLIB2_ROOT}/lib/${CMAKE_BUILD_TYPE})

add_library(pagedb STATIC ${PAGEDB_SRC})

add_executable(test_dbfile src/test_dbfile.cpp src/db_block.hpp src/db_block.cpp src/status.cpp src/db_file.cpp)
target_link_libraries(test_dbfile cxxlib2 pagedb)

add_executable(test_dbblock src/test_dbblock.cpp src/db_block.hpp src/db_block.cpp src/status.cpp src/db_file.cpp)
target_link_libraries(test_dbblock cxxlib2)
