cmake_minimum_required(VERSION 3.16)

project(
    BaBQ
    VERSION 0.1.0
    DESCRIPTION "BaBQ project"
    LANGUAGES C CXX
)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
        Debug Release RelWithDebInfo MinSizeRel
    )
endif()

find_package(Threads REQUIRED)

add_executable(babq
    src/main.cpp
)

target_include_directories(babq
    PRIVATE
        "${PROJECT_SOURCE_DIR}/include"
)

target_compile_features(babq
    PRIVATE
        c_std_11
        cxx_std_17
)

set_target_properties(babq PROPERTIES
    C_EXTENSIONS OFF
    CXX_EXTENSIONS OFF
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang" OR
   CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(babq PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
elseif(MSVC)
    target_compile_options(babq PRIVATE /W4)
endif()

target_link_libraries(babq
    PRIVATE
        Threads::Threads
)
