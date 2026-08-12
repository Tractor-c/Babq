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

option(ENABLE_TSAN "Enable ThreadSanitizer for data-race detection" OFF)

add_library(babq_headers INTERFACE)

target_include_directories(babq_headers
    INTERFACE
        "${PROJECT_SOURCE_DIR}/include"
)

target_compile_features(babq_headers
    INTERFACE
        cxx_std_17
)

target_link_libraries(babq_headers
    INTERFACE
        Threads::Threads
)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(babq_headers INTERFACE
        -Wall
        -Wextra
        -Wpedantic
    )
elseif(MSVC)
    target_compile_options(babq_headers INTERFACE /W4)
endif()

if(ENABLE_TSAN)
    target_compile_options(babq_headers INTERFACE -fsanitize=thread)
    target_link_options(babq_headers INTERFACE -fsanitize=thread)
endif()

enable_testing()
add_subdirectory(test)
