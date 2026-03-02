cmake_minimum_required(VERSION 3.21)

project(surge
    VERSION 0.1.0
    LANGUAGES C CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

# Warnings as errors - enforce high code quality
if(MSVC)
    add_compile_options(/W4 /WX /utf-8)
else()
    add_compile_options(-Wall -Wextra -Wpedantic -Werror -Wno-unused-parameter)
endif()

# clang-tidy integration
option(SURGE_ENABLE_CLANG_TIDY "Enable clang-tidy static analysis" OFF)
if(SURGE_ENABLE_CLANG_TIDY)
    find_program(CLANG_TIDY_EXE NAMES "clang-tidy")
    if(CLANG_TIDY_EXE)
        set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_EXE}")
        message(STATUS "clang-tidy found: ${CLANG_TIDY_EXE}")
    else()
        message(WARNING "clang-tidy requested but not found")
    endif()
endif()

# Export macros
if(WIN32)
    set(SURGE_API_EXPORT "__declspec(dllexport)")
    set(SURGE_API_IMPORT "__declspec(dllimport)")
    set(SURGE_CALL_CONVENTION "__cdecl")
else()
    set(SURGE_API_EXPORT "__attribute__((visibility(\"default\")))")
    set(SURGE_API_IMPORT "__attribute__((visibility(\"default\")))")
    set(SURGE_CALL_CONVENTION "")
endif()

# Find vcpkg dependencies
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(zstd CONFIG REQUIRED)
find_package(LibArchive REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(cxxopts CONFIG REQUIRED)

# Collect surge_core sources (all src/**/*.cpp except cli/main.cpp and supervisor/supervisor_main.cpp)
file(GLOB_RECURSE SURGE_CORE_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
)
list(FILTER SURGE_CORE_SOURCES EXCLUDE REGEX "src/cli/.*\\.cpp$")
list(FILTER SURGE_CORE_SOURCES EXCLUDE REGEX "src/supervisor/supervisor_main\\.cpp$")

# Collect CLI command sources (built into surge executable, not the shared library)
file(GLOB SURGE_CLI_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/cli/commands/*.cpp"
)

# Build vendor/bsdiff as a static library (includes bzip2 + libdivsufsort)
add_subdirectory(vendor/bsdiff)

# surge_core shared library
add_library(surge_core SHARED
    ${SURGE_CORE_SOURCES}
)

target_include_directories(surge_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/vendor/bsdiff/include
)

target_compile_definitions(surge_core
    PUBLIC
        $<$<BOOL:${WIN32}>:SURGE_PLATFORM_WINDOWS>
        $<$<NOT:$<BOOL:${WIN32}>>:SURGE_PLATFORM_LINUX>
    PRIVATE
        SURGE_BUILDING
)

target_link_libraries(surge_core
    PUBLIC
        CURL::libcurl
        OpenSSL::SSL
        OpenSSL::Crypto
        $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>
        LibArchive::LibArchive
        yaml-cpp::yaml-cpp
        nlohmann_json::nlohmann_json
        fmt::fmt
        spdlog::spdlog
        cxxopts::cxxopts
    PRIVATE
        bsdiff
)

if(UNIX AND NOT APPLE)
    target_link_libraries(surge_core PUBLIC pthread dl)
endif()

# surge CLI executable
add_executable(surge
    src/cli/main.cpp
    ${SURGE_CLI_SOURCES}
)

target_link_libraries(surge PRIVATE surge_core)

target_include_directories(surge PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# surge_supervisor executable
add_executable(surge_supervisor
    src/supervisor/supervisor_main.cpp
)

target_link_libraries(surge_supervisor PRIVATE surge_core)

target_include_directories(surge_supervisor PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# Install rules
include(GNUInstallDirs)

install(TARGETS surge_core surge surge_supervisor
    EXPORT surgeTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(DIRECTORY include/surge
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT surgeTargets
    FILE surgeTargets.cmake
    NAMESPACE surge::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/surge
)

# Testing
option(BUILD_TESTING "Build tests" ON)
if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()
