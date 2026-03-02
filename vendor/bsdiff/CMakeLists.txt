cmake_minimum_required(VERSION 3.21)

# Build bsdiff as a static library for the surge project.
# This bundles bzip2 and libdivsufsort as internal dependencies.

# --- bzip2 (static) --------------------------------------------------------
add_library(bzip2 STATIC
    3rdparty/bzip2/bzlib.c
    3rdparty/bzip2/compress.c
    3rdparty/bzip2/decompress.c
    3rdparty/bzip2/blocksort.c
    3rdparty/bzip2/crctable.c
    3rdparty/bzip2/huffman.c
    3rdparty/bzip2/randtable.c
)
set_property(TARGET bzip2 PROPERTY POSITION_INDEPENDENT_CODE ON)

# --- libdivsufsort (static) ------------------------------------------------
function(add_libdivsufsort)
    set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
    set(BUILD_EXAMPLES OFF)
    set(BUILD_DIVSUFSORT64 ON)
    set(BUILD_SHARED_LIBS OFF)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
    add_subdirectory(3rdparty/libdivsufsort)
endfunction()

add_libdivsufsort()

# --- bsdiff (static) -------------------------------------------------------
add_library(bsdiff STATIC
    source/bsdiff.c
    source/bspatch.c
    source/patch_packer_bz2.c
    source/compressor_bz2.c
    source/decompressor_bz2.c
    source/stream_file.c
    source/stream_memory.c
    source/stream_sub.c
    source/misc.c
)

target_include_directories(bsdiff
    PUBLIC  include
    PRIVATE 3rdparty/bzip2
    PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/3rdparty/libdivsufsort/include"
)

if(MSVC)
    target_compile_definitions(bsdiff PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

target_link_libraries(bsdiff
    PRIVATE bzip2
    PRIVATE divsufsort
    PRIVATE divsufsort64
)

set_property(TARGET bsdiff PROPERTY POSITION_INDEPENDENT_CODE ON)
