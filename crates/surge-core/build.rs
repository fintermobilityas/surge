use std::path::PathBuf;

fn main() {
    let manifest_dir = env_path("CARGO_MANIFEST_DIR");
    let vendor = manifest_dir.join("vendor");
    let out_dir = env_path("OUT_DIR");

    // --- bzip2 ---
    let bzip2_dir = vendor.join("3rdparty/bzip2");
    cc::Build::new()
        .files([
            bzip2_dir.join("bzlib.c"),
            bzip2_dir.join("compress.c"),
            bzip2_dir.join("decompress.c"),
            bzip2_dir.join("blocksort.c"),
            bzip2_dir.join("crctable.c"),
            bzip2_dir.join("huffman.c"),
            bzip2_dir.join("randtable.c"),
        ])
        .warnings(false)
        .compile("bzip2");

    // --- libdivsufsort ---
    let divsufsort_lib = vendor.join("3rdparty/libdivsufsort/lib");
    let divsufsort_inc = vendor.join("3rdparty/libdivsufsort/include");

    // Generate all headers
    generate_headers(&divsufsort_inc, &out_dir);

    // Build 32-bit divsufsort
    cc::Build::new()
        .files([
            divsufsort_lib.join("divsufsort.c"),
            divsufsort_lib.join("sssort.c"),
            divsufsort_lib.join("trsort.c"),
            divsufsort_lib.join("utils.c"),
        ])
        .include(&out_dir)
        .include(&divsufsort_inc)
        .define("HAVE_CONFIG_H", "1")
        .define("PROJECT_VERSION_FULL", "\"2.0.2\"")
        .warnings(false)
        .compile("divsufsort");

    // Build 64-bit divsufsort
    cc::Build::new()
        .files([
            divsufsort_lib.join("divsufsort.c"),
            divsufsort_lib.join("sssort.c"),
            divsufsort_lib.join("trsort.c"),
            divsufsort_lib.join("utils.c"),
        ])
        .include(&out_dir)
        .include(&divsufsort_inc)
        .define("HAVE_CONFIG_H", "1")
        .define("BUILD_DIVSUFSORT64", "1")
        .define("PROJECT_VERSION_FULL", "\"2.0.2\"")
        .warnings(false)
        .compile("divsufsort64");

    // --- bsdiff ---
    let bsdiff_src = vendor.join("source");
    cc::Build::new()
        .files([
            bsdiff_src.join("bsdiff.c"),
            bsdiff_src.join("bspatch.c"),
            bsdiff_src.join("patch_packer_bz2.c"),
            bsdiff_src.join("compressor_bz2.c"),
            bsdiff_src.join("decompressor_bz2.c"),
            bsdiff_src.join("stream_file.c"),
            bsdiff_src.join("stream_memory.c"),
            bsdiff_src.join("stream_sub.c"),
            bsdiff_src.join("misc.c"),
        ])
        .include(vendor.join("include"))
        .include(&bzip2_dir)
        .include(&out_dir)
        .include(&divsufsort_inc)
        .warnings(false)
        .compile("bsdiff");

    println!("cargo:rerun-if-changed={}", vendor.display());
}

fn generate_headers(template_dir: &std::path::Path, out_dir: &std::path::Path) {
    // Generate config.h
    let config_h = r#"#ifndef _CONFIG_H
#define _CONFIG_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <inttypes.h>

#define PROJECT_VERSION_FULL "2.0.2"

#define HAVE_INTTYPES_H 1
#define HAVE_STDINT_H 1
#define HAVE_STDLIB_H 1
#define HAVE_STRING_H 1
#define HAVE_MEMORY_H 1
#ifndef _MSC_VER
#define HAVE_STRINGS_H 1
#endif

#ifndef INLINE
# define INLINE inline
#endif

#ifdef _MSC_VER
#pragma warning(disable: 4127)
#endif

#ifdef __cplusplus
}
#endif

#endif /* _CONFIG_H */
"#;
    write_file(out_dir.join("config.h"), config_h);

    // Generate lfs.h
    let lfs_h = r#"#ifndef _LFS_H
#define _LFS_H 1

#ifdef __cplusplus
extern "C" {
#endif

#ifndef __STRICT_ANSI__
# define LFS_OFF_T off_t
# define LFS_FOPEN fopen
# define LFS_FTELL ftello
# define LFS_FSEEK fseeko
# define LFS_PRId  PRId64
#else
# define LFS_OFF_T long
# define LFS_FOPEN fopen
# define LFS_FTELL ftell
# define LFS_FSEEK fseek
# define LFS_PRId "ld"
#endif
#ifndef PRIdOFF_T
# define PRIdOFF_T LFS_PRId
#endif

#ifdef __cplusplus
}
#endif

#endif /* _LFS_H */
"#;
    write_file(out_dir.join("lfs.h"), lfs_h);

    // Read the template
    let template = read_file(template_dir.join("divsufsort.h.cmake"));

    // Generate divsufsort.h (32-bit)
    let header_32 = template
        .replace("@W64BIT@", "")
        .replace("@DIVSUFSORT_EXPORT@", "")
        .replace("@DIVSUFSORT_IMPORT@", "")
        .replace("@INLINE@", "inline")
        .replace("@SAUCHAR_TYPE@", "uint8_t")
        .replace("@SAINT_TYPE@", "int32_t")
        .replace("@SAINT32_TYPE@", "int32_t")
        .replace("@SAIDX_TYPE@", "int32_t")
        .replace("@SAIDX32_TYPE@", "int32_t")
        .replace("@SAINDEX_TYPE@", "int32_t")
        .replace("@SAINDEX32_TYPE@", "int32_t")
        .replace("@PRIdSAINT_T@", "PRId32")
        .replace("@PRIdSAIDX_T@", "PRId32")
        .replace("@PROJECT_VERSION_FULL@", "2.0.2")
        .replace("@INCFILE@", "#include <inttypes.h>")
        .replace("@SAINT_PRId@", "PRId32")
        .replace("@SAINDEX_PRId@", "PRId32");

    write_file(out_dir.join("divsufsort.h"), &header_32);

    // Generate divsufsort64.h (64-bit)
    let header_64 = template
        .replace("@W64BIT@", "64")
        .replace("@DIVSUFSORT_EXPORT@", "")
        .replace("@DIVSUFSORT_IMPORT@", "")
        .replace("@INLINE@", "inline")
        .replace("@SAUCHAR_TYPE@", "uint8_t")
        .replace("@SAINT_TYPE@", "int32_t")
        .replace("@SAINT32_TYPE@", "int32_t")
        .replace("@SAIDX_TYPE@", "int64_t")
        .replace("@SAIDX32_TYPE@", "int32_t")
        .replace("@SAINDEX_TYPE@", "int64_t")
        .replace("@SAINDEX32_TYPE@", "int32_t")
        .replace("@PRIdSAINT_T@", "PRId32")
        .replace("@PRIdSAIDX_T@", "PRId64")
        .replace("@PROJECT_VERSION_FULL@", "2.0.2")
        .replace("@INCFILE@", "#include <inttypes.h>")
        .replace("@SAINT_PRId@", "PRId32")
        .replace("@SAINDEX_PRId@", "PRId64");

    write_file(out_dir.join("divsufsort64.h"), &header_64);
}

fn env_path(name: &str) -> PathBuf {
    PathBuf::from(std::env::var(name).unwrap_or_else(|err| panic!("missing {name}: {err}")))
}

fn write_file(path: PathBuf, contents: &str) {
    std::fs::write(&path, contents).unwrap_or_else(|err| panic!("failed to write {}: {err}", path.display()));
}

fn read_file(path: PathBuf) -> String {
    std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}
