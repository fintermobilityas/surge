/// Current version of the Surge framework.
pub const VERSION: &str = "0.1.0";

/// Default Surge working directory name.
pub const SURGE_DIR: &str = ".surge";

/// Default manifest filename.
pub const MANIFEST_FILE: &str = "surge.yml";

/// Release index filename (YAML).
pub const RELEASES_FILE: &str = "releases.yml";

/// Compressed release index filename.
pub const RELEASES_FILE_COMPRESSED: &str = "releases.yml.zst";

/// Current manifest schema version.
pub const SCHEMA_VERSION: i32 = 1;

/// Default zstd compression level.
pub const DEFAULT_ZSTD_LEVEL: i32 = 9;

/// Default delta strategy used by `surge pack`.
pub const PACK_DEFAULT_DELTA_STRATEGY: &str = "sparse-file-ops";

/// Default compression format used by `surge pack`.
pub const PACK_DEFAULT_COMPRESSION_FORMAT: &str = "zstd";

/// Default zstd compression level used by `surge pack`.
pub const PACK_DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Default maximum delta chain length to target for future pack policy.
pub const PACK_DEFAULT_MAX_CHAIN_LENGTH: u32 = 8;

/// Default number of recent direct full archives to retain per RID.
pub const PACK_DEFAULT_KEEP_LATEST_FULLS: u32 = 2;

/// Default interval for keeping full checkpoint archives.
pub const PACK_DEFAULT_CHECKPOINT_EVERY: u32 = 10;

/// Default memory budget used by `surge pack` on the current node.
pub const PACK_DEFAULT_MAX_MEMORY_BYTES: i64 = 256 * 1024 * 1024;

/// SHA-256 hash output length in hex characters.
pub const SHA256_HEX_LEN: usize = 64;

/// Chunk size for streaming file operations (64 KB).
pub const IO_CHUNK_SIZE: usize = 64 * 1024;
