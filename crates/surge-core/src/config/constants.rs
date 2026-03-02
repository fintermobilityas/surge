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

/// SHA-256 hash output length in hex characters.
pub const SHA256_HEX_LEN: usize = 64;

/// Chunk size for streaming file operations (64 KB).
pub const IO_CHUNK_SIZE: usize = 64 * 1024;
