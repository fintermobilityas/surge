use thiserror::Error;

/// Error codes matching the C API (`surge_result` enum in surge_api.h).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorCode {
    Ok = 0,
    Error = -1,
    Cancelled = -2,
    NotFound = -3,
}

impl ErrorCode {
    #[must_use]
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Self::Ok,
            -2 => Self::Cancelled,
            -3 => Self::NotFound,
            _ => Self::Error,
        }
    }
}

/// All errors that can occur within the Surge framework.
#[derive(Debug, Error)]
pub enum SurgeError {
    #[error("Operation cancelled")]
    Cancelled,

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("YAML parse error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("XML parse error: {0}")]
    Xml(#[from] quick_xml::Error),

    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("Crypto error: {0}")]
    Crypto(String),

    #[error("Archive error: {0}")]
    Archive(String),

    #[error("Diff error: {0}")]
    Diff(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Lock error: {0}")]
    Lock(String),

    #[error("Update error: {0}")]
    Update(String),

    #[error("Pack error: {0}")]
    Pack(String),

    #[error("Supervisor error: {0}")]
    Supervisor(String),

    #[error("Platform error: {0}")]
    Platform(String),

    #[error("{0}")]
    Other(String),
}

impl SurgeError {
    /// Map this error to a C API error code.
    #[must_use]
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::Cancelled => ErrorCode::Cancelled,
            Self::NotFound(_) => ErrorCode::NotFound,
            _ => ErrorCode::Error,
        }
    }
}

pub type Result<T> = std::result::Result<T, SurgeError>;
