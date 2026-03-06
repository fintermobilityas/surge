#![deny(unsafe_code)]
#![allow(
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines
)]

pub mod archive;
pub mod config;
pub mod context;
pub mod crypto;
pub mod diff;
pub mod download;
pub mod error;
pub mod install;
pub mod installer_bundle;
pub mod lock;
pub mod pack;
pub mod platform;
pub mod releases;
pub mod storage;
pub mod storage_config;
pub mod supervisor;
pub mod update;
