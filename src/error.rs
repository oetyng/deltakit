//! .

use crate::encoding;

pub type Result<T> = std::result::Result<T, Error>;

/// Crate error
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Encoding error: {0}")]
    Encoding(#[from] encoding::Error),
    #[error("missing base chunk {0:?}")]
    MissingBase([u8; 32]),
}

pub type ApplyResult<T, E> = std::result::Result<T, ApplyError<E>>;

#[derive(thiserror::Error, Debug)]
pub enum ApplyError<E> {
    #[error("I/O error: {source}, {progress}")]
    Io {
        source: std::io::Error,
        progress: u64, // the next_offset at time of failure
    },
    #[error("Encoding error: {source}, {progress}")]
    Encoding {
        source: encoding::Error,
        progress: u64, // the next_offset at time of failure
    },
    #[error("OpStream error: {source}, {progress}")]
    OpStream {
        source: E,
        progress: u64, // the next_offset at time of failure
    },
}
