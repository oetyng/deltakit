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
