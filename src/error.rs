//! .

use crate::encoding;

/// Crate error
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Encoding error: {0}")]
    Encoding(#[from] encoding::Error),
}

pub type ApplyResult<T, E> = std::result::Result<T, ApplyError<E>>;

#[derive(thiserror::Error, Debug)]
pub enum ApplyError<E> {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Encoding error: {0}")]
    Encoding(#[from] encoding::Error),
    #[error("OpStream error: {0}")]
    OpStream(E),
}

pub type DeltaResult<T, E> = std::result::Result<T, DeltaError<E>>;

#[derive(thiserror::Error, Debug)]
pub enum DeltaError<E> {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Decoding error: {0}")]
    Decoding(#[from] encoding::Error),
    #[error("FastCDC error: {0}")]
    FastCDC(#[from] fastcdc::v2020::Error),
    #[error("Fetch error: {0}")]
    Fetch(E),
}
