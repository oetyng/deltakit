//! .

use compression::{compress, decompress};

use bidiff::DiffParams;
use std::io::{self, BufWriter, Cursor, Write};

// Generally, that will be (num_cpus - 1), leaving one core free for bookkeeping
// and other tasks, but as we do not know the number of cores on specific
// machines, we use 3, assuming everyone in 2025 has at least 4 cores
const SORT_PARTITIONS: usize = 3;
// 128KiB. Choosing a chunk size that's too large will result in suboptimal core
// utilization, whereas choosing a chunk size that's too small will result in
// increased memory usage for diminishing returns. 128KiB chosen over default 512KiB
// due to ceiling of 4MiB used for diffing.
const SCAN_CHUNK_SIZE: usize = 128 * 1024;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An I/O error occurred during file operations
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// An error occurred in the bidiff algorithm
    #[error("Bidiff error: {0}")]
    Bidiff(String),
    /// An error occurred while processing a zip archive
    #[error("Compression error: {0}")]
    Compression(String),
}

pub fn diff(before: &[u8], after: &[u8]) -> Result<Vec<u8>, Error> {
    let mut patch = Cursor::new(vec![]);

    let diff_params = DiffParams::new(SORT_PARTITIONS, Some(SCAN_CHUNK_SIZE))
        .map_err(|e| Error::Bidiff(format!("failed to create diff params: {}", e)))?;
    bidiff::simple_diff_with_params(before, after, &mut patch, &diff_params)
        .map_err(|e| Error::Bidiff(format!("failed to diff: {}", e)))?;

    Write::flush(&mut patch)?;
    let compressed_patch = compress(patch.get_ref())?;

    Ok(compressed_patch)
}

pub fn apply(base: &[u8], delta: &[u8]) -> Result<Vec<u8>, Error> {
    let patch = decompress(delta)?;
    let patch_reader = Cursor::new(patch);
    let base_cursor = Cursor::new(base);

    let mut fresh_r = bipatch::Reader::new(patch_reader, base_cursor)
        .map_err(|e| Error::Bidiff(format!("failed to create bidiff reader: {}", e)))?;
    let mut output_w = BufWriter::new(vec![]);
    io::copy(&mut fresh_r, &mut output_w)
        .map_err(|e| Error::Bidiff(format!("failed to copy: {}", e)))?;
    let after = output_w
        .into_inner()
        .map_err(|e| Error::Bidiff(format!("failed to get inner of bidiff reader: {}", e)))?;

    Ok(after)
}

mod compression {
    use super::Error;
    use std::io::{Read, Write};

    const ZSTD_COMPRESSION_LEVEL: i32 = 21;

    pub(super) fn compress(input: &[u8]) -> Result<Vec<u8>, Error> {
        let mut encoder = zstd::Encoder::new(Vec::new(), ZSTD_COMPRESSION_LEVEL)
            .map_err(|e| Error::Compression(format!("failed to create zstd encoder: {}", e)))?;
        encoder
            .write_all(input)
            .map_err(|e| Error::Compression(format!("failed to write: {}", e)))?;
        Ok(encoder
            .finish()
            .map_err(|e| Error::Compression(format!("failed to finish: {}", e)))?)
    }

    pub(super) fn decompress(input: &[u8]) -> Result<Vec<u8>, Error> {
        let mut output = Vec::new();
        let mut decoder = zstd::Decoder::new(input)
            .map_err(|e| Error::Compression(format!("failed to create zstd decoder: {}", e)))?;
        decoder
            .read_to_end(&mut output)
            .map_err(|e| Error::Compression(format!("failed to read: {}", e)))?;
        Ok(output)
    }
}
