//! .

#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Minimum FastCDC chunk size (bytes)
    pub min_size: u32,
    /// Average FastCDC chunk size (bytes)
    pub avg_size: u32,
    /// Maximum chunk size – also the cut-off for whole‑file diff path (bytes)
    pub max_size: u32,
    /// Patch must be ≤ threshold × literal to be used (range 0–1). Applies to both small files and chunks.
    /// If the patch is not at least this fraction smaller than raw bytes, it falls back to `Insert`.
    pub patch_threshold: f32,
}

/// Diff operation description, in an operation
/// sequence produced by `diff_async`.
#[derive(Clone, Debug)]
pub enum ChunkOp {
    /// Re‑use an existing chunk identified by SHA‑256
    Copy { hash: [u8; 32], length: u32 },
    /// Apply binary patch (`files_diff`) to base chunk (identified by hash)
    Patch {
        base_hash: [u8; 32],
        patch: Vec<u8>, // serialized patch bytes (create_patch output)
    },
    /// Insert literal bytes (used when patch is not beneficial)
    Insert { data: Vec<u8> },
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min_size: 64 * 1024,
            avg_size: 1 * 1024 * 1024,
            max_size: 4 * 1024 * 1024,
            patch_threshold: 0.8, // 20 % space gain required
        }
    }
}

/// Diff operation description, in an operation
/// sequence produced by `diff_async`.
#[derive(Clone, Debug)]
pub enum ApplyOp {
    /// Re‑use an existing chunk
    Data { bytes: Vec<u8>, offset: u64 },
    /// Apply binary patch (`files_diff`) to base chunk
    Patch {
        /// The base data
        base: Vec<u8>,
        /// Patch between base and next version
        patch: crate::encoding::Patch,
        ///
        offset: u64,
    },
}
