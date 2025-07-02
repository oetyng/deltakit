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
pub enum ApplyOp {
    /// Re‑use an existing chunk
    Data {
        index: u64,
        offset: u64,
        bytes: Vec<u8>,
    },
    /// Apply binary patch (`files_diff`) to base chunk
    Patch {
        /// The resulting chunk's index
        index: u64,
        /// The resulting chunk's offset in the file
        offset: u64,
        /// The base data
        base: Vec<u8>,
        /// Patch between base and next version
        patch: Vec<u8>,
    },
}

/// Diff operation description, in an operation
/// sequence produced by `diff_async`.
#[derive(Clone, Debug)]
pub enum ChunkOp {
    /// Re-use an existing chunk identified by SHA-256
    Copy {
        /// Sequence index for the application order of this chunk
        index: usize,
        /// Byte offset in the new file
        offset: u64,
        /// Length in bytes of the chunk
        length: usize,
        /// SHA-256 hash of the reused chunk
        hash: [u8; 32],
    },
    /// Apply binary patch to base chunk (identified by base_hash)
    Patch {
        /// Sequence index for the application order of this chunk
        index: usize,
        /// Byte offset in the new file
        offset: u64,
        /// Length in bytes of the chunk
        length: usize,
        /// SHA-256 hash of the resulting new chunk
        new_hash: [u8; 32],
        /// The patch that on top of base gives the new chunk/file
        patch: Vec<u8>,
        /// SHA-256 hash of the base chunk being patched
        old_hash: [u8; 32],
    },
    /// Insert literal bytes (used when patch is not beneficial)
    Insert {
        /// Sequence index for the application order of this chunk
        index: usize,
        /// Byte offset in the new file
        offset: u64,
        /// Length in bytes of the chunk
        length: usize,
        /// SHA-256 hash of the inserted data
        hash: [u8; 32],
        /// Raw bytes of the new chunk
        data: Vec<u8>,
    },
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
