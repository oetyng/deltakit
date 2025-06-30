//! .

use bincode::{Decode, Encode, config};
use files_diff::{CompressAlgorithm, DiffAlgorithm, Patch as Diff};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("diff: {0:?}")] // <-- iffy.. todo
    Diff(files_diff::Error),
    #[error("bincode encode: {0}")]
    BinEncode(#[from] bincode::error::EncodeError),
    #[error("bincode decode: {0}")]
    BinDecode(#[from] bincode::error::DecodeError),
}

pub(super) fn create_patch(before: &[u8], after: &[u8]) -> Result<Vec<u8>> {
    let res = files_diff::diff(
        before,
        after,
        files_diff::DiffAlgorithm::Bidiff1,
        files_diff::CompressAlgorithm::Zstd,
    );
    let diff = res.map_err(Error::Diff)?;
    let patch = Patch::from(diff);
    let bytes = bincode::encode_to_vec(&patch, config::standard())?;
    Ok(bytes)
}

pub(super) fn apply_patch(base: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
    let (patch, _): (Patch, _) = bincode::decode_from_slice(delta, config::standard())?;
    let res = files_diff::apply(base, &patch.into());
    res.map_err(Error::Diff)
}

#[derive(Clone, Debug, Encode, Decode)]
struct Patch {
    /// Algorithm used to generate this patch
    pub diff_algorithm: DiffAlgo,
    /// Compression method used for the patch data
    pub compress_algorithm: CompressAlgo,
    /// MD5 hash of the source file
    pub before_hash: String,
    /// MD5 hash of the target file
    pub after_hash: String,
    /// The actual patch data
    pub patch: Vec<u8>,
}

impl Patch {
    fn from(diff: Diff) -> Self {
        Patch {
            diff_algorithm: DiffAlgo::from(diff.diff_algorithm),
            compress_algorithm: CompressAlgo::from(diff.compress_algorithm),
            before_hash: diff.before_hash,
            after_hash: diff.after_hash,
            patch: diff.patch,
        }
    }

    fn into(self) -> Diff {
        Diff {
            diff_algorithm: DiffAlgo::into(self.diff_algorithm),
            compress_algorithm: CompressAlgo::into(self.compress_algorithm),
            before_hash: self.before_hash,
            after_hash: self.after_hash,
            patch: self.patch,
        }
    }
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq, Hash)]
enum DiffAlgo {
    Rsync020,
    Bidiff1,
}

impl DiffAlgo {
    fn from(algo: DiffAlgorithm) -> Self {
        match algo {
            DiffAlgorithm::Bidiff1 => DiffAlgo::Bidiff1,
            DiffAlgorithm::Rsync020 => DiffAlgo::Rsync020,
        }
    }

    fn into(algo: DiffAlgo) -> DiffAlgorithm {
        match algo {
            DiffAlgo::Bidiff1 => DiffAlgorithm::Bidiff1,
            DiffAlgo::Rsync020 => DiffAlgorithm::Rsync020,
        }
    }
}

#[derive(Clone, Copy, Debug, Encode, Decode, PartialEq, Eq, Hash)]
enum CompressAlgo {
    None,
    Zstd,
}

impl CompressAlgo {
    fn from(algo: CompressAlgorithm) -> Self {
        match algo {
            CompressAlgorithm::None => CompressAlgo::None,
            CompressAlgorithm::Zstd => CompressAlgo::Zstd,
        }
    }

    fn into(algo: CompressAlgo) -> CompressAlgorithm {
        match algo {
            CompressAlgo::None => CompressAlgorithm::None,
            CompressAlgo::Zstd => CompressAlgorithm::Zstd,
        }
    }
}
