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

pub(super) fn apply_patch_old(base: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
    let (patch, _): (Patch, _) = bincode::decode_from_slice(delta, config::standard())?;
    let res = files_diff::apply(base, &patch.into());
    res.map_err(Error::Diff)
}

pub(super) fn apply_patch(base: &[u8], delta: Patch) -> Result<Vec<u8>> {
    let res = files_diff::apply(base, &delta.into());
    res.map_err(Error::Diff)
}

pub(super) fn create_patch_old(before: &[u8], after: &[u8]) -> Result<Vec<u8>> {
    let res = files_diff::diff(
        before,
        after,
        DiffAlgorithm::Bidiff1,
        CompressAlgorithm::Zstd,
    );
    let diff = res.map_err(Error::Diff)?;
    let patch = Patch::from(diff);
    let bytes = bincode::encode_to_vec(&patch, config::standard())?;
    Ok(bytes)
}

pub(super) fn create_patch(before: &[u8], after: &[u8]) -> Result<Patch> {
    let res = files_diff::diff(
        before,
        after,
        DiffAlgorithm::Bidiff1,
        CompressAlgorithm::Zstd,
    );
    let diff = res.map_err(Error::Diff)?;
    Ok(Patch::from(diff))
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct Patch {
    /// MD5 hash of the old data
    pub before_hash: String,
    /// MD5 hash of the new data
    pub after_hash: String,
    /// The actual patch bytes
    pub patch: Vec<u8>,
}

impl Patch {
    fn from(diff: Diff) -> Self {
        Patch {
            before_hash: diff.before_hash,
            after_hash: diff.after_hash,
            patch: diff.patch,
        }
    }

    fn into(self) -> Diff {
        Diff {
            diff_algorithm: DiffAlgorithm::Bidiff1,
            compress_algorithm: CompressAlgorithm::Zstd,
            before_hash: self.before_hash,
            after_hash: self.after_hash,
            patch: self.patch,
        }
    }
}
