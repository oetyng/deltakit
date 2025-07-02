//! `deltakit` – streaming hybrid FastCDC + binary-diff crate.
//!
//! High‑level rules:
//! * Files ≤ `cfg.max_size` (default 4 MiB) ⇒ treat as a single chunk; produce **Patch** vs **Insert** by comparing
//!   fully encoded `files_diff` output with a configurable `patch_threshold` (ratio of literal).
//! * Files > `cfg.max_size` ⇒ stream with FastCDC.  For chunk _i_, consult `old_hashes[i]` – if identical, emit
//!   **Copy**. Otherwise fetch that single base chunk via the async fetcher and decide Patch vs Insert.
//! * No full scanning of the previous version is required; only bytes needed for mismatching chunks are fetched.
//!
//! SPDX‑License‑Identifier: Apache‑2.0 OR MIT

mod apply;
mod diff;
mod encoding;
mod error;
mod old_diff;
mod types;

pub use apply::apply_stream as apply;
pub use diff::diff_stream as diff;
pub use error::{ApplyError, DeltaError};
pub use types::{ApplyOp, ChunkOp, Config};
