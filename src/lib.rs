//! `deltakit` – streaming hybrid FastCDC + binary-diff crate.
//!
//! High‑level rules:
//! * Files ≤ `cfg.max_size` (default 4 MiB) ⇒ treat as a single chunk; produce **Patch** vs **Insert** by comparing
//!   fully encoded diff output with a configurable `patch_threshold` (ratio of literal).
//! * Files > `cfg.max_size` ⇒ stream with FastCDC. For chunk _i_, consult `old_hashes[i]` – if identical, emit
//!   **Copy**. Otherwise fetch that single base chunk via the async fetcher and decide **Patch** vs **Insert**.
//! * No full scanning of the previous version is required; only bytes needed for mismatching chunks are fetched.
//!
//! SPDX‑License‑Identifier: Apache‑2.0 OR MIT

mod apply;
mod diff;
mod encoding;
mod error;
mod types;

pub use apply::apply_stream as apply;
pub use diff::diff_stream as diff;
pub use encoding::{apply as apply_one, diff as diff_one};
pub use error::{ApplyError, DeltaError};
pub use types::{ApplyOp, ChunkOp, ChunkProgress, Config};

#[cfg(test)]
mod tests {
    use super::*;
    use fastcdc::v2020::AsyncStreamCDC;
    use sha2::{Digest, Sha256};
    use std::{
        collections::HashMap,
        io::{self, Read, Seek, Write},
        sync::Arc,
    };
    use tempfile::NamedTempFile;
    use tokio::{fs::OpenOptions, sync::mpsc};
    use tokio_stream::StreamExt;

    fn sha256(data: &[u8]) -> [u8; 32] {
        Sha256::digest(data).into()
    }

    #[tokio::test]
    async fn round_trip_diff_apply() -> Result<(), Box<dyn std::error::Error>> {
        /* ---------- sample payloads ---------- */
        let old_bytes = b"The quick brown fox jumps over the lazy dog".to_vec();
        let new_bytes = b"The quick brown fox jumps over the very lazy dog".to_vec();
        let old_hash = sha256(&old_bytes);

        /* ---------- shared base store + fetch ---------- */
        let store: Arc<HashMap<[u8; 32], Vec<u8>>> =
            Arc::new(HashMap::from([(old_hash, old_bytes.clone())]));

        let fetch = {
            let store = store.clone();
            move |hash| {
                let store = store.clone();
                async move { Ok::<Vec<u8>, ()>(store.get(&hash).unwrap().clone()) }
            }
        };

        // /* ---------- build streams ---------- */
        let cfg = Config {
            max_size: 1024, // small-file path
            ..Config::default()
        };
        let new_len = new_bytes.len() as u64;
        let cursor = io::Cursor::new(new_bytes.clone());
        let diff_stream = super::diff(cursor, new_len, cfg, vec![old_hash], fetch);
        let apply_stream = super::test_helper::chunk_to_apply(diff_stream, store.clone());

        /* ---------- sink (temp-file) ---------- */
        let tmp = NamedTempFile::new().unwrap();
        let sink = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .await?;
        let (tx, _rx) = mpsc::unbounded_channel();
        super::apply(sink, apply_stream, tx).await.unwrap();

        // --- verify ---
        let result = std::fs::read(tmp.path()).unwrap();
        assert_eq!(result, new_bytes);

        Ok(())
    }

    #[tokio::test]
    async fn chunks_map_to_file() -> Result<(), Box<dyn std::error::Error>> {
        /* ---------- Setup file ---------- */
        const SIZE: usize = 80 * 1024 * 1024;
        let mut old_bytes = vec![0u8; SIZE];
        for (i, v) in old_bytes.iter_mut().enumerate() {
            *v = (i % 251) as u8;
        }
        let mut new_bytes = old_bytes.clone();
        // flip a couple of bytes in two different chunks
        new_bytes[1_100_000] ^= 0xAA;
        new_bytes[5_200_000] ^= 0x55;

        let mut tmp = NamedTempFile::new()?;
        tmp.write(&new_bytes)?;

        /* ---------- build base-store + hash list ---------- */
        let cfg = Config::default();
        let mut old_hashes = Vec::new();
        let store = {
            let mut store = HashMap::new();
            let mut reader = io::Cursor::new(old_bytes.clone());
            let mut cdc =
                AsyncStreamCDC::new(&mut reader, cfg.min_size, cfg.avg_size, cfg.max_size);
            let cdc = cdc.as_stream();
            futures::pin_mut!(cdc);

            while let Some(Ok(cd)) = cdc.next().await {
                let hash = sha256(&cd.data);
                store.insert(hash, cd.data.to_vec());
                old_hashes.push(hash);
            }
            Arc::new(store)
        };

        /* ---------- fetch closure ---------- */
        let fetch = {
            let store = store.clone();
            move |h| {
                let store = store.clone();
                async move { Ok::<Vec<u8>, ()>(store.get(&h).unwrap().clone()) }
            }
        };

        /* ---------- Setup diff stream ---------- */
        let cfg = Config::default();
        let new_len = new_bytes.len() as u64;
        let cursor = io::Cursor::new(new_bytes.clone());
        let diff_stream = super::diff(cursor, new_len, cfg, old_hashes, fetch);
        futures::pin_mut!(diff_stream);

        fn validate(
            offset: u64,
            length: usize,
            tmp: &mut NamedTempFile,
            new_bytes: &[u8],
            data: Option<Vec<u8>>,
        ) {
            let mut orig_bytes = vec![0u8; length];
            tmp.seek(io::SeekFrom::Start(offset)).unwrap();
            tmp.read_exact(&mut orig_bytes).unwrap();
            let offset = offset as usize;
            assert_eq!(orig_bytes, &new_bytes[offset..offset + length]);
            data.map(|d| assert_eq!(orig_bytes, d));
        }

        let mut total_len = 0;

        /* ---------- Loop through chunks ---------- */
        while let Some(Ok(op)) = diff_stream.next().await {
            match op {
                ChunkOp::Copy { offset, length, .. } => {
                    total_len += length;
                    validate(offset, length, &mut tmp, &new_bytes, None);
                }
                ChunkOp::Insert {
                    offset,
                    length,
                    data,
                    ..
                } => {
                    total_len += length;
                    validate(offset, length, &mut tmp, &new_bytes, Some(data));
                }
                ChunkOp::Patch { length, .. } => {
                    total_len += length;
                }
            }
        }

        let file_size = tmp.as_file().metadata()?.len();
        assert_eq!(total_len, new_len as usize);
        assert_eq!(total_len, file_size as usize);

        Ok(())
    }

    #[tokio::test]
    async fn round_trip_large_file() -> Result<(), Box<dyn std::error::Error>> {
        /* ---------- make 8 MiB base + tweak ---------- */
        const SIZE: usize = 80 * 1024 * 1024;
        let mut old_bytes = vec![0u8; SIZE];
        for (i, v) in old_bytes.iter_mut().enumerate() {
            *v = (i % 251) as u8;
        }
        let mut new_bytes = old_bytes.clone();
        // flip a couple of bytes in two different chunks
        new_bytes[1_100_000] ^= 0xAA;
        new_bytes[5_200_000] ^= 0x55;

        /* ---------- build base-store + hash list ---------- */
        let cfg = Config::default();
        let mut old_hashes = Vec::new();
        let store = {
            let mut store = HashMap::new();
            let mut reader = io::Cursor::new(old_bytes.clone());
            let mut cdc =
                AsyncStreamCDC::new(&mut reader, cfg.min_size, cfg.avg_size, cfg.max_size);
            let cdc = cdc.as_stream();
            futures::pin_mut!(cdc);

            while let Some(Ok(cd)) = cdc.next().await {
                let hash = sha256(&cd.data);
                store.insert(hash, cd.data.to_vec());
                old_hashes.push(hash);
            }
            Arc::new(store)
        };

        /* ---------- fetch closure ---------- */
        let fetch = {
            let store = store.clone();
            move |h| {
                let store = store.clone();
                async move { Ok::<Vec<u8>, ()>(store.get(&h).unwrap().clone()) }
            }
        };

        // /* ---------- build streams ---------- */
        let new_len = new_bytes.len() as u64;
        let cursor = io::Cursor::new(new_bytes.clone());
        let diff_stream = super::diff(cursor, new_len, cfg, old_hashes, fetch);
        let apply_stream = super::test_helper::chunk_to_apply(diff_stream, store.clone());

        /* ---------- sink (temp-file) ---------- */
        let tmp = NamedTempFile::new().unwrap();
        let sink = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .await?;
        let (tx, _rx) = mpsc::unbounded_channel();
        super::apply(sink, apply_stream, tx).await.unwrap();

        // --- verify ---
        let result = std::fs::read(tmp.path()).unwrap();
        assert_eq!(result, new_bytes);

        Ok(())
    }
}

#[cfg(test)]
mod test_helper {
    use super::*;
    use crate::error::DeltaResult;

    use async_stream::try_stream;
    use futures::pin_mut;
    use std::{collections::HashMap, pin::Pin, sync::Arc};
    use tokio_stream::{Stream, StreamExt};

    /// Convert `ChunkOp` as it arrives into `ApplyOp` and forward directly.
    pub fn chunk_to_apply<'a, E: std::fmt::Debug + 'a>(
        diff: Pin<Box<dyn Stream<Item = DeltaResult<ChunkOp, E>> + Send + 'a>>,
        store: Arc<HashMap<[u8; 32], Vec<u8>>>,
    ) -> impl Stream<Item = Result<ApplyOp, E>> + Unpin {
        Box::pin(try_stream! {
            pin_mut!(diff);
            while let Some(op) = diff.next().await {
                match op.unwrap() {
                    ChunkOp::Copy { index, offset, hash, length, .. } => {
                        let bytes = store[&hash][..length].to_vec();
                        yield ApplyOp::Data { index: index as u64, offset, bytes };
                    }
                    ChunkOp::Patch { index, offset, old_hash, patch, .. } => {
                        let base  = store[&old_hash].clone();
                        yield ApplyOp::Patch { index: index as u64, offset, base, patch };
                    }
                    ChunkOp::Insert { index, offset, data, .. } => {
                        yield ApplyOp::Data { index: index as u64, offset, bytes: data };
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;
    use sha2::{Digest, Sha256};
    use std::{collections::HashMap, sync::Arc};
    use tempfile::NamedTempFile;
    use tokio::{fs::OpenOptions, sync::mpsc};

    fn sha256(d: &[u8]) -> [u8; 32] {
        Sha256::digest(d).into()
    }

    // mutate: up to 8 random byte-wise edits / inserts / deletes
    fn mutate(mut data: Vec<u8>, mut rng: proptest::test_runner::TestRng) -> Vec<u8> {
        use rand::{Rng, prelude::IndexedRandom};
        let actions = ["flip", "insert", "delete"];

        for _ in 0..rng.random_range(1..=8) {
            match *actions.choose(&mut rng).unwrap() {
                "flip" if !data.is_empty() => {
                    let i = rng.random_range(0..data.len());
                    data[i] ^= 1 << rng.random_range(0..8);
                }
                "insert" => {
                    let i = rng.random_range(0..=data.len());
                    data.insert(i, rng.random());
                }
                "delete" if !data.is_empty() => {
                    let i = rng.random_range(0..data.len());
                    data.remove(i);
                }
                _ => {}
            }
        }
        data
    }

    proptest! {
        // 1. Generates a 4–8 MiB random base file.
        // 2. Per test, performs 1 – 8 random byte-level flips/inserts/deletes to create a new version.
        // 3. Feeds the pair through diff_stream → apply_stream.
        // 4. Asserts the rebuilt file equals the mutation target.
        #[test]
        fn round_trip_random_edits(seed in any::<[u8; 32]>()) {
            // --- prepare deterministic RNG for reproducibility ---
            let mut rng = proptest::test_runner::TestRng::from_seed(prop::test_runner::RngAlgorithm::ChaCha, &seed);

            // --- generate base file 4–8 MiB of random bytes ---
            let len = rng.random_range(4 * 1024 * 1024..=8 * 1024 * 1024);
            let mut old_bytes = vec![0u8; len];
            rng.fill(&mut old_bytes[..]);

            // --- mutate to create new version ---
            let new_bytes = mutate(old_bytes.clone(), rng.clone());

            // --- build chunk store from old_bytes ---
            let cfg = Config::default();
            let mut store = HashMap::new();
            let mut old_hashes = Vec::new();
            let mut reader = std::io::Cursor::new(old_bytes.clone());

            for cd in fastcdc::v2020::StreamCDC::new(&mut reader, cfg.min_size, cfg.avg_size, cfg.max_size) {
                let cd = cd?;
                let h = sha256(&cd.data);
                store.insert(h, cd.data.to_vec());
                old_hashes.push(h);
            }
            let store = Arc::new(store);

            // --- fetch closure ---
            let fetch = {
                let store = store.clone();
                move |h| { let store = store.clone(); async move { Ok::<Vec<u8>, ()>(store[&h].clone()) } }
            };

            // /* ---------- build streams ---------- */
            let new_len = new_bytes.len() as u64;
            let cursor = std::io::Cursor::new(new_bytes.clone());
            let diff_stream = super::diff(cursor, new_len, cfg, old_hashes, fetch);
            let apply_stream = super::test_helper::chunk_to_apply(diff_stream, store.clone());

            /* ---------- sink temp-file ---------- */
            let tmp = NamedTempFile::new().unwrap();
            tokio_test::block_on(async {
                let sink = OpenOptions::new().read(true).write(true).open(tmp.path()).await.unwrap();
                let (tx,_rx) = mpsc::unbounded_channel();
                super::apply(sink, apply_stream, tx).await.unwrap();
            });

            // --- verify ---
            let result = std::fs::read(tmp.path()).unwrap();
            prop_assert_eq!(result, new_bytes);
        }
    }
}
