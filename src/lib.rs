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
mod encoding;
mod error;
mod lis;
mod types;

use error::Result;
pub use types::{ApplyOp, ChunkOp, Config};

use async_stream::try_stream;
use fastcdc::v2020::{AsyncStreamCDC, ChunkData};
use futures::pin_mut;
use sha2::{Digest, Sha256};
use std::{collections::HashSet, pin::Pin};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_stream::{Stream, StreamExt};

/// Returns a `Stream` of `ChunkOp` items representing the incremental delta between a prior version
/// (identified by its ordered list of chunk hashes) and a new version read from `new`.
///
/// # Small files (≤ `cfg.max_size`)
/// - `old_hashes` may be empty (new file) or contain exactly one hash (previous whole-file hash).
/// - When identical, the stream yields _no_ items.
/// - When modified, the fetcher is called once to obtain the old bytes, then exactly one
///   `Patch` or `Insert` op is yielded.
///
/// # Large files (> `cfg.max_size`)
/// - `old_hashes.len()` should match the chunk count of the previous version.
/// - Each content-defined chunk is compared by hash:
///   - If the hash matches the corresponding `old_hashes[index]`, a `Copy` op is yielded.
///   - Otherwise, the fetcher is invoked only for that chunk to generate a `Patch` or `Insert` op.
///   - Chunks beyond the end of `old_hashes` always yield `Insert`.
pub fn diff_stream<New, Fetch, Fut>(
    mut new: New,
    new_size: u64,
    cfg: Config,
    old_hashes: Vec<[u8; 32]>,
    mut fetch_base: Fetch,
) -> Pin<Box<dyn Stream<Item = Result<ChunkOp>>>>
where
    New: AsyncRead + Unpin + Send + 'static,
    Fetch: FnMut([u8; 32]) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Vec<u8>>> + Send,
{
    Box::pin(try_stream! {
        // -------- Small file path (Bidiff) --------
        if new_size <= cfg.max_size as u64 {
            let mut new_bytes = Vec::with_capacity(new_size as usize);
            new.read_to_end(&mut new_bytes).await?;
            // If we have an old hash, try to fetch bytes for diff; else treat as brand‑new Insert.
            if let Some(&old_hash) = old_hashes.first() {
                let old_bytes = fetch_base(old_hash).await?;
                if old_bytes == new_bytes {
                    // identical file, zero ops
                    return;
                }
                let patch = encoding::diff(&old_bytes, &new_bytes)?; // already zstd‑compressed + bincode
                if (patch.len() as f32) <= cfg.patch_threshold * (new_bytes.len() as f32) {
                    let new_hash = sha256(&new_bytes);
                    yield ChunkOp::Patch { index: 0, offset: 0, length: new_bytes.len(), new_hash, old_hash, patch };
                    return;
                }
            }
            // new file or patch not worthwhile
            let hash = sha256(&new_bytes);
            yield ChunkOp::Insert { index: 0, offset: 0, length: new_bytes.len(), hash, data: new_bytes };
            return;
        }

        // -------- Large file path (FastCDC + Bidiff) --------
        let lookup: HashSet<[u8;32]> = old_hashes.iter().copied().collect();
        let mut index = 0;

        let mut chunker = AsyncStreamCDC::new(&mut new, cfg.min_size, cfg.avg_size, cfg.max_size);
        let stream = chunker.as_stream();
        pin_mut!(stream);

        while let Some(Ok(ChunkData { data, offset, length, .. })) = stream.next().await {
            let hash = sha256(&data);
            if index < old_hashes.len() && hash == old_hashes[index] {
                // unchanged chunk
                yield ChunkOp::Copy { hash, length, index, offset };
            } else {
                // changed chunk – decide patch vs insert
                if index < old_hashes.len() && lookup.contains(&old_hashes[index]) {
                    let old_hash = old_hashes[index];
                    let base_bytes = fetch_base(old_hash).await?;
                    let patch = encoding::diff(&base_bytes, &data)?;
                    if (patch.len() as f32) <= cfg.patch_threshold * (data.len() as f32) {
                        yield ChunkOp::Patch { index, offset, length, new_hash: hash, patch, old_hash };
                    } else {
                        yield ChunkOp::Insert { index, offset, length, hash, data };
                    }
                } else {
                    // new chunk appended beyond previous length
                    yield ChunkOp::Insert { index, offset, length, hash, data };
                }
            }
            index += 1;
        }
    })
}

/* ---------------- helpers ---------------- */

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

/* ---------------- tests (only small subset shown) ---------------- */

#[cfg(test)]
mod tests {
    use crate::error::Error;

    use super::*;
    use rand::{Rng, SeedableRng};
    use std::{
        io::Cursor,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    fn deterministic_bytes(len: usize) -> Vec<u8> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(256);
        (0..len).map(|_| rng.random::<u8>()).collect()
    }

    // ------ Small, ops returned -------------------

    #[tokio::test]
    async fn small_file_insert_vs_patch() {
        let cfg = Config::default();
        let mut base = vec![0u8; 256 * 1024];
        for i in 0..base.len() {
            base[i] = (i % 251) as u8;
        }
        let mut next = base.clone();

        // tiny change – expect Patch
        next[123] ^= 0x55;
        let base_hash = sha256(&base);
        {
            let base_c = Arc::new(base.clone());
            let mut stream = diff_stream(
                Cursor::new(next.clone()),
                next.len() as u64,
                cfg,
                vec![base_hash],
                move |_hash| {
                    let b = base_c.clone();
                    async move { Ok(b.to_vec()) }
                },
            );
            while let Some(Ok(op)) = stream.next().await {
                assert!(matches!(op, ChunkOp::Patch { .. }));
            }
        }

        // big change – expect Insert
        // next: keep first 32 KiB unchanged, replace the remaining 224 KiB with random data
        let mut r = rand::rngs::StdRng::seed_from_u64(256); // deterministic size output for documentation purposes
        let mut next = base.clone();
        for byte in &mut next[32 * 1024..] {
            // 224 KiB region
            *byte = r.random();
        }
        // this yields (with deterministic seed):
        //   literal_size              = 262 144 B
        //   create_patch(&base, next) = 229 536 B (random bytes don’t compress)
        // so patch_size > 0.80 × literal_size ⇒ diff_async will return Insert

        let base_c = Arc::new(base.clone());
        let mut stream = diff_stream(
            Cursor::new(next.clone()),
            next.len() as u64,
            cfg,
            vec![base_hash],
            move |_hash| {
                let b = base_c.clone();
                async move { Ok(b.to_vec()) }
            },
        );
        while let Some(Ok(op)) = stream.next().await {
            assert!(matches!(op, ChunkOp::Insert { .. }));
        }
    }

    // #[tokio::test]
    // async fn small_file_roundtrip_patch() {
    //     let cfg = Config::default();
    //     let base = deterministic_bytes(1 * 1024 * 1024); // 1 MiB
    //     let mut next = base.clone();
    //     for b in &mut next[10_000..10_032] {
    //         *b ^= 0xFF;
    //     } // 32‑byte contiguous flip

    //     let base_hash = sha256(&base);

    //     let base_c = Arc::new(base.clone());
    //     let mut stream = diff_stream(
    //         Cursor::new(next.clone()),
    //         next.len() as u64,
    //         cfg,
    //         vec![base_hash],
    //         move |_hash| {
    //             let b = base_c.clone();
    //             async move { Ok(b.to_vec()) }
    //         },
    //     );

    //     let Some(Ok(op)) = stream.next().await else {
    //         panic!("Expect exactly one op");
    //     };
    //     assert!(matches!(op, ChunkOp::Patch { .. }));
    //     assert!(matches!(stream.next().await, None), "Expect exactly one op");

    //     let mut out = Vec::new();
    //     apply(&mut out, &[op], |_h| {
    //         let b = base.clone();
    //         async move { Ok(b) }
    //     })
    //     .await
    //     .unwrap();

    //     assert_eq!(out, next);
    // }

    // ------ Large, ops returned -------------------

    #[tokio::test]
    async fn large_file_unchanged_copy_only() {
        let cfg = Config::default();
        // 8 MiB deterministic data
        let data = deterministic_bytes(8 * 1024 * 1024);

        // Build the old‐hash list via FastCDC
        let mut cur = Cursor::new(data.clone());
        let mut old_hashes = Vec::new();

        let mut chunker = AsyncStreamCDC::new(&mut cur, cfg.min_size, cfg.avg_size, cfg.max_size);
        let stream = chunker.as_stream();
        pin_mut!(stream);

        while let Some(Ok(chunk)) = stream.next().await {
            old_hashes.push(sha256(&chunk.data));
        }

        // Diff against identical data; fetcher is never be called
        let mut stream = diff_stream(
            Cursor::new(data.clone()),
            data.len() as u64,
            cfg,
            old_hashes,
            |_hash| async { panic!("fetcher should not be called") },
        );

        let mut ops = 0;
        while let Some(Ok(op)) = stream.next().await {
            assert!(
                matches!(op, ChunkOp::Copy { .. }),
                "all ops must be Copy for identical data"
            );
            ops += 1;
        }

        assert!(ops >= 1, "should produce at least one op");
    }

    #[tokio::test]
    async fn large_file_single_chunk_change() {
        // force fixed-size chunks
        let cfg = Config {
            min_size: 1024,
            avg_size: 1024,
            max_size: 1024,
            patch_threshold: 0.8,
        };
        let mut data = vec![0u8; 4 * cfg.avg_size as usize];
        // fill with a pattern so chunks realign trivially
        for i in 0..data.len() {
            data[i] = (i / cfg.avg_size as usize) as u8;
        }

        // record original hashes/positions
        let (hashes, positions) = {
            let mut cur = Cursor::new(&data);
            let mut chunker =
                AsyncStreamCDC::new(&mut cur, cfg.min_size, cfg.avg_size, cfg.max_size);
            let stream = chunker.as_stream();
            pin_mut!(stream);
            let mut hs = Vec::new();
            let mut ps = Vec::new();
            let mut offset = 0;
            while let Some(Ok(chunk)) = stream.next().await {
                ps.push((offset, chunk.data.len()));
                hs.push(sha256(&chunk.data));
                offset += chunk.data.len();
            }
            (hs, ps)
        };

        // mutate exactly one chunk (the second)
        let (start, _len) = positions[1];
        data[start] ^= 0xFF;

        let fetcher = {
            let data_arc = Arc::new(data.clone());
            let hashes_arc = Arc::new(hashes.clone());
            let positions_arc = Arc::new(positions.clone());
            move |base_hash: [u8; 32]| {
                // Clone the Arcs into the async block
                let data_arc = data_arc.clone();
                let hashes_arc = hashes_arc.clone();
                let positions_arc = positions_arc.clone();
                async move {
                    // locate the index of this base_hash
                    let idx = hashes_arc
                        .iter()
                        .position(|&h| h == base_hash)
                        .expect("unknown base hash");
                    let (pos, len) = positions_arc[idx];
                    Ok(data_arc[pos..pos + len].to_vec())
                }
            }
        };

        // diff and count ops
        let mut stream = diff_stream(
            Cursor::new(data.clone()),
            data.len() as u64,
            cfg,
            hashes,
            fetcher,
        );
        let mut non_copy = 0;
        while let Some(Ok(chunk)) = stream.next().await {
            match chunk {
                ChunkOp::Copy { .. } => {}
                _ => non_copy += 1,
            }
        }
        assert_eq!(non_copy, 1, "expected exactly one Patch/Insert");
    }

    // --------- Boundaries -------------------------

    // files right at the ≤ max_size “small file” path
    #[tokio::test]
    async fn boundary_size_patch_path() {
        let cfg = Config::default();
        let max = cfg.max_size;
        let data = deterministic_bytes((max - 1) as usize);
        let h = sha256(&data);

        let data_c = Arc::new(data.clone());
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let fetch_ctr = fetch_count.clone();

        let mut stream = diff_stream(
            Cursor::new(data.clone()),
            data.len() as u64,
            cfg,
            vec![h],
            move |_h: [u8; 32]| {
                let d = data_c.clone();
                fetch_ctr.fetch_add(1, Ordering::SeqCst);
                async move { Ok(d.to_vec()) }
            },
        );
        let mut ops = 0;
        while let Some(Ok(op)) = stream.next().await {
            assert!(
                matches!(op, ChunkOp::Copy { .. }),
                "all ops must be Copy for identical data"
            );
            ops += 1;
        }

        assert_eq!(ops, 0, "expected no diff ops for small identical file");
        assert_eq!(
            fetch_count.load(Ordering::SeqCst),
            1,
            "expected exactly one fetch call in small-file path"
        );
    }

    // files just over max_size -> chunked path kicks in
    #[tokio::test]
    async fn boundary_size_chunk_path() {
        let cfg = Config::default();
        let size = cfg.max_size + 1;
        let data = deterministic_bytes(size as usize);

        // Build old_hash list
        let mut cur = Cursor::new(data.clone());
        let mut old_hashes = Vec::new();

        let mut chunker = AsyncStreamCDC::new(&mut cur, cfg.min_size, cfg.avg_size, cfg.max_size);
        let stream = chunker.as_stream();
        pin_mut!(stream);

        while let Some(Ok(chunk)) = stream.next().await {
            old_hashes.push(sha256(&chunk.data));
        }

        // Diff identical data -> all Copy ops, and more than one chunk
        let mut stream = diff_stream(
            Cursor::new(data.clone()),
            data.len() as u64,
            cfg,
            old_hashes,
            |_h| async { panic!("fetcher should not be called for chunked identical file") },
        );
        let mut ops = 0;
        while let Some(Ok(op)) = stream.next().await {
            assert!(
                matches!(op, ChunkOp::Copy { .. }),
                "all ops must be Copy for identical data"
            );
            ops += 1;
        }
        assert!(ops > 1, "expected multiple ops for chunked path");
    }

    // --------- Selective fetch --------------------

    #[tokio::test]
    async fn selective_fetch_invocations() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Configuration
        let cfg = Config::default();
        // 8 MiB deterministic data
        let base = deterministic_bytes(8 * 1024 * 1024);

        // Compute FastCDC chunk boundaries & hashes
        let mut cur = Cursor::new(base.clone());
        let mut hashes = Vec::new();
        let mut positions = Vec::new();
        let mut offset = 0;

        let mut chunker = AsyncStreamCDC::new(&mut cur, cfg.min_size, cfg.avg_size, cfg.max_size);
        let stream = chunker.as_stream();
        pin_mut!(stream);

        while let Some(Ok(chunk)) = stream.next().await {
            positions.push((offset, chunk.data.len()));
            hashes.push(sha256(&chunk.data));
            offset += chunk.data.len();
        }

        // Make two scattered changes: flip first byte in chunk 1 and chunk 3
        let mut next = base.clone();
        for &idx in [1, 3].iter().filter(|&&i| i < positions.len()) {
            let (start, _) = positions[idx];
            next[start] ^= 0xFF;
        }

        // Prepare fetch invocations
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let fetch_counter = fetch_count.clone();
        let hashes = Arc::new(hashes);
        let positions = Arc::new(positions);
        let base = Arc::new(base);
        let counter = fetch_counter.clone();

        let fetcher = {
            let hashes = hashes.clone();
            move |base_hash: [u8; 32]| {
                let hashes = hashes.clone();
                let positions = positions.clone();
                let base = base.clone();
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let idx = hashes.iter().position(|&h| h == base_hash).unwrap();
                    let (pos, len) = positions[idx];
                    Ok(base[pos..pos + len].to_vec())
                }
            }
        };

        // Diff, with fetcher that only retrieves changed chunk bytes
        let mut stream = diff_stream(
            Cursor::new(next.clone()),
            next.len() as u64,
            cfg,
            hashes.to_vec(),
            fetcher,
        );
        let mut non_copy = 0;
        while let Some(Ok(op)) = stream.next().await {
            if !matches!(op, ChunkOp::Copy { .. }) {
                non_copy += 1; // count non-Copy ops
            }
        }

        // The number of fetch calls must equal the number of changed chunks (non-Copy ops)
        assert_eq!(
            fetch_count.load(Ordering::SeqCst),
            non_copy,
            "fetcher should be called exactly once per non-Copy chunk"
        );
    }

    // --------- Error propagation ------------------

    #[tokio::test]
    async fn error_propagation_fetch_base_small_file() {
        let cfg = Config::default();
        // small‐file path: mutate one byte
        let base = deterministic_bytes(512);
        let mut next = base.clone();
        next[0] ^= 0xFF;
        let base_hash = sha256(&base);

        let mut stream = diff_stream(
            Cursor::new(next),
            512u64,
            cfg,
            vec![base_hash],
            move |hash: [u8; 32]| async move { Err(Error::MissingBase(hash)) },
        );
        while let Some(res) = stream.next().await {
            assert!(matches!(res, Err(Error::MissingBase(h)) if h == base_hash));
        }
    }

    // #[tokio::test]
    // async fn apply_error_propagation() {
    //     // prepare a Copy op pointing at a non‐existent chunk
    //     let ops = vec![ChunkOp::Copy {
    //         hash: [1; 32],
    //         length: 10,
    //         index: 0,
    //         offset: 0,
    //     }];
    //     let mut out = Vec::new();

    //     // fetcher always fails
    //     let result = apply(&mut out, &ops, move |h: &[u8; 32]| {
    //         let hash_owned = *h;
    //         async move { Err(Error::MissingBase(hash_owned)) }
    //     })
    //     .await;

    //     assert!(matches!(result, Err(Error::MissingBase(h)) if h == [1; 32]));
    // }

    // --------- Deterministic diff ------------------

    // temporary, to be replaced
    #[tokio::test]
    async fn deterministic_diff_small_file() {
        let cfg = Config::default();
        // Prepare a small deterministic file with a tiny change
        let mut base = deterministic_bytes(512 * 1024);
        for i in 0..100 {
            base[i] ^= 0xAA;
        }
        let mut next = base.clone();
        next[1024] ^= 0x55;
        let base_hash = sha256(&base);

        // Run diff twice

        let bc_1 = base.clone();
        let bc_2 = base.clone();
        let mut ops1 = vec![];
        let mut ops2 = vec![];

        let mut stream = diff_stream(
            Cursor::new(next.clone()),
            next.len() as u64,
            cfg,
            vec![base_hash],
            move |hash: [u8; 32]| {
                let b = bc_1.clone();
                async move {
                    assert_eq!(hash, base_hash);
                    Ok(b)
                }
            },
        );
        while let Some(Ok(op)) = stream.next().await {
            ops1.push(op);
        }

        let mut stream = diff_stream(
            Cursor::new(next.clone()),
            next.len() as u64,
            cfg,
            vec![base_hash],
            move |hash: [u8; 32]| {
                let b = bc_2.clone();
                async move {
                    assert_eq!(hash, base_hash);
                    Ok(b)
                }
            },
        );
        while let Some(Ok(op)) = stream.next().await {
            ops2.push(op);
        }

        // Compare via Debug representation for exact equality
        assert_eq!(format!("{:?}", ops1), format!("{:?}", ops2));
    }

    // temporary, to be replaced
    #[tokio::test]
    async fn deterministic_diff_large_file() {
        let cfg = Config::default();
        // 8 MiB deterministic data with one chunk flip
        let mut data = deterministic_bytes(8 * 1024 * 1024);
        // identify chunk boundaries + hashes
        let mut cur = Cursor::new(data.clone());
        let mut hashes = Vec::new();
        let mut positions = Vec::new();
        let mut offset = 0;

        let mut chunker = AsyncStreamCDC::new(&mut cur, cfg.min_size, cfg.avg_size, cfg.max_size);
        let stream = chunker.as_stream();
        pin_mut!(stream);

        while let Some(Ok(chunk)) = stream.next().await {
            positions.push((offset, chunk.data.len()));
            hashes.push(sha256(&chunk.data));
            offset += chunk.data.len();
        }

        // mutate first byte of chunk 2
        if let Some((start, _)) = positions.get(1) {
            data[*start] ^= 0xFF;
        }

        // Prepare clones for the first diff
        let ops1 = {
            let data_1 = Arc::new(data.clone());
            let hashes_1 = Arc::new(hashes.clone());
            let positions_1 = Arc::new(positions.clone());

            let mut stream = diff_stream(
                Cursor::new(data_1.to_vec()),
                data_1.len() as u64,
                cfg,
                hashes_1.to_vec(),
                {
                    let data = data_1.clone();
                    let hashes = hashes_1.clone();
                    let positions = positions_1.clone();
                    move |base_hash: [u8; 32]| {
                        let data = data.clone();
                        let hashes = hashes.clone();
                        let positions = positions.clone();
                        async move {
                            let idx = hashes.iter().position(|&h| h == base_hash).unwrap();
                            let (pos, len) = positions[idx];
                            Ok(data[pos..pos + len].to_vec())
                        }
                    }
                },
            );
            let mut ops = vec![];
            while let Some(Ok(op)) = stream.next().await {
                ops.push(op);
            }
            ops
        };

        // Prepare fresh clones for the second diff
        let ops2 = {
            let data_2 = Arc::new(data.clone());
            let hashes_2 = Arc::new(hashes.clone());
            let positions_2 = Arc::new(positions.clone());

            let mut stream = diff_stream(
                Cursor::new(data_2.to_vec()),
                data_2.len() as u64,
                cfg,
                hashes_2.to_vec(),
                {
                    let data = data_2.clone();
                    let hashes = hashes_2.clone();
                    let positions = positions_2.clone();
                    move |base_hash: [u8; 32]| {
                        let data = data.clone();
                        let hashes = hashes.clone();
                        let positions = positions.clone();
                        async move {
                            let idx = hashes.iter().position(|&h| h == base_hash).unwrap();
                            let (pos, len) = positions[idx];
                            Ok(data[pos..pos + len].to_vec())
                        }
                    }
                },
            );
            let mut ops = vec![];
            while let Some(Ok(op)) = stream.next().await {
                ops.push(op);
            }
            ops
        };

        // Compare ops for exact equality
        assert_eq!(format!("{:?}", ops1), format!("{:?}", ops2));
    }

    // ---------- Memory ceiling ---------------------

    mod mem_test {
        use super::*;
        use futures::StreamExt;
        use std::{
            mem::MaybeUninit,
            pin::Pin,
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
            task::{Context, Poll},
            thread,
            time::Duration,
        };
        use sysinfo::{Pid, ProcessesToUpdate, System};
        use tokio::io::{AsyncRead, ReadBuf};

        /// Emits `size` zeroes without buffering the whole stream.
        struct ZeroStream {
            remaining: u64,
        }

        impl ZeroStream {
            fn new(size: u64) -> Self {
                Self { remaining: size }
            }
        }

        impl AsyncRead for ZeroStream {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                if self.remaining == 0 {
                    return Poll::Ready(Ok(()));
                }
                let to_emit = buf.remaining().min(self.remaining as usize);
                // Initialize that many slots to zero
                let unfilled = unsafe { buf.unfilled_mut() };
                for slot in unfilled.iter_mut().take(to_emit) {
                    *slot = MaybeUninit::new(0);
                }
                buf.advance(to_emit);
                self.remaining -= to_emit as u64;
                Poll::Ready(Ok(()))
            }
        }

        #[cfg(all(test, target_os = "windows"))]
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn memory_ceiling_streaming() {
            use tokio::time::Instant;
            const SIZE: u64 = 1 << 30; // 1 GiB
            let cfg = Config::default();
            let mut now = Instant::now();

            // 1) Precompute FastCDC chunk hashes (no data retained)
            let mut old_hashes = Vec::new();
            {
                println!(
                    "[memory] -> 1.1) -- @{:.3} s, pre-computing hashes, acquiring stream..",
                    now.elapsed().as_millis() as f64 / 1000f64
                );
                now = Instant::now();
                let mut cur = ZeroStream::new(SIZE);
                let mut chunker =
                    AsyncStreamCDC::new(&mut cur, cfg.min_size, cfg.avg_size, cfg.max_size);
                let stream = chunker.as_stream();
                pin_mut!(stream);

                println!(
                    "[memory] -> 1.2) -- @{:.3} s, stream acquired, streaming..",
                    now.elapsed().as_millis() as f64 / 1000f64
                );
                now = Instant::now();

                while let Some(Ok(chunk)) = stream.next().await {
                    old_hashes.push(sha256(&chunk.data));
                }

                println!(
                    "[memory] -> 1.3) -- @{:.3} s, {} chunks streamed",
                    now.elapsed().as_millis() as f64 / 1000f64,
                    old_hashes.len()
                );
                now = Instant::now();
            }

            // 2) start RSS polling
            let peak_rss = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let peak = peak_rss.clone();
            let pid = Pid::from_u32(std::process::id());
            let stop = Arc::new(AtomicBool::new(false));
            let stop_c = stop.clone();

            println!(
                "[memory] --> 2.1) -- @{:.3} s, spawning poller..",
                now.elapsed().as_millis() as f64 / 1000f64
            );
            now = Instant::now();
            let poller = thread::spawn(move || {
                println!(
                    "[memory] --> 2.2) -- @{:.3} s, entered poller",
                    now.elapsed().as_millis() as f64 / 1000f64
                );
                now = Instant::now();
                let mut sys = System::new_all();
                while !stop_c.load(Ordering::Acquire) {
                    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
                    if let Some(proc) = sys.process(pid) {
                        let mem_bytes = proc.memory();
                        let _ = peak.fetch_max(mem_bytes, Ordering::Relaxed);
                    }
                    thread::sleep(Duration::from_millis(50));
                }
                println!(
                    "[memory] --> 2.3) -- @{:.3} s, exited poller",
                    now.elapsed().as_millis() as f64 / 1000f64
                );
            });

            // 3) Consume the streaming diff API
            println!(
                "[memory] ---> 3.1) -- @{:.3} s, creating diff stream",
                now.elapsed().as_millis() as f64 / 1000f64
            );
            now = Instant::now();
            let mut stream = diff_stream(
                ZeroStream::new(SIZE),
                SIZE,
                cfg.clone(),
                old_hashes.clone(),
                |_h| async { panic!("no fetch expected for identical chunks") },
            );
            println!(
                "[memory] ---> 3.2) -- @{:.3} s, beginning stream read..",
                now.elapsed().as_millis() as f64 / 1000f64
            );
            now = Instant::now();
            let mut first_log = 0;
            let mut op_count = 0;
            while let Some(item) = stream.next().await {
                if first_log == 0 {
                    println!(
                        "[memory] ---> 3.3) -- @{:.3} s, first chunk op read, continuing..",
                        now.elapsed().as_millis() as f64 / 1000f64
                    );
                    now = Instant::now();
                    first_log += 1;
                }
                op_count += 1;

                let op = item.unwrap();
                // drop any payload immediately
                match op {
                    ChunkOp::Copy { .. } => {}
                    ChunkOp::Patch { patch, .. } => drop(patch),
                    ChunkOp::Insert { data, .. } => drop(data),
                }
            }
            println!(
                "[memory] ---> 3.4) -- @{:.3} s, {op_count} chunk ops read",
                now.elapsed().as_millis() as f64 / 1000f64
            );
            now = Instant::now();

            // 4) Stop and join
            println!(
                "[memory] ----> 4.) -- @{:.3} s, stopping polling",
                now.elapsed().as_millis() as f64 / 1000f64
            );
            now = Instant::now();
            stop.store(true, Ordering::Release);
            poller.join().unwrap();

            // 5) Assert
            println!(
                "[memory] -----> 5.1) -- @{:.3} s, asserting mem results..",
                now.elapsed().as_millis() as f64 / 1000f64
            );
            let rss = bytes_to_mib(peak_rss.load(Ordering::SeqCst));
            let required = bytes_to_mib(cfg.max_size as u64 * 6); // 24 MiB: slight marginal, from observations of 20.3 MiB used
            assert!(
                rss < required,
                "Memory ceiling exceeded, RSS too high: {rss:.2} MiB, required < {required:.2} MiB"
            );
            println!(
                "[memory] -----> 5.2) -- @{:.3} s, All good, mem results are within bounds. RSS: {rss:.2} MiB, required: <{required:.2} MiB",
                now.elapsed().as_millis() as f64 / 1000f64
            );
        }

        fn bytes_to_mib(val: u64) -> f64 {
            val as f64 / (1024f64 * 1024f64)
        }
    }
}
