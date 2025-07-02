//! .

use crate::types::ChunkOp;
use crate::{encoding, sha256};

use crate::error::{DeltaError, DeltaResult};

use async_stream::try_stream;
use fastcdc::v2020::{AsyncStreamCDC, ChunkData};
use futures::pin_mut;
use sha2::{Digest, Sha256};
use std::io::SeekFrom;
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt},
    sync::mpsc,
};
use tokio_stream::{Stream, StreamExt};

use futures::{Future, stream::FuturesUnordered};

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
    pub fallback_window: isize,
}

pub fn diff_stream<'a, New, Fetch, Fut, E>(
    mut new: New,
    new_size: u64,
    cfg: Config,
    old_hashes: Vec<[u8; 32]>,
    fetch_base: Fetch,
) -> Pin<Box<dyn Stream<Item = DeltaResult<ChunkOp, E>> + Send + 'a>>
where
    New: AsyncRead + AsyncSeek + Unpin + Send + 'a,
    Fetch: Fn([u8; 32]) -> Fut + Send + Sync + 'a,
    Fut: Future<Output = Result<Vec<u8>, E>> + Send,
    E: Send + 'a,
{
    Box::pin(try_stream! {
        /* ---------------- small-file path ----------------- */
        // ---------- ≤ max_size  →  full-file bidiff ----------
        if new_size <= cfg.max_size as u64 {
            let mut new_bytes = Vec::with_capacity(new_size as usize);
            new.read_to_end(&mut new_bytes).await?;
            if let Some(&base_hash) = old_hashes.first() {
                let base = fetch_base(base_hash).await.map_err(DeltaError::Fetch)?;
                if base == new_bytes { return } // identical
                let patch = encoding::create_patch(&base, &new_bytes)?;
                if patch.len() as f32 <= cfg.patch_threshold * new_bytes.len() as f32 {
                    let hash = sha256(&new_bytes);
                    yield ChunkOp::Patch { index: 0, offset: 0, length: patch.len(), hash, base_hash, data: patch };
                    return;
                }
            }
            // this is a new file or patch isn't not worthwhile
            let hash = sha256(&new_bytes);
            yield ChunkOp::Insert { index: 0, offset: 0, length: new_bytes.len(), hash, data: new_bytes };
            return;
        }

        /* ------------------ large-file path ------------------- */
        // ---------- > max_size  →  FastCDC + window-LIS ----------
        /* pass-1: hash only, record lens */
        struct Rec { len: usize, hash: [u8;32] }
        let mut index: Vec<Rec> = Vec::new();

        {
            let mut cdc = AsyncStreamCDC::new(
                &mut new,
                cfg.min_size,
                cfg.avg_size,
                cfg.max_size,
            );
            let cdc = cdc.as_stream();
            pin_mut!(cdc);
            while let Some(Ok(cd)) = cdc.next().await {
                index.push(Rec { len: cd.length, hash: Sha256::digest(&cd.data).into() });
            }
        }

        /* window-LIS copy detection */
        let w  = cfg.fallback_window as isize;
        let map: HashMap<_,_> =
            old_hashes.iter().copied().enumerate().map(|(i,h)|(h,i)).collect();
        let mut pairs = Vec::<(usize,usize)>::new();
        for (new_idx, rec) in index.iter().enumerate() {
            if let Some(&old_idx) = map.get(&rec.hash) {
                if (old_idx as isize - new_idx as isize).abs() <= w { pairs.push((new_idx, old_idx)); }
            }
        }
        let lis = lis_indices(&pairs.iter().map(|&(_,o)| o).collect::<Vec<_>>());
        let is_copy: HashSet<usize> =
            lis.into_iter().map(|idx| pairs[idx].0).collect();

        /* rewind reader for pass-2 */
        new.seek(SeekFrom::Start(0)).await?;

        /* channel + worker pool */
        let (tx, mut rx) = mpsc::unbounded_channel::<ChunkOp>();
        let mut workers: FuturesUnordered<_> = FuturesUnordered::new();
        let fetch_arc = std::sync::Arc::new(fetch_base);

        /* pass-2 */
        let mut cdc = AsyncStreamCDC::new(
                &mut new,
                cfg.min_size,
                cfg.avg_size,
                cfg.max_size,
            );
        let cdc = cdc.as_stream();
        pin_mut!(cdc);

        for (idx, rec) in index.into_iter().enumerate() {
            /* poll ready patch results */
            while let Ok(op) = rx.try_recv() { yield op }

            // if let Some(res) = workers.next().now_or_never().flatten() { res? }

            /* read payload of current chunk */
            let cd = match cdc.next().await {
                Some(Ok(c)) => c,
                Some(Err(e)) => Err::<ChunkData, DeltaError<E>>(e.into())?,
                None         => break,
            };

            if is_copy.contains(&idx) {
                yield ChunkOp::Copy { index: idx, offset: cd.offset, length: rec.len, hash: rec.hash };
                continue;
            }

            if idx < old_hashes.len() {
                let bh        = old_hashes[idx];
                let new_bytes = cd.data;
                let offset    = cd.offset;
                let threshold = cfg.patch_threshold;
                let txc       = tx.clone();
                let fetch     = fetch_arc.clone();

                /* push future into local pool */
                workers.push(async move {
                    let base = fetch(bh).await.map_err(DeltaError::Fetch)?;
                    let patch = encoding::create_patch(&base, &new_bytes)?;
                    let op = if patch.len() as f32 <= threshold * new_bytes.len() as f32 {
                        let hash = sha256(&patch);
                        ChunkOp::Patch { index: idx, offset, length: patch.len(), hash, base_hash: bh, data: patch }
                    } else {
                        ChunkOp::Insert { index: idx, offset, length: cd.length, hash: rec.hash, data: new_bytes }
                    };
                    txc.send(op).ok();
                    Ok::<(), DeltaError<E>>(())
                });
            } else {
                yield ChunkOp::Insert { index: idx, offset: cd.offset, length: cd.length, hash: rec.hash, data: cd.data };
            }
        }

        /* drain channel & workers */
        drop(tx);
        while let Some(op) = rx.recv().await { yield op; }
        while let Some(res) = workers.next().await { res?; }
    })
}

/// O(k log k) LIS on the sequence of old indices
#[inline]
fn lis_indices(seq: &[usize]) -> Vec<usize> {
    let mut piles: Vec<usize> = Vec::new();
    let mut prev: Vec<Option<usize>> = vec![None; seq.len()];

    for (i, &val) in seq.iter().enumerate() {
        match piles.binary_search_by_key(&val, |&idx| seq[idx]) {
            Ok(pos) | Err(pos) => {
                if pos > 0 {
                    prev[i] = Some(piles[pos - 1]);
                }
                if pos == piles.len() {
                    piles.push(i);
                } else {
                    piles[pos] = i;
                }
            }
        }
    }

    // reconstruct
    let mut lis: Vec<usize> = Vec::with_capacity(piles.len());
    if let Some(&start) = piles.last() {
        let mut cur = start;
        loop {
            lis.push(cur);
            match prev[cur] {
                Some(p) => cur = p,
                None => break,
            }
        }
        lis.reverse();
    }
    lis
}
