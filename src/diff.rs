//! .

use crate::{
    encoding,
    error::{DeltaError, DeltaResult},
    types::{ChunkOp, Config},
};

use async_stream::try_stream;
use fastcdc::v2020::{AsyncStreamCDC, ChunkData};
use futures::{Future, pin_mut, stream::FuturesUnordered};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    io::SeekFrom,
    pin::Pin,
    sync::Arc,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt},
    sync::mpsc,
};
use tokio_stream::{Stream, StreamExt};

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
            if let Some(&old_hash) = old_hashes.first() {
                let base = fetch_base(old_hash).await.map_err(DeltaError::Fetch)?;
                if base == new_bytes { return } // identical
                let patch = encoding::diff(&base, &new_bytes)?;
                if patch.len() as f32 <= cfg.patch_threshold * new_bytes.len() as f32 {
                    let new_hash = sha256(&new_bytes);
                    yield ChunkOp::Patch { index: 0, offset: 0, length: new_bytes.len(), new_hash, old_hash, patch };
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

        /* fallback_window  */
        let num_chunks = old_hashes.len().max(index.len());
        let mut w = (num_chunks as f32).sqrt().ceil() as isize;   // √N  ≈ 32 for 1 k chunks
        w = w.clamp(4, 64);                // hard floor / ceiling

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
        let fetch_arc = Arc::new(fetch_base);

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
                let old_hash        = old_hashes[idx];
                let new_bytes = cd.data;
                let offset    = cd.offset;
                let threshold = cfg.patch_threshold;
                let txc       = tx.clone();
                let fetch     = fetch_arc.clone();

                /* push future into local pool */
                workers.push(async move {
                    let base = fetch(old_hash).await.map_err(DeltaError::Fetch)?;
                    let patch = encoding::diff(&base, &new_bytes)?;
                    let op = if patch.len() as f32 <= threshold * new_bytes.len() as f32 {
                        let hash = sha256(&new_bytes);
                        ChunkOp::Patch { index: idx, offset, length: cd.length, new_hash: hash, old_hash, patch }
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

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
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

#[cfg(test)]
mod lis_tests {
    use super::lis_indices;

    /// Convenience: convert index list to the actual subsequence
    fn subseq(src: &[usize], idx: &[usize]) -> Vec<usize> {
        idx.iter().map(|&i| src[i]).collect()
    }

    /// Assert the returned indices describe a *strictly increasing* subsequence.
    fn assert_lis(src: &[usize], idx: &[usize]) {
        let sub = subseq(src, idx);
        assert!(
            sub.windows(2).all(|w| w[0] < w[1]),
            "subsequence is not strictly increasing: {:?}",
            sub
        );
    }

    #[test]
    fn empty() {
        let seq: Vec<usize> = vec![];
        let lis = lis_indices(&seq);
        assert!(lis.is_empty());
    }

    #[test]
    fn already_increasing() {
        let seq = (0..10).collect::<Vec<_>>();
        let lis = lis_indices(&seq);
        assert_eq!(lis, (0..10).collect::<Vec<_>>());
        assert_lis(&seq, &lis);
    }

    #[test]
    fn strictly_decreasing() {
        let seq = (0..10).rev().collect::<Vec<_>>();
        let lis = lis_indices(&seq);
        assert_eq!(lis.len(), 1, "only one element can be increasing");
        assert_lis(&seq, &lis);
    }

    #[test]
    fn mixed_case() {
        //              0  1  2  3  4  5  6  7
        let seq = [5, 2, 8, 6, 3, 6, 9, 7];
        // One valid LIS is indices [1, 3, 5, 7] → values [2, 6, 6, 7]
        let lis = lis_indices(&seq);
        assert_lis(&seq, &lis);
        // result length must be maximal (4 here)
        assert_eq!(lis.len(), 4);
    }

    #[test]
    fn duplicates() {
        let seq = [3, 1, 2, 2, 2, 3, 4];
        let lis = lis_indices(&seq);
        assert_lis(&seq, &lis);
        // Longest increasing subsequence is [1,2,3,4] (len = 4)
        assert_eq!(lis.len(), 4);
        let values = subseq(&seq, &lis);
        assert_eq!(values, vec![1, 2, 3, 4]);
    }

    #[test]
    fn random_long() {
        // deterministic pseudo-random sequence
        let mut seq = Vec::with_capacity(1_000);
        let mut x = 1u32;
        for _ in 0..1_000 {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            seq.push((x % 10_000) as usize);
        }
        let lis = lis_indices(&seq);
        assert_lis(&seq, &lis);
        // sanity: LIS length should be ≥ log₂(N)
        assert!(lis.len() >= 10, "unexpectedly short LIS: {}", lis.len());
    }
}
