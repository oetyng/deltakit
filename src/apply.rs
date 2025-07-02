//! .

use crate::{encoding, error::{ApplyError, ApplyResult, Result}, types::{ApplyOp, ChunkOp}};

use std::{
    collections::BTreeMap,
};
use tokio::io::{AsyncSeek, AsyncWrite, AsyncWriteExt};
use tokio_stream::{Stream, StreamExt};

/// Apply a sequence of `ChunkOp`s to produce the new file.
///
/// * `fetch` must resolve chunk hashes from the previous version (or network).
pub async fn apply<W, Fetch, Fut>(mut sink: W, ops: &[ChunkOp], mut fetch: Fetch) -> Result<()>
where
    W: AsyncWrite + Unpin,
    Fetch: FnMut(&[u8; 32]) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<u8>>>,
{
    for op in ops {
        match op {
            ChunkOp::Copy { hash, .. } => {
                let bytes = fetch(hash).await?;
                sink.write_all(&bytes).await?;
            }
            ChunkOp::Patch {
                old_hash, patch, ..
            } => {
                let base = fetch(old_hash).await?;
                let new_chunk = encoding::apply(&base, patch)?;
                sink.write_all(&new_chunk).await?;
            }
            ChunkOp::Insert { data, .. } => sink.write_all(data).await?,
        }
    }
    sink.flush().await?;
    Ok(())
}

// For in-memory diffs:
pub async fn apply_iter<I, W, E>(sink: W, ops: I) -> ApplyResult<(), E>
where
    I: IntoIterator<Item = std::result::Result<ApplyOp, E>>,
    W: AsyncWrite + AsyncSeek + Unpin,
{
    let stream = futures::stream::iter(ops.into_iter());
    apply_stream(sink, stream).await
}

/// Consume a stream of `ApplyOp`s and apply them.
///
/// Does not require order of streamed chunks, however
/// memory footprint descreases the more ordered it is.
/// Incoming chunks will be buffered to a limit, to preserve
/// HDD/SSD performance etc.
///
/// The caller needs to decide if they want/need to sync after apply.
///
/// Resume on error:
/// 1. Catch ApplyError, seek sink to error.progress.
/// 2. Continue feeding the same ops stream (skipping those with offset < progress) into apply_stream again.
pub async fn apply_stream<W, Ops, E>(mut sink: W, mut ops: Ops) -> ApplyResult<(), E>
where
    W: AsyncWrite + AsyncSeek + Unpin,
    Ops: Stream<Item = std::result::Result<ApplyOp, E>> + Unpin,
{
    // preallocate(sink.get_ref(), target_len)?;

    let mut buffer = BTreeMap::<u64, Vec<u8>>::new();
    let mut next_offset = 0u64;

    while let Some(op_res) = ops.next().await {
        let op = match op_res {
            Ok(op) => op,
            Err(e) => {
                return Err(ApplyError::OpStream {
                    source: e,
                    progress: next_offset,
                });
            }
        };

        // 1) Turn ApplyOp into (offset, data)
        let (offset, data) = match op {
            ApplyOp::Data { bytes, offset } => (offset, bytes),
            ApplyOp::Patch {
                base,
                patch,
                offset,
            } => {
                let data = encoding::apply(&base, &patch).map_err(|e| ApplyError::Encoding {
                    source: e,
                    progress: next_offset,
                })?;
                (offset, data)
            }
        };

        // 2) Buffer it
        buffer.insert(offset, data);

        // 3) Drain in-order as far as possible
        while let Some(chunk) = buffer.remove(&next_offset) {
            // no seek needed if strictly sequential
            sink.write_all(&chunk).await.map_err(|e| ApplyError::Io {
                source: e,
                progress: next_offset,
            })?;
            next_offset += chunk.len() as u64;
        }
    }

    sink.flush().await.map_err(|e| ApplyError::Io {
        source: e,
        progress: next_offset,
    })?;

    Ok(())
}

#[cfg(unix)]
use std::os::unix::fs::FileExt;   // posix_fallocate on Linux / Android
use std::{io, fs::File};

/// Ensure `file` has at least `size` bytes allocated on disk.
/// Sparse allocation is fine on filesystems that support it.
fn preallocate(file: &File, size: u64) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // fallocate(KEEP_SIZE) – error ignored if not supported (e.g., tmpfs)
        let ret = unsafe {
            libc::fallocate64(file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 0, size as _)
        };
        if ret == -1 && io::Error::last_os_error().raw_os_error() != Some(libc::EOPNOTSUPP) {
            return Err(io::Error::last_os_error());
        }
    }
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::fs::FileExt as _; // SetEndOfFile + SetAllocation
        file.set_len(size)?;
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        file.set_len(size)?;               // HFS+/APFS allocate lazily
    }
    #[cfg(any(target_os = "freebsd", target_os = "netbsd"))]
    {
        let err = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, size as _) };
        if err != 0 && err != libc::EOPNOTSUPP {          // ZFS returns ENOTSUP
            return Err(io::Error::from_raw_os_error(err));
        }
    }
    #[cfg(target_os = "openbsd")]
    {
        file.set_len(size)?;      // creates sparse file; blocks allocated on write
    }
    Ok(())
}
