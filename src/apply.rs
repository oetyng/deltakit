//! .

use crate::{
    encoding,
    error::{ApplyError, ApplyResult, Result},
    types::{ApplyOp, ChunkOp},
};

use futures::{Sink, SinkExt, stream::FuturesUnordered};
use std::io::SeekFrom;
use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
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

pub struct ChunkProgress {
    pub index: u64,
    pub offset: u64,
    pub len: usize,
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

pub async fn apply_stream<W, Ops, Out, E>(
    mut sink: W,
    mut ops: Ops,
    mut progress_tx: Out, // mpsc::Sender<ChunkProgress>
) -> ApplyResult<(), E>
where
    W: AsyncWrite + AsyncSeek + Unpin,
    Ops: Stream<Item = std::result::Result<ApplyOp, E>> + Unpin,
    Out: Sink<ChunkProgress> + Unpin,
{
    // preallocate(&sink, size)
    //     .map_err(|e| ApplyError::Io { source: e, progress: 0 })?;

    // Pool of in-flight patch/data tasks
    let mut in_flight = FuturesUnordered::new();
    const MAX_CONCURRENCY: usize = 8;

    loop {
        // 1) Fill the pool up to MAX_CONCURRENCY
        while in_flight.len() < MAX_CONCURRENCY {
            match ops.next().await {
                Some(Ok(op)) => {
                    // Spawn a task that turns ApplyOp → (offset, data)
                    in_flight.push(spawn_apply(op));
                }
                Some(Err(e)) => return Err(ApplyError::OpStream(e)),
                None => break, // no more ops
            }
        }

        // 2) Await the next completed work item
        let (index, offset, data) = match in_flight.next().await {
            Some(Ok(n)) => n,
            Some(Err(e)) => return Err(e.into()),
            None => break, // If pool is empty and no more ops, we’re done
        };

        // 3) Random-access write immediately
        sink.seek(SeekFrom::Start(offset)).await?;
        sink.write_all(&data).await?;

        // 4) Report progress
        let _ = progress_tx
            .send(ChunkProgress {
                index,
                offset,
                len: data.len(),
            })
            .await;
    }

    sink.flush().await?;

    Ok(())
}

fn spawn_apply<E>(op: ApplyOp) -> impl Future<Output = ApplyResult<(u64, u64, Vec<u8>), E>> {
    // Spawn a task that turns ApplyOp → (offset, data)
    async move {
        let iod = match op {
            ApplyOp::Data {
                index,
                offset,
                bytes,
            } => (index, offset, bytes),
            ApplyOp::Patch {
                index,
                offset,
                base,
                patch,
            } => {
                let chunk = encoding::apply(&base, &patch)?;
                (index, offset, chunk)
            }
        };
        Ok::<_, ApplyError<E>>(iod)
    }
}

#[cfg(unix)]
use std::os::unix::fs::FileExt; // posix_fallocate on Linux / Android
use std::{fs::File, io};

/// Ensure `file` has at least `size` bytes allocated on disk.
/// Sparse allocation is fine on filesystems that support it.
pub fn preallocate(file: &File, size: u64) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // fallocate(KEEP_SIZE) – error ignored if not supported (e.g., tmpfs)
        let ret =
            unsafe { libc::fallocate64(file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 0, size as _) };
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
        file.set_len(size)?; // HFS+/APFS allocate lazily
    }
    #[cfg(any(target_os = "freebsd", target_os = "netbsd"))]
    {
        let err = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, size as _) };
        if err != 0 && err != libc::EOPNOTSUPP {
            // ZFS returns ENOTSUP
            return Err(io::Error::from_raw_os_error(err));
        }
    }
    #[cfg(target_os = "openbsd")]
    {
        file.set_len(size)?; // creates sparse file; blocks allocated on write
    }
    Ok(())
}
