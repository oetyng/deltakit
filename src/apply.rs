//! .

use crate::{
    encoding,
    error::{ApplyError, ApplyResult},
    types::{ApplyOp, ChunkProgress},
};

use futures::stream::FuturesUnordered;
use std::io::SeekFrom;
use tokio::{
    io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc::UnboundedSender,
};
use tokio_stream::{Stream, StreamExt};

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
pub async fn apply_stream<W, Ops, E>(
    mut sink: W,
    mut ops: Ops,
    progress_tx: UnboundedSender<ChunkProgress>,
) -> ApplyResult<(), E>
where
    W: AsyncWrite + AsyncSeek + Unpin,
    Ops: Stream<Item = std::result::Result<ApplyOp, E>> + Unpin,
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
        let _ = progress_tx.send(ChunkProgress {
            index,
            offset,
            len: data.len(),
        });
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
