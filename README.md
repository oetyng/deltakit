# deltakit

**deltakit** is a Rust library for a hybrid of streamed chunking with binary diff. It combines FastCDC (Content-Defined Chunking) with a per-chunk binary diff layer, combining content-aware change tracking for large files, with the efficiency of binary diff for small files and individual cdc chunks. This library is designed for use cases such as deduplicated backup, synchronization, and delta compression.

## Features

- **Streaming-compatible:** Works with async streams and `AsyncRead`, chunking and diffing data incrementally.
- **FastCDC v2020:** Uses the "FastCDC" content defined chunking algorithm (more info here https://github.com/nlfiedler/fastcdc-rs).
- **Per-chunk binary diffing:** Produces compact binary deltas for each content-defined chunk (more info on the binary diffing here https://github.com/divvun/bidiff).
- **Low runtime allocations:** Designed for zero-allocation and zero-copy use when possible.

## How it works

1. **Chunking:** Input data over 4 MiB is chunked using FastCDC.
2. **Hashing:** Each chunk is hashed, forming a stable identity for content-addressed storage or comparison.
3. **Binary differencing:** When comparing two versions, binary diffs are computed per changed chunk using a suffix-array-based algorithm.
4. **Patch generation:** A patch is produced, embedding per-chunk diffs in a compact format.
5. **Patch application:** Patches are applied using a per demand reader over the old chunks, and a slice over the patch, producing the new content.

For content <= 4 MiB binary diff is used directly, as it excels in this range of file size. It is also for this reason that it is a good combination for use with chunks in that size range.

## Use cases

- Incremental backups
- Deduplicated file storage
- Filesystem-level sync
- Content-addressed storage systems
- Versioned archives

## Status

This library is under active development and not yet stable. API changes may occur.

## License

MIT or Apache-2.0
