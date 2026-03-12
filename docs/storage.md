# AllocDB Storage and Recovery

## Scope

This document defines the v1 WAL, snapshot, and crash-recovery model.

## WAL

Use segmented append-only WAL files.

Each frame includes:

```text
magic
version
lsn
request_slot
record_type
payload_len
checksum
payload
```

Required properties:

- fixed upper bound on payload size
- manual binary encoding with explicit endianness
- CRC32C on each frame
- stop recovery at the first torn or invalid frame
- no `serde` or format whose layout is implicit

Current implementation anchor:

- `crates/allocdb-core/src/command_codec.rs`
- `crates/allocdb-core/src/wal.rs`
- `crates/allocdb-core/src/wal_file.rs`

The current code covers frame encoding, decoding, checksum validation, and in-memory recovery
scanning up to the last valid frame boundary, explicit command-payload encoding for client and
internal commands, plus file-backed append, sync, recovery scan, and truncate-to-valid-prefix
behavior for one WAL file.

## Snapshots

Snapshots are point-in-time images of the applied state at a specific `snapshot_lsn`.

The snapshot contains:

- resource table
- reservation table
- operation dedupe table
- expiration index state
- last applied LSN

Write snapshots by:

1. serializing to a temporary file
2. fsyncing the file
3. renaming atomically
4. fsyncing the directory if required by the platform

Current implementation anchor:

- `crates/allocdb-core/src/snapshot.rs`
- `crates/allocdb-core/src/snapshot_file.rs`

The current code covers snapshot encode, decode, allocator-state capture, and allocator restore
from one decoded snapshot, plus file-backed snapshot write and load with temp-file and rename
discipline.

Recovery implementation anchors:

- `crates/allocdb-core/src/recovery.rs`

## Recovery

Recovery is:

1. load the latest valid snapshot
2. scan later WAL segments in LSN order
3. verify checksums
4. replay frames into a fresh state machine
5. rebuild transient scheduler state from persisted state
6. resume accepting traffic

If a WAL tail is torn after crash, recovery truncates at the last valid frame boundary.

## Design Notes

- the WAL is the source of truth
- recovery must share the same apply logic used by live execution
- replay must not consult wall-clock time or external services
