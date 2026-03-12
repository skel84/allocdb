# AllocDB Storage and Recovery

## Scope

This document defines the v1 WAL, snapshot, and crash-recovery model.

## WAL

The current implementation uses one append-only WAL file.

Segmented WAL retention remains a plausible future refinement, but v1 now achieves safe checkpoint
coordination by rewriting the retained WAL prefix through a temp-file and rename path.

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
- classify WAL recovery stop reasons as `clean_eof`, `torn_tail`, or `corruption`
- auto-truncate only incomplete EOF torn tails
- fail closed on middle-of-log corruption
- fail closed on semantically invalid replay ordering such as non-monotonic `lsn` or
  `request_slot`
- no `serde` or format whose layout is implicit

Current implementation anchor:

- `crates/allocdb-core/src/command_codec.rs`
- `crates/allocdb-core/src/wal.rs`
- `crates/allocdb-core/src/wal_file.rs`

The current code covers frame encoding, decoding, checksum validation, and in-memory recovery
scanning up to the last valid frame boundary, explicit command-payload encoding for client and
internal commands, file-backed append, sync, recovery scan, truncate-to-valid-prefix behavior for
one WAL file, and safe WAL rewrite during checkpoint retention. Recovery distinguishes a
crash-torn tail from durable-log corruption: only torn tails are truncated automatically. Replay
also rejects checksum-valid WAL input whose frame metadata moves `lsn` backwards or rewinds
`request_slot`.

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
discipline. Restore validates configured table capacities, duplicate IDs, wheel bounds, and
trusted-core resource/reservation invariants before admitting decoded contents into live state.

Recovery implementation anchors:

- `crates/allocdb-core/src/recovery.rs`

## Recovery

Recovery is:

1. load the latest valid snapshot
2. scan later WAL frames in LSN order
3. verify checksums
4. replay frames into a fresh state machine
5. rebuild transient scheduler state from persisted state
6. resume accepting traffic

If a WAL tail is torn after crash, recovery truncates at the last valid frame boundary.

If recovery detects a checksum or framing error before EOF, the node fails closed and surfaces the
corruption instead of rewriting the WAL.

If recovery detects semantically invalid decoded snapshot contents or WAL replay ordering, the node
also fails closed and returns a structured error instead of panicking.

## Checkpoint Coordination

Current behavior:

- recovery loads the latest snapshot first
- replay skips WAL frames at or below the snapshot's `last_applied_lsn`
- snapshot writing is atomic at the file level through temp-file, fsync, and rename discipline
- the engine tracks the active snapshot anchor explicitly in memory
- each successful checkpoint rewrites the WAL through a temp-file and rename path
- rewritten WAL retains frames with `lsn > previous_snapshot_lsn`, preserving one-checkpoint
  overlap
- rewritten WAL appends a `snapshot_marker` frame at the active snapshot anchor so the retained WAL
  makes the current checkpoint boundary explicit

Crash-safety rule:

- snapshot replacement happens before WAL rewrite, so a crash after the new snapshot becomes
  durable but before WAL rewrite completes still recovers from the new snapshot plus the old WAL
- WAL rewrite uses temp-file, fsync, rename, and directory-sync discipline, so a crash during
  rewrite leaves either the old retained WAL or the fully rewritten retained WAL
- any future segmented implementation must preserve the same one-checkpoint overlap rule; suffix
  truncation alone is not enough

## Design Notes

- the WAL is the source of truth
- recovery must share the same apply logic used by live execution
- replay must not consult wall-clock time or external services
