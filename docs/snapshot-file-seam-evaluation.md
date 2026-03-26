# Snapshot File Seam Evaluation

## Purpose

This document closes `M12-T04` by evaluating whether `snapshot_file` is now clean enough to
extract as another shared internal runtime module after:

- shared `retire_queue`
- shared `wal`
- shared `wal_file`

The question is deliberately narrower than "can snapshot persistence be shared in theory?" The
question is whether the current three-engine code on `main` justifies a real extraction now.

## Decision

Do not extract a shared `snapshot_file` crate yet.

The seam is real only inside the smaller `quota-core` / `reservation-core` pair. It is not yet a
clean three-engine runtime boundary.

The correct outcome for this slice is:

- record that `snapshot_file` is not ready for extraction
- keep each engine's `snapshot_file` local
- move on to `M13`, the internal engine authoring boundary

## What Is Shared

All three engines share the same high-level persistence discipline:

- one snapshot file per engine
- temp-file write, sync, rename, and parent-directory sync
- snapshot bytes loaded before WAL replay
- fail-closed behavior on decode or integrity errors

That means there is still real family resemblance at the discipline level.

## Where The Seam Breaks

### `allocdb-core` uses a simpler file format

`allocdb-core` still stores only encoded snapshot bytes:

- no footer
- no checksum
- no explicit max-bytes bound
- decode-time corruption detection only

That is materially different from the newer engines.

### `quota-core` and `reservation-core` share a stronger format

`quota-core` and `reservation-core` both use the same stronger file-level discipline:

- footer magic
- persisted payload length
- CRC32C checksum
- explicit `max_snapshot_bytes`
- oversize rejection before decode

Those two modules are close enough to share helpers later, but that is not the same thing as a
repository-wide extraction candidate.

### The remaining commonality is below the current file wrapper

The shared part is mostly:

- temp-file naming
- write, sync, rename, and parent-directory sync
- footer read/write mechanics for the newer engines

But the live module boundary still mixes those mechanics with engine-specific constructor and error
surface choices:

- `allocdb-core` has no size-bound constructor argument
- `quota-core` and `reservation-core` expose integrity-specific error variants
- the three wrappers are still tied to engine-local snapshot schemas and recovery expectations

That makes a forced crate extraction likely to create awkward generic plumbing rather than reduce
maintenance cost.

## Why Extraction Is Premature

Extracting now would create a misleading shared layer:

- it would either erase the real allocdb-vs-quota/reservation format difference
- or it would introduce configuration branches that mostly exist to paper over that difference

That is the wrong direction for this roadmap. `M12` is about extracting only what is already
mechanically shared, not about normalizing divergent modules by force.

The current evidence supports:

- shared `retire_queue`
- shared `wal`
- shared `wal_file`

It does not yet support:

- shared `snapshot_file`

## What Would Change The Answer Later

Revisit this seam only if one of these becomes true:

- `allocdb-core` adopts the same footer/checksum/max-bytes discipline as the newer engines
- repeated snapshot-file fixes land independently in multiple engines
- a later authoring pass shows the snapshot-file helper boundary can stay below engine-local error
  and schema surfaces

Until then, local duplication is still cheaper than a fake shared abstraction.

## Recommended Next Step

Treat `M12` as complete after this readout.

The next step is `M13`, not more extraction pressure:

1. define the internal engine authoring boundary
2. write the runtime-vs-engine contract
3. reassess whether a fourth-engine or reduced-copy proof is still required
