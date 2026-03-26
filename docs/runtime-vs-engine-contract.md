# Runtime vs Engine Contract

## Purpose

This document is the focused internal contract for `M13-T01`.

Use it when deciding whether new code belongs in the shared runtime substrate or inside one
engine.

The rule is simple:

- if the code only preserves bounded durable execution discipline, it may belong in shared runtime
- if the code defines domain meaning, it belongs in the engine

## Shared Runtime Contract

The shared runtime exists to preserve trusted substrate behavior across engines.

It owns:

- bounded retirement bookkeeping
- WAL frame encoding, validation, checksum, and torn-tail detection
- append-only WAL file mechanics
- rewrite and truncation file mechanics

It currently maps to:

- `allocdb-retire-queue`
- `allocdb-wal-frame`
- `allocdb-wal-file`

### Shared runtime may know about

- bytes
- lengths
- checksums
- frame versions
- file descriptors and paths
- bounded queue behavior
- truncation and rewrite discipline

### Shared runtime must not know about

- commands
- result codes
- resources, buckets, pools, holds, reservations, or leases
- snapshot schemas
- engine invariants
- replay semantics above raw framing

## Engine Contract

Each engine owns the database-specific meaning layered on top of the substrate.

It owns:

- command surfaces
- domain config
- state-machine invariants
- snapshot schemas
- recovery semantics
- read models and result surfaces

Today that means each engine keeps local ownership of:

- command enums and codecs above raw frame bytes
- snapshot encode/decode
- snapshot file wrappers while formats still differ
- top-level recovery entry points
- logical-slot behavior such as refill, expiry, revoke, reclaim, and fencing

## Placement Rules

When adding new code, apply these rules in order.

### Rule 1

Start engine-local by default.

Do not begin from "how can this be shared?" Begin from "what engine behavior am I expressing?"

### Rule 2

Move code into shared runtime only if the seam is already proven.

That means at least one of:

- the code is mechanically identical across engines
- the same fix is being repeated in multiple engines
- a new engine slice would clearly avoid copy-paste by using the shared layer

### Rule 3

Keep extraction below the semantic line.

Good shared-runtime candidates:

- durable bytes-on-disk framing
- bounded retirement structures
- file rewrite and truncation helpers

Bad shared-runtime candidates:

- generic state-machine traits
- generic reserve/confirm/release APIs
- generic snapshot schemas
- generic engine config layers

### Rule 4

If an extraction needs engine-specific switches, it is not ready.

Examples of bad signals:

- feature flags that mirror engine names
- runtime branches on allocator/quota/reservation semantics
- generic types that only one engine can meaningfully use

## Current Map

### Shared now

- `allocdb-retire-queue`
- `allocdb-wal-frame`
- `allocdb-wal-file`

### Deferred

- `snapshot_file`
  - only clean inside the `quota-core` / `reservation-core` pair
- bounded collections beyond `retire_queue`
  - still need stable multi-engine shape
- recovery helpers above frame/file mechanics
  - still coupled to engine-local replay contracts

### Explicitly engine-local

- `allocdb-core` lease and fencing semantics
- `quota-core` debit and refill semantics
- `reservation-core` hold and expiry semantics

## Authoring Checklist

Before extracting any new module, answer these questions:

1. Is this code below the semantic line?
2. Is the shape already proven across multiple engines?
3. Would extraction reduce copy-paste immediately?
4. Can the shared module avoid engine-specific branches?

If any answer is "no", keep the code local.

## Practical Use

When writing a new engine or engine slice:

1. use the shared runtime only for already-extracted substrate
2. implement new semantics locally
3. copy new runtime-adjacent code locally if the seam is still uncertain
4. extract later only under demonstrated pressure

That keeps the repository honest and keeps future library claims evidence-based.
