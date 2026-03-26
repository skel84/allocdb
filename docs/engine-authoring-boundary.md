# Engine Authoring Boundary

## Purpose

This document closes the first `M13` question: after the `M12` micro-extractions, what is the
stable boundary between the shared runtime substrate and engine-local semantics?

This is an internal authoring rule, not a public framework story.

## Decision

The shared runtime owns only mechanically shared durability and bounded-state substrate.

Everything that defines domain meaning stays engine-local.

That means:

- use the extracted runtime crates where the seam is already proven
- keep new engine semantics local by default
- treat any new generic abstraction above the current substrate as suspect until repeated pressure
  proves otherwise

## What The Shared Runtime Owns Today

The shared runtime currently owns only the modules that are already shared on `main`.

### Shared and stable enough now

- `allocdb-retire-queue`
  - bounded retirement queue discipline
  - no domain meaning beyond ordered retirement bookkeeping
- `allocdb-wal-frame`
  - WAL frame versioning
  - frame header/footer validation
  - checksum verification
  - torn-tail and corruption detection at the frame level
- `allocdb-wal-file`
  - append-only durable file handle
  - replace/rewrite discipline
  - truncation and reopen behavior

### What these modules are allowed to know

Only substrate concerns:

- bytes
- lengths
- checksums
- file paths and file handles
- bounded queue mechanics
- ordering and truncation discipline

These modules must not know:

- command schemas
- result codes
- resource, bucket, pool, or hold semantics
- snapshot schemas
- engine-specific invariants

## What Stays Engine-Local

Each engine still owns the parts that define the database itself.

### Domain contract surface

Keep local:

- command enums
- command codecs above raw frame transport
- result codes and read models
- config surfaces tied to domain semantics

### Persistence schema

Keep local:

- snapshot encoding and decoding
- snapshot file wrappers while file formats still differ
- engine-specific recovery error surfaces

### State machine semantics

Keep local:

- apply rules
- invariants
- derived indexes
- logical-slot effects such as refill, expiry, revoke, reclaim, or fencing
- any internal command semantics above raw WAL framing

### Recovery entry points

Keep local:

- top-level recovery APIs
- replay orchestration that depends on engine-specific command decoding
- operational logging tied to one engine's semantics

## Authoring Rules For Future Work

### Rule 1: Start local unless the seam is already proven

When adding a new engine or engine slice:

- use the shared runtime crates only for seams already extracted
- keep new runtime-adjacent code local until at least two engines want the same thing in the same
  shape

### Rule 2: Do not generalize state-machine APIs

Do not introduce:

- generic state-machine traits
- generic apply pipelines
- generic snapshot schemas
- generic recovery entry points

Those layers still carry domain meaning and would create abstraction debt faster than maintenance
relief.

### Rule 3: Extract only below the semantic line

A module is a good runtime candidate only if it can stay below the line where domain meaning starts.

Good examples:

- bytes-on-disk framing
- bounded retirement bookkeeping
- file rewrite/truncate mechanics

Bad examples:

- "generic reserve/confirm/release" APIs
- "generic bucket/pool/resource" models
- "generic engine config" layers

### Rule 4: Prefer duplication over dishonest abstraction

If a candidate seam requires:

- engine-specific branches
- feature flags that mirror engine names
- generic types that only one engine can actually use

then it is not ready.

### Rule 5: New extractions need multi-engine pressure

Do not extract a new runtime module unless at least one of these is true:

- the code is already mechanically identical across engines
- the same fix or improvement is landing independently in multiple engines
- a new engine authoring pass clearly pays less copy-paste by using the shared layer

## Current Boundary Map

### Shared runtime

- `allocdb-retire-queue`
- `allocdb-wal-frame`
- `allocdb-wal-file`

### Deferred seams

- `snapshot_file`
  - deferred because the seam is still only clean inside the `quota-core` / `reservation-core`
    pair
- bounded collections beyond `retire_queue`
  - still need proof that the common surface is stable enough
- recovery helpers above file/frame mechanics
  - still too tied to engine-local replay contracts

### Explicit non-goals

- no public database-building library claim yet
- no renaming the repository around framework identity
- no generic engine kit above the current substrate

## Practical Consequence

A future engine author should think in this order:

1. write engine-local semantics first
2. consume the existing shared runtime only for proven substrate
3. copy new runtime-adjacent code locally if the seam is not already explicit
4. extract later only if repeated pressure proves the boundary

That keeps the repository honest:

- shared where the code is actually shared
- local where the semantics are still the database

## Next Step

With this boundary in place, the next `M13` step is narrower:

1. write the focused runtime-vs-engine contract note
2. decide whether that contract already makes a reduced-copy proof likely enough
3. only then choose whether `M14` still needs a full fourth-engine or can use a smaller engine
   slice proof
