# Lease Kernel Follow-On

## Status

Draft. This document defines the local planning baseline for the next AllocDB kernel work needed
before downstream consumers depend on new semantics.

It is a planning document, not part of the current API contract.

## Purpose

AllocDB already provides the right core shape for deterministic scarce-capacity ownership:

- one active owner per resource
- idempotent writes
- deterministic replay
- bounded retention
- late reuse is acceptable, early reuse is not

The current surface, however, is intentionally narrow and optimized for one-resource reservations.
Broader scarce-resource use cases need a few additional correctness primitives before
they can rely on AllocDB for multi-resource ownership and stale-holder safety.

This document defines that minimum follow-on scope.

## Guiding Rule

Make AllocDB a better scarce-resource lease kernel, not a bigger policy engine.

Everything added here must preserve the trusted-core discipline:

- deterministic replay
- bounded hot-path work
- explicit fail-closed behavior
- no wall-clock dependency in the state machine
- no policy hidden inside helper abstractions

## What Makes A Primitive Generic

A follow-on primitive belongs in AllocDB only if all three are true:

1. it can be defined without mentioning Kubernetes, Kueue, DRA, topology, model serving, or other
   domain-specific platform terms
2. it strengthens AllocDB's existing ownership, replay, idempotency, fencing, or safe-reuse
   invariants
3. it still makes sense for scarce resources such as seats, rooms, inventory, or compute slots

## Motivating Cases

This follow-on is motivated by recurring scarce-resource problems such as:

- adjacent seats that must be acquired together
- a room plus a parking spot
- an inventory kit made from multiple units
- a compute job that needs multiple slots together

Those cases all need the same underlying questions answered correctly:

- can this request own a concrete bundle right now
- can a stale actor be fenced after failover or revoke
- can ownership be revoked without permitting early reuse
- will replay and failover preserve the same ownership outcome

## What Must Stay Outside AllocDB

This follow-on does not move policy into the trusted core.

The following stay outside AllocDB:

- queueing and fair sharing
- tenant quotas
- topology search and placement heuristics
- DRA object management
- Kubernetes-specific status orchestration
- inference routing and rollout control
- chargeback and showback logic
- rich operator UX

If a feature can be implemented safely as external reconciliation around an explicit AllocDB
command, it should remain outside the kernel.

## Minimal Follow-On Scope

### 1. Atomic Bundle Ownership

AllocDB needs an all-or-nothing ownership primitive for one logical request that spans multiple
resources.

Required properties:

- the bundle becomes visible atomically or not at all
- failure must not leave partial ownership behind
- each member resource still obeys the one-active-owner rule
- replay and failover must preserve the same winning bundle outcome

This is the most important new primitive.

### 2. Fencing Or Generation Tokens

AllocDB needs a monotonic token that lets downstream consumers reject stale actors after revoke,
release, or failover-related ownership changes.

Required properties:

- the token changes whenever authority changes in a way that can invalidate prior holders
- stale holders can be rejected deterministically
- the token participates in replay and failover without ambiguity

### 3. Explicit Revoke Semantics

AllocDB needs an explicit way to withdraw ownership from a live lease.

Required properties:

- revocation is a first-class logged state transition
- revocation cannot permit early reuse
- duplicate revoke requests remain idempotent
- the final holder-visible outcome is explicit

### 4. Minimal Lease Lifecycle

The follow-on likely needs a lease lifecycle richer than today's `reserved` or `confirmed`
distinction, but the state set must remain minimal.

Rules:

- states must exist only when they change safety or replay rules
- operationally rich states such as scheduling, queue, or UI states stay outside the kernel
- names and transitions must make stale-holder and revoke behavior explicit

This planning work should resist adding speculative lifecycle states.

### 5. External Liveness Boundary

Long-running workloads need liveness and reclaim behavior, but the kernel must preserve its
logical-time discipline.

Required boundary:

- external controllers may observe heartbeats, pod state, or node loss
- the trusted core still applies explicit commands, not wall-clock-derived decisions
- timeout and reclaim semantics must still honor the late-not-early reuse rule

The starting bias should be to keep heartbeat observation outside the kernel and encode only the
resulting ownership transition as an explicit AllocDB command.

## Frozen Design Decisions For M9

These decisions are frozen for the current planning slice and should be treated as the default M9
direction unless a later milestone explicitly reopens them:

1. M9 decision: introduce a first-class `lease_id`; the older `reservation_id` naming survives
   only as compatibility surface while implementation transitions.
2. M9 decision: represent a bundle as one logical lease plus bounded member-resource records, not
   as loosely grouped peer reservation records.
3. M9 decision: the authoritative command surface becomes `reserve_bundle`, `activate`, `release`,
   `revoke`, and `reclaim`; the current single-resource `confirm` path remains only as a
   compatibility alias while the implementation catches up.
4. M9 decision: extend the result-code set with the lease-centric and bundle-specific outcomes
   needed for this model, including stale-fencing, lease-history, and bundle-capacity failures.
5. M9 decision: retained-history lookup becomes lease-centric; `get_lease` returns
   `lease_retired` after `retire_after_slot`, and member history retires with the parent lease.
6. M9 decision: the single-resource path is the semantic special case of bundle size `1`;
   specialized command forms may remain temporarily for compatibility or implementation reasons,
   but they must not diverge from the lease model.

## Constraints On The Solution

Any approved design must preserve these properties:

1. no resource has more than one active owner at the same logical point
2. bundle visibility is atomic
3. replay of the same committed log yields the same bundle, fencing, and revoke outcomes
4. stale holders are rejected without requiring wall-clock reasoning in the state machine
5. a resource may become reusable late, but never early
6. replication preserves the same safety rules and does not introduce a second apply path
7. all new retention, queue, and table growth remain explicitly bounded

## Recommended Scope Freeze

The follow-on should start with only these kernel primitives:

- bundle ownership
- fencing token
- revoke
- minimal lifecycle changes required by those three

Everything else should be deferred until one downstream integration slice proves that the narrower
kernel is insufficient.

In particular, defer by default:

- rich heartbeat state inside the kernel
- scheduling or placement policy
- topology constraints in the trusted core
- vendor-specific device metadata
- multi-cluster semantics
- shared-device accounting that weakens the current one-active-owner rule

## Implementation Strategy

The first implementation sequence should be:

1. freeze scope and non-goals
2. define the exact data model and command semantics
3. define replay, durability, and retention rules
4. define replication and failover invariants for the new primitives
5. implement the trusted-core apply path
6. extend API, WAL, snapshot, and recovery
7. extend replicated handling
8. add simulation, crash, retry, and failover coverage

Downstream integrations should not depend on real AllocDB integration until steps `1` through `4`
are explicit.

## Planning Output

This document is paired with:

- [lease-kernel-design.md](./lease-kernel-design.md) for the current local design choices under
  that follow-on scope
- [roadmap.md](./roadmap.md) for milestone-level sequencing
- [work-breakdown.md](./work-breakdown.md) for task-level planning
- [status.md](./status.md) for the current planning snapshot
