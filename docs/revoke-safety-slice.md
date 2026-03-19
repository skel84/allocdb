# Revoke Safety Slice

## Status

Draft local planning baseline for `M9-T08` / issue `#85`.

This document narrows the next implementation slice before code work starts. It is not a new
authoritative API surface; it is the execution plan for bringing the accepted revoke/reclaim
semantics into the current trusted-core implementation.

## Purpose

`M9-T07` established fencing and stale-holder rejection. The next kernel step is to withdraw
holder authority explicitly without permitting early reuse.

The implementation question for `M9-T08` is narrower than the full lease-model transition:

- how revoke enters the current core cleanly
- how reclaim becomes the only point where reuse is allowed
- how to keep the reservation-era compatibility surface from drifting away from the accepted
  lease-centric semantics

## Slice Goal

Implement the minimum revoke/reclaim behavior needed to preserve the late-not-early reuse rule in
the current single-node execution path.

For this slice, "done" means:

- the core can log and apply `revoke` and `reclaim`
- stale holders lose authority as soon as revoke commits
- resources remain unavailable until reclaim commits
- exact retries stay deterministic

## In Scope

`M9-T08` should include:

1. one explicit revoke command in the trusted core
2. one explicit reclaim command in the trusted core
3. one live non-terminal state for revoked-but-not-yet-reusable ownership
4. one terminal revoked outcome that preserves history after reclaim
5. the minimum executor and WAL-path plumbing needed for live apply and replay of those commands
6. invariant, negative-path, retry, and crash-recovery tests for the new safety rule

## Out Of Scope

`M9-T08` should not expand into:

- replication or failover work
- snapshot schema broadening beyond what is strictly needed for this slice
- broader public API and transport cleanup beyond the minimum already required by the live path
- heartbeat ingestion or wall-clock reclaim logic inside the state machine
- policy reasons or operator metadata attached to revoke/reclaim
- holder transfer or shared-resource semantics

Those belong to later slices, primarily `M9-T09` through `M9-T11`.

## Compatibility Rule

The accepted model is lease-centric, but the current implementation is still reservation-centric in
spelling and data layout.

For `M9-T08`, that bridge is allowed under one rule:

- reservation-era names may remain temporarily, but revoke/reclaim behavior must match the
  authoritative lease semantics exactly

That means:

- the current `reservation_id` may continue to serve as the implementation anchor for `lease_id`
- the existing `confirmed` state may continue as the compatibility spelling for authoritative
  `active`
- the slice must not introduce reservation-era shortcuts that would be invalid in the final lease
  model

## Exact Command Semantics For This Slice

### Revoke

Implementation intent:

```text
revoke(lease_id)
```

Current compatibility spelling may still route this through the reservation-era implementation, but
the effect must be:

- precondition: live lease exists and is currently `active`
- success: lease moves to `revoking`
- success: `lease_epoch` increments immediately
- success: member resources stay unavailable and keep pointing at the same live owner
- success: resource state becomes `revoking`
- success: no retirement is scheduled yet

Failure behavior:

- `lease_not_found` if the lease never existed
- `lease_retired` if retained history says the live record is already gone
- `invalid_state` for `reserved`, `revoking`, `released`, `expired`, or `revoked`

Duplicate behavior:

- exact retry with the same `operation_id` must return the cached original result
- a later distinct revoke with a different `operation_id` must not invent a second success; once a
  lease is already `revoking` or terminal, the answer is `invalid_state`

### Reclaim

Implementation intent:

```text
reclaim(lease_id)
```

Effect:

- precondition: live lease exists and is currently `revoking`
- success: lease moves to terminal `revoked`
- success: member resources return to `available`
- success: per-resource current owner pointers clear
- success: retirement is scheduled through the normal bounded history path

Failure behavior:

- `lease_not_found`
- `lease_retired`
- `invalid_state` for `reserved`, `active`, `released`, `expired`, or already `revoked`

Duplicate behavior:

- exact retry with the same `operation_id` must return the cached original result
- a later distinct reclaim on an already terminal record must not produce a second success

## Required Safety Properties

`M9-T08` must preserve these invariants:

1. revoke removes holder authority before reuse is possible
2. reclaim is the only transition that makes a revoked resource reusable
3. active or revoking leases are never freed by timer
4. late external reclaim is acceptable; early reclaim is not
5. holder-authorized commands that arrive after revoke with the old epoch fail deterministically
6. replay of committed revoke/reclaim commands yields the same resource availability outcome

## Implementation Boundaries

The slice should be built in this order:

1. add core state and command variants for revoke/reclaim
2. apply revoke/reclaim through the same executor path already used by reserve/confirm/release
3. keep WAL replay on the same apply logic as live execution
4. add resource-state and lease-state invariants for `revoking` and `revoked`
5. add retry and stale-holder regression coverage

Important boundary:

- if broader snapshot, transport, or replicated-surface work becomes necessary, keep the minimum
  unblocker here and defer the broader cleanup to `M9-T09` and `M9-T10`

## Tests This Slice Should Add

Minimum test set:

- revoke on active lease moves the lease to `revoking` and bumps `lease_epoch`
- revoke does not free member resources
- holder `release` or `confirm` with the old epoch after revoke fails deterministically
- reclaim from `revoking` returns resources to `available` and records terminal `revoked` history
- reclaim before revoke is `invalid_state`
- exact duplicate revoke and reclaim requests return cached committed results
- reserved, active, and revoking resources cannot be reused early
- crash/restart replay preserves `revoking` vs `revoked` outcomes

## Exit Condition

`M9-T08` is ready to hand off when:

- the exact revoke/reclaim behavior above is implemented or explicitly mapped to narrower code
  tasks
- `docs/status.md` points at `#85` instead of stale `#84` language
- later work is cleanly reserved for `M9-T09` through `M9-T11`
