# Quota Engine Semantics

## Purpose

This document freezes the v1 semantic boundary for the `quota-core` experiment.

`quota-core` is not a framework launch. It is a second deterministic engine experiment that stays
inside the current `allocdb` workspace.

The experiment exists to answer one architecture question:

- can the AllocDB execution discipline support a second domain without weakening determinism,
  replay, boundedness, or fail-closed recovery

## Repository Boundary

The repository decision for v1 is fixed:

- keep the repository identity as `allocdb`
- add `crates/quota-core` as a sibling engine crate
- do not create a new repo
- do not extract a shared runtime crate first
- do not rename public docs around a generic framework story yet

This keeps the engine experiment honest. The second engine must survive under the same constraints
as the first one before any extraction or repackaging work begins.

## Trusted-Core Rules

`quota-core` inherits the same trusted-core rules as `allocdb-core`:

- same snapshot plus same WAL must produce the same state and results
- the state machine must not read wall-clock time
- hot-path structures must stay explicitly bounded
- replay must use the same apply logic as live execution
- recovery must fail closed on corruption or monotonicity violations
- steady-state execution should avoid hidden allocation growth

The quota experiment is not allowed to relax those rules just because the domain is different.

## V1 Scope

The first slice is intentionally narrow.

### Domain objects

- `BucketId`
- `BucketRecord`
- `OperationRecord`

### Commands

- `CreateBucket`
- `Debit`

`Credit` is excluded from the initial public semantic surface. If test or repair support is needed,
it should be added later as an explicitly scoped administrative command rather than assumed in v1.

### Required bucket state

- `limit`
- `balance`
- `last_refill_slot`
- `refill_rate_per_slot`

## V1 Command Surface

The initial command contracts should stay concrete and boring.

### `CreateBucket`

Purpose:
- create one bounded quota bucket with an initial balance and limit

Required fields:
- `bucket_id`
- `limit`
- `initial_balance`
- `refill_rate_per_slot`
- `operation_id`
- `request_slot`

Rules:
- `initial_balance` must satisfy `0 <= initial_balance <= limit`
- `refill_rate_per_slot` may be zero, which means the bucket never refills
- creating an existing bucket returns a deterministic `AlreadyExists` result
- duplicate retry with the same `operation_id` and identical payload returns the cached outcome
- reusing the same `operation_id` with different payload returns `OperationConflict`

### `Debit`

Purpose:
- attempt to deduct one amount from one bucket exactly once within the configured dedupe window

Required fields:
- `bucket_id`
- `amount`
- `operation_id`
- `request_slot`

Rules:
- `amount` must be strictly positive
- refill is derived only from persisted bucket state plus `request_slot`
- debit succeeds only when `balance >= amount`
- successful debit reduces balance exactly once
- insufficient balance returns a deterministic `InsufficientFunds` result
- duplicate retry with the same `operation_id` and identical payload returns the cached outcome
- reusing the same `operation_id` with different payload returns `OperationConflict`

## V1 Result Codes

The initial result surface is:

- `Ok`
- `AlreadyExists`
- `BucketTableFull`
- `BucketNotFound`
- `InsufficientFunds`
- `OperationConflict`
- `OperationTableFull`
- `SlotOverflow`

This list is intentionally small. New result codes should be added only when a bounded operating
condition cannot be expressed clearly with the current set.

## Deferred Work

The following are explicitly out of scope for v1:

- bucket hierarchy
- tenancy or project policy
- rate scheduling or queueing
- billing or chargeback semantics
- rich read APIs
- batching or group commit
- generic runtime extraction
- a new repo or public framework identity

## Required Proofs Before Scope Expansion

The experiment does not move on to refill or extraction until these are true:

- `CreateBucket` and `Debit` are replay-equivalent
- duplicate retry behavior is proven for success and failure paths
- operation-table pressure returns deterministic rejection
- crash/restart does not invent or lose debits
- copied runtime substrate in `quota-core` remains local and does not push abstractions back into
  `allocdb-core`

## Stop Conditions

Stop and reassess if any of these happen:

- quota requires a materially different runtime contract from AllocDB
- the runtime copies diverge immediately for domain reasons
- the experiment starts widening into policy or product work before v1 semantics are proven
- extraction pressure appears before the copied substrate and domain semantics stabilize
