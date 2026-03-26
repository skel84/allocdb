# Reservation Engine Semantics

## Purpose

This document freezes the v1 semantic boundary for `reservation-core`.

It exists to keep the third-engine experiment narrow enough to answer the library question without
 drifting into commerce, ticketing, or booking product scope.

## Decision

`reservation-core` models scarce-capacity holds on one logical pool.

The v1 lifecycle is:

1. create pool
2. place hold
3. confirm hold
4. release hold
5. expire hold

Everything else is out of scope.

## Domain Model

### Pool

A pool is a bounded scarce-capacity container.

Required fields:

- `pool_id`
- `total_capacity`
- `held_capacity`
- `consumed_capacity`

### Hold

A hold is a temporary claim against one pool.

Required fields:

- `hold_id`
- `pool_id`
- `quantity`
- `deadline_slot`
- `state`

### Hold States

The v1 states are:

- `held`
- `confirmed`
- `released`
- `expired`

Only `held` is active. All other states are terminal.

## Commands

### CreatePool

Creates one pool with fixed total capacity.

### PlaceHold

Attempts to reserve `quantity` from one pool.

Success requirements:

- pool exists
- enough available capacity exists
- hold record can be inserted

Effect on success:

- `held_capacity += quantity`
- one `held` record is created

### ConfirmHold

Consumes a previously held quantity.

Success requirements:

- hold exists
- hold state is `held`
- hold is not overdue at the command slot

Effect on success:

- `held_capacity -= quantity`
- `consumed_capacity += quantity`
- hold state becomes `confirmed`

### ReleaseHold

Returns a previously held quantity back to availability.

Success requirements:

- hold exists
- hold state is `held`

Effect on success:

- `held_capacity -= quantity`
- hold state becomes `released`

### ExpireHold

Expires a previously held quantity after its deadline slot.

Success requirements:

- hold exists
- hold state is `held`
- `request_slot >= deadline_slot`

Effect on success:

- `held_capacity -= quantity`
- hold state becomes `expired`

## Result Surface

The v1 result space is intentionally small:

- `Ok`
- `AlreadyExists`
- `PoolTableFull`
- `PoolNotFound`
- `HoldTableFull`
- `HoldNotFound`
- `InsufficientCapacity`
- `HoldExpired`
- `InvalidState`
- `OperationConflict`
- `OperationTableFull`
- `SlotOverflow`

## Invariants

The engine must preserve all of the following:

- `0 <= held_capacity`
- `0 <= consumed_capacity`
- `held_capacity + consumed_capacity <= total_capacity`
- the same held quantity cannot be confirmed twice
- the same held quantity cannot be both confirmed and released
- expiry cannot consume capacity
- no command may increase effective capacity beyond `total_capacity`

## Idempotency Rules

The engine uses the same rule as the other engines:

- same `operation_id` + same payload returns the cached result
- same `operation_id` + different payload returns `OperationConflict`

No command may apply twice under retry.

## Time Rules

All time behavior is logical-slot based.

Allowed:

- `request_slot`
- persisted `deadline_slot`

Forbidden:

- wall-clock time
- process uptime
- background timers

## Non-Goals

V1 does not include:

- carts
- payments
- pricing
- multi-pool holds
- partial confirm
- partial release
- transfer between pools
- product inventory synchronization
- customer workflow
- read APIs beyond what tests and recovery need

## Extraction Intent

This engine is not a product commitment.

It is a third-engine pressure test. If it lands cleanly, the next seam evaluation should answer
whether a small shared runtime can support a later quota/credits build with materially less
duplication.
