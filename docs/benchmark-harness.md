# Benchmark Harness

## Purpose

`M5-T03` adds a deterministic benchmark harness for the single-node alpha.

The harness lives in `crates/allocdb-bench` and deliberately avoids statistical benchmark
frameworks. The goal is to make contention and boundedness runs reproducible, reviewable, and easy
to compare across branches.

## Entry Point

Run both default scenarios:

```sh
cargo run -p allocdb-bench -- --scenario all
```

Run one scenario with larger knobs:

```sh
cargo run -p allocdb-bench -- \
  --scenario one-resource-many-contenders \
  --hotspot-rounds 512 \
  --hotspot-contenders 64
```

Show CLI help:

```sh
cargo run -p allocdb-bench -- --help
```

## Default Scenarios

### `one-resource-many-contenders`

This scenario measures hot-spot contention on one resource.

Shape:

- create one resource
- for each logical slot:
  - one holder reserves it successfully
  - the remaining contenders attempt the same resource and deterministically receive
    `resource_busy`
  - the winning reservation is released in the same slot

Default knobs:

- `--hotspot-rounds 256`
- `--hotspot-contenders 32`

What to watch:

- `busy_reservations` vs `successful_reservations`
- `peak_operation_table_used` and `peak_operation_table_utilization_pct`
- `final_expiration_backlog` and `final_logical_slot_lag`

### `high-retry-pressure`

This scenario measures retry-cache behavior and operation-table boundedness.

Shape:

- fill the operation table with unique `create_resource` commands
- replay same-payload retries and conflicting `operation_id` reuses
- issue fresh operations while the table is full and observe deterministic
  `operation_table_full`
- advance the logical slot past the retry window and verify the table drains enough for a new
  write to succeed

Default knobs:

- `--retry-table-capacity 128`
- `--retry-duplicate-fanout 4`
- `--retry-full-rejection-attempts 32`

What to watch:

- `cached_duplicate_hits`
- `cached_conflict_hits`
- `operation_table_full_hits`
- `pre_retire_operation_table_used` vs `final_operation_table_used`
- `wal_bytes_after_fill` vs `wal_bytes_after_retry_pressure`

Interpretation:

- same-payload retries and conflicting reuses should hit the retry cache without WAL growth
- fresh requests against a full operation table still append and return a deterministic rejection
- advancing the slot past the retention window should free operation-table capacity again

## Output

Each scenario prints one key-value report with:

- elapsed wall-clock time
- total operations
- derived throughput
- scenario-specific result counters
- final or checkpointed engine/core metrics
- WAL byte counts where they help explain dedupe behavior

The timings are useful for branch-to-branch comparison on the same machine. The counts and metric
snapshots are the stronger regression signal because they are deterministic.

## Usage Notes

- Run on a quiet machine when comparing raw timings.
- Prefer comparing one scenario at a time when investigating regressions.
- Oversized knob combinations fail fast before engine startup. The harness caps derived core table
  capacities at 65,536 entries so bad CLI inputs do not turn into accidental OOMs.
- The high-retry scenario also rejects configurations that would generate more than 1,000,000
  worst-case submissions, so retry-cache stress stays bounded in runtime and WAL growth.
- Treat this harness as part of the single-node validation path, not as a substitute for the later
  deterministic simulator.
