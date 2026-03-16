# KubeVirt Jepsen Report

## Scope

This report captures the first fully green end-to-end Jepsen gate on the rebuilt homelab
KubeVirt profile.

Execution profile:

- environment: KubeVirt on the homelab cluster
- storage class: `longhorn-strict-local-wffc`
- placement: replica-only anti-affinity, one replica per host for each lane
- topology: `3` independent Jepsen lanes, each with `1` control VM and `3` replica VMs
- gate shape: the documented `15`-run matrix

Run families:

- control: `4`
- crash-restart: `4`
- partition-heal: `4`
- mixed-failover: `3`

Result:

- all `15` runs finished `state=passed`
- all `15` runs finished with `release_gate_passed=true`
- all `15` runs finished with `blockers=0`

## What We Proved

The final green matrix is credible evidence for stability and reliability of the current replicated
prototype on this test profile.

What the gate proved:

- the live replicated runtime survives repeated crash/restart, partition/heal, and mixed-failover
  sequences for the full documented `1800s` fault window
- the retry contract held under faults: ambiguous writes were resolved by later retry or cache
  reuse, not by duplicate committed execution
- strict-primary read fences held under faults: the gate did not observe stale successful reads
- the reservation allocator stayed safe under contention and failover: the gate did not observe
  double allocation
- expiration and recovery stayed coherent under faults: the gate did not observe early expiration
  release, and the replicated `tick_expirations` path remained recoverable through failover
- the external multi-lane KubeVirt profile itself was stable enough to run `3` Jepsen lanes in
  parallel once the storage and placement profile was corrected

In practical terms, the final evidence says:

- reads did not succeed from stale primaries
- writes were not published twice
- the same logical operation did not commit twice
- ambiguity did not remain unresolved at the end of any gate run
- expiration-related recovery behaved consistently enough to pass both partition-heal and
  mixed-failover gates after the backlog-drain fix

## What This Does Not Prove

This report is strong release-gate evidence for the current homelab KubeVirt profile, but it is
not a universal correctness proof.

Still out of scope:

- arbitrary packet loss, delay, or asymmetric per-link network faults
- large-cluster or high-throughput performance claims
- long multi-day soak behavior
- cloud-provider or heterogeneous-hardware portability
- production operational guarantees outside this tested KubeVirt profile

So the honest conclusion is:

- the current replicated prototype is stable and reliable on the tested release-gate profile
- this is strong evidence, not a proof of correctness for every deployment environment

## Artifact Policy

Raw Jepsen artifacts should not live in tracked git history.

Reasons:

- they are generated evidence, not maintained source
- they are environment-specific and timestamped
- they churn heavily and review poorly in git
- the stable value belongs in committed summaries and reports, not raw log bundles

Recommended split:

- commit: the report, docs updates, and any code fixes required to make the gate pass
- keep outside git: histories, status files, event logs, and fetched VM log archives

## Current Artifact Footprint

For this run set, the artifact roots are:

- `../.artifacts/kubevirt-jepsen-20260315/lane-a`
- `../.artifacts/kubevirt-jepsen-20260315/lane-b`
- `../.artifacts/kubevirt-jepsen-20260315/lane-c`

Current size snapshot:

- lane `a`: about `1.9M`
- lane `b`: about `992K`
- lane `c`: about `1.3M`
- total: about `4.2M`

The largest individual bundle directories are only on the order of `100K`, because the stored
evidence is mostly text history plus compressed logs. Even so, they should stay out of tracked
history and be treated as build/test artifacts.

## Representative Evidence

These links point at local, git-ignored `.artifacts/` paths inside one developer workspace. They
are representative evidence locations for rerun and audit work, not repository-hosted artifacts for
fresh clones.

Representative final status files:

- `../.artifacts/kubevirt-jepsen-20260315/lane-a/reservation_contention-crash-restart-status.txt`
- `../.artifacts/kubevirt-jepsen-20260315/lane-b/ambiguous_write_retry-partition-heal-status.txt`
- `../.artifacts/kubevirt-jepsen-20260315/lane-c/failover_read_fences-mixed-failover-status.txt`
- `../.artifacts/kubevirt-jepsen-20260315/lane-c/expiration_and_recovery-mixed-failover-status.txt`

Representative final artifact bundles:

- `../.artifacts/kubevirt-jepsen-20260315/lane-a/ambiguous_write_retry-mixed-failover-1773570559554`
- `../.artifacts/kubevirt-jepsen-20260315/lane-a/expiration_and_recovery-partition-heal-1773568653724`
- `../.artifacts/kubevirt-jepsen-20260315/lane-b/ambiguous_write_retry-partition-heal-1773568857293`
- `../.artifacts/kubevirt-jepsen-20260315/lane-c/expiration_and_recovery-mixed-failover-1773569536167`
