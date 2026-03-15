# Jepsen Testing & Fault Tolerance

To ensure AllocDB's determinism and strict resource-allocation guarantees hold up under real-world distributed systems chaos, the project employs rigorous **Jepsen testing**.

While a strong single-node execution core is the foundation of AllocDB's determinism, its replication layer is heavily tested against process crashes, network partitions, clock skew, and storage faults.

## Testing Environments

AllocDB tests its cluster resilience using two primary harnesses:
1. **Local QEMU Testbed:** A fully orchestrated local cluster of virtual machines, isolating replica nodes to test deep system faults (like disk write failures and kernel panics) that cannot be simulated purely in-process.
2. **KubeVirt Fleet:** A distributed Jepsen execution environment running over KubeVirt, allowing continuous, parallelized fault injection across multiple cluster instances.

## Faults Injected

The Jepsen nemesis continuously injects faults during high-concurrency client workloads to verify safety invariants. Faults include:
- **Process crashes:** Hard `SIGKILL` on primary and backup replicas.
- **Network partitions:** Asymmetric and symmetric network isolation of nodes.
- **Storage failures:** Simulating fsync failures and disk IO stalls.
- **Clock skew:** Advancing and rewinding clocks to ensure the logical-time TTL scheduler remains purely log-driven and is not corrupted by system clocks.

## Safety Invariants Verified

During and after the fault windows, the Jepsen testing validates:
1. **No Double Allocations:** A resource is never held by two active reservations at the same time, regardless of cluster view changes or failovers.
2. **Bounded TTLs:** A resource may become reusable *late* (if the primary is partitioned or delayed), but it is *never* made available *early*.
3. **Strict Serializability:** All committed operations produce a completely deterministic state machine result. Replaying the WAL on any healthy replica always yields the identical allocator state.
4. **No Phantom Reads:** Clients never read uncommitted or rolled-back state from a deposed primary.

By relying on Jepsen, AllocDB ensures that its "one resource, one winner" product contract is mathematically and empirically sound under severe infrastructure degradation.
