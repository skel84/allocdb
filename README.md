# AllocDB

**AllocDB** is a deterministic, specialized database engineered for the "one resource, one winner" problem. It is designed to handle scarce resources—such as concert tickets, inventory, or compute slots—under extreme contention without the race conditions or "ghost reservations" typical of ad-hoc systems built on top of general-purpose RDBMS or KV stores.

## Why AllocDB?

Most modern reservation logic relies on row-level locking in PostgreSQL or TTL keys in Redis. While these work for low-concurrency use cases, they often break during high-contention events or infrastructure failovers, leading to double-allocations or inconsistent state.

AllocDB solves this by making resource allocation a first-class, deterministic primitive.

### Core Principles

* **Strict Determinism:** AllocDB is a replicated state machine where **Same Snapshot + Same WAL == Same State + Same Results**. The core state machine does not read wall-clock time, use randomness, or depend on thread scheduling.


* **Boundedness by Design:** Every hot-path structure (queues, batches, and tables) has an explicit, pre-allocated limit. This eliminates tail latency spikes and makes system overload a visible, deterministic business result rather than a transport ambiguity.


* **TigerStyle Rust:** Inspired by the engineering discipline of TigerBeetle, the "trusted core" targets **allocation-free steady-state execution**. All capacity is allocated at startup, eliminating the performance jitter of a dynamic heap.


* **Logical Time:** Instead of fragile system clocks, AllocDB uses "logical slots" for all expirations and TTLs. This ensures resources are never reused prematurely due to clock skew.



## Verified by Chaos

We don't just promise safety; we prove it. AllocDB is rigorously validated using a **Jepsen testing harness** that injects:

* **Process Crashes:** Hard `SIGKILL` on primary and backup replicas.

* **Network Partitions:** Both symmetric and asymmetric isolation.

* **Storage Faults:** Simulated `fsync` failures and disk stalls.

* **Clock Skew:** Testing that logical-time scheduling remains purely log-driven.


## A Note on the Development Process

AllocDB was built as an exploration into high-level systems engineering through **AI-human co-engineering**.

The implementation was generated using **Codex** under the direction of strict architectural constraints (TigerStyle) and safety invariants. Because we "distrusted" the generated code by default, the development process focused on building an exhaustive validation suite—including deterministic simulation and Jepsen testing—before the core was considered stable.

The result is a fascinating case study: by enforcing strict determinism and "no-allocation" Rust patterns, we've created an AI-assisted core that consistently passes the industry's most rigorous distributed systems tests.

---

### Implementation Status

* **Single-Node Core:** Complete. Replay-equivalent, crash-safe, and bounded.


* 
**Verification:** Jepsen-tested against local QEMU and distributed KubeVirt fleets.


* 
**Replication:** Replicated prototype with view change and failover gates in progress.



---

**Next Steps:**

* Read the [Architecture Overview](https://www.google.com/search?q=./docs/architecture.md).


* Explore the [TigerStyle Principles](https://www.google.com/search?q=./docs/principles.md).


* Check out the [Jepsen Testing Report](https://www.google.com/search?q=./docs/jepsen-testing.md).
