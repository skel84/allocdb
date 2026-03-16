---
title: "Building AllocDB: A Deterministic, Jepsen-Verified Resource Database via AI-Human Co-engineering"
slug: "allocdb-launch"
date: 2026-03-15
authors: [allocdb]
tags: [allocdb, launch, announcement]
---

**The Problem**
Handling "one resource, one winner" (like concert tickets or GPU slots) is deceptively hard. Most systems use Postgres row-locks or Redis TTLs, which often leak or double-allocate during network partitions or primary failovers.

**The Solution: AllocDB**
AllocDB is a specialized database built in Rust, engineered specifically for resource allocation under high contention. It is a replicated state machine where **Same Snapshot + Same WAL == Same State**.

**Core Engineering Principles:**

* **TigerStyle Rust:** The "trusted core" targets zero dynamic memory allocation after startup. All queues and tables have fixed, pre-allocated capacities to eliminate tail latency spikes.


* **Logical Time:** We don't use wall-clocks. All expirations happen in "logical slots," making the entire system 100% reproducible and immune to clock skew.


* **Strict Determinism:** The core state machine is isolated from randomness and thread scheduling.



**The "AI-Human" Process**
AllocDB was an experiment in high-level system guidance. I provided the architectural constraints—pointing the LLM (Codex) toward the engineering discipline of TigerBeetle and the science of Viewstamped Replication.

The implementation, **including the entire test suite**, was generated through an extensive iterative chat. Because I "distrusted" the AI-generated core by default, I had it build a rigorous **Jepsen harness**. We spent weeks "bullying" the database with network partitions, SIGKILLs, and storage faults until the AI-generated code held up under industry-standard chaos.
