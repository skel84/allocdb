# AllocDB Docs

## Reading Order

1. [TigerStyle Source](./tiger_style.md)
2. [AllocDB Principles](./principles.md)
3. [PRD Overview](./prd.md)
4. [Engineering Design Overview](./design.md)

## Product Docs

- [Product Scope](./product.md)
- [Semantics and API](./semantics.md)
- [Alpha API](./api.md)
- [PRD Overview](./prd.md)

## Engineering Docs

- [Architecture](./architecture.md)
- [Fault Model](./fault-model.md)
- [Jepsen Refactor Plan](./jepsen-refactor-plan.md)
- [Lease Kernel Design Decisions](./lease-kernel-design.md)
- [Lease Kernel Follow-On](./lease-kernel-follow-on.md)
- [Quota Engine Plan](./quota-engine-plan.md)
- [Quota Engine Semantics](./quota-semantics.md)
- [Quota Runtime Seam Evaluation](./quota-runtime-seam-evaluation.md)
- [Reservation Engine Plan](./reservation-engine-plan.md)
- [Reservation Engine Semantics](./reservation-semantics.md)
- [Reservation Runtime Seam Evaluation](./reservation-runtime-seam-evaluation.md)
- [Runtime Extraction Roadmap](./runtime-extraction-roadmap.md)
- [Revoke Safety Slice](./revoke-safety-slice.md)
- [Operator Runbook](./operator-runbook.md)
- [KubeVirt Jepsen Report](./kubevirt-jepsen-report.md)
- [Real Cluster E2E Roadmap](./real-cluster-e2e-roadmap.md)
- [Replication Notes](./replication.md)
- [Benchmark Harness](./benchmark-harness.md)
- [Roadmap](./roadmap.md)
- [Current Status](./status.md)
- [Work Breakdown](./work-breakdown.md)
- [Spikes](./spikes.md)
- [Storage and Recovery](./storage.md)
- [Implementation Rules](./implementation.md)
- [Testing Strategy](./testing.md)
- [Engineering Design Overview](./design.md)

## Repository Workflow

- [Contributing Guide](https://github.com/skel84/allocdb/blob/main/CONTRIBUTING.md)

## Source Style Docs

- [TigerStyle Source](./tiger_style.md)
- [AllocDB Principles](./principles.md)
- [Research Lineage](./research.md)

## Structure Notes

- `prd.md` is the stable top-level product entry point.
- `design.md` is the stable top-level engineering entry point.
- The focused docs carry the detailed rules.

## Planned Follow-Ups

- split `replication.md` into protocol/recovery/validation docs after single-node semantics are stable
