# Real Cluster E2E Roadmap

## Purpose

Define the minimum AllocDB work needed to support a real-cluster end-to-end test with
`gpu_control_plane`.

## Tracking

- AllocDB issue: `skel84/allocdb#99`
- GPU Control Plane issue: `skel84/gpu_control_plane#35`

## Shared E2E Contract

The first real-cluster e2e should prove this path:

1. A workload enters Kueue and waits for quota.
2. `gpu_control_plane` asks AllocDB to commit ownership.
3. AllocDB commits ownership and exposes it through its deployed service.
4. `gpu_control_plane` materializes the matching DRA objects.
5. The workload progresses, runs, and eventually releases or revokes cleanly.

The test should use:

- one Kubernetes cluster
- one deployed AllocDB service
- one deployed `gpu_control_plane`
- one deterministic GPU pool mapping
- one repeatable test workload

## AllocDB Roadmap

### 1. Package a deployable service shape

- provide a container image for the replicated node service
- provide a manifest or overlay that starts the service in-cluster
- keep the runtime shape aligned with the current replicated node implementation

### 2. Make persistence explicit

- define the PVC or volume layout for WAL, snapshots, and replica metadata
- keep startup and restart paths tied to the durable workspace layout
- document the data retained across restart and rejoin

### 3. Expose cluster health clearly

- add readiness and liveness probes that reflect real service state
- keep metrics and operator-visible health signals available from the deployed service

### 4. Document operational flows

- startup
- restart
- isolate and heal
- failover and rejoin
- controlled shutdown

### 5. Prove the deployed service in smoke tests

- service starts from the packaged manifest
- client submit works against the deployed service
- primary reads work
- restart and rejoin preserve durable state
- failover stays fail-closed and does not violate ownership safety

## Exit Criteria

This repo is ready for the real-cluster e2e when:

- AllocDB can be deployed from a documented manifest or overlay
- the deployed service survives restart with durable state intact
- a minimal smoke proves submit/read behavior in-cluster
- the operational runbook matches the deployed shape
