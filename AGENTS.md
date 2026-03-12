# Project Agents

Primary Rust development guidance for this repository lives in
[`.rust-skills/AGENTS.md`](./.rust-skills/AGENTS.md).

Project-specific documentation starts at:

- [`docs/README.md`](./docs/README.md)

Project-specific design and engineering rules live in:

- [`docs/prd.md`](./docs/prd.md)
- [`docs/design.md`](./docs/design.md)
- [`docs/principles.md`](./docs/principles.md)
- [`docs/tiger_style.md`](./docs/tiger_style.md)

## Project Execution Rules

- Work in small, testable chunks. Prefer changes that can be reviewed, tested, and explained in one
  pass over broad speculative edits.
- Keep the system buildable after each chunk whenever feasible. If a larger refactor is required,
  stage it as a short sequence of intermediate, verifiable steps.
- Write extensive tests for every meaningful behavior change. Favor invariant tests, negative-path
  tests, recovery tests, and regression tests over shallow happy-path coverage.
- Treat tests as part of the deliverable. A task is not complete when code compiles; it is complete
  when the behavior is exercised credibly.
- Keep documentation up to date with the code and design. If a change affects behavior, invariants,
  failure modes, operational semantics, testing strategy, or implementation sequencing, update the
  relevant docs in the same unit of work.
- Keep [`docs/status.md`](./docs/status.md) current as the single-file progress snapshot for the
  repository. Update it whenever milestone state, implementation coverage, or the recommended next
  step materially changes.
- Prefer fixing stale docs immediately over leaving follow-up documentation debt.
- Add extensive logging where it materially improves debuggability or operational clarity. Use the
  right log level:
  - `error` for invariant breaks, corruption, and failed operations that require intervention
  - `warn` for degraded but expected conditions such as overload, lag, or rejected requests
  - `info` for meaningful lifecycle and state-transition events
  - `debug` for detailed execution traces useful in development
  - `trace` only for very high-volume diagnostic detail
- Logging must be structured and purposeful. Do not add noisy logs that obscure signal or hide bugs.
- As an LLM agent, iterate autonomously. Re-read the relevant code and docs, test your assumptions,
  self-check intermediate results, and keep pushing until the current goal is actually resolved.
- Do not stop at the first plausible answer. Verify behavior, inspect the consequences of your
  change, and use follow-up edits or tests to close gaps you discover along the way.
