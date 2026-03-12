# Project Agents

Additional workspace-local Rust development guidance, when present, lives in
[`.rust-skills/AGENTS.md`](./.rust-skills/AGENTS.md).

Project-specific documentation starts at:

- [`docs/README.md`](./docs/README.md)
- [`CONTRIBUTING.md`](./CONTRIBUTING.md)

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
  relevant docs in the same task or PR.
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
- After GitHub workflow setup, do substantive work on issue branches, not directly on `main`.
- Every substantial change should link to a tracked GitHub issue or explicitly explain why it is
  issue-less.
- Use the GitHub Project `AllocDB` as the operational work board. Keep planned work on the board,
  not only in milestone pages or local docs.
- When you start active work on an issue, move its project status to `In Progress`. Leave it there
  through implementation and PR review, and move it to `Done` only after merge or explicit
  completion.
- Treat milestones as roadmap buckets and the GitHub Project as the live execution queue.
- Keep each PR scoped to one planned task such as `M2-T08`, or one tightly related bundle small
  enough to review in one pass.
- Prefer running [`scripts/preflight.sh`](./scripts/preflight.sh) before push or PR updates so the
  local validation path matches CI.
- Treat CodeRabbit as part of the required review path when it is enabled on the repository.
  Wait for its status to complete before merge. If it completes without a visible review comment or
  review thread, request visible output with `@coderabbitai summary`.
  Address every substantive CodeRabbit comment explicitly before merge by either applying the
  change or documenting why it is not being applied. Apply correctness, safety, recovery, test,
  and docs-alignment feedback by default; document why you reject suggestions that would weaken
  determinism, boundedness, or trusted-core discipline.
- After review-driven edits, rerun the relevant validation commands before considering the work
  ready to merge.
- Keep PR comments concise. For review follow-ups, state what changed and which validation command
  was rerun, but do not paste raw command output into PR comments unless the failure output or
  specific lines are themselves important.
- As an LLM agent, iterate autonomously. Re-read the relevant code and docs, test your assumptions,
  self-check intermediate results, and keep pushing until the current goal is actually resolved.
- Do not stop at the first plausible answer. Verify behavior, inspect the consequences of your
  change, and use follow-up edits or tests to close gaps you discover along the way.
