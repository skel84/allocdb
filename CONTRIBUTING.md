# Contributing

## Workflow

AllocDB should be developed through small GitHub issues and reviewable pull requests.

The default path for substantive work is:

1. open or refine a GitHub issue
2. create a branch from `main`
3. implement one task-sized change
4. run the required validation locally
5. open a pull request
6. triage human review and CodeRabbit feedback
7. rerun validation after review-driven changes
8. merge only when the branch is green and the review notes are resolved

Do not use `main` as the day-to-day development branch.

## Planning IDs

- milestones use `M0`, `M1`, `M1H`, `M2`, `M3`, `M4`, `M5`, `M6`
- planned tasks use `M#-T#`, for example `M2-T08`
- approved spikes use `M#-S#`, for example `M1-S01`

When a GitHub issue maps to planned work, reuse the same identifier in the title.

Examples:

- `M2-T08 Safe checkpoint coordination and WAL retention`
- `M3-T06 Finalize indefinite-outcome retry behavior`
- `M5-T02 Expose recovery status and operational signals`

## Branch Naming

Prefer short branch names derived from the task or issue.

Examples:

- `m2-t08-safe-checkpoint`
- `m3-t06-indefinite-retries`
- `docs/review-workflow`

## Issue Rules

- every substantial code or design change should link to an issue
- keep each issue scoped to one task or one tightly related doc change
- add the appropriate milestone and area labels
- close or retitle stale issues instead of letting the tracker drift

## Pull Request Rules

- keep PRs small enough to review in one pass
- link the issue in the PR body
- explain behavioral impact, not just file edits
- include the exact validation commands you ran
- update docs in the same PR when behavior, invariants, failure modes, or operator-visible
  semantics change

## Required Validation

Run the local preflight before opening or merging a PR:

- `scripts/preflight.sh`

That command is the preferred local path because CI runs the same script.

It currently runs:

- `cargo fmt --all --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test`
- `scripts/check_repo.sh`

Add narrower commands too when they strengthen confidence for the specific change.

## Local Hooks

Install the repository-managed hooks once per clone:

- `scripts/install-hooks.sh`

This configures git to use `.githooks/` and runs `scripts/preflight.sh` on `pre-push`.

## Review Policy

- human review focuses first on correctness, regressions, missing tests, recovery behavior, and
  docs drift
- CodeRabbit is part of the review path when enabled on the repository
- every CodeRabbit suggestion or comment that affects the PR must be addressed explicitly before
  merge
- apply CodeRabbit suggestions by default when they improve correctness, safety, testing,
  observability, or documentation
- reject CodeRabbit suggestions when they add churn without value or weaken determinism,
  boundedness, dependency discipline, or trusted-core isolation

Addressing CodeRabbit means one of:

- `applied`
- `not applied`
- short reason when not applied

Do not leave substantive CodeRabbit comments unresolved or silently ignored.

## Merge Policy

Until the repository has more human reviewers, self-merge is acceptable only after:

- `scripts/preflight.sh` passes locally
- required checks pass
- review comments are addressed
- CodeRabbit feedback is triaged

When the team grows, switch to at least one human approval before merge.

## Repository Guardrails

The GitHub repository is configured so that `main`:

- requires pull requests before merge
- requires the `checks` CI job and uses strict status checks
- requires resolved review conversations
- requires linear history
- disallows force pushes and branch deletion

When enabled on the repository, CodeRabbit is part of the normal review path and its feedback must
be addressed before merge.

## Labels And Milestones

Recommended milestone shape:

- `M2 Durability and Recovery`
- `M3 Submission Pipeline`
- `M4 Deterministic Simulation`
- `M5 Single-Node Alpha`
- `M6 Replication Design Gate`

Recommended labels:

- `area:core`
- `area:node`
- `area:storage`
- `area:docs`
- `type:task`
- `type:bug`
- `type:docs`
- `type:test`
- `type:infra`
- `priority:p0`
- `priority:p1`
- `status:blocked`

## Logging And Tests

- new behavior should come with tests that exercise invariants, negative paths, and regressions
- logs should stay structured and deliberate
- use `error` for corruption and invariant breaks
- use `warn` for bounded overload and rejected requests
- use `info` for meaningful lifecycle and state transitions
- use `debug` and `trace` only when the extra volume is justified
