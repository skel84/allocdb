#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

run_step() {
  printf 'preflight: %s\n' "$1"
  shift
  "$@"
}

run_step "cargo fmt --all --check" cargo fmt --all --check
run_step "cargo clippy --all-targets --all-features -- -D warnings" \
  cargo clippy --all-targets --all-features -- -D warnings
run_step "cargo test" cargo test
run_step "scripts/check_repo.sh" scripts/check_repo.sh

printf 'preflight: ok\n'
