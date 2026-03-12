#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

git config core.hooksPath .githooks
printf 'hooks: core.hooksPath set to %s\n' '.githooks'
printf 'hooks: pre-push will now run scripts/preflight.sh\n'
