#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

status=0

fail() {
  printf 'repo-check: %s\n' "$1" >&2
  status=1
}

check_max_lines() {
  local file="$1"
  local max_lines="$2"
  local line_count

  line_count="$(wc -l < "$file" | tr -d ' ')"
  if (( line_count > max_lines )); then
    fail "$file has $line_count lines; limit is $max_lines"
  fi
}

check_forbidden_pattern() {
  local pattern="$1"
  local description="$2"
  local matches

  matches="$(rg -n --glob '*.rs' "$pattern" crates/allocdb-core/src || true)"
  if [[ -n "$matches" ]]; then
    fail "trusted core contains forbidden ${description}:\n$matches"
  fi
}

check_allowed_dependencies() {
  local dependency
  local allowed='allocdb-retire-queue crc32c log'

  while IFS= read -r dependency; do
    if [[ ! " $allowed " =~ [[:space:]]"$dependency"[[:space:]] ]]; then
      fail "crates/allocdb-core/Cargo.toml declares non-approved dependency '$dependency'"
    fi
  done < <(
    awk '
      /^\[dependencies\]/ { in_deps = 1; next }
      /^\[/ { in_deps = 0 }
      in_deps && /^[A-Za-z0-9_-]+[[:space:]]*=/ {
        name = $1
        sub(/[[:space:]]*=.*/, "", name)
        print name
      }
    ' crates/allocdb-core/Cargo.toml
  )
}

while IFS= read -r file; do
  case "$file" in
    *tests.rs) check_max_lines "$file" 450 ;;
    *) check_max_lines "$file" 420 ;;
  esac
done < <(find crates/allocdb-core/src -type f -name '*.rs' | sort)

check_max_lines "docs/status.md" 220

check_allowed_dependencies
check_forbidden_pattern '\bunsafe\b' 'unsafe code'
check_forbidden_pattern '\b(tokio|async_std|serde|serde_json|anyhow|thiserror)\b' 'disallowed dependency usage'
check_forbidden_pattern '\b(HashMap|BTreeMap)\b' 'unordered or tree maps in the trusted core'
check_forbidden_pattern 'Arc<[^>]*Mutex|Mutex<[^>]*Arc|Rc<[^>]*RefCell|RefCell<[^>]*Rc' 'shared mutable ownership primitives'
check_forbidden_pattern '\basync\s+fn\b' 'async functions'

if (( status == 0 )); then
  printf 'repo-check: ok\n'
fi

exit "$status"
