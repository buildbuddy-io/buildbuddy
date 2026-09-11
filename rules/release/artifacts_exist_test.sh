#!/usr/bin/env bash
set -euo pipefail
export AFTER_STATUS=0 RUN_STATUS=0

# Both components confirm their artifacts.
"$CHECK"

# Neither component's failure can be hidden by a successful sibling.
export AFTER_STATUS=1
if "$CHECK"; then
  echo >&2 'Release predicate ignored failure from after'
  exit 1
fi
export AFTER_STATUS=0 RUN_STATUS=1
if "$CHECK"; then
  echo >&2 'Release predicate ignored failure from run'
  exit 1
fi
