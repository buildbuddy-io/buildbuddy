#!/usr/bin/env bash
set -euo pipefail
export RELEASE_LOG="$TEST_TMPDIR/predicates.log"
export AFTER_STATUS=0 RUN_STATUS=0

expect_failure() {
  if "$@"; then echo >&2 "Unexpected success: $*"; exit 1; fi
}
assert_both_ran() {
  [[ "$(cat "$RELEASE_LOG")" == $'after\nrun' ]]
}

# Both predicates must confirm, in dependency order.
: > "$RELEASE_LOG"
"$CHECK"
assert_both_ran

# Missing/error from either branch cannot be hidden by a successful sibling.
for status in 1 7; do
  : > "$RELEASE_LOG"
  export AFTER_STATUS="$status" RUN_STATUS=0
  expect_failure "$CHECK"
  [[ "$(cat "$RELEASE_LOG")" == after ]]

  : > "$RELEASE_LOG"
  export AFTER_STATUS=0 RUN_STATUS="$status"
  expect_failure "$CHECK"
  assert_both_ran

done
: > "$RELEASE_LOG"
export AFTER_STATUS=1 RUN_STATUS=7
expect_failure "$CHECK"
[[ "$(cat "$RELEASE_LOG")" == after ]]

# No artifacts to confirm is vacuously successful.
: > "$RELEASE_LOG"
"$EMPTY_CHECK"
[[ ! -s "$RELEASE_LOG" ]]

# Disabling actions preserves the original bare-target behavior; the fixture
# macro separately asserts that none of the suffixed targets are created.
export AFTER_STATUS=0 RUN_STATUS=0 EXPECTED_ACTION=plain
"$PLAIN"
assert_both_ran
