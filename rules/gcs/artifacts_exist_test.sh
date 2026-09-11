#!/usr/bin/env bash
set -euo pipefail

export MOCK_STATE="$TEST_TMPDIR/state" MOCK_LOG="$TEST_TMPDIR/gsutil.log"
mkdir -p "$MOCK_STATE" "$TEST_TMPDIR/bin"
cp "$MOCK_GSUTIL" "$TEST_TMPDIR/bin/gsutil"
chmod +x "$TEST_TMPDIR/bin/gsutil"
export PATH="$TEST_TMPDIR/bin:$PATH"
# The GCS wrappers must continue to clear this for gsutil.
export PYTHONSAFEPATH=1

fail() { echo >&2 "$*"; exit 1; }
expect_failure() {
  if "$@"; then fail "Unexpected success: $*"; fi
}
reset_state() {
  rm -f "$MOCK_STATE/partial" "$MOCK_STATE/complete" "$MOCK_STATE/marker"
  : > "$MOCK_LOG"
}
assert_stat_only() {
  [[ "$(cat "$MOCK_LOG")" == 'stat gs://test-bucket/release/abc123/_SUCCESS' ]] || fail "Check did more than stat the exact marker"
}

# An old upload or partial upload without _SUCCESS is not ready.
reset_state
expect_failure "$CHECK" "$SHA_FILE"
assert_stat_only
: > "$MOCK_STATE/complete"
: > "$MOCK_LOG"
expect_failure "$CHECK" "$SHA_FILE"
assert_stat_only

# Success is two separate calls, complete copy first, empty marker second.
reset_state
"$PUSH" "$SHA_FILE" "$PAYLOAD" "$SHA_FILE"
[[ -f "$MOCK_STATE/marker" ]] || fail 'Missing completion marker'
[[ "$(wc -l < "$MOCK_LOG")" == 2 ]] || fail 'Expected exactly two copies'
expected="-m -h Cache-Control:no-store cp -r -Z $PAYLOAD $SHA_FILE gs://test-bucket/release/abc123/"
[[ "$(head -n 1 "$MOCK_LOG")" == "$expected" ]] || fail 'Payload/hash copy changed'
[[ "$(tail -n 1 "$MOCK_LOG")" == *' gs://test-bucket/release/abc123/_SUCCESS' ]] || fail 'Wrong marker path'
: > "$MOCK_LOG"
"$CHECK" "$SHA_FILE"
assert_stat_only
: > "$MOCK_LOG"
"$POISON_CHECK" "$SHA_FILE"
assert_stat_only

# Failure after a partial payload copy must not attempt a marker upload.
reset_state
export MOCK_FAIL_COPY=1
expect_failure "$PUSH" "$SHA_FILE" "$PAYLOAD" "$SHA_FILE"
unset MOCK_FAIL_COPY
[[ -f "$MOCK_STATE/partial" && ! -f "$MOCK_STATE/marker" ]]
[[ "$(wc -l < "$MOCK_LOG")" == 1 ]]
: > "$MOCK_LOG"
expect_failure "$CHECK" "$SHA_FILE"
assert_stat_only

# A failed marker upload is a failed push and is not confirmed.
reset_state
export MOCK_FAIL_MARKER=1
expect_failure "$PUSH" "$SHA_FILE" "$PAYLOAD" "$SHA_FILE"
unset MOCK_FAIL_MARKER
[[ -f "$MOCK_STATE/complete" && ! -f "$MOCK_STATE/marker" ]]
expect_failure "$CHECK" "$SHA_FILE"

# Auth/network/other stat errors are nonzero, even with a marker present.
reset_state
touch "$MOCK_STATE/marker"
export MOCK_STAT_ERROR=1
expect_failure "$CHECK" "$SHA_FILE"
unset MOCK_STAT_ERROR
assert_stat_only

# Missing/unreadable/empty/invalid identities fail before any cloud operation.
: > "$TEST_TMPDIR/empty.sha"
printf 'abc123/other' > "$TEST_TMPDIR/invalid.sha"
printf 'abc123\nother' > "$TEST_TMPDIR/multiline.sha"
for script in "$PUSH" "$CHECK" "$DELETE"; do
  : > "$MOCK_LOG"
  expect_failure "$script"
  for input in "$TEST_TMPDIR/missing.sha" "$TEST_TMPDIR/empty.sha" "$TEST_TMPDIR/invalid.sha" "$TEST_TMPDIR/multiline.sha" "$TEST_TMPDIR"; do
    expect_failure "$script" "$input" "$PAYLOAD"
  done
  [[ ! -s "$MOCK_LOG" ]] || fail 'Invalid identity contacted GCS'
done

# Hash files need not have a final newline.
printf abc123 > "$TEST_TMPDIR/no-newline.sha"
: > "$MOCK_LOG"
"$CHECK" "$TEST_TMPDIR/no-newline.sha"
assert_stat_only

# A payload may not impersonate the completion marker.
: > "$TEST_TMPDIR/_SUCCESS"
: > "$MOCK_LOG"
expect_failure "$PUSH" "$SHA_FILE" "$TEST_TMPDIR/_SUCCESS"
[[ ! -s "$MOCK_LOG" ]]

# Unversioned uploads work without reading /dev/null, but must never reuse or
# publish a completion marker: their contents can change under the same key.
reset_state
"$UNVERSIONED_PUSH" /dev/null "$PAYLOAD"
[[ -f "$MOCK_STATE/complete" && ! -f "$MOCK_STATE/marker" ]]
[[ "$(cat "$MOCK_LOG")" == "-m cp -r -Z $PAYLOAD gs://test-bucket/mutable/" ]]
touch "$MOCK_STATE/marker"
: > "$MOCK_LOG"
expect_failure "$UNVERSIONED_CHECK"
[[ ! -s "$MOCK_LOG" ]]
"$UNVERSIONED_DELETE" /dev/null
[[ "$(cat "$MOCK_LOG")" == '-m rm -r gs://test-bucket/mutable/' ]]
: > "$MOCK_LOG"
"$DELETE" "$SHA_FILE"
[[ "$(cat "$MOCK_LOG")" == '-m rm -r gs://test-bucket/release/abc123/' ]]
