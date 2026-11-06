#!/usr/bin/env bash

# --- begin runfiles.bash initialization v3 ---
# Copy-pasted from the Bazel Bash runfiles library v3.
set -uo pipefail; set +e; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v3 ---

set -euo pipefail

wrapper=$(rlocation "$1")
stock_helper=$(rlocation "$2")
test_dir="${TEST_TMPDIR:?run this with bazel test}"
mkdir -p "$test_dir/bin"
ln -s "$stock_helper" "$test_dir/bin/docker-credential-gcr"

# Clear inherited credentials and metadata overrides. These explicit paths keep
# both helper state and Application Default Credentials inside the test sandbox.
helper_env=(env -i
  "PATH=$test_dir/bin:/usr/bin:/bin"
  "HOME=$test_dir"
  "DOCKER_CREDENTIAL_GCR_CONFIG=$test_dir/helper_config.json"
  "DOCKER_CREDENTIAL_GCR_STORE=$test_dir/helper_store.json"
)

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# Capture stdout, stderr, and status separately so an auth failure cannot pass as
# an anonymous fallback merely because both commands exit nonzero.
run_helper() {
  local name=$1
  shift
  local status=0
  "${helper_env[@]}" "$@" <<< 'https://gcr.io' > "$test_dir/$name.stdout" 2> "$test_dir/$name.stderr" || status=$?
  printf '%s\n' "$status" > "$test_dir/$name.status"
}

expect_passthrough() {
  local behavior=$1
  for stream in stdout stderr status; do
    cmp "$test_dir/stock.$stream" "$test_dir/wrapper.$stream" || fail "$behavior: $stream changed"
  done
}

# Exercise the real pinned binary, not a mock of its current diagnostic. A helper
# upgrade that changes the missing-ADC error must fail this test.
printf '%s\n' '{"TokenSources":["env"]}' > "$test_dir/helper_config.json"
run_helper stock "$stock_helper" get
[[ $(cat "$test_dir/stock.status") == 1 ]] || fail "missing ADC: stock helper should fail"
grep -Fq 'failed to detect default credentials: credentials: could not find default credentials' "$test_dir/stock.stdout" || fail "missing ADC: stock diagnostic changed"
run_helper wrapper "$wrapper" get
[[ $(cat "$test_dir/wrapper.status") == 1 ]] || fail "missing ADC: wrapper should report credentials not found"
printf '%s\n' 'credentials not found in native keychain' > "$test_dir/expected.stdout"
cmp "$test_dir/expected.stdout" "$test_dir/wrapper.stdout" || fail "missing ADC: anonymous fallback diagnostic changed"
cmp "$test_dir/stock.stderr" "$test_dir/wrapper.stderr" || fail "missing ADC: stderr changed"

# A non-expired synthetic token exercises successful credential lookup without
# contacting an identity provider or a registry.
printf '%s\n' '{"TokenSources":["store"]}' > "$test_dir/helper_config.json"
printf '%s\n' '{"gcrCreds":{"access_token":"test-access-token","token_expiry":"2099-01-01T00:00:00Z"}}' > "$test_dir/helper_store.json"
run_helper stock "$stock_helper" get
[[ $(cat "$test_dir/stock.status") == 0 ]] || fail "stored credentials: stock helper should succeed"
grep -Fq '"Secret":"test-access-token"' "$test_dir/stock.stdout" || fail "stored credentials: expected token was not returned"
run_helper wrapper "$wrapper" get
expect_passthrough "stored credentials"

# Invalid stored credentials must remain an auth error, not trigger fallback.
printf '%s\n' '{' > "$test_dir/helper_store.json"
run_helper stock "$stock_helper" get
[[ $(cat "$test_dir/stock.status") != 0 ]] || fail "malformed credential store: stock helper should fail"
run_helper wrapper "$wrapper" get
expect_passthrough "malformed credential store"

# Commands other than get are delegated to the real helper unchanged.
run_helper stock "$stock_helper" version
[[ $(cat "$test_dir/stock.status") == 0 ]] || fail "version command: stock helper should succeed"
run_helper wrapper "$wrapper" version
expect_passthrough "version command"

echo 'PASS'
