#!/usr/bin/env bash
# Tests for branch_highest_version.sh, run against temporary git repositories.

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

SCRIPT=$(rlocation "$BRANCH_HIGHEST_VERSION")

# Bazel sets TEST_TMPDIR to a per-test directory inside the sandbox.
TMP="${TEST_TMPDIR:?run this with bazel test}"
# Keep this compatible with older git (the Linux RBE image has git 2.25), and
# isolated from the host's git config.
export HOME="$TMP" XDG_CONFIG_HOME="$TMP/xdg" GIT_CONFIG_NOSYSTEM=1
export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@example.com
export GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@example.com

failures=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}
# expect_output DESCRIPTION EXPECTED
expect_output() {
  local desc=$1 want=$2 got
  if ! got=$("$SCRIPT" 2>/dev/null); then
    fail "$desc: command failed, want '$want'"
  elif [[ "$got" != "$want" ]]; then
    fail "$desc: got '$got', want '$want'"
  fi
}
# expect_failure DESCRIPTION
expect_failure() {
  local desc=$1 got
  if got=$("$SCRIPT" 2>/dev/null); then
    fail "$desc: succeeded with '$got', want failure"
  elif [[ -n "$got" ]]; then
    fail "$desc: printed '$got' on failure, want no output"
  fi
}
# Give every commit and tag a later timestamp than the last, so creation-date
# order is well defined (and differs from version order where it matters).
now=1700000000
tick() {
  now=$((now + 60))
  export GIT_AUTHOR_DATE="@$now +0000" GIT_COMMITTER_DATE="@$now +0000"
}
commit() {
  tick
  git commit -q --allow-empty -m "$1"
}
tag() {
  tick
  git tag -a "$1" -m "$1"
}

git init -q "$TMP/repo"
cd "$TMP/repo"
git symbolic-ref HEAD refs/heads/master

expect_failure "no commits"
commit a
expect_failure "no tags"

tag v2.9.0
commit b
tag v2.10.0
tag not-a-version
tag v2.11.0-rc1
tag cli-v5.0.0
expect_output "versions sort numerically, non-vX.Y.Z tags ignored" v2.10.0

# Cut a release branch and tag it, then keep committing and tagging master.
commit c
git checkout -q -b bb_release_1
commit cherry-pick-1
tag v2.11.0
git checkout -q master
commit d
tag v2.12.0
expect_output "master sees its own newest tag" v2.12.0

# A lower version tagged later doesn't win: versions sort by number, not by
# creation date.
commit e
tag v2.1.99
expect_output "lower version tagged later is ignored" v2.12.0

# Cut a newer release branch with a higher version than anything on master.
git checkout -q -b bb_release_2
commit cherry-pick-3
tag v2.13.0
git checkout -q master
expect_output "master ignores higher tags on other branches" v2.12.0

git checkout -q bb_release_1
expect_output "release branch ignores newer master tags" v2.11.0
commit cherry-pick-2
expect_output "untagged commit resolves the branch's tag" v2.11.0
tag v2.11.1
expect_output "patch tag" v2.11.1

# Shallow clones may be missing tagged commits, so refuse to guess.
git clone -q --depth 1 --branch bb_release_1 "file://$TMP/repo" "$TMP/shallow"
cd "$TMP/shallow"
git fetch -q --tags
expect_failure "shallow clone"

if ((failures > 0)); then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "PASS"
