#!/usr/bin/env bash
# Tests for branch_highest_version.sh, run against temporary git repositories.
set -euo pipefail

SCRIPT="$PWD/$BRANCH_HIGHEST_VERSION"

TMP=$(mktemp -d "${TEST_TMPDIR:-/tmp}/branch_highest_version_test.XXXXXX")
# Keep this compatible with older git (the Linux RBE image has git 2.25), and
# isolated from the host's git config.
export HOME="$TMP" GIT_CONFIG_NOSYSTEM=1
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
commit() {
  git commit -q --allow-empty -m "$1"
}
tag() {
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

git checkout -q bb_release_1
expect_output "release branch ignores newer master tags" v2.11.0
commit cherry-pick-2
expect_output "untagged commit resolves the branch's tag" v2.11.0
tag v2.11.1
expect_output "patch tag" v2.11.1

# v2.11.1 is the newest tag by creation date, but it isn't in master's history.
git checkout -q master
expect_output "master ignores release branch tags" v2.12.0

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
