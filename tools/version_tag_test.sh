#!/usr/bin/env bash
# Tests for latest_version_tag.sh and version_tag_at_head.sh, run against
# temporary git repositories.
set -euo pipefail

LATEST="$PWD/$LATEST_VERSION_TAG"
AT_HEAD="$PWD/$VERSION_TAG_AT_HEAD"

TMP=$(mktemp -d "${TEST_TMPDIR:-/tmp}/version_tag_test.XXXXXX")
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
# expect_output DESCRIPTION EXPECTED COMMAND...
expect_output() {
  local desc=$1 want=$2 got
  shift 2
  if ! got=$("$@" 2>/dev/null); then
    fail "$desc: command failed, want '$want'"
  elif [[ "$got" != "$want" ]]; then
    fail "$desc: got '$got', want '$want'"
  fi
}
# expect_failure DESCRIPTION COMMAND...
expect_failure() {
  local desc=$1 got
  shift
  if got=$("$@" 2>/dev/null); then
    fail "$desc: succeeded with '$got', want failure"
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

expect_failure "no commits or tags" "$LATEST"
commit a
expect_failure "no tags" "$LATEST"
expect_failure "no tags at HEAD" "$AT_HEAD"

tag v2.9.0
commit b
tag v2.10.0
tag not-a-version
tag v2.11.0-rc1
tag cli-v5.0.0
expect_output "versions sort numerically, non-vX.Y.Z tags ignored" v2.10.0 "$LATEST"
expect_output "tag at HEAD" v2.10.0 "$AT_HEAD"

# Cut a release branch and tag it, then keep committing and tagging master.
commit c
git checkout -q -b bb_release_1
tag v2.11.0
git checkout -q master
commit d
tag v2.12.0
expect_output "master sees its own newest tag" v2.12.0 "$LATEST"

git checkout -q bb_release_1
expect_output "release branch ignores newer master tags" v2.11.0 "$LATEST"
expect_output "release branch tag at HEAD" v2.11.0 "$AT_HEAD"

# Untagged cherry-pick: the stamped version would be stale, so the guard fails.
commit cherry-pick
expect_output "untagged cherry-pick still resolves the branch tag" v2.11.0 "$LATEST"
expect_failure "untagged cherry-pick fails the guard" "$AT_HEAD"
tag v2.11.1
expect_output "patch tag" v2.11.1 "$LATEST"
expect_output "patch tag at HEAD" v2.11.1 "$AT_HEAD"

# A commit with several version tags resolves to the highest one.
tag v2.11.2
expect_output "highest of several tags at HEAD" v2.11.2 "$AT_HEAD"

# HEAD tagged with a lower version than a tag in its history: the stamped
# version wouldn't match the tag at HEAD.
commit e
tag v2.11.0-hotfix
tag v1.0.0
expect_failure "tag at HEAD lower than stamped version" "$AT_HEAD"

# Shallow clones may be missing tagged commits, so refuse to guess.
git clone -q --depth 1 --branch bb_release_1 "file://$TMP/repo" "$TMP/shallow"
cd "$TMP/shallow"
expect_failure "shallow clone" "$LATEST"
expect_failure "shallow clone guard" "$AT_HEAD"

if ((failures > 0)); then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "PASS"
