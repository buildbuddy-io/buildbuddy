#!/usr/bin/env bash
# Smoke-checks an RBE image (local or remote) by running commands in it with
# `docker run --rm`.
#
# Usage:
#   rbe_image_check.sh [--family=20.04|22.04|24.04] IMAGE
#       Checks that the tools the family's Dockerfile installs are present,
#       that https works, and that `git ls-remote` against GitHub works.
#       Exits nonzero if anything fails.
#   rbe_image_check.sh --packages IMAGE
#       Prints "<package> <version>" for every installed dpkg package, sorted,
#       to stdout.
#
# If --family is not given, it is read from /etc/os-release in the image.
set -euo pipefail

family=""
packages=0
image=""
for arg in "$@"; do
  case "$arg" in
    --family=*) family="${arg#--family=}" ;;
    --packages) packages=1 ;;
    -*) echo "unknown flag: $arg" >&2; exit 2 ;;
    *) image="$arg" ;;
  esac
done
if [[ -z "$image" ]]; then
  echo "usage: $0 [--family=20.04|22.04|24.04] [--packages] IMAGE" >&2
  exit 2
fi

run() { docker run --rm "$image" bash -c "$1"; }

if ((packages)); then
  run "dpkg-query -W -f='\${Package} \${Version}\n'" | LC_ALL=C sort
  exit 0
fi

if [[ -z "$family" ]]; then
  family=$(run '. /etc/os-release && echo "$VERSION_ID"')
fi

# Tools each family's Dockerfile (dockerfiles/rbe-ubuntu*-04) installs.
common="gcc g++ make python3 git curl wget unzip zip ssh docker"
case "$family" in
  20.04) tools="$common python2 python java javac clang go" ;;
  22.04) tools="$common python sudo" ;;
  24.04) tools="$common python sudo cmake" ;;
  *) echo "unsupported family: $family" >&2; exit 2 ;;
esac

echo "Checking $image (Ubuntu $family)"
run "
  set -u
  fail=0
  for t in $tools; do
    if command -v \$t >/dev/null; then echo \"ok      \$t\"; else echo \"MISSING \$t\"; fail=1; fi
  done
  if curl -fsS -o /dev/null https://www.google.com; then echo 'ok      https'; else echo 'FAILED  https'; fail=1; fi
  if git ls-remote https://github.com/bazelbuild/bazel HEAD >/dev/null; then echo 'ok      git ls-remote'; else echo 'FAILED  git ls-remote'; fail=1; fi
  exit \$fail
"
