#!/usr/bin/env bash
# Smoke-checks the RBE image it runs in. Run it inside the image, e.g. as an
# action on BuildBuddy RBE or with
# `docker run --rm -v "$PWD/dockerfiles:/c:ro" IMAGE /c/rbe_image_check.sh`.
#
# Usage:
#   rbe_image_check.sh
#       Checks that the tools the image's Dockerfile installs are present, that
#       https works, and that `git ls-remote` against GitHub works. The Ubuntu
#       release is read from /etc/os-release.
#       Exits nonzero if anything fails.
#   rbe_image_check.sh --packages
#       Prints "<package> <version>" for every installed dpkg package, sorted,
#       to stdout. Fails if the list is empty.
set -euo pipefail

packages=0
for arg in "$@"; do
  case "$arg" in
    --packages) packages=1 ;;
    *) echo "usage: $0 [--packages]" >&2; exit 2 ;;
  esac
done

if ((packages)); then
  pkgs=$(dpkg-query -W -f='${Package} ${Version}\n' | LC_ALL=C sort)
  if [[ -z "$pkgs" ]]; then
    echo "dpkg-query listed no packages" >&2
    exit 1
  fi
  printf '%s\n' "$pkgs"
  exit 0
fi

family=$(. /etc/os-release && echo "${VERSION_ID:-}")

# Tools each family's Dockerfile (dockerfiles/rbe-ubuntu*-04) installs.
common="gcc g++ make python3 git curl wget ed file less nc unzip zip ssh docker"
case "$family" in
  20.04)
    tools="$common python2 python java javac clang go"
    # The Dockerfile adds this with ENV PATH, which RBE actions don't inherit.
    PATH="$PATH:/usr/local/go/bin"
    ;;
  22.04) tools="$common python sudo" ;;
  24.04) tools="$common python sudo cmake" ;;
  *) echo "unsupported family: $family" >&2; exit 2 ;;
esac

echo "Checking Ubuntu $family ($(uname -m))"
fail=0
for t in $tools; do
  if command -v "$t" >/dev/null; then echo "ok      $t"; else echo "MISSING $t"; fail=1; fi
done
if curl -fsS -o /dev/null https://www.google.com; then echo 'ok      https'; else echo 'FAILED  https'; fail=1; fi
if git ls-remote https://github.com/bazelbuild/bazel HEAD >/dev/null; then echo 'ok      git ls-remote'; else echo 'FAILED  git ls-remote'; fail=1; fi
exit "$fail"
