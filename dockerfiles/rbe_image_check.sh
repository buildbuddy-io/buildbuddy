#!/usr/bin/env bash
# Smoke-checks the RBE image it runs in. Run it inside the image, e.g. as an
# action on BuildBuddy RBE or with
# `docker run --rm -v "$PWD/dockerfiles:/c:ro" IMAGE /c/rbe_image_check.sh`.
#
# Usage:
#   rbe_image_check.sh
#       Checks that the tools the image's Dockerfile installs are present (and
#       that the ones it downloads outside apt run), the image's locale, user
#       and iptables setup, that https works, and that `git ls-remote` against
#       GitHub works. The Ubuntu release is read from /etc/os-release.
#       Exits nonzero if anything fails.
#   rbe_image_check.sh --packages
#       Prints "<package> <version>" for every installed dpkg package (status
#       "ii"; removed packages with leftover config files are skipped), sorted,
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
  pkgs=$(dpkg-query -W -f='${db:Status-Abbrev} ${Package} ${Version}\n' |
    awk '$1 == "ii" {print $2, $3}' | LC_ALL=C sort)
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
# Binaries the Dockerfile downloads outside apt can be built for the wrong
# arch, so those are also run once.
runs=()
setup=0
case "$family" in
  20.04)
    tools="$common python2 python java javac clang go"
    runs=("go version" "clang --version" "java -version" "javac -version")
    # The Dockerfile adds this with ENV PATH, which RBE actions don't inherit.
    PATH="$PATH:/usr/local/go/bin"
    ;;
  22.04) tools="$common python sudo"; setup=1 ;;
  24.04) tools="$common python sudo cmake"; setup=1 ;;
  *) echo "unsupported family: $family" >&2; exit 2 ;;
esac

echo "Checking Ubuntu $family ($(uname -m))"
fail=0
check() {
  local name=$1
  shift
  if "$@" >/dev/null 2>&1; then echo "ok      $name"; else echo "FAILED  $name"; fail=1; fi
}
for t in $tools; do
  if command -v "$t" >/dev/null; then echo "ok      $t"; else echo "MISSING $t"; fail=1; fi
done
for r in "${runs[@]}"; do
  check "$r" $r
done
if ((setup)); then
  check 'en_US.UTF-8 locale' bash -c "locale -a | grep -qix 'en_US.utf8'"
  check 'buildbuddy user' id buildbuddy
  check 'iptables-legacy' bash -c "update-alternatives --query iptables | grep -qx 'Value: /usr/sbin/iptables-legacy'"
fi
check https curl -fsS --connect-timeout 10 --max-time 30 -o /dev/null https://www.google.com
check 'git ls-remote' timeout 60 git ls-remote https://github.com/bazelbuild/bazel HEAD
exit "$fail"
