#!/usr/bin/env bash
set -e

# Try 2 different ways of running the sandbox wrapper.
# - root
# - pseudo-root

PKG=enterprise/server/remote_execution/containers/linux_sandbox/sandbox_wrapper
bazel build -c opt "$PKG"

rm -rf /tmp/sandbox-wrapper.bin
cp ./bazel-bin/$PKG/sandbox_wrapper_/sandbox_wrapper /tmp/sandbox-wrapper.bin

export SANDBOX=$(which linux-sandbox)

sudo echo # get root pw

# shellcheck disable=SC2016

# unshare -mr ab 'sudo,' sh -ec '
# PROG="$1"

trap "
echo >&2 cleaning up...
mount | grep sandbox-wrapper.test | perl -pe 's@.*on (.*?) .*@\1@' | tee /dev/stderr | sudo xargs --no-run-if-empty umount
" EXIT

PROG="$(which realtime) strace --follow-forks --absolute-timestamps=us --summary-only --output=/tmp/strace.out unshare -mr"

ROOT=/tmp/sandbox-wrapper.test/$(uuidgen)
mkdir -p "$ROOT"/execroot/workspace "$ROOT"/tmp
ln -s usr/bin "$ROOT"/execroot/bin
ln -s usr/lib "$ROOT"/execroot/lib
ln -s usr/lib64 "$ROOT"/execroot/lib64
${PROG:-} \
  /tmp/sandbox-wrapper.bin \
  -execroot="$ROOT"/execroot \
  -overlay.tmp="$ROOT/tmp" \
  -overlay.src_root=/ \
  -overlay.src=/bin \
  -overlay.src=/usr \
  -overlay.src=/lib \
  -overlay.src=/lib64 \
  -- \
  $SANDBOX \
  -R -N -H \
  -h "$ROOT"/execroot \
  -W "$ROOT"/execroot/workspace \
  -w /tmp -w /dev/shm \
  -M "$ROOT"/tmp/overlay/mnt/usr -m /usr \
  -- \
  /bin/true
# ' . {}
