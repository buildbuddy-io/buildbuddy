#!/usr/bin/env bash
set -eu
cd "$(mktemp -d)" && DIR="$PWD" && trap 'rm -rf "$DIR"' EXIT && cd "$DIR"

# Example 2: Basic sandboxed execution

echo "Setting up workspace"
mkdir "$DIR"/workspace
touch "$DIR"/workspace/input.txt

# Note: these will fail unless run as uid 0
echo "Setting up overlay mounts"
mkdir -p "$DIR.overlay"/overlay/{usr,bin,lib,lib64}/{work,upper,mount}
MOUNT_ARGS=()
for d in usr bin lib lib64 ; do
  echo "Creating overlay for /$d"
  MOUNT_ARGS+=(
    -M "$DIR.overlay/overlay/$d/mount"
    -m "/$d"
  )
  mount \
    -t overlay\
    -o "lowerdir=/$d,workdir=$DIR.overlay/overlay/$d/work,upperdir=$DIR.overlay/overlay/$d/upper" \
    overlay "$DIR.overlay/overlay/$d/mount"
done

echo "Running action on overlayfs with linux-sandbox..."
time \
  linux-sandbox \
  -h "$DIR" \
  -W "$DIR/workspace" \
  "${MOUNT_ARGS[@]}" \
  -R \
  -D /dev/stderr \
  -- \
  sh -c '
    cp input.txt output.txt
    echo "Wrote output.txt"
  '
