#!/usr/bin/env bash
set -eu
cd "$(mktemp -d)" && DIR="$PWD" && trap 'rm -rf "$DIR"' EXIT && cd "$DIR"

# Example 2: Basic sandboxed execution

echo "Setting up workspace"
mkdir -p "$DIR/execroot/workspace"
touch "$DIR/execroot/workspace/input.txt"

echo "Running action with linux-sandbox..."
time \
  linux-sandbox \
  -h "$DIR/execroot" \
  -W "$DIR/execroot/workspace" \
  -M /usr -M /bin -M /lib -M /lib64 \
  -R \
  -D /dev/stderr \
  -- \
  sh -c '
    cp input.txt output.txt
    echo "Wrote output.txt"
  '
