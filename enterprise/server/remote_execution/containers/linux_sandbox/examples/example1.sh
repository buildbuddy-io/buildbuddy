#!/usr/bin/env bash
set -eu
cd "$(mktemp -d)" && DIR="$PWD" && trap 'rm -rf "$DIR"' EXIT && cd "$DIR"

# Example 1: "Bare" execution

echo "Setting up workspace"
mkdir -p "$DIR/execroot/workspace"
touch "$DIR/execroot/workspace/input.txt"

echo "Running action..."
cd "$DIR/execroot/workspace"
time \
  sh -c '
    cp input.txt output.txt
    echo "Wrote output.txt"
  '
