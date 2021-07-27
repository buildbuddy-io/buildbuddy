#!/bin/bash
set -euo pipefail

get_bundle_info() {
  bazel build -c opt //enterprise/app:app_bundle >&2
  local bundle_path="bazel-bin/enterprise/app/app_bundle/app.js"
  local size_bytes
  local compressed_size_bytes
  size_bytes=$(cat "$bundle_path" | wc -c)
  compressed_size_bytes=$(cat "$bundle_path" | gzip | wc -c)

  echo "{\"size_bytes\": $size_bytes, \"compressed_size_bytes\": $compressed_size_bytes}"
}

# Compute size AFTER (current dir)

cd "$(dirname "$0")/.." # repo root
after_info=$(get_bundle_info)

# Compute size BEFORE

: "${BEFORE_DIR:=}"
if [[ -n "$BEFORE_DIR" ]]; then
  cd "$BEFORE_DIR"
else
  cd "$(mktemp -d)"
  pwd
  tmpdir="$PWD"
  cleanup() { rm -rf "$tmpdir"; }
  trap cleanup EXIT
  # Fresh clone repo to avoid messing with current working tree
  git clone https://github.com/buildbuddy-io/buildbuddy --depth=1 .
fi
before_info=$(get_bundle_info)

python3 -c "
before = $before_info
after = $after_info
diff = {
  'size_bytes': after['size_bytes'] - before['size_bytes'],
  'compressed_size_bytes': after['compressed_size_bytes'] - before['compressed_size_bytes'],
}

def table(header, info):
  print(header)
  print('size (KB)\t%d' % (info['size_bytes'] / 1000))
  print('gzipped (KB)\t%d' % (info['compressed_size_bytes'] / 1000))
  print()

table('BEFORE', before)
table('AFTER', after)
table('DIFF', diff)
"
