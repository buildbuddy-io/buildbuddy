#!/usr/bin/env bash
set -e

# Get a hash of the plugin contents, excluding the build artifact and the saved hash.
sha256=$(find . -type f | grep -v -E 'plugin_deps.sum|go-highlight-bb-plugin' | xargs shasum -a 256)

# If the plugin binary already exists and the hash of its deps hasn't changed, do nothing.
existing_sha256=$(cat plugin_deps.sum 2>/dev/null || true)
if [[ -e ./go-highlight-bb-plugin ]] && [[ "$existing_sha256" == "$sha256" ]]; then
  exit 0
fi

echo -e "\x1b[90mBuilding go deps plugin...\x1b[m"

go get
go build -o . ./...

echo "$sha256" >plugin_deps.sum
