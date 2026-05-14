#!/usr/bin/env bash
set -eu

# These expected hashes were computed with:
# printf GENERATED_FILE_CONTENTS | shasum
# printf CHILD_CONTENTS | shasum
EXPECTED_HASHES=$(cat <<EOF
7ae83756fa71a6793c8429fa941697497cb97eff  bazel-out/CONFIG/bin/rules/sha/testdata/generated_file.txt
797f24c0a77095eb03f5a1f183245eb371d6db00  rules/sha/testdata/dir.ln/child.txt
EOF
)
# This expected hash was computed by copying the "EXPECTED_HASHES" declaration
# above, then running:
# echo "$EXPECTED_HASHES" | shasum
EXPECTED_HASH='1c385057a72428ea0499c51856a90e638c9dd7df'
ACTUAL_HASH=$(cat "$TESTDATA_SUM")
if [ "$ACTUAL_HASH" != "$EXPECTED_HASH" ]; then
  echo >&2 "Unexpected .sum contents: '$ACTUAL_HASH' does not match expected hash '$EXPECTED_HASH'"
  echo >&2 "Re-run with --action_env=BB_RULES_SHA_DEBUG=1 to see the intermediate file hashes."
  exit 1
fi
