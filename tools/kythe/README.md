# Local Kythe workflow runner

`//tools/kythe:local_kythe` runs a local version of the
`Generate Kythe Annotations` workflow action.

## Run

```bash
# From repo root:
bazel run //tools/kythe:local_kythe -- .

# Faster loop: skip C++ indexing
bazel run //tools/kythe:local_kythe -- --skip_cxx .

# Use proto-only scope for a fast sanity check
bazel run //tools/kythe:local_kythe -- --scope proto .

# Use full workspace scope (slower)
bazel run //tools/kythe:local_kythe -- --scope full .

# Custom output path
bazel run //tools/kythe:local_kythe -- --out_path /tmp/kythe_serving.sst .

# Dry run (show what would happen)
bazel run //tools/kythe:local_kythe -- --dry_run .

# Force nested bazel version via bazelisk semantics
bazel run //tools/kythe:local_kythe -- --bazel_version 9.0.0 .
```

## Flags

- `--repo_root <path>`: repo root to index (default current directory)
- `--out_path <path>`: output file (default `<repo_root>/kythe_serving.sst`)
- `--skip_cxx`: skip C++ indexer + memcached setup
- `--bazel_version <v>`: sets `USE_BAZEL_VERSION=<v>` for nested Bazel commands
- `--distdir <path>`: sets `--distdir=<path>` for nested Bazel commands
- `--scope <mode>`: build scope (`proto`, `default`, or `full`, default `default`)
  - `proto` targets: `//proto/...`
  - `default` targets: `//app/... //server/... //enterprise/server/... //proto/...`
  - default exclusions: `-//server/util/bazel/... -//tools/probers/... -//server/testutil/...`
- `--dry_run`: print planned commands and exit
- `-h, --help`: usage

## Notes

- This script intentionally runs nested `bazel build`, same as workflow behavior.
- For Bazel <8 with bzlmod enabled, it may append Kythe entries to `MODULE.bazel`.
- It downloads a Kythe prebuilt archive into `<repo_root>/kythe-v*`.
- You can also set `USE_BAZEL_VERSION` env var instead of `--bazel_version`.
- On Bazel 9+, the script adds Java rule autoload flags required by current Kythe BUILD.
- You can also set `BAZEL_DISTDIR` env var instead of `--distdir`.
