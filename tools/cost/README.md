# tools/cost

Measures the cost of building specific artifacts cold (no remote cache, no
remote executor, no remote downloader, empty `--repository_cache`, fresh
`--output_base`).

For each target the report shows:

- number of external deps fetched
- total bytes downloaded for external deps
- cold build wall time
- cold build size on disk

## Usage

```sh
tools/cost/cold_build.sh \
  //enterprise/server/cmd/goinit \
  //enterprise/server/cmd/ci_runner \
  //cli/cmd/bb \
  //tools/lint
```

Add `--json` to emit machine-readable output, `--keep` to preserve the
ephemeral output base / repository cache for inspection.

Each target gets its own isolated `--output_base` and `--repository_cache`
under a tempdir, so runs do not share work. Only `shared.bazelrc` is loaded;
`user.bazelrc` is intentionally ignored so personal remote-cache settings
don't pollute the measurement.
