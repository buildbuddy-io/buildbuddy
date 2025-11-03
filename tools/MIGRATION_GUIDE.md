# Migration Guide: dev_qa.py → Bazel Integration Tests

This guide shows how to migrate from using `tools/dev_qa.py` to the new Bazel integration test targets.

## Quick Command Reference

| Old Command (dev_qa.py) | New Command (Bazel) |
|------------------------|---------------------|
| `tools/dev_qa.py` | `bazel test //tools:dev_qa_all --test_env=BB_API_KEY` |
| `tools/dev_qa.py --repos=buildbuddy` | `bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY` |
| `tools/dev_qa.py --repos=bazel-gazelle` | `bazel test //tools:dev_qa_bazel_gazelle --test_env=BB_API_KEY` |
| `tools/dev_qa.py --repos=abseil-cpp` | `bazel test //tools:dev_qa_abseil_cpp --test_env=BB_API_KEY` |
| `tools/dev_qa.py --repos=rules_python` | `bazel test //tools:dev_qa_rules_python --test_env=BB_API_KEY` |
| `tools/dev_qa.py --repos=all` | `bazel test //tools:dev_qa_all --test_env=BB_API_KEY` |
| `tools/dev_qa.py --repos=buildbuddy,bazel-gazelle` | `bazel test //tools:dev_qa_buildbuddy //tools:dev_qa_bazel_gazelle --test_env=BB_API_KEY` |

## Migration Steps

### 1. Add dependencies to your workspace

Choose one based on your setup:

#### If using MODULE.bazel (recommended):

Add to your `MODULE.bazel`:

```python
bazel_dep(name = "rules_bazel_integration_test", version = "0.34.0")

bazel_binaries = use_extension(
    "@rules_bazel_integration_test//:extensions.bzl",
    "bazel_binaries",
)
bazel_binaries.download(version = "7.4.0")
use_repo(bazel_binaries, "bazel_binaries")
```

See `tools/MODULE.bazel.example` for a complete example.

#### If using WORKSPACE:

Add to your `WORKSPACE`:

```python
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_bazel_integration_test",
    sha256 = "80cd42b7e8e4ec8e156a4e3c39950646e998afa61a2e0887c72423d635a50ef5",
    urls = [
        "https://github.com/bazel-contrib/rules_bazel_integration_test/releases/download/v0.34.0/rules_bazel_integration_test.v0.34.0.tar.gz",
    ],
)

load("@rules_bazel_integration_test//bazel_integration_test:deps.bzl", "bazel_integration_test_rules_dependencies")
bazel_integration_test_rules_dependencies()

# ... additional setup (see tools/WORKSPACE.example for complete config)

load("@rules_bazel_integration_test//bazel_integration_test:defs.bzl", "bazel_binaries")
bazel_binaries(versions = ["7.4.0"])
```

See `tools/WORKSPACE.example` for a complete example.

### 2. Enable the test targets

In `tools/BUILD`, uncomment these lines:

```python
load("@bazel_binaries//:defs.bzl", "bazel_binaries")
dev_qa_tests(bazel_binaries = bazel_binaries)
```

### 3. Verify it works

```bash
# Set your API key
export BB_API_KEY=your-api-key

# Run a single test to verify
bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY
```

### 4. Update CI/CD scripts

If you have CI scripts that call `tools/dev_qa.py`, update them:

**Old CI script:**
```bash
#!/bin/bash
export BB_API_KEY="${SECRET_API_KEY}"
python tools/dev_qa.py --repos=all
```

**New CI script:**
```bash
#!/bin/bash
export BB_API_KEY="${SECRET_API_KEY}"
bazel test //tools:dev_qa_all --test_env=BB_API_KEY
```

Or add to `.bazelrc`:
```
test:ci --test_env=BB_API_KEY
```

Then in CI:
```bash
bazel test //tools:dev_qa_all --config=ci
```

### 5. (Optional) Delete the old Python script

Once you've verified the new tests work:

```bash
git rm tools/dev_qa.py
```

## Key Differences

### Environment Variables

Both systems use the same environment variables:
- `BB_API_KEY` (required): BuildBuddy API key
- `QA_ROOT` (optional): Clone directory (default: `~/buildbuddy-qa`)

**Old:** Environment variables are read directly by Python
**New:** Pass via `--test_env=VAR_NAME` or set in `.bazelrc`

### Repository Cloning

**Both systems:**
- Clone repos to `~/buildbuddy-qa` (or `$QA_ROOT`)
- Reuse existing clones on subsequent runs
- Check out specific commit SHAs

No changes needed to your cloned repositories.

### Configuration

**Old:** Configuration in Python dictionary at top of `dev_qa.py`
**New:** Configuration in Starlark dictionary in `tools/dev_qa.bzl`

To update commit SHAs or Bazel commands, edit `REPO_CONFIGS` in `tools/dev_qa.bzl`.

### Output

**Old:**
```
Results:
- buildbuddy: SUCCESS (https://app.buildbuddy.dev/invocation/xxx)
- bazel-gazelle: SUCCESS (https://app.buildbuddy.dev/invocation/yyy)
```

**New:**
Bazel test output with individual test results:
```
//tools:dev_qa_buildbuddy                                          PASSED
//tools:dev_qa_bazel_gazelle                                       PASSED
```

Each test outputs its invocation URL during execution.

## Benefits of the New System

### 1. Better Caching
Bazel caches successful test runs. If nothing changed, tests won't re-run.

### 2. Parallel Execution
Run tests in parallel automatically:
```bash
bazel test //tools:dev_qa_all --test_env=BB_API_KEY --jobs=4
```

### 3. Selective Testing
Easily test just what changed:
```bash
# Only test BuildBuddy repo
bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY
```

### 4. Integration with Bazel Query
Find all dev QA tests:
```bash
bazel query 'kind(script_test, //tools:dev_qa_*)'
```

### 5. Better CI Integration
Standard Bazel test targets work with all Bazel-aware CI systems (BuildBuddy, BuildKite, etc.)

## Troubleshooting

### "Error: Cannot find or execute dev_qa_runner.sh"

Make sure `tools/BUILD` has:
```python
exports_files(["dev_qa_runner.sh"])
```

### "Unknown repository: buildbuddy"

The repository name must match a key in `REPO_CONFIGS` in `tools/dev_qa.bzl`.

### Tests hang or timeout

Increase test timeout:
```bash
bazel test //tools:dev_qa_buildbuddy --test_timeout=3600  # 1 hour
```

Or set in `.bazelrc`:
```
test:dev-qa --test_timeout=3600
```

## Keeping Both Systems During Transition

You can run both systems side-by-side during migration:

```bash
# Old system
python tools/dev_qa.py --repos=buildbuddy

# New system
bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY

# Compare results
```

Once you're confident the new system works, remove `tools/dev_qa.py`.

## Getting Help

- [rules_bazel_integration_test docs](https://github.com/bazel-contrib/rules_bazel_integration_test)
- See `tools/DEV_QA_README.md` for detailed usage
- Check example configs: `tools/MODULE.bazel.example` or `tools/WORKSPACE.example`
