# Dev QA Tests - Bazel Integration Test Version

This directory contains Bazel integration tests for running QA builds against BuildBuddy using external repositories.

## Overview

The dev QA tests have been migrated from a Python script (`dev_qa.py`) to use `rules_bazel_integration_test`. This provides several advantages:

- **Bazel-native**: Tests run as standard Bazel test targets
- **Better caching**: Bazel can cache successful test runs
- **Parallel execution**: Bazel handles running multiple tests in parallel
- **CI integration**: Works with standard Bazel CI workflows
- **Easier maintenance**: Configuration is centralized in `dev_qa.bzl`

## Setup

### 1. Add rules_bazel_integration_test to your workspace

#### For MODULE.bazel (bzlmod):

```python
# In your MODULE.bazel
bazel_dep(name = "rules_bazel_integration_test", version = "0.34.0")

# Configure Bazel binaries for testing
bazel_binaries = use_extension(
    "@rules_bazel_integration_test//:extensions.bzl",
    "bazel_binaries",
)
bazel_binaries.download(version = "7.4.0")  # Or your preferred version
use_repo(bazel_binaries, "bazel_binaries")
```

#### For WORKSPACE (legacy):

```python
# In your WORKSPACE
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

load("@rules_bazel_integration_test//bazel_integration_test:defs.bzl", "bazel_binaries")
bazel_binaries(versions = ["7.4.0"])
```

### 2. Enable the tests in tools/BUILD

Uncomment the last two lines in `tools/BUILD`:

```python
load("@bazel_binaries//:defs.bzl", "bazel_binaries")
dev_qa_tests(bazel_binaries = bazel_binaries)
```

### 3. Set your BuildBuddy API key

The tests require a BuildBuddy API key to be set in the `BB_API_KEY` environment variable.

```bash
export BB_API_KEY=your-api-key-here
```

## Running Tests

### Run all dev QA tests

```bash
bazel test //tools:dev_qa_all --test_env=BB_API_KEY
```

### Run a specific repository test

```bash
# Test BuildBuddy
bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY

# Test bazel-gazelle
bazel test //tools:dev_qa_bazel_gazelle --test_env=BB_API_KEY

# Test abseil-cpp
bazel test //tools:dev_qa_abseil_cpp --test_env=BB_API_KEY

# Test rules_python
bazel test //tools:dev_qa_rules_python --test_env=BB_API_KEY
```

### Passing the API key via .bazelrc

To avoid typing `--test_env=BB_API_KEY` every time, add this to your `.bazelrc`:

```bash
# In .bazelrc
test:dev-qa --test_env=BB_API_KEY
```

Then run tests with:

```bash
bazel test //tools:dev_qa_all --config=dev-qa
```

## Configuration

### Repository Configurations

Repository configurations are defined in `tools/dev_qa.bzl` in the `REPO_CONFIGS` dictionary. Each entry includes:

- `repo_url`: Git URL of the repository
- `commit_sha`: Specific commit to test against
- `bazel_cmd`: Bazel command to run (can be multi-line for readability)

Example:

```python
REPO_CONFIGS = {
    "buildbuddy": {
        "repo_url": "https://github.com/buildbuddy-io/buildbuddy",
        "commit_sha": "a97d4303c9485db089a33a1049fe480d0122687d",
        "bazel_cmd": """test //...
            --config=remote-dev
            --remote_executor=remote.buildbuddy.dev
            --test_tag_filters=-docker,-bare,-performance""",
    },
    # ... more repos
}
```

### Adding a New Repository

To add a new repository to test:

1. Add an entry to `REPO_CONFIGS` in `tools/dev_qa.bzl`
2. The test target will be automatically generated as `dev_qa_<repo_name>`
3. Update the comments in `tools/BUILD` to document the new test target

### Testing a Subset of Repositories

You can test only specific repositories by passing a `repos` argument to `dev_qa_tests()`:

```python
# In tools/BUILD
dev_qa_tests(
    bazel_binaries = bazel_binaries,
    repos = ["buildbuddy", "bazel-gazelle"],  # Only test these two
)
```

## Environment Variables

- `BB_API_KEY` (required): BuildBuddy API key for remote execution
- `QA_ROOT` (optional): Directory where external repos are cloned (default: `~/buildbuddy-qa`)

## How It Works

1. **Test Generation**: The `dev_qa_tests()` macro in `dev_qa.bzl` generates a `script_test` target for each repository in `REPO_CONFIGS`

2. **Script Runner**: Each test uses `dev_qa_runner.sh` which:
   - Clones the external repository (or updates if already cloned)
   - Checks out the specified commit
   - Injects the BuildBuddy RBE toolchain into the WORKSPACE
   - Pins the Bazel version (for non-BuildBuddy repos)
   - Runs the specified Bazel command with remote execution
   - Reports results with an invocation URL

3. **Integration Test Framework**: The `rules_bazel_integration_test` framework provides:
   - The Bazel binary via `BIT_BAZEL_BINARY` environment variable
   - Test isolation and cleanup
   - Integration with Bazel's test infrastructure

## Comparison with Original dev_qa.py

| Feature | dev_qa.py | Bazel Integration Tests |
|---------|-----------|------------------------|
| Run all tests | `tools/dev_qa.py` | `bazel test //tools:dev_qa_all` |
| Run specific test | `tools/dev_qa.py --repos=buildbuddy` | `bazel test //tools:dev_qa_buildbuddy` |
| Parallel execution | Manual | Automatic via Bazel |
| Caching | None | Bazel test caching |
| CI integration | Custom | Standard Bazel CI |
| Configuration | Python constants | Starlark dict |

## Troubleshooting

### "BB_API_KEY environment variable must be set"

Set your BuildBuddy API key:
```bash
export BB_API_KEY=your-api-key-here
```

### "BIT_BAZEL_BINARY not set"

This error means the script was run outside of the integration test framework. Always run via `bazel test`, not directly.

### Tests are slow

The first run will clone all repositories, which can take time. Subsequent runs will reuse the cloned repos in `~/buildbuddy-qa` (or `$QA_ROOT`).

### Need to re-test with fresh clone

Delete the repository directory:
```bash
rm -rf ~/buildbuddy-qa/buildbuddy  # or specific repo name
```

Or set a different QA_ROOT:
```bash
bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY --test_env=QA_ROOT=/tmp/fresh-qa
```

## Migration Notes

If you're migrating from `tools/dev_qa.py`:

1. The commit SHAs and Bazel commands are the same as in the Python script
2. Repository cloning behavior is identical (reuses existing clones)
3. BuildBuddy toolchain injection works the same way
4. Invocation IDs are still generated and reported

The main difference is how you invoke the tests (via `bazel test` instead of `python tools/dev_qa.py`).
