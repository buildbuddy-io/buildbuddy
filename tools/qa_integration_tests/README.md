# QA Integration Tests

This directory contains integration tests that validate BuildBuddy's remote execution and caching capabilities by building real-world open-source projects using their release tarballs.

## Overview

These tests use [`rules_bazel_integration_test`](https://github.com/bazel-contrib/rules_bazel_integration_test) to:
1. Download source tarballs from GitHub releases
2. Inject BuildBuddy's RBE toolchain into the project's MODULE.bazel
3. Execute Bazel builds/tests remotely on BuildBuddy's infrastructure
4. Verify successful execution

This complements the existing `tools/dev_qa.py` script by using Bazel-native integration testing.

## Running Tests Locally

### Prerequisites

Set your BuildBuddy API key:
```bash
export BB_API_KEY=your-api-key-here
```

### Run a specific test

```bash
# Run the abseil-cpp integration test
bazel test //tools/qa_integration_tests:abseil_cpp_qa_test \
  --test_output=all \
  --action_env=BB_API_KEY
```

### Run all QA integration tests

```bash
bazel test //tools/qa_integration_tests:all_qa_integration_tests \
  --test_output=all \
  --action_env=BB_API_KEY
```

## Current Tests

### abseil-cpp
- **Version**: 20250814.1
- **Source**: https://github.com/abseil/abseil-cpp/archive/refs/tags/20250814.1.tar.gz
- **Command**: `build //... --cxxopt=-std=c++14`
- **Bazel Version**: 8.4.2

## Updating Test Versions

To update the abseil-cpp version:

1. Find the latest release at https://github.com/abseil/abseil-cpp/releases
2. Update the `QA_TARBALL_URL` in `BUILD.bazel` with the new release tarball URL
3. Update the `QA_STRIP_PREFIX` with the new extracted directory name (usually `abseil-cpp-{version}`)
4. Test locally to verify the update works

## Adding New Repository Tests

To add a new repository (e.g., bazel-gazelle):

1. Create workspace directory:
   ```bash
   mkdir -p workspaces/bazel_gazelle
   touch workspaces/bazel_gazelle/.gitkeep
   ```

2. Add a new `bazel_integration_test` target in `BUILD.bazel`:
   ```python
   bazel_integration_test(
       name = "bazel_gazelle_qa_test",
       bazel_binaries = bazel_binaries,
       bazel_version = "8.4.2",
       env = {
           "QA_TARBALL_URL": "https://github.com/bazelbuild/bazel-gazelle/archive/refs/tags/vX.Y.Z.tar.gz",
           "QA_STRIP_PREFIX": "bazel-gazelle-X.Y.Z",
           "QA_BAZEL_COMMAND": "build //... --build_tag_filters=-local",
       },
       tags = integration_test_utils.DEFAULT_INTEGRATION_TEST_TAGS + ["no-sandbox"],
       test_runner = ":qa_test_runner",
       timeout = "long",
       workspace_path = "workspaces/bazel_gazelle",
   )
   ```

3. Add to the test suite:
   ```python
   test_suite(
       name = "all_qa_integration_tests",
       tests = [
           ":abseil_cpp_qa_test",
           ":bazel_gazelle_qa_test",  # Add new test here
       ],
       ...
   )
   ```

4. Run update_deleted_packages:
   ```bash
   bazel run @rules_bazel_integration_test//tools:update_deleted_packages
   ```

5. Test the new integration test locally

## Architecture

- **qa_test_runner.sh**: Custom shell script that handles tarball download, extraction, toolchain injection, and Bazel execution
- **BUILD.bazel**: Defines test targets using `bazel_integration_test` macro
- **workspaces/**: Empty directories that serve as test workspace roots (populated at test runtime)

## Benefits Over tools/dev_qa.py

- **Bazel-native**: Integrates with Bazel's test framework, caching, and parallelization
- **Versioned releases**: Uses official release tarballs instead of arbitrary git commits
- **Faster**: No git clone, just tarball download
- **Reproducible**: Bazel caches successful test results
- **Isolated**: Each test runs in its own sandbox

## Troubleshooting

**Test fails to download tarball:**
- Check internet connectivity
- Verify the tarball URL is correct
- Ensure `no-sandbox` tag is present (required for network access)

**Test fails with "MODULE.bazel not found":**
- Verify the project uses bzlmod (has MODULE.bazel)
- Check the `QA_STRIP_PREFIX` matches the extracted directory name

**Test fails with authentication error:**
- Ensure `BB_API_KEY` environment variable is set
- Pass it to Bazel with `--action_env=BB_API_KEY`

**View invocation results:**
- Check the test output for the invocation URL
- Visit https://app.buildbuddy.dev/invocation/{invocation_id}
