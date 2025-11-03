# Dev QA Implementation Summary

This document summarizes the implementation of Bazel integration tests to replace `tools/dev_qa.py`.

## What Was Created

### Core Implementation Files

1. **`tools/dev_qa_runner.sh`** - Main test runner script
   - Clones external repositories
   - Checks out specific commits
   - Injects BuildBuddy RBE toolchain
   - Runs Bazel commands with remote execution
   - Reports results with invocation URLs

2. **`tools/dev_qa.bzl`** - Starlark macro definitions
   - `REPO_CONFIGS` dictionary with repository configurations
   - `dev_qa_test()` function to create individual test targets
   - `dev_qa_tests()` function to generate all tests and test suite
   - Automatic normalization of multi-line Bazel commands

3. **`tools/BUILD`** - Updated BUILD file
   - Exports `dev_qa_runner.sh` for use by generated tests
   - Loads and invokes `dev_qa_tests()` macro (commented out by default)
   - Documentation comments on usage

### Documentation Files

4. **`tools/DEV_QA_README.md`** - Complete usage documentation
   - Setup instructions
   - How to run tests
   - Configuration guide
   - Troubleshooting tips

5. **`tools/MIGRATION_GUIDE.md`** - Migration guide from dev_qa.py
   - Command equivalents
   - Step-by-step migration instructions
   - Comparison of old vs new system
   - CI/CD update guidance

6. **`tools/MODULE.bazel.example`** - Example bzlmod configuration
   - Shows how to add rules_bazel_integration_test
   - Configures bazel_binaries extension

7. **`tools/WORKSPACE.example`** - Example WORKSPACE configuration
   - Legacy WORKSPACE setup for non-bzlmod users
   - Complete dependency chain

8. **`tools/verify_dev_qa_setup.sh`** - Setup verification script
   - Checks if all required files are present
   - Verifies configuration
   - Tests environment variables
   - Queries for test targets
   - Provides actionable feedback

## How It Works

### Architecture

```
tools/BUILD
    ↓ loads
tools/dev_qa.bzl (REPO_CONFIGS)
    ↓ generates
script_test targets for each repo
    ↓ each wraps
tools/dev_qa_runner.sh
    ↓ which
1. Clones external repo
2. Injects BuildBuddy toolchain
3. Runs Bazel command via BIT_BAZEL_BINARY
4. Reports results
```

### Test Generation Flow

1. `dev_qa_tests()` macro iterates over `REPO_CONFIGS`
2. For each repo, generates a wrapper shell script using `genrule`
3. Creates a `script_test` target that:
   - Uses the generated wrapper script
   - Depends on `dev_qa_runner.sh`
   - Uses the Bazel binary provided by `rules_bazel_integration_test`
4. Creates a `test_suite` named `dev_qa_all` containing all tests

### Repository Configurations

Each repository in `REPO_CONFIGS` has:
- `repo_url`: GitHub URL
- `commit_sha`: Specific commit to test (matches original dev_qa.py)
- `bazel_cmd`: Command to run (can be multi-line for readability)

Example:
```python
"buildbuddy": {
    "repo_url": "https://github.com/buildbuddy-io/buildbuddy",
    "commit_sha": "a97d4303c9485db089a33a1049fe480d0122687d",
    "bazel_cmd": """test //...
        --config=remote-dev
        --remote_executor=remote.buildbuddy.dev
        --test_tag_filters=-docker,-bare,-performance""",
}
```

## Setup Required

### 1. Add Dependency to Workspace

**For bzlmod** (add to `MODULE.bazel`):
```python
bazel_dep(name = "rules_bazel_integration_test", version = "0.34.0")

bazel_binaries = use_extension(
    "@rules_bazel_integration_test//:extensions.bzl",
    "bazel_binaries",
)
bazel_binaries.download(version = "7.4.0")
use_repo(bazel_binaries, "bazel_binaries")
```

**For WORKSPACE** (add to `WORKSPACE`):
See `tools/WORKSPACE.example` for complete setup.

### 2. Enable Tests in tools/BUILD

Uncomment these lines in `tools/BUILD`:
```python
load("@bazel_binaries//:defs.bzl", "bazel_binaries")
dev_qa_tests(bazel_binaries = bazel_binaries)
```

### 3. Set BB_API_KEY

```bash
export BB_API_KEY=your-api-key
```

## Usage

### Run All Tests
```bash
bazel test //tools:dev_qa_all --test_env=BB_API_KEY
```

### Run Specific Test
```bash
bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY
```

### List All Tests
```bash
bazel query 'kind(script_test, //tools:dev_qa_*)'
```

## Generated Test Targets

When enabled, the following test targets are created:

- `//tools:dev_qa_buildbuddy` - Tests BuildBuddy repo
- `//tools:dev_qa_bazel_gazelle` - Tests bazel-gazelle repo
- `//tools:dev_qa_abseil_cpp` - Tests abseil-cpp repo
- `//tools:dev_qa_rules_python` - Tests rules_python repo
- `//tools:dev_qa_all` - Test suite running all tests

## Advantages Over dev_qa.py

1. **Bazel-native**: Integrates with standard Bazel workflows
2. **Caching**: Bazel caches successful tests
3. **Parallelization**: Automatic parallel test execution
4. **CI Integration**: Works with standard Bazel CI systems
5. **Selective Testing**: Easy to run subset of tests
6. **Better Reporting**: Integrated with Bazel test infrastructure
7. **Type Safety**: Starlark catches configuration errors at load time

## Comparison with Original

| Aspect | dev_qa.py | New Implementation |
|--------|-----------|-------------------|
| Language | Python | Bash + Starlark |
| Invocation | `python tools/dev_qa.py` | `bazel test //tools:dev_qa_all` |
| Config | Python dict | Starlark dict in .bzl |
| Parallel | Manual (`--jobs`) | Automatic (Bazel) |
| Caching | None | Bazel test cache |
| Repo cloning | To `~/buildbuddy-qa` | To `~/buildbuddy-qa` (same) |
| Toolchain injection | Via Python string | Via Bash heredoc (same logic) |
| Results | Custom output | Bazel test results + invocation URLs |

## Environment Variables

Both systems use the same environment variables:

- **`BB_API_KEY`** (required): BuildBuddy API key for remote execution
- **`QA_ROOT`** (optional): Directory for cloning repos (default: `~/buildbuddy-qa`)

## Files Modified

- `tools/BUILD` - Added dev_qa test configuration (commented out)

## Files Created

- `tools/dev_qa_runner.sh` - Main test runner
- `tools/dev_qa.bzl` - Test generation macros
- `tools/DEV_QA_README.md` - Usage documentation
- `tools/MIGRATION_GUIDE.md` - Migration instructions
- `tools/MODULE.bazel.example` - Example bzlmod config
- `tools/WORKSPACE.example` - Example WORKSPACE config
- `tools/verify_dev_qa_setup.sh` - Setup verification script
- `tools/DEV_QA_IMPLEMENTATION_SUMMARY.md` - This file

## Testing the Implementation

### 1. Verify Setup
```bash
./tools/verify_dev_qa_setup.sh
```

### 2. Run a Single Test
```bash
export BB_API_KEY=your-key
bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY --test_output=streamed
```

### 3. Run All Tests
```bash
bazel test //tools:dev_qa_all --test_env=BB_API_KEY
```

## Maintenance

### Updating Commit SHAs

Edit `REPO_CONFIGS` in `tools/dev_qa.bzl`:
```python
"buildbuddy": {
    "commit_sha": "new_commit_sha_here",
    # ...
}
```

### Adding a New Repository

Add to `REPO_CONFIGS` in `tools/dev_qa.bzl`:
```python
"new_repo": {
    "repo_url": "https://github.com/org/new_repo",
    "commit_sha": "abc123...",
    "bazel_cmd": "build //...",
}
```

A test target `//tools:dev_qa_new_repo` will be automatically generated.

### Changing Bazel Version

Update the version in your `MODULE.bazel` or `WORKSPACE`:
```python
bazel_binaries.download(version = "7.5.0")  # New version
```

## Next Steps

1. Add `rules_bazel_integration_test` dependency to your workspace
2. Enable the tests in `tools/BUILD`
3. Run `./tools/verify_dev_qa_setup.sh` to verify setup
4. Test with a single repo: `bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY`
5. Once working, update CI scripts to use new Bazel commands
6. (Optional) Remove `tools/dev_qa.py` once fully migrated

## Support

- See `tools/DEV_QA_README.md` for detailed usage
- See `tools/MIGRATION_GUIDE.md` for migration help
- Run `./tools/verify_dev_qa_setup.sh` to diagnose issues
- Check example configs in `tools/*.example` files
