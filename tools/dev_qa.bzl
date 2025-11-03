"""Dev QA test definitions for testing BuildBuddy against external repositories."""

load("@rules_bazel_integration_test//bazel_integration_test:defs.bzl", "script_test")

# Repository configurations for dev QA tests
# NOTE: If you change these commit_shas, be sure to update dev-qa.yaml in buildbuddy-internal
REPO_CONFIGS = {
    "buildbuddy": {
        "repo_url": "https://github.com/buildbuddy-io/buildbuddy",
        "commit_sha": "a97d4303c9485db089a33a1049fe480d0122687d",
        "bazel_cmd": """test //...
            --config=remote-dev
            --remote_executor=remote.buildbuddy.dev
            --remote_cache=remote.buildbuddy.dev
            --bes_backend=remote.buildbuddy.dev
            --bes_results_url=https://app.buildbuddy.dev/invocation/
            --flaky_test_attempts=3
            --build_metadata=TAGS=dev-qa
            --test_tag_filters=-docker,-bare,-performance
            --remote_grpc_log=grpc_log.bin""",
    },
    "bazel-gazelle": {
        "repo_url": "https://github.com/bazelbuild/bazel-gazelle",
        "commit_sha": "f44f85943a3f6bde872e2d39c5b552e21a797975",
        "bazel_cmd": """build //...
            --remote_executor=remote.buildbuddy.dev
            --remote_cache=remote.buildbuddy.dev
            --bes_backend=remote.buildbuddy.dev
            --bes_results_url=https://app.buildbuddy.dev/invocation/
            --remote_timeout=10m
            --jobs=100
            --build_metadata=TAGS=dev-qa
            --build_tag_filters=-local
            --noenable_bzlmod
            --extra_execution_platforms=@buildbuddy_toolchain//:platform
            --platforms=@buildbuddy_toolchain//:platform
            --remote_grpc_log=grpc_log.bin""",
    },
    "abseil-cpp": {
        "repo_url": "https://github.com/abseil/abseil-cpp",
        "commit_sha": "e968256406fd7898d7fde880e31e54b041d32a7e",
        "bazel_cmd": """build //...
            --cxxopt="-std=c++14"
            --remote_executor=remote.buildbuddy.dev
            --remote_cache=remote.buildbuddy.dev
            --bes_backend=remote.buildbuddy.dev
            --bes_results_url=https://app.buildbuddy.dev/invocation/
            --remote_timeout=10m
            --jobs=100
            --build_metadata=TAGS=dev-qa
            --noenable_bzlmod
            --extra_execution_platforms=@buildbuddy_toolchain//:platform
            --platforms=@buildbuddy_toolchain//:platform
            --remote_grpc_log=grpc_log.bin""",
    },
    "rules_python": {
        "repo_url": "https://github.com/bazelbuild/rules_python",
        "commit_sha": "da10ac49efee1b02cbfa3b22a39e68bf3fe5bbe2",
        "bazel_cmd": """build //...
            --remote_executor=remote.buildbuddy.dev
            --remote_cache=remote.buildbuddy.dev
            --bes_backend=remote.buildbuddy.dev
            --bes_results_url=https://app.buildbuddy.dev/invocation/
            --remote_timeout=10m
            --jobs=100
            --build_metadata=TAGS=dev-qa
            --extra_execution_platforms=@buildbuddy_toolchain//:platform
            --platforms=@buildbuddy_toolchain//:platform
            --remote_grpc_log=grpc_log.bin""",
    },
}

def _normalize_command(cmd):
    """Normalize a multi-line command string to a single line."""
    # Remove newlines and extra whitespace
    # Split on whitespace (newlines, spaces, tabs) and rejoin
    parts = []
    for line in cmd.split("\n"):
        line = line.strip()
        if line:
            parts.extend(line.split(" "))
    # Filter out empty parts and rejoin
    return " ".join([p for p in parts if p])

def dev_qa_test(name, repo_name, repo_url, commit_sha, bazel_cmd, bazel_binaries, **kwargs):
    """Define a dev QA test for a single repository.

    Args:
        name: Name of the test target.
        repo_name: Name of the repository (used for cloning directory).
        repo_url: Git URL of the repository.
        commit_sha: Commit SHA to checkout and test.
        bazel_cmd: Bazel command to run (will be normalized to single line).
        bazel_binaries: The bazel_binaries struct from @bazel_binaries//:defs.bzl.
        **kwargs: Additional arguments to pass to script_test.
    """

    # Normalize the Bazel command to a single line
    normalized_cmd = _normalize_command(bazel_cmd)

    # Create the script_test using dev_qa_runner.sh directly
    # Pass configuration via environment variables
    script_test(
        name = name,
        srcs = [":dev_qa_runner.sh"],
        bazel_binaries = bazel_binaries,
        bazel_version = bazel_binaries.versions.current,
        tags = ["manual", "external", "dev-qa"],
        env = {
            "DEV_QA_REPO_NAME": repo_name,
            "DEV_QA_REPO_URL": repo_url,
            "DEV_QA_COMMIT_SHA": commit_sha,
            "DEV_QA_BAZEL_CMD": normalized_cmd,
        },
        deps = ["@bazel_tools//tools/bash/runfiles"],
        **kwargs
    )

def dev_qa_tests(bazel_binaries, repos = None):
    """Define dev QA tests for multiple repositories.

    Args:
        bazel_binaries: The bazel_binaries struct from @bazel_binaries//:defs.bzl.
        repos: Optional list of repository names to test. If None, tests all repos in REPO_CONFIGS.
    """

    # Determine which repos to test
    repos_to_test = repos if repos != None else REPO_CONFIGS.keys()

    # Create a test for each repository
    test_targets = []
    for repo_name in repos_to_test:
        if repo_name not in REPO_CONFIGS:
            fail("Unknown repository: {}. Available repos: {}".format(
                repo_name,
                ", ".join(REPO_CONFIGS.keys()),
            ))

        config = REPO_CONFIGS[repo_name]
        test_name = "dev_qa_" + repo_name.replace("-", "_")

        dev_qa_test(
            name = test_name,
            repo_name = repo_name,
            bazel_binaries = bazel_binaries,
            **config
        )

        test_targets.append(":" + test_name)

    # Create a test suite to run all tests
    native.test_suite(
        name = "dev_qa_all",
        tags = ["manual", "external", "dev-qa"],
        tests = test_targets,
    )

    # Return the list of test targets for reference
    return test_targets
