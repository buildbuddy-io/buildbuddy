load(":dev_qa.bzl", "dev_qa_tests")

package(default_visibility = ["//visibility:public"])

# Export the dev_qa_runner.sh script so it can be used by generated test scripts
exports_files(["dev_qa_runner.sh"])

# Define all dev QA tests
# Note: These tests require BB_API_KEY environment variable to be set
# Run with: bazel test //tools:dev_qa_all --test_env=BB_API_KEY
#
# To test specific repos:
# bazel test //tools:dev_qa_buildbuddy --test_env=BB_API_KEY
# bazel test //tools:dev_qa_bazel_gazelle --test_env=BB_API_KEY
# bazel test //tools:dev_qa_abseil_cpp --test_env=BB_API_KEY
# bazel test //tools:dev_qa_rules_python --test_env=BB_API_KEY

# bazel_binaries is configured in MODULE.bazel
load("@bazel_binaries//:defs.bzl", "bazel_binaries")

dev_qa_tests(bazel_binaries = bazel_binaries)
