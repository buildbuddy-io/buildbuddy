load("@io_bazel_rules_go//go:def.bzl", "go_test")

# Dev QA: a single test target that runs every dev QA suite as a subtest, so
# that each suite shows up as a test case on one invocation. See
# dev_qa_test.go for the list of suites and the flags / env vars each needs.
#
# Driven by .github/workflows/dev-qa.yaml in buildbuddy-internal.
go_test(
    name = "dev_qa_test",
    size = "enormous",
    timeout = "eternal",
    srcs = ["dev_qa_test.go"],
    data = [
        "dev_qa_test_runner.sh",
        "//server/testutil/testbazel:bazel-9.2.0_crossplatform",
        "//server/util/bazel:bazel-8.4.2_crossplatform",
        "//tools/bb",
    ],
    tags = [
        "exclusive",
        "manual",
    ],
    x_defs = {
        "runnerScriptRlocationpath": "$(rlocationpath dev_qa_test_runner.sh)",
        # Bazel version used to build the third-party repos.
        "bazel8Rlocationpath": "$(rlocationpath //server/util/bazel:bazel-8.4.2_crossplatform)",
        # Bazel version used to build buildbuddy itself (webdriver suite).
        # Keep in sync with .bazelversion.
        "bazel9Rlocationpath": "$(rlocationpath //server/testutil/testbazel:bazel-9.2.0_crossplatform)",
        "bbRlocationpath": "$(rlocationpath //tools/bb)",
    },
    deps = [
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
