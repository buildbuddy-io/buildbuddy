load("@io_bazel_rules_go//go:def.bzl", "go_test")

# Common dependencies for all test targets
_DEPS = [
    "//enterprise/server/test/integration/remote_execution/rbetest",
    "//server/util/testing/flags",
    "@com_github_stretchr_testify//require",
    "@io_bazel_rules_go//go/runfiles",
]

# Bazel 6.x test
go_test(
    name = "bazelrbe_prober_test_bazel6",
    size = "medium",
    srcs = ["bazelrbe_prober_test.go"],
    data = [
        "//server/util/bazel:bazel-6.5.0_crossplatform",
        "//server/util/bazel:bazel-6.5.0_extract_installation",
        "//tools/probers/bazelrbe",
    ],
    x_defs = {
        "proberBinaryRunfilePath": "$(rlocationpath //tools/probers/bazelrbe)",
        "bazelBinaryRunfilePath": "$(rlocationpath //server/util/bazel:bazel-6.5.0_crossplatform)",
        "outdirRunfilePath": "$(rlocationpath //server/util/bazel:bazel-6.5.0_extract_installation)",
    },
    deps = _DEPS,
)

# Bazel 7.x test
go_test(
    name = "bazelrbe_prober_test_bazel7",
    size = "medium",
    srcs = ["bazelrbe_prober_test.go"],
    data = [
        "//server/util/bazel:bazel-7.7.1_crossplatform",
        "//server/util/bazel:bazel-7.7.1_extract_installation",
        "//tools/probers/bazelrbe",
    ],
    x_defs = {
        "proberBinaryRunfilePath": "$(rlocationpath //tools/probers/bazelrbe)",
        "bazelBinaryRunfilePath": "$(rlocationpath //server/util/bazel:bazel-7.7.1_crossplatform)",
        "outdirRunfilePath": "$(rlocationpath //server/util/bazel:bazel-7.7.1_extract_installation)",
    },
    deps = _DEPS,
)

# Bazel 8.x test
go_test(
    name = "bazelrbe_prober_test_bazel8",
    size = "medium",
    srcs = ["bazelrbe_prober_test.go"],
    data = [
        "//server/util/bazel:bazel-8.5.0_crossplatform",
        "//server/util/bazel:bazel-8.5.0_extract_installation",
        "//tools/probers/bazelrbe",
    ],
    x_defs = {
        "proberBinaryRunfilePath": "$(rlocationpath //tools/probers/bazelrbe)",
        "bazelBinaryRunfilePath": "$(rlocationpath //server/util/bazel:bazel-8.5.0_crossplatform)",
        "outdirRunfilePath": "$(rlocationpath //server/util/bazel:bazel-8.5.0_extract_installation)",
    },
    deps = _DEPS,
)

# Test suite to run all versions
test_suite(
    name = "bazelrbe_prober_test",
    tests = [
        ":bazelrbe_prober_test_bazel6",
        ":bazelrbe_prober_test_bazel7",
        ":bazelrbe_prober_test_bazel8",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
