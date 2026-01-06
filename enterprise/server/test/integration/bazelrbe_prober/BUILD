load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "bazelrbe_prober_test_bazel6",
    size = "large",
    srcs = ["bazelrbe_prober_test.go"],
    data = [
        "//server/util/bazel:bazel-6.5.0_crossplatform",
        "//tools/probers/bazelrbe",
    ],
    x_defs = {
        "bazelRlocationpath": "$(rlocationpath //server/util/bazel:bazel-6.5.0_crossplatform)",
        "proberRlocationpath": "$(rlocationpath //tools/probers/bazelrbe)",
        "bazelMajorVersion": "6",
    },
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

go_test(
    name = "bazelrbe_prober_test_bazel7",
    size = "large",
    srcs = ["bazelrbe_prober_test.go"],
    data = [
        "//server/util/bazel:bazel-7.6.1_crossplatform",
        "//tools/probers/bazelrbe",
    ],
    x_defs = {
        "bazelRlocationpath": "$(rlocationpath //server/util/bazel:bazel-7.6.1_crossplatform)",
        "proberRlocationpath": "$(rlocationpath //tools/probers/bazelrbe)",
        "bazelMajorVersion": "7",
    },
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

go_test(
    name = "bazelrbe_prober_test_bazel8",
    size = "large",
    srcs = ["bazelrbe_prober_test.go"],
    data = [
        "//server/util/bazel:bazel-8.5.0_crossplatform",
        "//tools/probers/bazelrbe",
    ],
    x_defs = {
        "bazelRlocationpath": "$(rlocationpath //server/util/bazel:bazel-8.5.0_crossplatform)",
        "proberRlocationpath": "$(rlocationpath //tools/probers/bazelrbe)",
        "bazelMajorVersion": "8",
    },
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
