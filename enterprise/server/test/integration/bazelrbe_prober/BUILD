load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "bazelrbe_prober_test",
    size = "large",
    srcs = ["bazelrbe_prober_test.go"],
    data = [
        "//server/util/bazel:bazel-6.5.0_crossplatform",
        "//server/util/bazel:bazel-7.7.1_crossplatform",
        "//server/util/bazel:bazel-8.5.0_crossplatform",
        "//tools/probers/bazelrbe",
    ],
    shard_count = 3,
    tags = ["docker"],
    x_defs = {
        "bazelrbeProberRunfilePath": "$(rlocationpath //tools/probers/bazelrbe)",
        "bazel6Runfilepath": "$(rlocationpath //server/util/bazel:bazel-6.5.0_crossplatform)",
        "bazel7Runfilepath": "$(rlocationpath //server/util/bazel:bazel-7.7.1_crossplatform)",
        "bazel8Runfilepath": "$(rlocationpath //server/util/bazel:bazel-8.5.0_crossplatform)",
    },
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
