load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "bazelrbe_prober_test",
    size = "medium",
    srcs = ["bazelrbe_prober_test.go"],
    data = ["//tools/probers/bazelrbe"],
    x_defs = {
        "proberBinaryRunfilePath": "$(rlocationpath //tools/probers/bazelrbe)",
    },
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//server/testutil/testbazel",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
