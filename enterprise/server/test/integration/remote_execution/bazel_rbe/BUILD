load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "bazel_rbe_test",
    srcs = ["bazel_rbe_test.go"],
    shard_count = 6,
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//server/metrics",
        "//server/testutil/testbazel",
        "//server/testutil/testmetrics",
        "//server/util/bazel",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
