load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "bazel_rbe_test",
    size = "small",
    srcs = ["bazel_rbe_test.go"],
    shard_count = 15,
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//server/metrics",
        "//server/testutil/testbazel",
        "//server/testutil/testmetrics",
        "//server/util/bazel",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
