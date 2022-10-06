load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "remote_asset_test",
    srcs = ["remote_asset_test.go"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/testutil/testhttp",
        "//server/testutil/testshell",
        "//server/util/bazel",
        "@com_github_stretchr_testify//require",
    ],
)
