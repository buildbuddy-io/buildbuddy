load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "cli_test",
    srcs = ["cli_test.go"],
    shard_count = 3,
    deps = [
        "//cli/log",
        "//cli/testutil/testcli",
        "//proto:cache_go_proto",
        "//proto:invocation_go_proto",
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/util/retry",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//require",
    ],
)
