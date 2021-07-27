load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "remote_execution_test",
    srcs = ["remote_execution_test.go"],
    args = ["--test.v"],
    data = [
        "//enterprise/server/test/integration/remote_execution/command:testcommand",
    ],
    shard_count = 4,
    deps = [
        "//enterprise/server/scheduling/scheduler_server",
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testfs",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
