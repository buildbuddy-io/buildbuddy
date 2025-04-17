load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "action_cache_server_test",
    srcs = ["action_cache_server_test.go"],
    deps = [
        "//enterprise/server/clientidentity",
        "//server/interfaces",
        "//server/remote_cache/action_cache_server",
        "//server/remote_cache/digest",
        "//server/util/random",
        "//server/util/status",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
