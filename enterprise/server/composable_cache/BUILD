load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "composable_cache",
    srcs = ["composable_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/composable_cache",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/util/status",
    ],
)

go_test(
    name = "composable_cache_test",
    srcs = ["composable_cache_test.go"],
    deps = [
        ":composable_cache",
        "//enterprise/server/backends/redis_cache",
        "//enterprise/server/testutil/enterprise_testenv",
        "//enterprise/server/testutil/testredis",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testdigest",
        "//server/util/prefix",
        "@com_github_stretchr_testify//require",
    ],
)
