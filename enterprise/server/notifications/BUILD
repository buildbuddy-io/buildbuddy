load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "notifications",
    srcs = ["notifications.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/notifications",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/redisutil",
        "//proto:notification_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/util/db",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/query_builder",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "notifications_test",
    srcs = ["notifications_test.go"],
    deps = [
        ":notifications",
        "//enterprise/server/testutil/enterprise_testenv",
        "//enterprise/server/testutil/testredis",
        "//proto:notification_go_proto",
        "//server/interfaces",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
