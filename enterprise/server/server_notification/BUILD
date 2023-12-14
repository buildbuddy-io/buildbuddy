load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "server_notification",
    srcs = ["server_notification.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/server_notification",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/backends/pubsub",
        "//proto:server_notification_go_proto",
        "//server/real_environment",
        "//server/util/alert",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//reflect/protoreflect",
    ],
)

go_test(
    name = "server_notification_test",
    srcs = ["server_notification_test.go"],
    deps = [
        ":server_notification",
        "//enterprise/server/testutil/testredis",
        "//proto:server_notification_go_proto",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
