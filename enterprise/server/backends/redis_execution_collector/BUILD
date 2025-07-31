load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "redis_execution_collector",
    srcs = ["redis_execution_collector.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_execution_collector",
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

go_test(
    name = "redis_execution_collector_test",
    srcs = ["redis_execution_collector_test.go"],
    deps = [
        ":redis_execution_collector",
        "//enterprise/server/testutil/testredis",
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/util/status",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)
