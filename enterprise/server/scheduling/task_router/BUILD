load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "task_router",
    srcs = ["task_router.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_router",
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/hash",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

go_test(
    name = "task_router_test",
    srcs = ["task_router_test.go"],
    deps = [
        ":task_router",
        "//enterprise/server/testutil/enterprise_testenv",
        "//enterprise/server/testutil/testredis",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_exp//slices",
    ],
)
