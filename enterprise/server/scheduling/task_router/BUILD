load("@io_bazel_rules_go//go:def.bzl", "go_library")

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
        "//server/util/log",
        "//server/util/perms",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@org_golang_google_protobuf//proto",
    ],
)
