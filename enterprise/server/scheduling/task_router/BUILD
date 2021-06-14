load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "task_router",
    srcs = ["task_router.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_router",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)
