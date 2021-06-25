load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "priority_task_scheduler",
    srcs = ["priority_task_scheduler.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_task_scheduler",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/executor",
        "//enterprise/server/scheduling/priority_queue",
        "//enterprise/server/scheduling/task_leaser",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)
