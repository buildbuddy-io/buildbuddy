load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "task_leaser",
    srcs = ["task_leaser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_leaser",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/auth",
        "//proto:scheduler_go_proto",
        "//server/environment",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
    ],
)
