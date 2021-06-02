load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "priority_queue",
    srcs = ["priority_queue.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_queue",
    visibility = ["//visibility:public"],
    deps = ["//proto:scheduler_go_proto"],
)
