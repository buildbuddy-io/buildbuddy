load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "vmexec_client",
    srcs = ["vmexec_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vmexec_client",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/util/procstats",
        "//proto:remote_execution_go_proto",
        "//proto:vmexec_go_proto",
        "//server/interfaces",
        "//server/util/background",
        "//server/util/status",
        "//server/util/tracing",
        "@org_golang_x_sync//errgroup",
    ],
)
