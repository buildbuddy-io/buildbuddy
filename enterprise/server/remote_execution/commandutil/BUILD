load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "commandutil",
    srcs = ["commandutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
    ],
)
