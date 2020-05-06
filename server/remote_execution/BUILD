load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "execution_server",
    srcs = ["execution_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_execution/execution_server",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server:environment",
        "//server/util:status",
    ],
)
