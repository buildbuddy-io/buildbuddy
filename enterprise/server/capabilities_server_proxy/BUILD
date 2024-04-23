load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "capabilities_server_proxy",
    srcs = ["capabilities_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/capabilities_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/util/status",
    ],
)
