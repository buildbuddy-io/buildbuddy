load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "content_addressable_storage_server_proxy",
    srcs = ["content_addressable_storage_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/content_addressable_storage_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/status",
    ],
)
