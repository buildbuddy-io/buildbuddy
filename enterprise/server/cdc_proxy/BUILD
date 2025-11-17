load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "cdc_proxy",
    srcs = ["cdc_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cdc_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/chunker",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:storage_go_proto",
        "//server/remote_cache/digest",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
