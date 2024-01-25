load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "codesearch",
    srcs = ["codesearch.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/codesearch",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:codesearch_service_go_proto",
        "//proto:index_go_proto",
        "//proto:search_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
