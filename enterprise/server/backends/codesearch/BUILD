load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

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
        "//server/tables",
        "//server/util/claims",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/git",
        "//server/util/grpc_client",
        "//server/util/status",
    ],
)
