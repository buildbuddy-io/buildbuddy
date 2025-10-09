load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "routing_content_addressable_storage_client",
    srcs = ["routing_content_addressable_storage_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/routing_content_addressable_storage_client",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/status",
        "@org_golang_google_grpc//:grpc",
    ],
)
