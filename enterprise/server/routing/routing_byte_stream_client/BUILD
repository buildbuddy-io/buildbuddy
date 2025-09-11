load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "routing_byte_stream_client",
    srcs = ["routing_byte_stream_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/routing_byte_stream_client",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/batch_operator",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
