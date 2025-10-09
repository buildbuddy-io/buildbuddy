load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "routing_service",
    srcs = ["routing_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/routing_service",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:routing_go_proto",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/alert",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
    ],
)
