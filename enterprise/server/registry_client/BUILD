load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "registry_client_lib",
    srcs = ["registry_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/registry_client",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:registry_go_proto",
        "//server/util/grpc_client",
        "//server/util/log",
    ],
)

go_binary(
    name = "registry_client",
    embed = [":registry_client_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)
