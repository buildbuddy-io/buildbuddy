load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "grpc_client",
    srcs = ["grpc_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/grpc_client",
    visibility = ["//visibility:public"],
    deps = [
        "//server/rpc/interceptors",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//credentials/google",
        "@org_golang_google_grpc//keepalive",
    ],
)
