load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "build_event_proxy",
    srcs = ["build_event_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:publish_build_event_go_proto",
        "//server/environment",
        "//server/util/flagutil",
        "//server/util/grpc_client",
        "//server/util/log",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_protobuf//types/known/emptypb",
    ],
)
