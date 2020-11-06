load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["sidecar.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/cmd/sidecar",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:publish_build_event_go_proto",
        "//server/build_event_protocol/build_event_proxy:go_default_library",
        "//server/build_event_protocol/build_event_server:go_default_library",
        "//server/config:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/nullauth:go_default_library",
        "//server/real_environment:go_default_library",
        "//server/rpc/filters:go_default_library",
        "//server/util/grpc_server:go_default_library",
        "//server/util/healthcheck:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//reflection:go_default_library",
    ],
)

go_binary(
    name = "sidecar",
    embed = [":go_default_library"],
    visibility = ["//visibility:public"],
)
