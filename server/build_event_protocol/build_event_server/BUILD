load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "build_event_server",
    srcs = ["build_event_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_events_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_protobuf//types/known/emptypb",
    ],
)
