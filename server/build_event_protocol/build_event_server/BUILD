load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["build_event_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_events_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/build_event_protocol/build_event_handler:go_default_library",
        "//server/environment:go_default_library",
        "@io_bazel_rules_go//proto/wkt:empty_go_proto",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
