load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

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
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "build_event_server_test",
    srcs = ["build_event_server_test.go"],
    deps = [
        ":build_event_server",
        "//proto:publish_build_event_go_proto",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
    ],
)
