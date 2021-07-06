load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "target_tracker",
    srcs = ["target_tracker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/target_tracker",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/build_event_protocol/accumulator",
        "//server/environment",
        "//server/tables",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "//server/util/timeutil",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@org_golang_x_sync//errgroup",
    ],
)
