load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["target_tracker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/target_tracker",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/build_event_protocol/accumulator:go_default_library",
        "//server/environment:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/perms:go_default_library",
        "//server/util/query_builder:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_jinzhu_gorm//:go_default_library",
    ],
)
