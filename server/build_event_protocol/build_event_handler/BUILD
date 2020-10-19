load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["build_event_handler.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:build_events_go_proto",
        "//proto:cache_go_proto",
        "//proto:invocation_go_proto",
        "//proto:publish_build_event_go_proto",
        "//proto:user_id_go_proto",
        "//server/build_event_protocol/build_status_reporter:go_default_library",
        "//server/build_event_protocol/event_parser:go_default_library",
        "//server/environment:go_default_library",
        "//server/remote_cache/hit_tracker:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/perms:go_default_library",
        "//server/util/protofile:go_default_library",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
    ],
)
