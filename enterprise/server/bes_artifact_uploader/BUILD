load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bes_artifact_uploader",
    srcs = ["bes_artifact_uploader.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/bes_artifact_uploader",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/build_event_publisher",
        "//proto:build_event_stream_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_x_sync//errgroup",
    ],
)
