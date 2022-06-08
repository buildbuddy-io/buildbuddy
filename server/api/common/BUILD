load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "common",
    srcs = ["common.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/api/common",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:invocation_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/remote_cache/digest",
        "//server/util/timeutil",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
