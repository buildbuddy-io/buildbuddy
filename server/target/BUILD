load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "target",
    srcs = ["target.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/target",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:target_go_proto",
        "//proto:target_pagination_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/environment",
        "//server/util/db",
        "//server/util/git",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
