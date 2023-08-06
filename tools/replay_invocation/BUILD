load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "replay_invocation_lib",
    srcs = ["replay_invocation.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/replay_invocation",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:build_events_go_proto",
        "//proto:invocation_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/backends/blobstore",
        "//server/build_event_protocol/build_event_handler",
        "//server/real_environment",
        "//server/util/grpc_client",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/protofile",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/anypb",
    ],
)

go_binary(
    name = "replay_invocation",
    embed = [":replay_invocation_lib"],
    visibility = ["//visibility:public"],
)
