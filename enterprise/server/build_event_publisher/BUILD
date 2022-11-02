load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "build_event_publisher",
    srcs = ["build_event_publisher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/build_event_publisher",
    deps = [
        "//enterprise/server/auth",
        "//proto:build_event_stream_go_proto",
        "//proto:build_events_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/anypb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
