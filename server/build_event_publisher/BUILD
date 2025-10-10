load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "build_event_publisher",
    srcs = ["build_event_publisher.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_publisher",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:build_events_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/util/authutil",
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

go_test(
    name = "build_event_publisher_test",
    srcs = ["build_event_publisher_test.go"],
    deps = [
        ":build_event_publisher",
        "//proto:build_events_go_proto",
        "//proto:publish_build_event_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//types/known/anypb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
