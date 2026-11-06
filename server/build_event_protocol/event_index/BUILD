load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "event_index",
    srcs = ["event_index.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_index",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:invocation_go_proto",
        "//proto:target_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/api/common",
        "//server/build_event_protocol/accumulator",
        "//server/util/flag",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)

go_test(
    name = "event_index_test",
    srcs = ["event_index_test.go"],
    deps = [
        ":event_index",
        "//proto:build_event_stream_go_proto",
        "//proto:invocation_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/api/common",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
