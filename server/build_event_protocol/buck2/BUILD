load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "buck2",
    srcs = ["buck2.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/buck2",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:buck_data_go_proto",
        "//proto:build_event_stream_go_proto",
        "@org_golang_google_protobuf//types/known/anypb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

go_test(
    name = "buck2_test",
    size = "small",
    srcs = ["buck2_test.go"],
    deps = [
        ":buck2",
        "//proto:buck_data_go_proto",
        "//proto:build_event_stream_go_proto",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/anypb",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
