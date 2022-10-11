load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "scorecard",
    srcs = ["scorecard.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/scorecard",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:cache_go_proto",
        "//proto:invocation_go_proto",
        "//proto:pagination_go_proto",
        "//server/environment",
        "//server/util/paging",
        "//server/util/status",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "scorecard_test",
    size = "small",
    srcs = ["scorecard_test.go"],
    deps = [
        ":scorecard",
        "//proto:cache_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//encoding/prototext",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/fieldmaskpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
