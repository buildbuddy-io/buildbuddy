load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "testbuddy",
    srcs = ["testbuddy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/testbuddy",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/flaghistory",
        "//cli/log",
        "//cli/login",
        "//cli/util/download",
        "//cli/workspace",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//proto:target_go_proto",
        "//proto:test_buddy_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/test_buddy/identity",
        "//server/test_buddy/junit",
        "//server/test_buddy/normalize",
        "//server/util/git",
        "//server/util/grpc_client",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protowire",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "testbuddy_test",
    srcs = ["testbuddy_test.go"],
    deps = [
        ":testbuddy",
        "//cli/util/download/downloadtest",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//proto:target_go_proto",
        "//proto:test_buddy_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/test_buddy/junit",
        "//server/test_buddy/normalize",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
