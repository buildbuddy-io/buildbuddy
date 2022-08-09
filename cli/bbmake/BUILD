load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "bbmake",
    srcs = ["bbmake.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/bbmake",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/build_event_publisher",
        "//proto:build_event_stream_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/util/grpc_client",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "bbmake_test",
    srcs = ["bbmake_test.go"],
    data = glob(["testdata/**"]),
    deps = [
        ":bbmake",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)
