load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testbes",
    testonly = 1,
    srcs = ["testbes.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testbes",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_events_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/testutil/testfs",
        "//server/util/grpc_client",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_protobuf//types/known/emptypb",
    ],
)
