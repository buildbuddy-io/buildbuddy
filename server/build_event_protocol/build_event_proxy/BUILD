load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "build_event_proxy",
    srcs = ["build_event_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:publish_build_event_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_protobuf//types/known/emptypb",
    ],
)

go_test(
    name = "build_event_proxy_test",
    srcs = ["build_event_proxy_test.go"],
    embed = [":build_event_proxy"],
    deps = [
        "//proto:publish_build_event_go_proto",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/emptypb",
    ],
)
