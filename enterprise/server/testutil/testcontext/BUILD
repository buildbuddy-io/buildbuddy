load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testcontext",
    srcs = ["test_context.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testcontext",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/bazel_request",
        "//server/util/log",
        "@com_github_gogo_protobuf//proto",
        "@org_golang_google_grpc//metadata",
    ],
)
