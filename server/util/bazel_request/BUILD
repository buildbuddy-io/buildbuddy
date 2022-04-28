load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bazel_request",
    srcs = ["bazel_request.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazel_request",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//proto",
    ],
)
