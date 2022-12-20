load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "bazel_request",
    srcs = ["bazel_request.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bazel_request",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protowire",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "bazel_request_test",
    srcs = ["bazel_request_test.go"],
    deps = [
        ":bazel_request",
        "//proto:remote_execution_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//proto",
    ],
)
