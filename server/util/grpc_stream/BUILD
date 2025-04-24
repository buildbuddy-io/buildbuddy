load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "grpc_stream",
    srcs = ["grpc_stream.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/grpc_stream",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/proto",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//reflect/protoreflect",
    ],
)

go_test(
    name = "grpc_stream_test",
    srcs = ["grpc_stream_test.go"],
    deps = [
        ":grpc_stream",
        "//server/environment",
        "//server/testutil/testenv",
        "//server/testutil/testport",
        "//server/util/grpc_client",
        "//server/util/grpc_server",
        "//server/util/proto",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)
