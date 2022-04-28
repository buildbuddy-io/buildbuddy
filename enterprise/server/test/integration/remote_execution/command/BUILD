load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "testcommand",
    embed = [":command_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "command_lib",
    srcs = ["testcommand.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/command",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/test/integration/remote_execution/proto:remoteexecutiontest_go_proto",
        "//server/util/log",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_protobuf//encoding/prototext",
    ],
)
