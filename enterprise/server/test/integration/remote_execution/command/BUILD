load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_binary(
    name = "testcommand",
    embed = [":command_lib"],
)

go_library(
    name = "command_lib",
    srcs = ["testcommand.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/command",
    deps = [
        "//enterprise/server/test/integration/remote_execution/proto:remoteexecutiontest_go_proto",
        "//server/util/log",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_protobuf//encoding/prototext",
    ],
)
