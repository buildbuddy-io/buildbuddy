load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

# gazelle:default_visibility //enterprise/server/test/integration/remote_execution:__subpackages__
package(default_visibility = [
    "//enterprise/server/test/integration/remote_execution:__subpackages__",
])

proto_library(
    name = "remoteexecutiontest_proto",
    srcs = ["remote_execution_test.proto"],
)

go_proto_library(
    name = "remoteexecutiontest_go_proto",
    compilers = [
        "@io_bazel_rules_go//proto:go_proto",
        "@io_bazel_rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/proto",
    proto = ":remoteexecutiontest_proto",
)
