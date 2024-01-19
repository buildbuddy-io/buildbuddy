load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

package(default_visibility = ["//enterprise:__subpackages__"])

proto_library(
    name = "linux_sandbox_proto",
    srcs = ["execution_statistics.proto"],
)

go_proto_library(
    name = "linux_sandbox_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/proto",
    proto = ":linux_sandbox_proto",
)
