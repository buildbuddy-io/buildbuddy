load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

package(default_visibility = ["//enterprise:__subpackages__"])

proto_library(
    name = "execution_statistics_proto",
    srcs = ["execution_statistics.proto"],
)

go_proto_library(
    name = "execution_statistics_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/proto/execution_statistics",
    proto = ":execution_statistics_proto",
)

proto_library(
    name = "sandboxd_proto",
    srcs = [":sandboxd.proto"],
    deps = [
        "@com_google_protobuf//:duration_proto",
        "@googleapis//google/rpc:status_proto",
    ],
)

go_proto_library(
    name = "sandboxd_go_proto",
    compilers = [
        "@io_bazel_rules_go//proto:go_proto",
        "@io_bazel_rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/proto/sandboxd",
    proto = ":sandboxd_proto",
    deps = [
        "//proto:grpc_status_go_proto",
    ],
)
