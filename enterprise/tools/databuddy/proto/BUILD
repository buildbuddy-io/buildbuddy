load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//proto:defs.bzl", "ts_proto_library")

package(default_visibility = ["//enterprise:__subpackages__"])

proto_library(
    name = "databuddy_proto",
    srcs = ["databuddy.proto"],
)

go_proto_library(
    name = "databuddy_go_proto",
    compilers = [
        "@io_bazel_rules_go//proto:go_proto",
        "@io_bazel_rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/databuddy/proto/databuddy",
    proto = ":databuddy_proto",
)

ts_proto_library(
    name = "databuddy_ts_proto",
    proto = ":databuddy_proto",
)
