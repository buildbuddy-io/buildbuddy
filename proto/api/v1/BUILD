load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")
load("//proto:defs.bzl", "ts_proto_library")

proto_library(
    name = "common_proto",
    srcs = [
        "common.proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@googleapis//google/rpc:status_proto",
    ],
)

go_proto_library(
    name = "common_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common",
    proto = ":common_proto",
    visibility = ["//visibility:public"],
)

ts_proto_library(
    name = "common_ts_proto",
    proto = ":common_proto",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:duration_ts_proto",
        "//proto:grpc_status_ts_proto",
        "//proto:timestamp_ts_proto",
    ],
)

proto_library(
    name = "api_v1_proto",
    srcs = [
        "action.proto",
        "file.proto",
        "invocation.proto",
        "log.proto",
        "service.proto",
        "target.proto",
        "workflow.proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":common_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@googleapis//google/rpc:status_proto",
    ],
)

go_proto_library(
    name = "api_v1_go_proto",
    compilers = [
        "@io_bazel_rules_go//proto:go_proto",
        "@io_bazel_rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/api/v1",
    proto = ":api_v1_proto",
    visibility = ["//visibility:public"],
    deps = [
        ":common_go_proto",
        "//proto:grpc_status_go_proto",
    ],
)
