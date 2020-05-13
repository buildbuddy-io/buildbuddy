load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

proto_library(
    name = "api_v1_proto",
    srcs = [
        "action.proto",
        "common.proto",
        "file.proto",
        "service.proto",
        "target.proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

go_proto_library(
    name = "api_v1_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/api/v1",
    proto = ":api_v1_proto",
    visibility = ["//visibility:public"],
)

go_library(
    name = "go_default_library",
    embed = [":api_v1_go_proto"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/api/v1",
    visibility = ["//visibility:public"],
)
