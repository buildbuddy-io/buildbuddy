load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_grpc_library", "go_proto_library")
load("//rules:typescript_proto_library.bzl", "ts_proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "http_proto",
    srcs = [
        "http.proto",
    ],
)

proto_library(
    name = "annotations_proto",
    srcs = [
        "annotations.proto",
    ],
    deps = [
        ":http_proto",
        "@com_google_protobuf//:descriptor_proto",
    ],
)

proto_library(
    name = "build_proto",
    srcs = [
        "build_events.proto",
        "build_status.proto",
        "publish_build_event.proto",
    ],
    deps = [
        ":annotations_proto",
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

go_grpc_library(
    name = "build_go_proto",
    importpath = "proto",
    proto = ":build_proto",
)

go_grpc_library(
    name = "build_go_proto",
    importpath = "proto",
    proto = ":build_proto",
)

ts_proto_library(
    name = "build_ts_proto",
    deps = [":build_proto"],
)
