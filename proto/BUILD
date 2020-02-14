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

go_proto_library(
    name = "build_event_stream_go_proto",
    importpath = "github.com/buchgr/bazel-remote/event/proto",
    proto = ":build_event_stream_proto",
    visibility = ["//visibility:public"],
    deps = [
        ":command_line_go_proto",
        ":invocation_policy_go_proto",
    ],
)

proto_library(
    name = "build_event_stream_proto",
    srcs = ["build_event_stream.proto"],
    deps = [
        ":command_line_proto",
        ":invocation_policy_proto",
    ],
)

go_proto_library(
    name = "invocation_policy_go_proto",
    importpath = "blaze.invocation_policy",
    proto = ":invocation_policy_proto",
    deps = [
    ],
)

proto_library(
    name = "invocation_policy_proto",
    srcs = ["invocation_policy.proto"],
    deps = [
    ],
)

go_proto_library(
    name = "command_line_go_proto",
    importpath = "command_line",
    proto = ":command_line_proto",
    deps = [
        ":option_filters_go_proto",
    ],
)

proto_library(
    name = "command_line_proto",
    srcs = ["command_line.proto"],
    deps = [
        ":option_filters_proto",
    ],
)

go_proto_library(
    name = "option_filters_go_proto",
    importpath = "options",
    proto = ":option_filters_proto",
    deps = [
    ],
)

proto_library(
    name = "option_filters_proto",
    srcs = ["option_filters.proto"],
    deps = [
    ],
)
