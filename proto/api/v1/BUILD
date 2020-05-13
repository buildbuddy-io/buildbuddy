load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")
load("//rules:typescript_proto_library.bzl", "ts_proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "api_service_proto",
    srcs = ["service.proto"],
    deps = [
        ":action_proto",
        ":file_proto",
        ":target_proto",
    ],
)

proto_library(
    name = "action_proto",
    srcs = ["action.proto"],
    deps = [
        ":common_proto",
        ":file_proto",
    ],
)

proto_library(
    name = "common_proto",
    srcs = ["common.proto"],
    deps = [
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
    ],
)

proto_library(
    name = "file_proto",
    srcs = ["file.proto"],
)

proto_library(
    name = "target_proto",
    srcs = ["target.proto"],
    deps = [
        ":common_proto",
    ],
)

# Go proto rules below here

go_proto_library(
    name = "api_service_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/api/v1/api_service",
    proto = ":api_service_proto",
    deps = [
        ":action_go_proto",
        ":file_go_proto",
        ":target_go_proto",
    ],
)

go_proto_library(
    name = "action_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/api/v1/action",
    proto = ":action_proto",
    deps = [
        ":common_go_proto",
        ":file_go_proto",
    ],
)

go_proto_library(
    name = "common_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common",
    proto = ":common_proto",
    deps = [
    ],
)

go_proto_library(
    name = "file_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/api/v1/file",
    proto = ":file_proto",
    deps = [
    ],
)

go_proto_library(
    name = "target_go_proto",
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/api/v1/target",
    proto = ":target_proto",
    deps = [
        ":common_go_proto",
    ],
)

# TypeScript proto rules below here

ts_proto_library(
    name = "buildbuddy_service_ts_proto",
    deps = [":buildbuddy_service_proto"],
)
