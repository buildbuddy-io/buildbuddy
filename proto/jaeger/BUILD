load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "jaeger_proto",
    srcs = [
        "model.proto",
        "query.proto",
    ],
    deps = [
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@googleapis//google/api:annotations_proto",
    ],
)

go_proto_library(
    name = "jaeger_go_proto",
    compilers = [
        "@io_bazel_rules_go//proto:go_proto",
        "@io_bazel_rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/proto/jaeger",
    proto = ":jaeger_proto",
    deps = [
        "@org_golang_google_genproto_googleapis_api//annotations",
    ],
)
