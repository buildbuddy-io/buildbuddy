load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["telemetry_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/telemetry",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:telemetry_go_proto",
        "//server/backends/blobstore:go_default_library",
        "//server/environment:go_default_library",
        "//server/util/grpc_client:go_default_library",
        "@com_github_google_uuid//:go_default_library",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
