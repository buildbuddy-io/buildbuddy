load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "telemetry",
    srcs = ["telemetry_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/telemetry",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:telemetry_go_proto",
        "//server/backends/blobstore",
        "//server/environment",
        "//server/util/grpc_client",
        "@com_github_google_uuid//:uuid",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
