load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "gcp",
    srcs = ["gcp.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/log/gcp",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_rs_zerolog//:zerolog",
        "@com_google_cloud_go_logging//:logging",
        "@go_googleapis//google/logging/v2:logging_go_proto",
        "@org_golang_google_protobuf//types/known/structpb",
    ],
)
