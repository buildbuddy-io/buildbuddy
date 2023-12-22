load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "jaegerquery_lib",
    srcs = ["jaegerquery.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/jaegerquery",
    visibility = ["//visibility:private"],
    deps = [
        "//proto/jaeger:jaeger_go_proto",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_exp//maps",
    ],
)

go_binary(
    name = "jaegerquery",
    embed = [":jaegerquery_lib"],
    visibility = ["//visibility:public"],
)
