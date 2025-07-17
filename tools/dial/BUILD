load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "dial_lib",
    srcs = ["dial.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/dial",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:publish_build_event_go_proto",
        "//server/util/authutil",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "dial",
    embed = [":dial_lib"],
    visibility = ["//visibility:public"],
)
