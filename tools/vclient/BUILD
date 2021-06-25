load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "vclient_lib",
    srcs = ["vclient.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/vclient",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/util/vsock",
        "//proto:vmexec_go_proto",
        "//server/util/log",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

go_binary(
    name = "vclient",
    embed = [":vclient_lib"],
    visibility = ["//visibility:public"],
)
