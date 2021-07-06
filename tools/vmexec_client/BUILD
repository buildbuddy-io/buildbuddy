load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "vmexec_client",
    embed = [":vmexec_client_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "vmexec_client_lib",
    srcs = ["vmexec_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/vmexec_client",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/util/vsock",
        "//proto:vmexec_go_proto",
        "//server/util/log",
    ],
)
