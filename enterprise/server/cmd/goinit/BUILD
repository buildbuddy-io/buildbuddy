load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "goinit_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/goinit",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/util/vsock",
        "//enterprise/server/vmexec",
        "//proto:vmexec_go_proto",
        "//server/util/log",
        "//server/util/rlimit",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sys//unix",
    ],
)

go_binary(
    name = "goinit",
    embed = [":goinit_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)
