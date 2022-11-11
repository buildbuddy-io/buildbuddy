load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "sidecar",
    srcs = ["sidecar.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/sidecar",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/storage",
        "//cli/version",
        "//proto:sidecar_go_proto",
        "//server/util/grpc_client",
        "@com_github_google_shlex//:shlex",
    ],
)
