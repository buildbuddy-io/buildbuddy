load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "commandtypes",
    srcs = ["commandtypes.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/commandtypes",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
    ],
)
