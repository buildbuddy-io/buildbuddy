load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "protofile",
    srcs = ["protofile.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/protofile",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/proto",
        "//server/util/status",
    ],
)
