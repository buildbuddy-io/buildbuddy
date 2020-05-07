load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["interfaces.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/interfaces",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:invocation_go_proto",
        "//server/tables:go_default_library",
    ],
)
