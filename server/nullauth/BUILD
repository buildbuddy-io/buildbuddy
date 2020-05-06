load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["nullauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/nullauth",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/status:go_default_library",
    ],
)
