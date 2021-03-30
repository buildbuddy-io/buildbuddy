load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "nullauth",
    srcs = ["nullauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/nullauth",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/tables",
        "//server/util/status",
    ],
)
