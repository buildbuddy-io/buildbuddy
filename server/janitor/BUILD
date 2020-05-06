load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["janitor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/janitor",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment:go_default_library",
        "//server/tables:go_default_library",
    ],
)
