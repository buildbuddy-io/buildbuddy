load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "janitor",
    srcs = ["janitor.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/janitor",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/tables",
        "//server/util/log",
    ],
)
