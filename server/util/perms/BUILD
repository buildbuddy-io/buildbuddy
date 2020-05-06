load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["perms.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/perms",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment:go_default_library",
        "//server/tables:go_default_library",
    ],
)
