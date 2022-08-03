load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "makedb",
    srcs = ["makedb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/bbmake/makedb",
    visibility = ["//visibility:public"],
    deps = ["//server/util/status"],
)
