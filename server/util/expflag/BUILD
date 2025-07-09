load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "expflag",
    srcs = ["expflag.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/expflag",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
    ],
)
