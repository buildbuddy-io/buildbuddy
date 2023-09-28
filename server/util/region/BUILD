load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "region",
    srcs = ["region.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/region",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:config_go_proto",
        "//server/util/flag",
    ],
)
